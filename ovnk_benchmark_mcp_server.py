#!/usr/bin/env python3
"""
OpenShift OVN-Kubernetes Benchmark MCP Server
Main server entry point using FastMCP with streamable-http transport
Fixed SSE stream handling and resource management
"""

import asyncio
import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict
import signal
import sys

from tools.ovnk_benchmark_openshift_cluster_info import ClusterInfoCollector, collect_cluster_information, get_cluster_info_json
from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery
from tools.ovnk_benchmark_prometheus_nodes_usage import nodeUsageCollector
from analysis.ovnk_benchmark_analysis_cluster_stat import analyze_cluster_status as analyze_cluster_status_module, ClusterStatAnalyzer
from tools.ovnk_benchmark_prometheus_basicinfo import ovnBasicInfoCollector, get_pod_phase_counts

from tools.ovnk_benchmark_prometheus_kubeapi import kubeAPICollector
from tools.ovnk_benchmark_prometheus_pods_usage import PodsUsageCollector, collect_ovn_duration_usage
from tools.ovnk_benchmark_prometheus_ovnk_sync import OVNSyncDurationCollector,get_ovn_sync_summary_json,collect_ovn_sync_metrics_duration
from tools.ovnk_benchmark_prometheus_ovnk_ovs import OVSUsageCollector
from tools.ovnk_benchmark_prometheus_nodes_usage import nodeUsageCollector
from ocauth.ovnk_benchmark_auth import OpenShiftAuth
from config.ovnk_benchmark_config import Config
from elt.ovnk_benchmark_elt_duckdb import PerformanceELT
from storage.ovnk_benchmark_storage_ovnk import PrometheusStorage
from analysis.ovnk_benchmark_performance_analysis_allovnk import OVNKPerformanceAnalyzer
from analysis.ovnk_benchmark_performance_analysis_clusters_api import ClusterAPIPerformanceAnalyzer
from analysis.ovnk_benchmark_performance_analysis_overall import (
    OverallPerformanceAnalyzer,
    analyze_overall_performance_with_auth
)

import fastmcp
from fastmcp.server import FastMCP

import warnings
# Suppress urllib3 deprecation warning triggered by kubernetes client using HTTPResponse.getheaders()
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message=r"HTTPResponse\.getheaders\(\) is deprecated"
)

# Suppress anyio stream warnings
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module="anyio.streams.memory"
)

# Configure logging with more granular control
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Allow overriding log level via env
_server_log_level = os.environ.get("OVNK_LOG_LEVEL", "INFO").upper()
try:
    root_level = getattr(logging, _server_log_level, logging.INFO)
except Exception:
    root_level = logging.INFO
logging.getLogger().setLevel(root_level)
logger.setLevel(root_level)

# Ensure submodule logs are visible in server output
_sub_loggers = [
    "tools.ovnk_benchmark_prometheus_basequery",
    "ocauth.ovnk_benchmark_auth",
]
for lname in _sub_loggers:
    l = logging.getLogger(lname)
    l.setLevel(root_level)
    l.propagate = True

# Reduce noise from overly chatty libs
logging.getLogger("mcp.server.streamable_http").setLevel(logging.WARNING)
logging.getLogger("anyio").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
logger.debug(f"Logging configured. Root level={root_level}, OVNK_LOG_LEVEL={_server_log_level}")

# Configure timezone
os.environ['TZ'] = 'UTC'

# Global shutdown event
shutdown_event = asyncio.Event()

class ClusterInfoRequest(BaseModel):
    """Request model for OpenShift cluster information queries"""
    include_node_details: bool = Field(
        default=True,
        description="Whether to include detailed node information including capacity, versions, and status for all nodes grouped by role (master/worker/infra)"
    )
    include_resource_counts: bool = Field(
        default=True,
        description="Whether to include comprehensive resource counts across all namespaces including pods, services, secrets, configmaps, and network policies"
    )
    include_network_policies: bool = Field(
        default=True,
        description="Whether to include detailed network policy information including NetworkPolicy, AdminNetworkPolicy, EgressFirewall, EgressIP, and UserDefinedNetwork counts"
    )
    include_operator_status: bool = Field(
        default=True,
        description="Whether to include cluster operator health status and identify any unavailable or degraded operators"
    )
    include_mcp_status: bool = Field(
        default=True,
        description="Whether to include Machine Config Pool (MCP) status information showing update progress and any degraded pools"
    )
    save_to_file: bool = Field(
        default=False,
        description="Whether to save the collected cluster information to a timestamped JSON file for documentation and audit purposes"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class AnalyzeClusterStatusRequest(BaseModel):
    """Request model for cluster status analysis with detailed health assessment"""
    include_detailed_analysis: bool = Field(
        default=True,
        description="Whether to include detailed component analysis with health scoring, recommendations, and performance alerts"
    )
    generate_report: bool = Field(
        default=True,
        description="Whether to generate a human-readable summary report in addition to structured analysis data"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class MetricsRequest(BaseModel):
    """Request model for basic metrics queries"""
    duration: str = Field(
        default="1h", 
        description="Query duration (e.g., 5m, 1h, 1d, 7d)"
    )
    start_time: Optional[str] = Field(
        default=None, 
        description="Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)"
    )
    end_time: Optional[str] = Field(
        default=None, 
        description="End time in ISO format (YYYY-MM-DDTHH:MM:SSZ)"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class PODsContainerRequest(BaseModel):
    """Request model for pod-specific metrics queries"""
    pod_pattern: str = Field(
        default="ovnkube.*", 
        description="Regex pattern for pod names (e.g., 'ovnkube.*', 'multus.*')"
    )
    container_pattern: str = Field(
        default="ovnkube-controller", 
        description="Regex pattern for container names (e.g., 'ovnkube-controller', 'kube-rbac-proxy.*')"
    )
    label_selector: str = Field(
        default=".*", 
        description="Regex pattern for pod label selectors"
    )
    namespace_pattern: str = Field(
        default="openshift-ovn-kubernetes", 
        description="Regex pattern for namespace (e.g., 'openshift-ovn-kubernetes', 'openshift-multus')"
    )
    top_n: int = Field(
        default=10, 
        description="Number of top results to return (1-50)"
    )
    duration: str = Field(
        default="1h", 
        description="Query duration for range queries (e.g., 5m, 1h, 1d)"
    )
    start_time: Optional[str] = Field(
        default=None, 
        description="Start time in ISO format"
    )
    end_time: Optional[str] = Field(
        default=None, 
        description="End time in ISO format"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class PODsRequest(BaseModel):
    """Request model for pod-specific metrics queries"""
    pod_pattern: str = Field(
        default="ovnkube.*", 
        description="Regex pattern for pod names (e.g., 'ovnkube.*', 'multus.*')"
    )
    container_pattern: str = Field(
        default=".*", 
        description="Regex pattern for container names (e.g., 'ovnkube-controller', 'kube-rbac-proxy.*')"
    )
    label_selector: str = Field(
        default=".*", 
        description="Regex pattern for pod label selectors"
    )
    namespace_pattern: str = Field(
        default="openshift-ovn-kubernetes", 
        description="Regex pattern for namespace (e.g., 'openshift-ovn-kubernetes', 'openshift-multus')"
    )
    top_n: int = Field(
        default=10, 
        description="Number of top results to return (1-50)"
    )
    duration: str = Field(
        default="1h", 
        description="Query duration for range queries (e.g., 5m, 1h, 1d)"
    )
    start_time: Optional[str] = Field(
        default=None, 
        description="Start time in ISO format"
    )
    end_time: Optional[str] = Field(
        default=None, 
        description="End time in ISO format"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class PODsMultusRequest(BaseModel):
    """Request model for pod-specific metrics queries"""
    pod_pattern: str = Field(
        default="multus-.*|network-metrics-.*", 
        description="Regex pattern for pod names (e.g., 'ovnkube.*', 'multus.*')"
    )
    container_pattern: str = Field(
        default=".*", 
        description="Regex pattern for container names (e.g., 'ovnkube-controller', 'kube-rbac-proxy.*')"
    )
    label_selector: str = Field(
        default=".*", 
        description="Regex pattern for pod label selectors"
    )
    namespace_pattern: str = Field(
        default="openshift-multus", 
        description="Regex pattern for namespace (e.g., 'openshift-ovn-kubernetes', 'openshift-multus')"
    )
    top_n: int = Field(
        default=10, 
        description="Number of top results to return (1-50)"
    )
    duration: str = Field(
        default="1h", 
        description="Query duration for range queries (e.g., 5m, 1h, 1d)"
    )
    start_time: Optional[str] = Field(
        default=None, 
        description="Start time in ISO format"
    )
    end_time: Optional[str] = Field(
        default=None, 
        description="End time in ISO format"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class PrometheusBasicInfoRequest(BaseModel):
    """Request model for Prometheus basic OVN information queries"""
    include_pod_status: bool = Field(
        default=True,
        description="Whether to include cluster-wide pod phase counts and status information"
    )
    include_db_metrics: bool = Field(
        default=True,
        description="Whether to include OVN database size metrics (Northbound and Southbound)"
    )
    custom_metrics: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional dictionary of custom metric_name -> prometheus_query pairs for additional data collection"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class OVNKSyncDurationRequest(BaseModel):
    """Request model for OVN-Kubernetes sync duration metrics queries"""
    query_type: str = Field(
        default="instant",
        description="Type of metrics query: 'instant' for current snapshot of sync duration metrics or 'duration' for time-range analysis with statistical aggregation over specified time period. Instant queries show current state, duration queries provide trend analysis with min/avg/max calculations."
    )
    duration: Optional[str] = Field(
        default=None,
        description="Analysis time window when query_type='duration' using Prometheus time format (e.g., '5m', '15m', '30m', '1h', '2h', '6h', '24h'). Required for duration-based analysis. Shorter durations (5m-30m) for recent performance, longer durations (1h-24h) for trend analysis and baseline establishment."
    )
    timestamp: Optional[str] = Field(
        default=None,
        description="Specific timestamp for instant queries in ISO format (YYYY-MM-DDTHH:MM:SSZ). When provided, retrieves sync metrics at exact point in time instead of current metrics. Useful for historical analysis, incident correlation, or comparing metrics across different time points."
    )
    end_time: Optional[str] = Field(
        default=None,
        description="End time for duration queries in ISO format (YYYY-MM-DDTHH:MM:SSZ). Defaults to current time if not specified. Combined with duration parameter to define analysis time window. Essential for analyzing historical performance periods or specific time ranges."
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class PerformanceDataRequest(BaseModel):
    """Request model for performance data storage"""
    metrics_data: Dict[str, Any] = Field(
        description="Metrics data dictionary to store in database"
    )
    timestamp: Optional[str] = Field(
        default=None, 
        description="ISO timestamp for the data (defaults to current time)"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class HealthCheckRequest(BaseModel):
    """Empty request model for health check"""
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class OVNBasicInfoRequest(BaseModel):
    """Request model for OVN basic information queries"""
    include_pod_status: bool = Field(
        default=True,
        description="Whether to include cluster-wide pod phase counts and status information"
    )
    include_db_metrics: bool = Field(
        default=True,
        description="Whether to include OVN database size metrics (Northbound and Southbound)"
    )
    custom_metrics: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional dictionary of custom metric_name -> prometheus_query pairs for additional data collection"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class ComprehensivePerformanceRequest(BaseModel):
    """Request model for comprehensive OVNK performance analysis"""
    duration: Optional[str] = Field(
        default=None,
        description="Analysis duration using Prometheus time format (e.g., '5m', '1h', '1d'). If not provided, performs instant analysis"
    )
    generate_report: bool = Field(
        default=True,
        description="Whether to generate a human-readable summary report in addition to structured data"
    )
    save_to_file: bool = Field(
        default=False,
        description="Whether to save analysis results to JSON file for documentation and audit purposes"
    )
    include_risk_assessment: bool = Field(
        default=True,
        description="Whether to include automated risk assessment with threshold-based alerts and recommendations"
    )
    performance_thresholds: Optional[Dict[str, Dict[str, float]]] = Field(
        default=None,
        description="Custom performance thresholds for risk assessment (overrides defaults). Structure: {'cpu': {'warning': 70.0, 'critical': 90.0}, 'memory': {'warning': 1073741824, 'critical': 2147483648}, 'sync_duration': {'warning': 5.0, 'critical': 10.0}, 'db_size': {'warning': 104857600, 'critical': 524288000}}"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class OCPOVERALLPerformanceRequest(BaseModel):
    """Request model for overall OCP cluster performance analysis"""
    duration: str = Field(
        default="1h",
        description="Analysis duration for metrics collection using Prometheus time format (e.g., '5m', '30m', '1h', '2h', '24h'). Longer durations provide more comprehensive trend analysis but take more time to process. Recommended values: '5m' for quick checks, '1h' for standard analysis, '24h' for trend analysis."
    )
    include_detailed_analysis: bool = Field(
        default=True,
        description="Whether to include detailed component-level analysis in the response. When True, provides comprehensive breakdown of each component's performance metrics, resource usage, and health status. Set to False for faster execution when only summary metrics are needed."
    )
    focus_areas: Optional[List[str]] = Field(
        default=None,
        description="Optional list of specific focus areas to emphasize in analysis. Available areas: ['cluster', 'api', 'ovnk', 'nodes', 'databases', 'sync']. Use to optimize analysis time by focusing on specific components. Examples: ['cluster', 'api'] for control plane focus, ['ovnk', 'sync'] for networking focus, ['nodes'] for compute focus. If not specified, analyzes all areas comprehensively."
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


# Initialize FastMCP app
app = FastMCP("ovnk-benchmark-mcp")

# Global components
auth_manager: Optional[OpenShiftAuth] = None
config: Optional[Config] = None
prometheus_client: Optional[PrometheusBaseQuery] = None
storage: Optional[PrometheusStorage] = None
cluster_analyzer: Optional[ClusterStatAnalyzer] = None


async def initialize_components():
    """Initialize global components with proper error handling"""
    global auth_manager, config, prometheus_client, storage, cluster_analyzer
    
    try:
        config = Config()
        auth_manager = OpenShiftAuth(config.kubeconfig_path)
        await auth_manager.initialize()        
        prometheus_client = PrometheusBaseQuery(
            auth_manager.prometheus_url,
            auth_manager.prometheus_token
        )
        
        storage = PrometheusStorage()
        await storage.initialize()
        
        cluster_analyzer = ClusterStatAnalyzer()
            
        logger.info("All components initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize components: {e}")
        raise

async def cleanup_resources():
    """Clean up global resources on shutdown"""
    global auth_manager, storage
    
    logger.info("Cleaning up resources...")
    
    try:
        if storage:
            await storage.close()
    except Exception as e:
        logger.error(f"Error cleaning up storage: {e}")
    
    try:
        if auth_manager:
            await auth_manager.cleanup()
    except Exception as e:
        logger.error(f"Error cleaning up auth manager: {e}")
    
    except Exception as e:
        logger.error(f"Error cleaning up cluster collector: {e}")
    
    logger.info("Resource cleanup completed")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()

# Setup signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


@app.tool(
    name="get_mcp_health_status",
    description="""Health check for the MCP server. Verifies MCP server is running, Prometheus connectivity, and Kubernetes API connectivity. Returns component statuses and timestamps."""
)
async def get_mcp_health_status(request: HealthCheckRequest) -> Dict[str, Any]:
    """Return health status for MCP server, Prometheus, and KubeAPI with improved error handling"""
    global auth_manager, prometheus_client
    
    try:
        # Ensure components exist
        if not auth_manager or not prometheus_client:
            try:
                await initialize_components()
            except Exception as init_error:
                return {
                    "status": "error", 
                    "error": f"Component initialization failed: {init_error}",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }

        # Test Prometheus connectivity with timeout
        prometheus_ok = False
        prometheus_error: Optional[str] = None
        try:
            # Add timeout to prevent hanging
            prometheus_ok = await asyncio.wait_for(
                prometheus_client.test_connection(),
                timeout=10.0
            )
          
        except asyncio.TimeoutError:
            prometheus_error = "Connection timeout after 10 seconds"
            prometheus_ok = False
        except Exception as e:
            prometheus_error = str(e)
            prometheus_ok = False
            logger.error(f"Prometheus connection error: {e}")

        # Test Kube API connectivity with timeout
        kubeapi_ok = False
        kubeapi_error: Optional[str] = None
        try:
            if auth_manager:
                kubeapi_ok = await asyncio.wait_for(
                    auth_manager.test_kubeapi_connection(),
                    timeout=10.0
                )
        except asyncio.TimeoutError:
            kubeapi_error = "Connection timeout after 10 seconds"
            kubeapi_ok = False
        except Exception as e:
            kubeapi_error = str(e)
            kubeapi_ok = False
            logger.error(f"KubeAPI connection error: {e}")

        status = "healthy" if prometheus_ok and kubeapi_ok else ("degraded" if prometheus_ok or kubeapi_ok else "unhealthy")

        return {
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mcp_server": {
                "running": True,
                "transport": "streamable-http",
            },
            "prometheus": {
                "connected": prometheus_ok,
                "url": getattr(auth_manager, "prometheus_url", None) if auth_manager else None,
                "error": prometheus_error,
            },
            "kubeapi": {
                "connected": kubeapi_ok,
                "node_count": (auth_manager.cluster_info.get("node_count") if (auth_manager and auth_manager.cluster_info) else None),
                "error": kubeapi_error,
            },
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "error", "error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="get_openshift_cluster_info",
    description="""Collect comprehensive OpenShift cluster information including detailed node status, resource utilization, network policy configurations, cluster operator health, and infrastructure details. This tool provides a complete cluster inventory and health overview essential for understanding cluster architecture, capacity, and operational status.

This tool leverages the ClusterInfoCollector to gather structured information about your OpenShift cluster including:

CLUSTER ARCHITECTURE:
- Cluster name, version, and infrastructure platform (AWS, Azure, GCP, VMware, etc.)
- API server endpoint and cluster identification
- Total node count with role-based categorization (master/worker/infra nodes)
- Collection timestamp for data freshness verification

NODE INFORMATION (when include_node_details=True):
- Detailed node inventory grouped by role (master, worker, infrastructure nodes)
- Per-node specifications: CPU capacity, memory capacity, architecture, kernel version
- Container runtime versions, kubelet versions, and OS image information  
- Node health status (Ready/NotReady/SchedulingDisabled) and schedulability
- Node creation timestamps and lifecycle information
- Hardware and software configuration details for capacity planning

RESOURCE INVENTORY (when include_resource_counts=True):
- Comprehensive resource counts across all namespaces
- Pod distribution and total pod count for capacity monitoring
- Service, Secret, and ConfigMap counts for resource utilization analysis
- Cross-namespace resource distribution patterns
- Resource density metrics for capacity planning

NETWORK POLICY CONFIGURATION (when include_network_policies=True):
- NetworkPolicy count for standard Kubernetes network segmentation
- AdminNetworkPolicy (ANP) count for cluster-wide network security policies  
- EgressFirewall policy count for egress traffic control and security
- EgressIP configuration count for source IP management
- UserDefinedNetwork (UDN) count for custom network configurations
- Network security posture assessment through policy distribution analysis

CLUSTER OPERATOR HEALTH (when include_operator_status=True):
- Complete cluster operator inventory and availability status
- Identification of unavailable or degraded cluster operators
- Operator health trends and stability indicators
- Critical operator failure detection for immediate attention
- Operator dependency analysis for troubleshooting

MACHINE CONFIG POOL STATUS (when include_mcp_status=True):
- Machine Config Pool health and update status (Updated/Updating/Degraded)
- Configuration synchronization status across node pools
- Update progress tracking for maintenance windows
- Configuration drift detection and remediation status
- Node pool stability and configuration consistency

OPERATIONAL METADATA:
- Data collection timestamp for freshness verification
- Collection success metrics and any partial failure indicators
- Structured JSON format for integration with monitoring and automation systems
- Optional file export for documentation, compliance, and historical analysis

Parameters:
- include_node_details (default: true): Include comprehensive node information with hardware specs, software versions, and health status for all cluster nodes
- include_resource_counts (default: true): Include detailed resource inventory counts across all namespaces for capacity analysis
- include_network_policies (default: true): Include network policy configurations and counts for security posture assessment
- include_operator_status (default: true): Include cluster operator health status and identify any degraded or unavailable operators
- include_mcp_status (default: true): Include Machine Config Pool status and update progress information
- save_to_file (default: false): Save complete cluster information to timestamped JSON file for documentation and audit trails

Use this tool for:
- Pre-deployment cluster readiness verification and infrastructure validation
- Capacity planning and resource allocation analysis based on current utilization
- Security posture assessment through network policy and operator health analysis
- Operational health monitoring and cluster status reporting
- Troubleshooting cluster issues by understanding current configuration and status
- Compliance reporting and infrastructure documentation for audit purposes
- Baseline establishment for cluster monitoring and change tracking over time
- Executive reporting on cluster architecture, capacity, and operational health

The tool provides structured, comprehensive cluster information suitable for both technical analysis and management reporting, enabling informed decisions about cluster operations, capacity planning, and infrastructure optimization."""
)
async def get_openshift_cluster_info(request: ClusterInfoRequest) -> Dict[str, Any]:
    """
    Collect comprehensive OpenShift cluster information including detailed node status,
    resource utilization, network policy configurations, cluster operator health,
    and infrastructure details.
    
    Provides complete cluster inventory and health overview essential for understanding
    cluster architecture, capacity, and operational status.
    """
    try:
        logger.info("Starting comprehensive cluster information collection...")
        
        # Initialize collector
        collector = ClusterInfoCollector()
        
        # Add timeout to prevent hanging during cluster information collection
        cluster_info = await asyncio.wait_for(
            collector.collect_cluster_info(),
            timeout=60.0  # Extended timeout for comprehensive collection
        )
        
        # Convert to dictionary format
        cluster_data = collector.to_dict(cluster_info)
        
        # Apply filtering based on request parameters
        if not request.include_node_details:
            # Remove detailed node information but keep counts
            cluster_data.pop('master_nodes', None)
            cluster_data.pop('infra_nodes', None) 
            cluster_data.pop('worker_nodes', None)
            logger.info("Node details excluded from response")
        
        if not request.include_resource_counts:
            # Remove resource counts
            resource_fields = ['namespaces_count', 'pods_count', 'services_count', 
                             'secrets_count', 'configmaps_count']
            for field in resource_fields:
                cluster_data.pop(field, None)
            logger.info("Resource counts excluded from response")
        
        if not request.include_network_policies:
            # Remove network policy information
            policy_fields = ['networkpolicies_count', 'adminnetworkpolicies_count',
                           'egressfirewalls_count', 'egressips_count', 'udn_count']
            for field in policy_fields:
                cluster_data.pop(field, None)
            logger.info("Network policy information excluded from response")
        
        if not request.include_operator_status:
            cluster_data.pop('unavailable_cluster_operators', None)
            logger.info("Operator status excluded from response")
        
        if not request.include_mcp_status:
            cluster_data.pop('mcp_status', None)
            logger.info("Machine Config Pool status excluded from response")
        
        # Save to file if requested
        if request.save_to_file:
            try:
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                filename = f"cluster_info_{timestamp}.json"
                
                with open(filename, 'w') as f:
                    json.dump(cluster_data, f, indent=2, default=str)
                
                cluster_data['saved_file'] = filename
                logger.info(f"Cluster information saved to {filename}")
            except Exception as save_error:
                logger.warning(f"Failed to save cluster info to file: {save_error}")
                cluster_data['save_file_error'] = str(save_error)
        
        # Add collection metadata
        cluster_data['collection_metadata'] = {
            'tool_name': 'get_openshift_cluster_info',
            'parameters_applied': {
                'include_node_details': request.include_node_details,
                'include_resource_counts': request.include_resource_counts,
                'include_network_policies': request.include_network_policies,
                'include_operator_status': request.include_operator_status,
                'include_mcp_status': request.include_mcp_status,
                'save_to_file': request.save_to_file
            },
            'collection_duration_seconds': 60.0,
            'data_freshness': cluster_data.get('collection_timestamp'),
            'total_fields_collected': len(cluster_data)
        }
        
        # Log collection summary
        node_summary = f"Nodes: {cluster_data.get('total_nodes', 0)} total"
        if request.include_node_details:
            masters = len(cluster_data.get('master_nodes', []))
            workers = len(cluster_data.get('worker_nodes', []))
            infra = len(cluster_data.get('infra_nodes', []))
            node_summary += f" ({masters} master, {workers} worker, {infra} infra)"
        
        unavailable_ops = len(cluster_data.get('unavailable_cluster_operators', []))
        degraded_mcps = len([status for status in cluster_data.get('mcp_status', {}).values() 
                           if status in ['Degraded', 'Updating']])
        
        logger.info(f"Cluster information collection completed - {node_summary}, "
                   f"Unavailable operators: {unavailable_ops}, Degraded MCPs: {degraded_mcps}")
        
        return cluster_data
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting cluster information - cluster may be experiencing issues or have extensive resources",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 60,
            "suggestion": "Try running with fewer details enabled or check cluster responsiveness"
        }
    except Exception as e:
        logger.error(f"Error collecting cluster information: {e}")
        return {
            "error": str(e), 
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "get_openshift_cluster_info"
        }


@app.tool(
    name="query_cluster_node_usage",
    description="""Get comprehensive cluster node resource usage metrics including CPU utilization, memory consumption, and network utilization across all worker, control plane, and infrastructure nodes grouped by role. This tool provides both instant snapshots and duration-based node performance metrics with statistical analysis (min/avg/max) for capacity planning and performance monitoring.

RESOURCE METRICS COLLECTED:
- CPU usage percentages per node with statistical analysis (min/avg/max values over time period)
- Memory utilization in MB including total usage calculations from available memory metrics
- Network receive/transmit rates in bytes/second with comprehensive device filtering (excludes loopback, virtual, and container interfaces)
- Per-node resource consumption trends and patterns for capacity planning analysis

NODE ORGANIZATION:
- Nodes automatically grouped by role: controlplane, worker, infra, and workload categories
- Role-based summary statistics providing group-level resource utilization overview
- Individual node metrics with instance mapping from Prometheus endpoints to node names
- Node count and distribution analysis across different roles for infrastructure planning

TOP RESOURCE CONSUMERS:
- Top 5 worker nodes by maximum CPU usage with both peak and average utilization metrics
- Top 5 worker nodes by maximum memory usage with both peak and average consumption data
- Resource ranking helps identify nodes requiring attention or optimization
- Performance comparison data for load balancing and capacity distribution analysis

STATISTICAL ANALYSIS:
- Minimum, average, and maximum values for each metric over the specified time period
- Time-based trend analysis showing resource utilization patterns and peaks
- Group-level aggregated statistics for role-based capacity planning
- Historical performance data for establishing baselines and identifying trends

METADATA AND TIMESTAMPS:
- Query execution timestamp and timezone information (UTC) for data correlation
- Duration coverage and time range details for analysis context
- Data freshness indicators and collection success metrics
- Instance-to-node name mapping for cross-referencing with other monitoring systems

Parameters:
- duration (default: "1h"): Query duration using Prometheus time format (e.g., "5m", "15m", "1h", "3h", "1d", "7d") - longer durations provide better trend analysis but require more processing time
- end_time (optional): End time in ISO format (YYYY-MM-DDTHH:MM:SSZ) for historical analysis - defaults to current time for recent data analysis
- start_time (optional): Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ) - automatically calculated from duration and end_time if not provided

Use this tool for:
- Identifying node-level resource bottlenecks and capacity constraints before they impact applications
- Capacity planning and resource allocation optimization based on historical usage patterns
- Performance monitoring and trend analysis for proactive infrastructure management
- Load balancing analysis to identify unevenly distributed workloads across nodes
- Infrastructure health monitoring and operational dashboard integration
- Pre-maintenance planning by identifying high-resource utilization nodes
- Cost optimization analysis by understanding actual vs. allocated resource usage
- Troubleshooting cluster performance issues through node-level resource correlation

The tool provides comprehensive node-level visibility essential for effective OpenShift cluster resource management and operational excellence."""
)

async def query_cluster_node_usage(request: MetricsRequest) -> Dict[str, Any]:
    """
    Get comprehensive cluster node resource usage metrics including CPU utilization, 
    memory consumption, and network utilization across all worker, control plane, 
    and infrastructure nodes grouped by role.
    
    Provides both instant snapshots and duration-based node performance metrics with 
    statistical analysis for capacity planning and performance monitoring.
    """
    global prometheus_client, auth_manager
    try:
        if not prometheus_client or not auth_manager:
            await initialize_components()
            
        # Initialize the node usage collector
        collector = nodeUsageCollector(prometheus_client, auth_manager)
        
        logger.info(f"Collecting node usage data for duration: {request.duration}")
        
        # Add timeout to prevent hanging during node usage collection
        usage_data = await asyncio.wait_for(
            collector.collect_usage_data(
                duration=request.duration,
                end_time=request.end_time
            ),
            timeout=45.0  # Reasonable timeout for node metrics collection
        )
        
        # Log collection summary for operational visibility
        total_nodes = sum(len(group['nodes']) for group in usage_data.get('groups', {}).values())
        top_cpu_count = len(usage_data.get('top_usage', {}).get('cpu', []))
        top_memory_count = len(usage_data.get('top_usage', {}).get('memory', []))
        
        logger.info(f"Node usage collection completed - Total nodes: {total_nodes}, "
                   f"Top CPU consumers: {top_cpu_count}, Top memory consumers: {top_memory_count}")
        
        return usage_data
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting node usage metrics - cluster may be experiencing issues or have many nodes",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 45,
            "suggestion": "Try reducing the duration parameter or check cluster node responsiveness"
        }
    except Exception as e:
        logger.error(f"Error collecting node usage metrics: {e}")
        return {
            "error": str(e), 
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_cluster_node_usage"
        }

@app.tool(
    name="analyze_cluster_status",
    description="""Perform comprehensive cluster status analysis with health scoring, performance alerts, and actionable recommendations. This tool analyzes collected cluster information to provide detailed health assessments, identify potential issues, and generate strategic recommendations for cluster optimization.

CLUSTER HEALTH ASSESSMENT:
- Overall cluster health score (0-100) based on component availability and performance
- Component-level health analysis including nodes, operators, and machine config pools
- Critical issue identification with severity-based alerting (CRITICAL, HIGH, MEDIUM, LOW)
- Health trend analysis and performance degradation detection

NODE GROUP ANALYSIS:
- Master, worker, and infrastructure node group health scoring
- Node availability analysis (Ready vs NotReady states)
- Scheduling status evaluation (SchedulingDisabled detection)
- Resource capacity analysis including CPU cores and memory allocation per node group
- Problematic node identification with specific node names for immediate attention

CLUSTER OPERATOR HEALTH:
- Cluster operator availability assessment with estimated total operator count
- Unavailable operator identification by name for targeted troubleshooting  
- Operator health percentage calculation based on availability ratios
- Critical operator failure detection requiring immediate intervention

MACHINE CONFIG POOL ANALYSIS:
- MCP status evaluation (Updated, Updating, Degraded states)
- Configuration synchronization health across node pools
- Update progress monitoring and degradation detection
- Pool-specific health scoring for maintenance planning

RESOURCE UTILIZATION INSIGHTS:
- Pod density analysis across cluster nodes for capacity planning
- Service-to-pod ratios indicating application architecture patterns
- Namespace distribution analysis for workload organization assessment
- Secret and ConfigMap density metrics for resource management optimization

NETWORK POLICY ANALYSIS:
- Network security posture assessment through policy distribution
- NetworkPolicy, AdminNetworkPolicy, and EgressFirewall analysis
- Network complexity scoring based on policy types and counts
- User-defined network configuration impact analysis

PERFORMANCE ALERTS:
- Automated alert generation based on predefined thresholds and best practices
- Severity-based categorization (CRITICAL for master node issues, HIGH for operator failures)
- Resource-specific alerts with current vs threshold value comparisons
- Component-specific alerting for targeted remediation efforts

ACTIONABLE RECOMMENDATIONS:
- Priority-based recommendations starting with critical issues requiring immediate attention
- Node-specific remediation guidance for NotReady or degraded nodes
- Cluster operator restoration recommendations with specific operator names
- Machine config pool remediation guidance for configuration issues
- Resource optimization suggestions based on utilization patterns

ANALYSIS METADATA:
- Analysis execution timestamp and duration for performance tracking
- Total items analyzed for scope validation and coverage assessment
- Collection type identification for correlation with data sources
- Analysis completeness indicators and success metrics

Parameters:
- include_detailed_analysis (default: true): Include comprehensive component-level analysis with health scoring, detailed breakdowns, and statistical analysis for each cluster component
- generate_report (default: true): Generate human-readable summary report in addition to structured JSON data for management reporting and operational documentation

Use this tool for:
- Proactive cluster health monitoring and early issue detection before service impact
- Post-maintenance validation ensuring all cluster components are functioning properly
- Capacity planning analysis based on current resource utilization and distribution patterns
- Operational readiness assessment before critical deployments or maintenance windows
- Troubleshooting cluster issues through comprehensive component health analysis
- Executive reporting on cluster operational health and infrastructure stability
- Compliance reporting demonstrating cluster health monitoring and issue resolution processes
- Strategic planning for cluster upgrades, expansions, or optimization initiatives

This tool transforms raw cluster information into actionable intelligence, enabling proactive cluster management and informed decision-making for infrastructure operations."""
)
async def analyze_cluster_status_tool(request: AnalyzeClusterStatusRequest) -> Dict[str, Any]:
    """
    Perform comprehensive cluster status analysis with health scoring, performance alerts,
    and actionable recommendations based on collected cluster information.
    
    Transforms raw cluster data into actionable intelligence for proactive cluster management.
    """
    try:
        logger.info("Starting comprehensive cluster status analysis...")
        
        # First collect current cluster information
        collector = ClusterInfoCollector()
        
        # Add timeout for cluster data collection
        cluster_info = await asyncio.wait_for(
            collector.collect_cluster_info(),
            timeout=60.0
        )
        
        # Convert to dictionary format for analysis
        cluster_data = collector.to_dict(cluster_info)
        
        logger.info(f"Cluster data collected - Total nodes: {cluster_data.get('total_nodes', 0)}, "
                   f"Unavailable operators: {len(cluster_data.get('unavailable_cluster_operators', []))}")
        
        # Perform analysis using the imported function
        analysis_result = await asyncio.wait_for(
            analyze_cluster_status_module(cluster_data),
            timeout=30.0
        )
        
        # Add analysis execution metadata
        analysis_result['execution_metadata'] = {
            'tool_name': 'analyze_cluster_status',
            'parameters_applied': {
                'include_detailed_analysis': request.include_detailed_analysis,
                'generate_report': request.generate_report
            },
            'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            'cluster_data_freshness': cluster_data.get('collection_timestamp'),
            'total_alerts_generated': len(analysis_result.get('alerts', [])),
            'recommendations_count': len(analysis_result.get('recommendations', []))
        }
        
        # Generate human-readable report if requested
        if request.generate_report:
            try:
                # Use the analyzer to generate the report
                analyzer = ClusterStatAnalyzer()
                report_text = analyzer.generate_report(analysis_result)
                analysis_result['human_readable_report'] = report_text
                logger.info("Human-readable report generated successfully")
            except Exception as report_error:
                logger.warning(f"Failed to generate human-readable report: {report_error}")
                analysis_result['report_generation_error'] = str(report_error)
        
        # Apply filtering based on request parameters
        if not request.include_detailed_analysis:
            # Remove detailed breakdowns but keep summary data
            simplified_result = {
                'metadata': analysis_result.get('metadata', {}),
                'cluster_health': analysis_result.get('cluster_health', {}),
                'alerts_summary': {
                    'total_alerts': len(analysis_result.get('alerts', [])),
                    'critical_alerts': len([a for a in analysis_result.get('alerts', []) 
                                          if a.get('severity') == 'CRITICAL']),
                    'high_alerts': len([a for a in analysis_result.get('alerts', []) 
                                      if a.get('severity') == 'HIGH'])
                },
                'recommendations': analysis_result.get('recommendations', []),
                'execution_metadata': analysis_result.get('execution_metadata', {})
            }
            
            if request.generate_report:
                simplified_result['human_readable_report'] = analysis_result.get('human_readable_report')
            
            analysis_result = simplified_result
            logger.info("Detailed analysis excluded from response")
        
        # Log analysis summary
        health_score = analysis_result.get('cluster_health', {}).get('overall_score', 0)
        total_alerts = len(analysis_result.get('alerts', []))
        critical_alerts = len([a for a in analysis_result.get('alerts', []) 
                              if a.get('severity') == 'CRITICAL'])
        recommendations_count = len(analysis_result.get('recommendations', []))
        
        logger.info(f"Cluster status analysis completed - Health score: {health_score}/100, "
                   f"Total alerts: {total_alerts}, Critical alerts: {critical_alerts}, "
                   f"Recommendations: {recommendations_count}")
        
        return analysis_result
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout during cluster status analysis - cluster may be experiencing issues or have extensive resources",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_details": "Either cluster data collection (60s) or analysis (30s) timed out",
            "suggestion": "Check cluster responsiveness and try again with simplified analysis"
        }
    except Exception as e:
        logger.error(f"Error during cluster status analysis: {e}")
        return {
            "error": str(e), 
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "analyze_cluster_status"
        }

@app.tool(
    name="query_prometheus_basic_info",
    description="""Query basic OVN-Kubernetes infrastructure information including OVN database sizes and cluster-wide pod status from Prometheus metrics. This tool provides essential baseline metrics for OVN-K cluster health monitoring and capacity planning.

CORE METRICS COLLECTED:

OVN DATABASE METRICS (when include_db_metrics=True):
- OVN Northbound database size in bytes with maximum values across all instances
- OVN Southbound database size in bytes with maximum values across all instances
- Database size metrics help monitor OVN control plane storage requirements and growth patterns
- Instance labels and metadata for identifying specific database instances and their locations
- Critical for detecting database bloat, storage capacity planning, and performance optimization

POD STATUS METRICS (when include_pod_status=True):
- Cluster-wide pod counts grouped by phase (Running, Pending, Failed, Succeeded, Unknown)
- Total pod count across all namespaces for capacity monitoring and resource utilization analysis  
- Pod distribution analysis showing cluster workload patterns and potential scheduling issues
- Phase-specific counts help identify stuck pods, resource constraints, and cluster health issues
- Essential for capacity planning, troubleshooting deployment issues, and operational monitoring

CUSTOM METRICS (when custom_metrics specified):
- Support for additional Prometheus queries specified as metric_name -> query_string pairs
- Maximum value extraction for custom metrics with full label preservation
- Flexible extension point for collecting specific OVN-K or cluster metrics not covered by default collection
- Custom metrics enable targeted monitoring for specific use cases, troubleshooting scenarios, or performance analysis

OPERATIONAL METADATA:
- Query execution timestamps for data freshness verification and correlation with other monitoring data
- Error handling and reporting for individual metric collection failures without failing entire operation
- Structured JSON output format suitable for integration with monitoring dashboards and automation systems
- Query type identification (instant vs range) and measurement units for proper data interpretation

Parameters:
- include_pod_status (default: true): Collect cluster-wide pod phase distribution and total counts for workload monitoring
- include_db_metrics (default: true): Collect OVN Northbound and Southbound database size metrics for storage monitoring  
- custom_metrics (optional): Dictionary of additional Prometheus queries in format {"metric_name": "prometheus_query"} for extended monitoring

Use this tool for:
- Baseline cluster health monitoring and establishing normal operational parameters
- OVN database growth tracking and storage capacity planning for long-term infrastructure planning
- Quick cluster status overview combining networking infrastructure and workload distribution
- Troubleshooting cluster-wide issues by understanding pod distribution patterns and database health
- Capacity planning analysis using both infrastructure (database) and workload (pod) metrics
- Integration with monitoring dashboards requiring core OVN-K infrastructure metrics
- Operational health checks before and after maintenance activities or configuration changes
- Performance baseline establishment for trending analysis and anomaly detection over time

The tool provides essential OVN-K infrastructure visibility combining both control plane (database) and data plane (pod status) metrics for comprehensive cluster monitoring and operational awareness."""
)
async def query_prometheus_basic_info(request: PrometheusBasicInfoRequest) -> Dict[str, Any]:
    """
    Query basic OVN-Kubernetes infrastructure information including OVN database sizes
    and cluster-wide pod status from Prometheus metrics.
    
    Provides essential baseline metrics for OVN-K cluster health monitoring and capacity planning.
    """
    global prometheus_client, auth_manager
    
    try:
        if not prometheus_client or not auth_manager:
            await initialize_components()
        
        logger.info("Starting basic OVN infrastructure information collection...")
        
        results = {
            "collection_timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_prometheus_basic_info"
        }
        
        # Collect OVN database metrics if requested
        if request.include_db_metrics:
            try:
                logger.debug("Collecting OVN database size metrics...")
                
                collector = ovnBasicInfoCollector(
                    auth_manager.prometheus_url, 
                    auth_manager.prometheus_token
                )
                
                # Use default metrics if no custom metrics specified for DB collection
                db_metrics = None if request.custom_metrics is None else {}
                
                # Add timeout to prevent hanging
                db_results = await asyncio.wait_for(
                    collector.collect_max_values(db_metrics),
                    timeout=15.0
                )
                
                results["ovn_database_metrics"] = db_results
                logger.info("OVN database metrics collected successfully")
                
            except asyncio.TimeoutError:
                logger.warning("Timeout collecting OVN database metrics")
                results["ovn_database_metrics"] = {
                    "error": "Timeout collecting database metrics",
                    "timeout_seconds": 15
                }
            except Exception as e:
                logger.error(f"Error collecting OVN database metrics: {e}")
                results["ovn_database_metrics"] = {"error": str(e)}
        
        # Collect pod status metrics if requested  
        if request.include_pod_status:
            try:
                logger.debug("Collecting cluster-wide pod status metrics...")
                
                # Add timeout to prevent hanging
                pod_status_results = await asyncio.wait_for(
                    get_pod_phase_counts(
                        auth_manager.prometheus_url,
                        auth_manager.prometheus_token
                    ),
                    timeout=15.0
                )
                
                results["pod_status_metrics"] = pod_status_results
                logger.info(f"Pod status metrics collected - Total pods: {pod_status_results.get('total_pods', 0)}")
                
            except asyncio.TimeoutError:
                logger.warning("Timeout collecting pod status metrics")
                results["pod_status_metrics"] = {
                    "error": "Timeout collecting pod status metrics",
                    "timeout_seconds": 15
                }
            except Exception as e:
                logger.error(f"Error collecting pod status metrics: {e}")
                results["pod_status_metrics"] = {"error": str(e)}
        
        # Collect custom metrics if specified
        if request.custom_metrics:
            try:
                logger.debug(f"Collecting {len(request.custom_metrics)} custom metrics...")
                
                collector = ovnBasicInfoCollector(
                    auth_manager.prometheus_url,
                    auth_manager.prometheus_token
                )
                
                # Add timeout to prevent hanging
                custom_results = await asyncio.wait_for(
                    collector.collect_max_values(request.custom_metrics),
                    timeout=20.0
                )
                
                results["custom_metrics"] = custom_results
                logger.info(f"Custom metrics collected: {list(request.custom_metrics.keys())}")
                
            except asyncio.TimeoutError:
                logger.warning("Timeout collecting custom metrics")
                results["custom_metrics"] = {
                    "error": "Timeout collecting custom metrics", 
                    "timeout_seconds": 20
                }
            except Exception as e:
                logger.error(f"Error collecting custom metrics: {e}")
                results["custom_metrics"] = {"error": str(e)}
        
        # Add collection summary metadata
        results["collection_metadata"] = {
            "parameters_applied": {
                "include_pod_status": request.include_pod_status,
                "include_db_metrics": request.include_db_metrics,
                "custom_metrics_count": len(request.custom_metrics) if request.custom_metrics else 0
            },
            "metrics_collected": len([k for k in results.keys() if k.endswith("_metrics")]),
            "collection_success": not any("error" in str(v) for v in results.values() if isinstance(v, dict))
        }
        
        # Log collection summary
        metrics_collected = []
        if request.include_db_metrics:
            metrics_collected.append("OVN databases")
        if request.include_pod_status:
            metrics_collected.append("pod status")  
        if request.custom_metrics:
            metrics_collected.append(f"{len(request.custom_metrics)} custom metrics")
        
        logger.info(f"Basic OVN info collection completed - Collected: {', '.join(metrics_collected)}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in basic OVN info collection: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_prometheus_basic_info"
        }

@app.tool(
    name="query_ovnk_pods_metrics",
    description="""Query OVN-Kubernetes pod resource usage metrics including CPU and memory consumption for ovnkube-controller, ovnkube-node, and related pods. This tool provides detailed resource utilization data for OVN-K components with intelligent pod filtering.

Parameters:
- pod_pattern (default: "ovnkube.*"): Regex pattern for pod names (e.g., "ovnkube.*", "multus.*", "ovnkube-node.*")
- container_pattern (default: ".*"): Regex pattern for container names (e.g., "ovnkube-controller", "kube-rbac-proxy.*")
- label_selector (default: ".*"): Regex pattern for pod label selectors
- namespace_pattern (default: "openshift-ovn-kubernetes"): Regex pattern for namespace filtering
- top_n (default: 10): Number of top resource consuming pods to return (1-50)
- duration (default: "1h"): Query duration for analysis (e.g., "5m", "1h", "1d")
- start_time (optional): Start time in ISO format for historical queries
- end_time (optional): End time in ISO format for historical queries

Returns detailed pod metrics including:
- Top resource consuming pods ranked by CPU and memory usage
- Per-pod CPU utilization percentages with min/avg/max statistics
- Memory usage in bytes with readable format (MB/GB)
- Pod metadata including node placement and resource limits
- Container-level resource breakdown within pods
- Performance trends over the specified duration

Use this tool to identify resource-intensive OVN-K pods, troubleshoot performance issues, or monitor resource consumption patterns."""
)
async def query_ovnk_pods_metrics(request: PODsRequest) -> Dict[str, Any]:
    """
    Query OVN-Kubernetes pod resource usage metrics including CPU, memory consumption,
    and performance characteristics for ovnkube-controller, ovnkube-node, and related pods.
    
    Provides detailed resource utilization data for OVN-K components.
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()

        # Add timeout to prevent hanging
        pods_duration_summary = await asyncio.wait_for(
            collect_ovn_duration_usage(prometheus_client, request.duration,auth_manager),
            timeout=45.0
        )
        return pods_duration_summary
    
    except asyncio.TimeoutError:
        return {"error": "Timeout collecting pod metrics", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error querying pod metrics: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}


@app.tool(
    name="query_ovnk_containers_metrics", 
    description="""Query detailed container-level metrics within OVN-Kubernetes pods including per-container CPU and memory usage, and resource limits utilization. This tool enables fine-grained analysis of individual container performance within OVN-K pods.

Parameters:
- pod_pattern (default: "ovnkube.*"): Regex pattern for pod names to analyze
- container_pattern (default: ".*"): Regex pattern for specific container names (e.g., "ovnkube-controller", "northd", "sbdb")
- label_selector (default: ".*"): Regex pattern for pod label selectors
- namespace_pattern (default: "openshift-ovn-kubernetes"): Target namespace pattern
- top_n (default: 10): Number of top containers to return based on resource usage
- duration (default: "1h"): Analysis time window (e.g., "5m", "1h", "1d") 
- start_time (optional): Historical query start time in ISO format
- end_time (optional): Historical query end time in ISO format

Returns container-level metrics including:
- Individual container CPU and memory usage within pods
- Resource limit utilization percentages
- Container restart counts and health status
- Performance comparison between containers in the same pod
- Resource allocation efficiency analysis
- Container-specific performance bottlenecks

Use this tool for deep-dive container analysis, identifying which specific containers within OVN-K pods are consuming the most resources, or troubleshooting container-level performance issues."""
)
async def query_ovnk_containers_metrics(request: PODsContainerRequest) -> Dict[str, Any]:
    """
    Query detailed container-level metrics within OVN-Kubernetes pods including
    per-container CPU, memory usage, and resource limits utilization.
    
    Enables fine-grained analysis of individual container performance within OVN-K pods.
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
        
        collector = PodsUsageCollector(prometheus_client,auth_manager)
        
        # Add timeout to prevent hanging
        result = await asyncio.wait_for(
            collector.collect_duration_usage(
                request.duration, 
                request.pod_pattern, 
                request.container_pattern, 
                request.namespace_pattern
            ),
            timeout=45.0
        )
        return result
    
    except asyncio.TimeoutError:
        return {"error": "Timeout collecting container metrics", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error querying container metrics: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}


@app.tool(
    name="query_ovnk_ovs_metrics",
    description="""Query Open vSwitch (OVS) performance metrics including CPU usage, memory consumption, flow table statistics, bridge statistics, and connection metrics. This tool is critical for monitoring OVS dataplane performance and flow processing efficiency.

Parameters:
- pod_pattern (default: "ovnkube.*"): Regex pattern for OVS-related pods
- container_pattern (default: ".*"): Container pattern for OVS components
- label_selector (default: ".*"): Label selector pattern
- namespace_pattern (default: "openshift-ovn-kubernetes"): Target namespace
- top_n (default: 10): Number of top results to return
- duration (default: "1h"): Analysis duration (e.g., "5m", "1h", "1d")
- start_time (optional): Historical query start time
- end_time (optional): Historical query end time

Returns comprehensive OVS metrics including:
- ovs-vswitchd CPU and memory usage per node
- ovsdb-server resource consumption 
- Dataplane flow counts (ovs_vswitchd_dp_flows_total)
- Bridge-specific flow statistics for br-int and br-ex
- OVS connection metrics (stream_open, rconn_overflow, rconn_discarded)
- Flow table efficiency and processing performance
- Per-node OVS component health status

Use this tool to monitor OVS dataplane performance, identify flow processing bottlenecks, troubleshoot network connectivity issues, or analyze OVS resource consumption patterns."""
)
async def query_ovnk_ovs_metrics(request: PODsRequest) -> Dict[str, Any]:
    """
    Query Open vSwitch (OVS) performance metrics including CPU usage, memory consumption,
    flow table statistics, bridge statistics, and connection metrics.
    
    Critical for monitoring OVS dataplane performance and flow processing efficiency.
    """
    global prometheus_client, auth_manager
    try:
        if not prometheus_client or not auth_manager:
            await initialize_components()
        
        collector = OVSUsageCollector(prometheus_client, auth_manager)
        
        # Add timeout to prevent hanging
        range_results = await asyncio.wait_for(
            collector.collect_all_ovs_metrics(request.duration),
            timeout=45.0
        )
        return range_results
    except asyncio.TimeoutError:
        return {"error": "Timeout collecting OVS metrics", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error querying OVS metrics: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}


@app.tool(
    name="query_multus_metrics",
    description="""Query Multus CNI metrics including network attachment processing times, resource usage, and network interface management performance. This tool is essential for monitoring secondary network interface provisioning and management in multi-network environments.

Parameters:
- pod_pattern (default: "multus.*"): Regex pattern for Multus-related pods (e.g., "multus.*", "network-metrics.*")
- container_pattern (default: ".*"): Container pattern within Multus pods  
- label_selector (default: ".*"): Label selector for filtering pods
- namespace_pattern (default: "openshift-multus"): Target namespace for Multus components
- top_n (default: 10): Number of top resource consuming pods to return
- duration (default: "1h"): Analysis time window (e.g., "5m", "1h", "1d")
- start_time (optional): Historical analysis start time in ISO format
- end_time (optional): Historical analysis end time in ISO format

Returns Multus-specific metrics including:
- Multus daemon CPU and memory usage per node
- Network attachment definition processing performance
- Secondary interface provisioning latency and success rates
- Multi-network pod resource consumption
- CNI plugin invocation metrics and error rates
- Network attachment controller performance

Use this tool to monitor multi-network performance, troubleshoot secondary network interface issues, analyze Multus resource consumption, or validate multi-network configuration efficiency."""
)
async def query_multus_metrics(request: PODsMultusRequest) -> Dict[str, Any]:
    """
    Query Multus CNI metrics including network attachment processing times,
    resource usage, and network interface management performance.
    
    Essential for monitoring secondary network interface provisioning and management.
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
                
        collector = PodsUsageCollector(prometheus_client,auth_manager)
        
        # Add timeout to prevent hanging
        result = await asyncio.wait_for(
            collector.collect_duration_usage(
                request.duration, 
                request.pod_pattern, 
                request.container_pattern, 
                request.namespace_pattern
            ),
            timeout=45.0
        )
        return result

    except asyncio.TimeoutError:
        return {"error": "Timeout collecting Multus metrics", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error querying Multus metrics: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="query_ovnk_sync_duration_seconds_metrics",
    description="""Query comprehensive OVN-Kubernetes synchronization duration metrics including controller ready times, node synchronization performance, resource reconciliation durations, and service processing rates. This tool provides critical visibility into OVN-K control plane performance, sync bottlenecks, and operational efficiency.

COMPREHENSIVE SYNC METRICS COLLECTED:

CONTROLLER READINESS METRICS:
- ovnkube_controller_ready_duration_seconds: Time required for OVN-K controllers to achieve ready state after startup, restart, or configuration changes
- Top 10 longest controller ready durations with pod-to-node mapping for infrastructure correlation
- Controller startup performance analysis identifying slow-starting controllers and potential configuration issues
- Ready state trend analysis for capacity planning and maintenance window optimization

NODE READINESS METRICS:
- ovnkube_node_ready_duration_seconds: Time required for OVN-K node agents to synchronize and reach operational state
- Top 10 longest node ready durations with node location mapping for geographic or infrastructure correlation
- Node-level sync performance assessment identifying problematic nodes requiring attention
- Network infrastructure readiness analysis for troubleshooting connectivity and configuration issues

CONTROLLER SYNC DURATION METRICS:
- ovnkube_controller_sync_duration_seconds: Resource-specific synchronization times including pods, services, network policies, and ingress resources
- Top 20 longest sync durations with detailed resource breakdown (resource type, name, namespace)
- Per-resource synchronization performance enabling identification of problematic resources or resource types
- Sync duration trend analysis with statistical aggregation (min/avg/max) for performance baseline establishment
- Resource-specific bottleneck identification for targeted optimization and troubleshooting

SERVICE PROCESSING RATE METRICS:
- ovnkube_controller_sync_service_total rate: Service synchronization throughput measured in operations per second
- Top 10 highest service processing rates indicating controller load and throughput capacity
- Service sync performance trends and processing efficiency analysis
- Throughput capacity assessment for service-heavy workload planning and optimization

STATISTICAL ANALYSIS AND AGGREGATION:
- Instant queries: Current snapshot with top performers and immediate performance indicators
- Duration queries: Statistical analysis with min/avg/max values, trend identification, and performance degradation detection
- Cross-component performance correlation enabling holistic control plane health assessment
- Resource utilization efficiency metrics and optimization opportunity identification

RESOURCE IDENTIFICATION AND MAPPING:
- Detailed resource information extraction including resource name, type (Pod, Service, NetworkPolicy), and namespace
- Pod-to-node mapping for correlating sync performance with underlying infrastructure
- Resource-specific performance patterns enabling targeted troubleshooting and optimization
- Cross-namespace sync performance analysis for multi-tenant environment monitoring

PERFORMANCE THRESHOLDS AND ALERTING:
- Automatic identification of unusually long sync durations requiring immediate attention
- Performance baseline establishment through historical trend analysis
- Sync duration threshold monitoring for proactive performance management
- Resource synchronization efficiency assessment with optimization recommendations

OPERATIONAL METADATA:
- Collection timestamp with UTC timezone for consistent time correlation across monitoring systems
- Query execution metadata including data point counts, metric collection success rates, and error handling
- Structured JSON output format compatible with monitoring dashboards, alerting systems, and automation tools
- Performance analysis summary with overall maximum values, averages, and performance outliers

Parameters:
- query_type (default: "instant"): Query method - "instant" for current sync metrics snapshot or "duration" for time-range trend analysis
- duration (optional): Required for duration queries - time window using Prometheus format ("5m", "30m", "1h", "6h", "24h")
- timestamp (optional): Specific time for instant historical queries in ISO format (YYYY-MM-DDTHH:MM:SSZ)
- end_time (optional): End time for duration queries in ISO format, defaults to current time

OPERATIONAL USE CASES:
- Performance troubleshooting: Identify sync bottlenecks causing application deployment delays or network policy application issues
- Capacity planning: Analyze controller and node sync performance trends for infrastructure scaling decisions
- Incident response: Correlate sync duration spikes with application performance issues or network connectivity problems
- Maintenance planning: Establish performance baselines and identify optimal maintenance windows based on sync load patterns
- Resource optimization: Identify resource types or namespaces with consistently high sync durations requiring configuration optimization
- Health monitoring: Continuous monitoring of OVN-K control plane performance for proactive issue detection
- Compliance reporting: Document sync performance metrics for SLA compliance and performance audit requirements
- Troubleshooting automation: Programmatic identification of sync performance issues for automated alerting and remediation

This tool is essential for maintaining optimal OVN-Kubernetes control plane performance and ensuring efficient network resource management in OpenShift clusters."""
)
async def query_ovnk_sync_duration_seconds_metrics(request: OVNKSyncDurationRequest) -> Dict[str, Any]:
    """
    Query comprehensive OVN-Kubernetes synchronization duration metrics including controller 
    ready times, node synchronization performance, resource reconciliation durations, 
    and service processing rates.
    
    Provides critical visibility into OVN-K control plane performance, sync bottlenecks, 
    and operational efficiency.
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
        
        # Initialize the OVN sync duration collector
        collector = OVNSyncDurationCollector(prometheus_client)
        
        # Determine query type and execute appropriate collection method
        if request.query_type == "instant":
            logger.info("Collecting instant OVN sync duration metrics...")
            
            # Add timeout to prevent hanging during instant collection
            result = await asyncio.wait_for(
                collector.collect_comprehensive_metrics(request.timestamp),
                timeout=30.0
            )
            
            # Log collection summary
            overall_summary = result.get('overall_summary', {})
            metrics_collected = overall_summary.get('metrics_collected', 0)
            total_data_points = overall_summary.get('total_data_points', 0)
            
            logger.info(f"Instant sync metrics collection completed - "
                       f"Metrics collected: {metrics_collected}, "
                       f"Total data points: {total_data_points}")
            
        elif request.query_type == "duration":
            if not request.duration:
                return {
                    "error": "Duration parameter is required for duration-based queries",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "suggestion": "Provide duration parameter (e.g., '5m', '1h', '6h') for time-range analysis"
                }
            
            logger.info(f"Collecting OVN sync duration metrics for duration: {request.duration}")
            
            # Add timeout to prevent hanging during duration collection
            result = await asyncio.wait_for(
                collector.collect_duration_metrics(request.duration, request.end_time),
                timeout=45.0
            )
            
            # Log collection summary
            overall_summary = result.get('overall_summary', {})
            metrics_collected = overall_summary.get('metrics_collected', 0)
            total_series = overall_summary.get('total_series', 0)
            
            logger.info(f"Duration sync metrics collection completed - "
                       f"Metrics collected: {metrics_collected}, "
                       f"Total time series: {total_series}")
            
        else:
            return {
                "error": f"Invalid query_type '{request.query_type}'. Must be 'instant' or 'duration'",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "valid_query_types": ["instant", "duration"]
            }
        
        return result
        
    except asyncio.TimeoutError:
        timeout_seconds = 30 if request.query_type == "instant" else 45
        return {
            "error": f"Timeout collecting OVN sync duration metrics - operation exceeded {timeout_seconds} seconds",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": timeout_seconds,
            "query_type": request.query_type,
            "suggestion": "Cluster may be experiencing performance issues or have extensive OVN resources. Try shorter duration or check cluster responsiveness."
        }
    except Exception as e:
        logger.error(f"Error querying OVN sync duration metrics: {e}")
        return {
            "error": str(e), 
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_ovnk_sync_duration_seconds_metrics",
            "query_type": request.query_type
        }

@app.tool(
    name="query_kube_api_metrics",
    description="""Query Kubernetes API server performance metrics including request rates, response times, error rates, and resource consumption. This tool is essential for monitoring cluster control plane health and performance, providing insights into API server latency and throughput.

Parameters:  
- duration (default: "5m"): Query duration using Prometheus time format (e.g., "5m", "1h", "1d")
- start_time (optional): Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ) for historical analysis
- end_time (optional): End time in ISO format (YYYY-MM-DDTHH:MM:SSZ) for historical analysis

Returns comprehensive API server metrics including:
- Read-only API call latency (LIST/GET operations) with p99 percentiles
- Mutating API call latency (POST/PUT/DELETE/PATCH operations) with p99 percentiles  
- Request rates per verb and resource type
- Error rates by HTTP status code and operation type
- Current inflight requests and queue depths
- etcd request duration metrics for backend storage performance
- Overall health scoring and performance alerts

Use this tool to diagnose API server performance issues, identify slow operations, monitor control plane health, or troubleshoot cluster responsiveness problems."""
)
async def query_kube_api_metrics(request: MetricsRequest) -> Dict[str, Any]:
    """
    Query Kubernetes API server performance metrics including request rates, 
    response times, error rates, and resource consumption.
    
    Essential for monitoring cluster control plane health and performance.
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
        
        kube_api_metrics = kubeAPICollector(prometheus_client)
        
        # Add timeout to prevent hanging
        result = await asyncio.wait_for(
            kube_api_metrics.get_metrics(request.duration, request.start_time, request.end_time),
            timeout=30.0
        )
        return result
    except asyncio.TimeoutError:
        return {"error": "Timeout querying Kube API metrics", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error querying Kube API metrics: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="analyze_ovnk_comprehensive_performance",
    description="""Perform comprehensive OVN-Kubernetes performance analysis across all components including pods, OVS, sync operations, and database metrics with automated risk assessment and actionable recommendations. This is the primary tool for complete OVNK performance evaluation and health monitoring.

Parameters:
- duration (optional): Analysis time window using Prometheus format (e.g., "5m", "1h", "1d", "7d"). If not provided, performs instant snapshot analysis using latest available data points
- generate_report (default: true): Whether to generate a human-readable summary report with key findings, risk assessment, and recommendations in addition to structured JSON data
- save_to_file (default: false): Whether to save complete analysis results to timestamped JSON file for documentation, audit trails, and historical reference
- include_risk_assessment (default: true): Whether to include automated risk assessment with threshold-based alerting, severity classification (critical/warning/normal), and prioritized recommendations
- performance_thresholds (optional): Custom performance thresholds for risk assessment that override system defaults. Use structure: {'cpu': {'warning': 70.0, 'critical': 90.0}, 'memory': {'warning': 1073741824, 'critical': 2147483648}, 'sync_duration': {'warning': 5.0, 'critical': 10.0}, 'db_size': {'warning': 104857600, 'critical': 524288000}}

Returns comprehensive performance analysis including:

CORE METRICS ANALYSIS:
- OVN database sizes (Northbound/Southbound) with growth trends and storage utilization alerts
- Pod resource utilization across ovnkube-controller, ovnkube-node, and multus components with CPU/memory statistics
- OVS dataplane performance including vswitchd and ovsdb-server resource consumption and flow processing efficiency
- Sync operation performance with controller and node sync duration analysis and resource reconciliation times
- Cluster-wide pod status distribution with health indicators and failed pod analysis

AUTOMATED RISK ASSESSMENT:
- Critical risks requiring immediate attention with specific component identification and recommended actions
- Warning-level risks for proactive monitoring and preventive maintenance planning
- Performance threshold violations with severity classification and historical context
- Component-specific health scoring and degradation indicators
- Risk categorization by type (cpu_usage, memory_usage, sync_duration, database_size) and affected components

INTELLIGENT RECOMMENDATIONS:
- Prioritized action items based on risk severity and operational impact
- Resource optimization suggestions including scaling recommendations and resource limit adjustments
- Performance tuning guidance specific to identified bottlenecks and inefficiencies
- Capacity planning insights based on current utilization trends and growth patterns
- Maintenance recommendations for database cleanup, configuration optimization, and monitoring improvements

PERFORMANCE SUMMARIES:
- Overall cluster health status (normal/warning/critical) with confidence scoring
- Component-level performance statistics with min/avg/max values and trend indicators
- Resource utilization efficiency analysis across different OVNK components
- Performance comparison between components for load balancing insights
- Key performance indicators (KPIs) with historical baselines when available

OPERATIONAL INSIGHTS:
- Executive summary suitable for management reporting and stakeholder communication
- Technical findings for engineering teams with specific metrics and thresholds
- Trend analysis identifying performance degradation or improvement over time
- Capacity planning data with utilization forecasts and scaling recommendations
- Compliance reporting for operational SLAs and performance requirements

Use this tool for:
- Regular operational health monitoring and proactive issue identification
- Performance troubleshooting and root cause analysis for degraded cluster performance
- Capacity planning and resource optimization initiatives
- Pre-maintenance health verification and post-deployment validation
- Executive reporting on infrastructure health and performance metrics
- Compliance auditing and operational SLA monitoring
- Performance baseline establishment for future comparison and trend analysis
- Automated alerting integration with threshold-based risk assessment

This tool consolidates data from multiple specialized collectors (basic info, OVS metrics, sync operations, pod usage) into a unified analysis with intelligent interpretation, making it the primary choice for comprehensive OVNK performance evaluation."""
)
async def analyze_ovnk_comprehensive_performance(request: ComprehensivePerformanceRequest) -> Dict[str, Any]:
    """
    Perform comprehensive OVN-Kubernetes performance analysis across all components including
    pods, OVS, sync operations, and database metrics with automated risk assessment.
    
    This is the primary tool for complete OVNK performance evaluation and health monitoring.
    """
    global prometheus_client, auth_manager
    try:
        if not prometheus_client or not auth_manager:
            await initialize_components()
        
        logger.info(f"Starting comprehensive OVNK performance analysis (duration: {request.duration})")
        
        # Initialize the performance analyzer
        analyzer = OVNKPerformanceAnalyzer(
            prometheus_url=auth_manager.prometheus_url,
            token=auth_manager.prometheus_token,
            auth_client=auth_manager
        )
        
        # Apply custom thresholds if provided
        if request.performance_thresholds:
            logger.info("Applying custom performance thresholds")
            analyzer.thresholds.update(request.performance_thresholds)
        
        # Perform comprehensive analysis with timeout
        analysis_result = await asyncio.wait_for(
            analyzer.analyze_comprehensive_performance(request.duration),
            timeout=120.0  # Extended timeout for comprehensive analysis
        )
        
        # Check if analysis completed successfully
        if 'error' in analysis_result:
            return {
                'error': f"Analysis failed: {analysis_result['error']}",
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        
        # Generate human-readable report if requested
        if request.generate_report:
            try:
                summary_report = analyzer.generate_summary_report(analysis_result)
                analysis_result['summary_report'] = summary_report
                logger.info("Summary report generated successfully")
            except Exception as report_error:
                logger.warning(f"Failed to generate summary report: {report_error}")
                analysis_result['summary_report_error'] = str(report_error)
        
        # Save to file if requested
        if request.save_to_file:
            try:
                saved_file = analyzer.save_analysis_to_file(analysis_result)
                analysis_result['saved_file'] = saved_file
                logger.info(f"Analysis saved to file: {saved_file}")
            except Exception as save_error:
                logger.warning(f"Failed to save analysis to file: {save_error}")
                analysis_result['save_file_error'] = str(save_error)
        
        # Add tool-specific metadata
        analysis_result['tool_metadata'] = {
            'tool_name': 'analyze_ovnk_comprehensive_performance',
            'parameters_used': {
                'duration': request.duration,
                'generate_report': request.generate_report,
                'save_to_file': request.save_to_file,
                'include_risk_assessment': request.include_risk_assessment,
                'custom_thresholds_applied': bool(request.performance_thresholds)
            },
            'analysis_completion_time': datetime.now(timezone.utc).isoformat()
        }
        
        # Log analysis summary
        risk_summary = analysis_result.get('risk_assessment', {})
        total_risks = risk_summary.get('total_risks', 0)
        critical_risks = risk_summary.get('critical_risks', 0)
        warning_risks = risk_summary.get('warning_risks', 0)
        overall_health = analysis_result.get('performance_summary', {}).get('overall_health', 'unknown')
        
        logger.info(f"Comprehensive analysis completed - Health: {overall_health}, "
                   f"Total risks: {total_risks} (Critical: {critical_risks}, Warning: {warning_risks})")
        
        return analysis_result
        
    except asyncio.TimeoutError:
        return {
            'error': 'Timeout during comprehensive performance analysis - this may indicate cluster performance issues',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'suggestion': 'Try reducing the analysis duration or checking cluster health'
        }
    except Exception as e:
        logger.error(f"Error in comprehensive performance analysis: {e}")
        return {
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

@app.tool(
    name="analyze_overall_ocp_performance",
    description="""Perform comprehensive overall OpenShift cluster performance analysis combining cluster health, node utilization, API server performance, OVNK networking components, OVS dataplane metrics, pod resource usage, and database performance. This tool provides a unified view of the entire cluster's health and performance status with automated health scoring, critical issue detection, and actionable recommendations.

Parameters:
- duration (default: "1h"): Analysis duration for metrics collection using Prometheus time format (e.g., '5m', '30m', '1h', '2h', '24h'). Longer durations provide more comprehensive trend analysis but take more time to process. Recommended: '5m' for quick checks, '1h' for standard analysis, '24h' for trend analysis.
- include_detailed_analysis (default: true): Whether to include detailed component-level analysis in the response. When True, provides comprehensive breakdown of each component's performance metrics, resource usage, and health status. Set to False for faster execution when only summary metrics are needed.
- focus_areas (optional): Optional list of specific focus areas to emphasize in analysis. Available areas: ['cluster', 'api', 'ovnk', 'nodes', 'databases', 'sync']. Examples: ['cluster', 'api'] for control plane focus, ['ovnk', 'sync'] for networking focus, ['nodes'] for compute focus. If not specified, analyzes all areas comprehensively.

Returns comprehensive cluster analysis including:
- Overall cluster health score (0-100) with weighted component scoring across cluster health (25%), API performance (25%), OVNK networking (30%), and node utilization (20%)
- Individual component health scores for cluster operators, API server latency, OVNK networking performance, and node resource utilization
- Critical issues requiring immediate attention with severity classification and impact assessment
- Warning issues for proactive monitoring and preventive maintenance planning
- Cluster general information including node counts, resource distributions, network policy counts, and operator status
- Node resource utilization analysis grouped by role (master/worker/infra) with CPU, memory, disk, and network metrics
- OVNK pod performance analysis including ovnkube-controller, ovnkube-node resource consumption and health status
- Container-level metrics within OVNK pods for granular resource usage analysis
- Multus CNI performance metrics for secondary network interface management
- OVS (Open vSwitch) dataplane performance including flow processing, bridge statistics, and connection metrics
- OVN synchronization duration analysis for control plane performance assessment
- Kubernetes API server latency analysis including read-only and mutating operation performance
- OVN database size monitoring for Northbound and Southbound databases
- Cluster-wide pod status distribution and health indicators
- Prioritized recommendations for performance optimization, capacity planning, and issue resolution
- Execution metrics showing analysis duration and component collection success rates

Use this tool for:
- Comprehensive cluster health monitoring and operational dashboards
- Performance troubleshooting and root cause analysis across all cluster components
- Capacity planning and resource optimization analysis
- Pre-maintenance cluster health verification and post-deployment validation
- Executive reporting on overall infrastructure performance and health status
- Automated alerting and monitoring system integration
- Performance baseline establishment and trend analysis over time
- Multi-component performance correlation analysis to identify systemic issues

The tool automatically weighs component importance, detects cross-component performance issues, and provides holistic cluster health assessment suitable for both technical teams and management reporting."""
)
async def analyze_overall_ocp_performance(request: OCPOVERALLPerformanceRequest) -> Dict[str, Any]:
    """
    Perform comprehensive overall OpenShift cluster performance analysis combining
    cluster health, node utilization, API server performance, OVNK networking components,
    OVS dataplane metrics, pod resource usage, and database performance.
    
    Provides unified view of entire cluster health and performance with automated scoring
    and actionable recommendations.
    """
    global auth_manager
    try:
        if not auth_manager:
            await initialize_components()
        
        logger.info(f"Starting overall OCP performance analysis for duration: {request.duration}")
        
        # Add timeout to prevent hanging
        result = await asyncio.wait_for(
            analyze_overall_performance_with_auth(
                duration=request.duration,
                include_detailed_analysis=request.include_detailed_analysis,
                focus_areas=request.focus_areas,
                auth_client=auth_manager
            ),
            timeout=120.0  # 2 minutes timeout for comprehensive analysis
        )
        
        logger.info("Overall OCP performance analysis completed successfully")
        return result
        
    except asyncio.TimeoutError:
        return {
            'error': 'Timeout during overall performance analysis - cluster may be experiencing significant issues',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'analysis_duration': request.duration,
            'timeout_seconds': 120
        }
    except Exception as e:
        logger.error(f"Error in overall performance analysis: {e}")
        return {
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'analysis_duration': request.duration
        }


async def shutdown_handler():
    """Handle graceful shutdown"""
    logger.info("Shutdown handler called")
    await cleanup_resources()
    logger.info("Shutdown complete")


async def main():
    """Main entry point with improved error handling and graceful shutdown"""
    try:
        # Initialize components
        await initialize_components()
        logger.info("MCP server starting...")
        
        # Create tasks for server and shutdown handler
        server_task = asyncio.create_task(
            app.run_async(
                transport="streamable-http",
                host="0.0.0.0",
                port=8000
            )
        )
        
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        
        # Wait for either server completion or shutdown signal
        done, pending = await asyncio.wait(
            [server_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel pending tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # If shutdown was triggered, clean up
        if shutdown_task in done:
            logger.info("Shutdown signal received, cleaning up...")
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass
            await shutdown_handler()
        
        # If server task completed (likely due to error), check for exceptions
        if server_task in done:
            try:
                await server_task
            except Exception as e:
                logger.error(f"Server task failed: {e}")
                raise
    
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down...")
        await shutdown_handler()
    except Exception as e:
        logger.error(f"Error starting server: {e}")
        await shutdown_handler()
        raise
    finally:
        logger.info("Main function exiting")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGracefully shutting down...")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)