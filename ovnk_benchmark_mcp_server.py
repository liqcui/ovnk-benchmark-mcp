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
from tools.ovnk_benchmark_prometheus_ovnk_latency import OVNLatencyCollector,get_ovn_sync_summary_json,collect_ovn_sync_metrics_duration
from tools.ovnk_benchmark_prometheus_ovnk_ovs import OVSUsageCollector
from tools.ovnk_benchmark_prometheus_nodes_usage import nodeUsageCollector
from ocauth.ovnk_benchmark_auth import OpenShiftAuth
from config.ovnk_benchmark_config import Config
from elt.ovnk_benchmark_elt_duckdb import PerformanceELT
from storage.ovnk_benchmark_storage_ovnk import PrometheusStorage


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

class GeneralClusterStatusRequest(BaseModel):
    """Request model for general cluster status check and health assessment"""
    include_detailed_analysis: bool = Field(
        default=True,
        description="Whether to include detailed component analysis with health scoring, performance metrics breakdown, recommendations, and alerts for each cluster component"
    )
    generate_summary_report: bool = Field(
        default=True, 
        description="Whether to generate a human-readable executive summary report with key findings, risk assessment, and prioritized action items in addition to structured analysis data"
    )
    health_check_scope: Optional[List[str]] = Field(
        default=None,
        description="Optional list to limit health check scope to specific areas: ['operators', 'nodes', 'networking', 'storage', 'mcps']. If not specified, performs comprehensive health assessment across all cluster components"
    )
    performance_baseline_comparison: bool = Field(
        default=False,
        description="Whether to include comparison against performance baselines and historical trends when available for identifying performance degradation or improvement patterns"
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

class OVNLatencyRequest(BaseModel):
    """Request model for OVN-Kubernetes latency metrics collection and analysis"""
    duration: Optional[str] = Field(
        default=None,
        description="Query duration for range queries using Prometheus time format (e.g., '5m', '30m', '1h', '2h', '24h'). When specified, collects metrics over the time range for trend analysis. When omitted, performs instant query for current state. Longer durations provide more comprehensive analysis but take more time to process. Recommended: '5m' for quick checks, '1h' for standard analysis, '24h' for trend analysis."
    )
    end_time: Optional[str] = Field(
        default=None,
        description="End time for range queries in ISO format (YYYY-MM-DDTHH:MM:SSZ). Only used with duration parameter. When not specified, uses current time as end point. Useful for analyzing historical time periods or specific incident timeframes."
    )
    include_controller_metrics: bool = Field(
        default=True,
        description="Whether to include controller-specific latency metrics including sync duration percentiles, pod annotation latency, service sync latency, and network programming duration. Controller metrics are essential for understanding control plane performance and policy application delays."
    )
    include_node_metrics: bool = Field(
        default=True,
        description="Whether to include node-specific latency metrics including CNI request durations for ADD/DEL operations, node sync duration percentiles, and pod creation latency. Node metrics are crucial for understanding data plane performance and pod startup delays."
    )
    include_pod_latency: bool = Field(
        default=True,
        description="Whether to include detailed pod lifecycle latency metrics such as first seen LSP creation, port binding duration, and annotation processing time. These metrics help identify bottlenecks in pod creation workflows and network setup delays."
    )
    include_service_latency: bool = Field(
        default=True,
        description="Whether to include service-related latency metrics including sync service latency and network programming duration for services. Service metrics are important for understanding load balancer configuration delays and service endpoint updates."
    )
    metric_categories: Optional[List[str]] = Field(
        default=None,
        description="Optional list to filter specific metric categories. Available categories: ['ready_duration', 'percentile_latency', 'pod_latency', 'cni_latency', 'service_latency', 'network_programming']. When specified, only returns metrics from selected categories. Use to reduce response size and focus on specific performance areas. Example: ['pod_latency', 'cni_latency'] for pod creation analysis."
    )
    top_n_results: int = Field(
        default=5,
        description="Number of top latency results to return per metric (1-50). Controls the size of detailed results for each metric type. Higher values provide more comprehensive view of performance distribution but increase response size. Recommended: 5-10 for quick analysis, 20+ for detailed troubleshooting."
    )
    include_statistics: bool = Field(
        default=True,
        description="Whether to include statistical analysis (max, avg, percentiles) for each metric category. Statistics provide summary insights and help identify performance trends and outliers across the cluster."
    )
    # save_to_file removed
    
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
        # save_to_file handling removed
        
        # Add collection metadata
        cluster_data['collection_metadata'] = {
            'tool_name': 'get_openshift_cluster_info',
            'parameters_applied': {
                'include_node_details': request.include_node_details,
                'include_resource_counts': request.include_resource_counts,
                'include_network_policies': request.include_network_policies,
                'include_operator_status': request.include_operator_status,
                'include_mcp_status': request.include_mcp_status,
                # 'save_to_file': request.save_to_file
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
    name="query_prometheus_basic_info",
    description="""Query comprehensive OVN-Kubernetes basic infrastructure information including OVN database metrics, cluster-wide pod status distribution, top alerts analysis, pod distribution across nodes, and network latency metrics from Prometheus. This tool provides essential baseline metrics and operational insights for OVN-K cluster health monitoring, capacity planning, and performance analysis.

COMPREHENSIVE METRICS COLLECTION:

OVN DATABASE METRICS (when include_db_metrics=True):
- OVN Northbound database size in bytes with maximum values across all instances for storage monitoring
- OVN Southbound database size in bytes with maximum values across all instances for capacity planning  
- Database size trends help monitor OVN control plane storage requirements and growth patterns
- Instance labels and metadata for identifying specific database locations and performance characteristics
- Critical for detecting database bloat, storage capacity planning, and performance optimization initiatives

CLUSTER-WIDE POD STATUS ANALYSIS (when include_pod_status=True):
- Complete pod phase distribution (Running, Pending, Failed, Succeeded, Unknown) across all namespaces
- Total pod count for cluster-wide capacity monitoring and workload density analysis
- Pod health indicators and failure pattern identification for operational monitoring
- Resource utilization insights through pod distribution patterns and scheduling effectiveness
- Essential for capacity planning, troubleshooting deployment issues, and cluster health assessment

TOP ALERTS MONITORING (when include_top_alerts=True):
- Top 6 active alerts ranked by severity and occurrence count for immediate operational attention
- Alert severity classification (critical, warning, info) with impact assessment and priority ranking
- Alert frequency analysis showing trending issues and recurring problems requiring systematic resolution
- Alert metadata including alert names, severity levels, occurrence counts, and timestamps
- Proactive issue identification enabling preventive maintenance and system reliability improvements

POD DISTRIBUTION ANALYSIS (when include_pod_distribution=True):
- Top 6 nodes by pod count showing workload distribution patterns and potential scheduling imbalances
- Node role identification (master, worker, infra) with workload characteristics and capacity utilization
- Per-node pod density analysis for load balancing insights and capacity optimization
- Node metadata including names, roles, pod counts, and timestamps for operational correlation
- Load balancing assessment and capacity planning guidance based on current distribution patterns

NETWORK LATENCY METRICS (when include_latency_metrics=True):
- API server request duration percentiles (99th percentile) for control plane performance monitoring
- etcd request duration percentiles for backend storage performance and responsiveness analysis
- OVN controller latency percentiles (95th percentile) for network control plane performance assessment
- Network RTT (Round Trip Time) percentiles for inter-component communication performance evaluation
- Custom latency metrics from optional metrics-latency.yml configuration file for specialized monitoring

OPERATIONAL METADATA AND INTEGRATION:
- Collection timestamp and timezone information (UTC) for data freshness verification and correlation
- Query execution success metrics and error reporting for individual component collection failures
- Structured JSON output format suitable for monitoring dashboard integration and automation systems
- Component-specific metadata including query types, measurement units, and result counts
- Data quality indicators and collection completeness reporting for operational confidence

FLEXIBLE CONFIGURATION OPTIONS:
- Custom metrics support through metric_name -> prometheus_query dictionary for specialized monitoring requirements
- Optional metrics file integration (metrics-latency.yml) for standardized latency monitoring configurations
- Selective component collection enabling focused analysis and reduced response times when needed
- Error isolation ensuring partial failures don't prevent successful collection of other components
- Extensible architecture supporting additional metric categories and specialized monitoring requirements

COMPREHENSIVE SUMMARY REPORTING:
- Unified JSON summary combining all collected metrics with correlation and cross-component analysis
- Statistical summaries and key performance indicators for executive reporting and dashboard integration
- Collection metadata including execution duration, success rates, and component health indicators
- Structured format enabling programmatic analysis, alerting integration, and automated reporting workflows
- Historical baseline establishment for trend analysis and performance degradation detection over time

Parameters:
- include_pod_status (default: true): Collect cluster-wide pod phase distribution and status information for workload monitoring and capacity planning
- include_db_metrics (default: true): Collect OVN Northbound and Southbound database size metrics for storage monitoring and growth analysis
- include_top_alerts (default: true): Collect top 6 active alerts by severity for immediate operational awareness and proactive issue identification
- include_pod_distribution (default: true): Collect top 6 nodes by pod count for load balancing analysis and capacity distribution assessment
- include_latency_metrics (default: true): Collect network and API latency percentiles for performance monitoring and bottleneck identification
- custom_metrics (optional): Dictionary of additional Prometheus queries in format {"metric_name": "prometheus_query"} for specialized monitoring
- metrics_file (optional): Path to metrics-latency.yml file containing standardized latency metric definitions for consistent monitoring
- comprehensive_collection (default: true): Collect all available metrics in single operation for complete infrastructure overview

SPECIALIZED USE CASES:
- Daily operational health monitoring combining infrastructure status, workload distribution, and performance metrics
- Capacity planning analysis using database growth, pod distribution, and resource utilization patterns
- Performance baseline establishment for SLA monitoring and trend analysis over time
- Alert correlation analysis combining active alerts with infrastructure metrics for root cause identification
- Load balancing optimization using pod distribution and node capacity analysis
- Network performance monitoring through latency metrics and OVN database responsiveness
- Executive reporting with comprehensive infrastructure health and performance summaries

INTEGRATION AND AUTOMATION:
- Monitoring dashboard data source for unified OVN-K infrastructure visibility
- Automated alerting system integration with threshold-based risk assessment capabilities
- Capacity planning automation using growth trends and utilization forecasting
- Performance regression detection through historical baseline comparison and trend analysis
- Operational runbook integration providing context for troubleshooting and incident response

The tool provides comprehensive OVN-K infrastructure baseline metrics essential for operational monitoring, capacity planning, performance analysis, and proactive issue identification, making it ideal for both real-time operations and strategic infrastructure management initiatives."""
)
async def query_prometheus_basic_info(request: PrometheusBasicInfoRequest) -> Dict[str, Any]:
    """
    Query comprehensive OVN-Kubernetes basic infrastructure information including OVN database metrics,
    cluster-wide pod status distribution, top alerts analysis, pod distribution across nodes,
    and network latency metrics from Prometheus.
    
    Provides essential baseline metrics and operational insights for OVN-K cluster health monitoring,
    capacity planning, and performance analysis.
    """
    global prometheus_client, auth_manager
    
    try:
        if not prometheus_client or not auth_manager:
            await initialize_components()
        
        logger.info("Starting comprehensive basic OVN infrastructure information collection...")
        
        # Initialize the enhanced OVN basic info collector
        collector = ovnBasicInfoCollector(
            auth_manager.prometheus_url, 
            auth_manager.prometheus_token
        )
        
        # Collect comprehensive summary with all metrics
        logger.debug("Collecting comprehensive metrics summary...")
        
        # Add timeout to prevent hanging during comprehensive collection
        comprehensive_summary = await asyncio.wait_for(
            collector.collect_comprehensive_summary(),
            timeout=60.0  # Extended timeout for comprehensive collection
        )
        
        # Apply parameter-based filtering
        results = {
            "collection_timestamp": comprehensive_summary.get("collection_timestamp"),
            "prometheus_url": comprehensive_summary.get("prometheus_url"),
            "tool_name": "query_prometheus_basic_info_enhanced"
        }
        
        # Include metrics based on request parameters
        if request.include_db_metrics:
            results["ovn_database_metrics"] = comprehensive_summary.get("metrics", {}).get("ovn_database", {})
            logger.info("OVN database metrics included in response")
        
        if request.include_pod_status:
            results["pod_status_metrics"] = comprehensive_summary.get("metrics", {}).get("pod_status", {})
            logger.info("Pod status metrics included in response")
        
        # Always include additional comprehensive metrics from the enhanced collector
        results["alerts_summary"] = comprehensive_summary.get("metrics", {}).get("alerts", {})
        results["pod_distribution"] = comprehensive_summary.get("metrics", {}).get("pod_distribution", {})
        results["latency_metrics"] = comprehensive_summary.get("metrics", {}).get("latency", {})
        
        # Handle custom metrics if specified
        if request.custom_metrics:
            try:
                logger.debug(f"Collecting {len(request.custom_metrics)} custom metrics...")
                
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
        
        # Add comprehensive collection summary
        original_summary = comprehensive_summary.get("summary", {})
        results["collection_summary"] = {
            "total_metric_categories": original_summary.get("total_metric_categories", 5),
            "successful_collections": original_summary.get("successful_collections", 0),
            "failed_collections": original_summary.get("failed_collections", 0),
            "parameters_applied": {
                "include_pod_status": request.include_pod_status,
                "include_db_metrics": request.include_db_metrics,
                "custom_metrics_count": len(request.custom_metrics) if request.custom_metrics else 0
            },
            "collection_method": "comprehensive_enhanced"
        }
        
        # Log collection summary with enhanced metrics
        alerts_count = len(results.get("alerts_summary", {}).get("top_alerts", []))
        top_nodes_count = len(results.get("pod_distribution", {}).get("top_nodes", []))
        latency_metrics_count = len(results.get("latency_metrics", {}).get("metrics", {}))
        
        logger.info(f"Enhanced basic info collection completed - "
                   f"Alerts: {alerts_count}, Top nodes: {top_nodes_count}, "
                   f"Latency metrics: {latency_metrics_count}")
        
        return results
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting comprehensive basic information - cluster may be experiencing issues",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 60,
            "suggestion": "Try limiting the scope with selective parameters or check cluster responsiveness"
        }
    except Exception as e:
        logger.error(f"Error in enhanced basic info collection: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_prometheus_basic_info_enhanced"
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
    name="get_ovnk_latency_metrics",
    description="""Collect and analyze comprehensive OVN-Kubernetes latency metrics including controller sync operations, CNI request processing, pod lifecycle latencies, and service configuration delays. This tool provides detailed insights into network control plane and data plane performance, enabling identification of bottlenecks, performance degradation, and optimization opportunities.

This tool leverages the OVNLatencyCollector to gather structured latency metrics from OVN-Kubernetes components across the entire cluster, providing both instant snapshots and historical trend analysis.

CONTROLLER PERFORMANCE METRICS (include_controller_metrics=true):
- Controller sync duration 95th percentile: Measures control plane responsiveness for policy updates and configuration changes
- Pod annotation latency 99th percentile: Time from pod creation to OVN annotation completion, critical for pod startup performance
- Service sync latency and 99th percentile: Service configuration and load balancer setup delays affecting application connectivity
- Network programming duration for pods and services: Time to apply network configurations, impacting policy enforcement speed
- Controller ready duration: Bootstrap and restart times affecting cluster recovery

NODE PERFORMANCE METRICS (include_node_metrics=true):
- Node sync duration 95th percentile: Node-level configuration synchronization performance and health
- CNI ADD request latency 99th percentile: Pod network interface creation time, directly impacting pod startup delays
- CNI DEL request latency 99th percentile: Network cleanup performance during pod deletion
- Node ready duration: Node bootstrap times affecting cluster scaling and recovery operations

POD LIFECYCLE LATENCY ANALYSIS (include_pod_latency=true):
- Pod creation first seen LSP latency 99th percentile: Time from pod detection to logical switch port creation in OVN
- Pod creation port binding latency 99th percentile: Duration for OVN port binding operations affecting network connectivity
- End-to-end pod networking setup time: Complete workflow from pod scheduling to network readiness
- Resource correlation: Pod names, nodes, and resource types for targeted troubleshooting

SERVICE NETWORKING PERFORMANCE (include_service_latency=true):
- Service synchronization latency trends and percentiles
- Network programming duration for service endpoint updates
- Load balancer configuration delays impacting traffic routing
- Service-to-service communication setup time

COMPREHENSIVE STATISTICAL ANALYSIS (include_statistics=true):
- Performance percentiles (95th, 99th) for all metric categories to identify outliers and trends
- Maximum and average latency values with human-readable time formats (ms/s/min/h)
- Data point counts and distribution analysis for statistical significance
- Top N worst-performing operations with detailed context (pod names, nodes, resources)
- Cross-component performance correlation and bottleneck identification

OPERATIONAL INSIGHTS AND TROUBLESHOOTING:
- Component health assessment through latency thresholds and performance trends
- Resource correlation (pod names, node assignments, namespaces) for targeted investigation  
- Performance baseline establishment for capacity planning and SLA monitoring
- Incident correlation with specific time periods using duration and end_time parameters
- Historical trend analysis for performance degradation detection and optimization planning

QUERY CAPABILITIES:
- Instant queries: Current performance snapshot for real-time monitoring and alerts
- Duration queries: Historical analysis over specified time periods (5m to 24h+) for trend identification
- Flexible time range selection: Custom start/end times for incident investigation
- Category filtering: Focus on specific performance areas (pod, service, CNI, controller) for targeted analysis
- Configurable result limits: Balance between comprehensive data and response performance

METRICS CATEGORIZATION:
- ready_duration: Component startup and bootstrap performance metrics
- percentile_latency: Statistical performance analysis with 95th/99th percentiles  
- pod_latency: Complete pod creation workflow timing and network setup delays
- cni_latency: Container Network Interface operation performance for pod networking
- service_latency: Service configuration and load balancer setup performance
- network_programming: OVN policy application and configuration enforcement timing

Parameters:
- duration (optional): Prometheus time format for range queries ('5m', '1h', '24h') - omit for instant query
- end_time (optional): ISO timestamp for historical analysis - requires duration parameter
- include_controller_metrics (default: true): Controller sync operations, policy application, service configuration
- include_node_metrics (default: true): Node synchronization, CNI operations, infrastructure performance  
- include_pod_latency (default: true): Pod lifecycle timing, network setup delays, annotation processing
- include_service_latency (default: true): Service synchronization, load balancer configuration, endpoint updates
- metric_categories (optional): Filter specific categories ['pod_latency', 'cni_latency', 'service_latency', etc.]
- top_n_results (default: 10): Number of detailed results per metric (1-50) for performance analysis depth
- include_statistics (default: true): Statistical analysis, percentiles, and performance summaries
- save_to_file (default: false): Export results to JSON for documentation and further analysis

Use this tool for:
- Performance monitoring and SLA compliance verification for network operations
- Bottleneck identification during pod creation, service updates, or policy application
- Capacity planning through historical trend analysis and performance baseline establishment
- Incident investigation with specific time range analysis and component correlation
- Optimization planning by identifying highest-impact latency improvements
- Operational health assessment of OVN-Kubernetes control and data plane components
- Development and testing performance validation for network policy and configuration changes
- Executive reporting on network infrastructure performance and reliability metrics

The tool provides comprehensive latency analysis suitable for both real-time operations monitoring and strategic performance optimization, enabling data-driven decisions about network infrastructure scaling, optimization, and troubleshooting priorities."""
)
async def get_ovnk_latency_metrics(request: OVNLatencyRequest) -> Dict[str, Any]:
    """
    Collect and analyze comprehensive OVN-Kubernetes latency metrics including controller sync operations,
    CNI request processing, pod lifecycle latencies, and service configuration delays.
    
    Provides detailed insights into network control plane and data plane performance for
    bottleneck identification and optimization opportunities.
    """
    try:
        global auth_manager, prometheus_client
        
        # Ensure components are initialized
        if not auth_manager or not prometheus_client:
            await initialize_components()
        
        logger.info(f"Starting OVN latency metrics collection - "
                   f"Duration: {request.duration or 'instant'}, "
                   f"Categories: {request.metric_categories or 'all'}")
        
        # Initialize the OVN latency collector
        collector = OVNLatencyCollector(prometheus_client)
        
        # Determine timeout based on query type and duration
        timeout = 300.0  # Default for instant queries
        if request.duration:
            # Increase timeout for longer duration queries
            duration_seconds = _parse_duration_to_seconds(request.duration)
            if duration_seconds > 3600:  # > 1 hour
                timeout = 1200.0
            elif duration_seconds > 300:  # > 5 minutes
                timeout = 600.0
            else:
                timeout = 300.0
        
        # Collect comprehensive metrics with timeout
        metrics_data = await asyncio.wait_for(
            collector.collect_comprehensive_enhanced_metrics(
                time=None,
                duration=request.duration,
                end_time=request.end_time,
                categories=request.metric_categories
            ),
            timeout=timeout
        )
        
        # Apply category filtering if specified
        if request.metric_categories:
            filtered_data = {
                'collection_timestamp': metrics_data.get('collection_timestamp'),
                'timezone': metrics_data.get('timezone'),
                'collection_type': metrics_data.get('collection_type'),
                'query_type': metrics_data.get('query_type'),
                'query_parameters': metrics_data.get('query_parameters', {}),
                'overall_summary': {}  # Will be regenerated
            }
            
            # Category mapping to metric sections
            category_mapping = {
                'ready_duration': 'ready_duration_metrics',
                'percentile_latency': 'percentile_latency_metrics', 
                'pod_latency': 'pod_latency_metrics',
                'cni_latency': 'cni_latency_metrics',
                'service_latency': 'service_latency_metrics',
                'network_programming': 'network_programming_metrics'
            }
            
            # Filter based on requested categories
            for category in request.metric_categories:
                if category in category_mapping:
                    section_key = category_mapping[category]
                    if section_key in metrics_data:
                        filtered_data[section_key] = metrics_data[section_key]
            
            # Regenerate summary for filtered data
            collector._generate_enhanced_summary(filtered_data)
            metrics_data = filtered_data
            
            logger.info(f"Applied category filtering: {request.metric_categories}")
        
        # Apply component-specific filtering
        if not request.include_controller_metrics:
            # Remove controller-specific metrics
            sections_to_filter = ['percentile_latency_metrics', 'pod_latency_metrics', 
                                'service_latency_metrics', 'network_programming_metrics']
            for section in sections_to_filter:
                if section in metrics_data:
                    # Filter out controller metrics within each section
                    filtered_section = {k: v for k, v in metrics_data[section].items() 
                                      if v.get('component') != 'controller'}
                    metrics_data[section] = filtered_section
            
            logger.info("Controller metrics filtered out")
        
        if not request.include_node_metrics:
            # Remove node-specific metrics
            sections_to_filter = ['ready_duration_metrics', 'percentile_latency_metrics', 'cni_latency_metrics']
            for section in sections_to_filter:
                if section in metrics_data:
                    # Filter out node metrics within each section
                    filtered_section = {k: v for k, v in metrics_data[section].items() 
                                      if v.get('component') != 'node'}
                    metrics_data[section] = filtered_section
            
            logger.info("Node metrics filtered out")
        
        # Apply top N filtering to detailed results
        if request.top_n_results != 10:  # Only modify if different from default
            _apply_top_n_filtering(metrics_data, request.top_n_results)
            logger.info(f"Applied top {request.top_n_results} filtering to detailed results")
        
        # Remove statistics if not requested
        if not request.include_statistics:
            _remove_statistics_from_results(metrics_data)
            logger.info("Statistics removed from results")
        
        
        # Add collection metadata
        metrics_data['collection_metadata'] = {
            'tool_name': 'get_ovnk_latency_metrics',
            'parameters_applied': {
                'duration': request.duration,
                'end_time': request.end_time,
                'include_controller_metrics': request.include_controller_metrics,
                'include_node_metrics': request.include_node_metrics,
                'include_pod_latency': request.include_pod_latency,
                'include_service_latency': request.include_service_latency,
                'metric_categories': request.metric_categories,
                'top_n_results': request.top_n_results,
                'include_statistics': request.include_statistics,
                # 'save_to_file': request.save_to_file
            },
            'collection_timeout_seconds': timeout,
            'query_type': request.duration and 'duration' or 'instant'
        }
        
        # Log collection summary
        summary = metrics_data.get('overall_summary', {})
        total_metrics = summary.get('total_metrics_collected', 0)
        successful_metrics = summary.get('successful_metrics', 0)
        failed_metrics = summary.get('failed_metrics', 0)
        
        top_latency = None
        if summary.get('overall_max_latency'):
            max_latency = summary['overall_max_latency']
            top_latency = f"{max_latency['readable']['value']}{max_latency['readable']['unit']} ({max_latency['metric']})"
        
        logger.info(f"OVN latency collection completed - "
                   f"Metrics: {successful_metrics}/{total_metrics} successful, "
                   f"Failed: {failed_metrics}, "
                   f"Top latency: {top_latency or 'N/A'}")
        
        return metrics_data
        
    except asyncio.TimeoutError:
        return {
            "error": f"Timeout collecting OVN latency metrics after {timeout} seconds - cluster may be under heavy load or experiencing performance issues",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": timeout,
            "suggestion": "Try reducing the duration range, limiting categories, or checking cluster performance"
        }
    except Exception as e:
        logger.error(f"Error collecting OVN latency metrics: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "get_ovnk_latency_metrics"
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
    name="perform_general_cluster_status_check",
    description="""Perform comprehensive general cluster status check and health assessment including cluster operator status, node health, Machine Config Pool (MCP) status, networking components health, and overall cluster operational readiness. This tool provides essential cluster health monitoring and operational status verification distinct from performance analysis tools.

CORE HEALTH ASSESSMENTS:

CLUSTER OPERATOR STATUS:
- Complete inventory of all cluster operators with availability and health status
- Identification of unavailable, degraded, or progressing operators requiring attention
- Operator dependency analysis and impact assessment for failed or degraded operators
- Version consistency checks and upgrade status monitoring across all operators
- Critical operator failure detection with immediate remediation recommendations

NODE HEALTH EVALUATION:
- Node readiness status (Ready/NotReady/SchedulingDisabled) across all cluster nodes
- Node role distribution analysis (master/worker/infra) with capacity assessment
- Node resource capacity and allocatable resources for capacity planning
- Hardware health indicators including CPU, memory, and disk capacity status
- Node condition analysis (DiskPressure, MemoryPressure, PIDPressure, NetworkUnavailable)
- Node age analysis and lifecycle management recommendations

MACHINE CONFIG POOL (MCP) STATUS:
- MCP health status (Updated/Updating/Degraded) for all node pools
- Configuration synchronization progress and update completion status
- Failed update identification with rollback recommendations when applicable
- Node configuration consistency verification across pools
- Update queue analysis and maintenance window optimization recommendations

NETWORKING COMPONENT HEALTH:
- OVN-Kubernetes component operational status and readiness verification
- Network policy enforcement health and configuration validation
- DNS resolution functionality and service discovery health checks
- Ingress controller status and traffic routing operational verification
- CNI plugin health and network interface provisioning capability assessment

STORAGE SYSTEM HEALTH:
- Persistent volume provisioning capability and storage class availability
- Storage operator health and CSI driver operational status verification
- Volume attachment health and mounting success rate analysis
- Storage capacity monitoring and expansion capability assessment

OVERALL CLUSTER READINESS:
- Cluster API responsiveness and control plane health verification
- etcd cluster health and data consistency validation
- Authentication and authorization system operational status
- Resource quota utilization and namespace-level health assessment
- Certificate validity and expiration monitoring for security components

AUTOMATED HEALTH SCORING:
- Component-level health scores (0-100) with weighted importance for overall impact
- Overall cluster health score combining all component assessments
- Risk categorization (Critical/High/Medium/Low) with severity impact analysis
- Health trend analysis when baseline data is available for comparison
- Predictive health alerts for components showing degradation patterns

ACTIONABLE RECOMMENDATIONS:
- Immediate action items for critical health issues requiring urgent attention
- Preventive maintenance recommendations for warning-level health issues
- Capacity planning guidance based on current utilization and growth trends
- Configuration optimization suggestions for improved stability and performance
- Maintenance window planning with priority-based remediation schedules

EXECUTIVE REPORTING:
- Executive summary suitable for management stakeholders and operational reporting
- Key performance indicators (KPIs) for cluster operational health and readiness
- Risk assessment summary with business impact analysis and mitigation strategies
- Compliance status reporting for operational SLAs and availability requirements
- Historical health trend summary when baseline comparison data is available

OPERATIONAL METADATA:
- Health check execution timestamp and data freshness verification
- Assessment scope and coverage details for audit and compliance purposes
- Component response times and data collection success rates
- Integration compatibility for monitoring dashboards and alerting systems
- Structured JSON output format for automation and programmatic consumption

Parameters:
- include_detailed_analysis (default: true): Include comprehensive component-level health analysis with detailed metrics, scoring, and specific recommendations for each cluster component
- generate_summary_report (default: true): Generate executive summary report with key findings, prioritized action items, and business impact assessment for stakeholder communication
- health_check_scope (optional): Limit assessment to specific areas (['operators', 'nodes', 'networking', 'storage', 'mcps']) for focused health checks or faster execution
- performance_baseline_comparison (default: false): Include historical baseline comparison when available to identify health trends, degradation patterns, or improvement verification

DISTINCTION FROM PERFORMANCE TOOLS:
- Focus on operational health and readiness rather than detailed performance metrics
- Emphasizes cluster component availability and functional status over resource utilization
- Provides immediate operational actionability rather than deep performance analysis
- Suitable for daily health monitoring and operational readiness verification
- Complements performance analysis tools with foundational health assessment

Use this tool for:
- Daily operational health monitoring and cluster readiness verification
- Pre-maintenance health assessment and go/no-go decision support
- Post-deployment health validation and operational readiness confirmation
- Incident response initial assessment and scope determination for cluster issues
- Compliance reporting and operational SLA monitoring for availability requirements
- Change management health verification before and after configuration changes
- Executive reporting on infrastructure health and operational status
- Automated health monitoring integration with alerting and dashboard systems

This tool provides essential cluster health visibility focusing on operational readiness and component availability, making it ideal for operations teams and daily health monitoring workflows."""
)
async def perform_general_cluster_status_check(request: GeneralClusterStatusRequest) -> Dict[str, Any]:
    """
    Perform comprehensive general cluster status check and health assessment including
    cluster operator status, node health, MCP status, networking components health,
    and overall cluster operational readiness.
    
    This tool provides essential cluster health monitoring and operational status verification
    distinct from performance analysis tools.
    """
    global auth_manager
    try:
        if not auth_manager:
            await initialize_components()
        
        logger.info("Starting general cluster status check and health assessment...")
        
        # Collect cluster information
        cluster_data = await collect_cluster_information()
        
        analyzer = ClusterStatAnalyzer()
        analysis_result = analyzer.analyze_metrics_data(cluster_data)
        
        # Generate and display report
        report = analyzer.generate_report(analysis_result)
        
        # Return the analysis result as JSON/dict
        return analysis_result
        
    except asyncio.TimeoutError:
        return {
            'error': 'Timeout during cluster status check - cluster may be experiencing significant issues',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'timeout_seconds': 90,
            'suggestion': 'Check cluster API responsiveness and component availability',
            'tool_name': 'perform_general_cluster_status_check'
        }
    except Exception as e:
        logger.error(f"Error in general cluster status check: {e}")
        return {
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'tool_name': 'perform_general_cluster_status_check'
        }


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

# Add these helper functions at the end of the file, before main()

def _parse_duration_to_seconds(duration: str) -> int:
    """Parse Prometheus duration string to seconds"""
    import re
    
    # Handle common duration formats
    match = re.match(r'^(\d+)([smhd])$', duration.lower())
    if not match:
        return 300  # Default 5 minutes
    
    value = int(match.group(1))
    unit = match.group(2)
    
    multipliers = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    return value * multipliers.get(unit, 60)


def _apply_top_n_filtering(metrics_data: Dict[str, Any], top_n: int) -> None:
    """Apply top N filtering to statistics sections in metrics data"""
    for section_key, section_data in metrics_data.items():
        if isinstance(section_data, dict) and section_key.endswith('_metrics'):
            for metric_key, metric_data in section_data.items():
                if isinstance(metric_data, dict) and 'statistics' in metric_data:
                    stats = metric_data['statistics']
                    if 'top_6' in stats:
                        # Adjust the top results count
                        current_results = stats.get('top_6', [])
                        stats[f'top_{min(top_n, len(current_results))}'] = current_results[:top_n]
                        # Keep original key for compatibility but limit results
                        stats['top_6'] = current_results[:min(6, top_n)]


def _remove_statistics_from_results(metrics_data: Dict[str, Any]) -> None:
    """Remove statistical analysis from metrics data"""
    for section_key, section_data in metrics_data.items():
        if isinstance(section_data, dict) and section_key.endswith('_metrics'):
            for metric_key, metric_data in section_data.items():
                if isinstance(metric_data, dict) and 'statistics' in metric_data:
                    # Keep only basic info, remove detailed statistics
                    basic_stats = {
                        'count': metric_data['statistics'].get('count', 0)
                    }
                    metric_data['statistics'] = basic_stats


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