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
from analysis.ovnk_benchmark_analysis_cluster_stat import analyze_cluster_status, ClusterStatAnalyzer

from tools.ovnk_benchmark_prometheus_kubeapi import kubeAPICollector
from tools.ovnk_benchmark_prometheus_pods_usage import PodsUsageCollector, collect_ovn_duration_usage
from tools.ovnk_benchmark_prometheus_ovnk_sync import OVNSyncDurationCollector
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

class ClusterStatusRequest(BaseModel):
    """Request model for cluster status analysis operations"""
    include_detailed_analysis: bool = Field(
        default=True,
        description="Whether to include detailed component analysis with health scoring and recommendations"
    )

    output_format: str = Field(
        default="json",
        description="Output format for the report: 'json', 'text', or 'both'"
    )
  
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