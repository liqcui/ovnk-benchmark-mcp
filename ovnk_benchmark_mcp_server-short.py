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

# Add these imports to the existing imports section
from tools.ovnk_benchmark_openshift_cluster_stat import ClusterStatCollector
from analysis.ovnk_benchmark_analysis_cluster_stat import ClusterStatAnalyzer
from tools.ovnk_benchmark_prometheus_ovnk_basicinfo import ovnBasicInfoCollector, get_pod_phase_counts

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

from tools.ovnk_benchmark_openshift_general_info import OpenShiftGeneralInfo,collect_cluster_information,get_cluster_info_json
from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery
from tools.ovnk_benchmark_prometheus_kubeapi import KubeAPIMetrics
from tools.ovnk_benchmark_prometheus_pods_usage import PodsUsageCollector, collect_ovn_duration_usage
from tools.ovnk_benchmark_prometheus_ovnk_sync import OVNSyncDurationCollector
from tools.ovnk_benchmark_prometheus_ovnk_ovs import OVSUsageCollector
from tools.ovnk_benchmark_prometheus_nodes_usage import NodeUsageQuery
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

# Configure timezone
os.environ['TZ'] = 'UTC'

# Global shutdown event
shutdown_event = asyncio.Event()


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

class GeneralInfoRequest(BaseModel):
    """Request model for OpenShift general information queries"""
    namespace: Optional[str] = Field(
        default=None, 
        description="Specific namespace to query (optional, queries all if not specified)"
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
    """Request model for overall performance analysis"""
    duration: str = Field(
        default="1h",
        description="Analysis duration for metrics collection (e.g., '5m', '30m', '1h', '2h', '24h'). Longer durations provide more comprehensive trend analysis but take more time to process."
    )
    include_detailed_analysis: bool = Field(
        default=True,
        description="Whether to include detailed component analysis in the response. Set to False for faster summary-only results."
    )
    focus_areas: Optional[List[str]] = Field(
        default=None,
        description="Optional list of focus areas to emphasize in analysis: ['cluster', 'api', 'ovnk', 'nodes', 'databases', 'sync']. If not specified, analyzes all areas."
    )


# Initialize FastMCP app
app = FastMCP("ovnk-benchmark-mcp")

# Global components
auth_manager: Optional[OpenShiftAuth] = None
config: Optional[Config] = None
prometheus_client: Optional[PrometheusBaseQuery] = None
storage: Optional[PrometheusStorage] = None
cluster_collector: Optional[ClusterStatCollector] = None
cluster_analyzer: Optional[ClusterStatAnalyzer] = None


async def initialize_components():
    """Initialize global components with proper error handling"""
    global auth_manager, config, prometheus_client, storage, cluster_collector, cluster_analyzer
    
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
        
        # Initialize cluster status components
        cluster_collector = ClusterStatCollector(config.kubeconfig_path)
        await cluster_collector.initialize()
        cluster_analyzer = ClusterStatAnalyzer()
            
        logger.info("All components initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize components: {e}")
        raise

async def cleanup_resources():
    """Clean up global resources on shutdown"""
    global auth_manager, storage, cluster_collector
    
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
    
    try:
        if cluster_collector:
            await cluster_collector.cleanup()
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
    name="get_openshift_general_info",
    description="""Get comprehensive OpenShift cluster information including NetworkPolicy counts, AdminNetworkPolicy counts, EgressFirewall counts, node status, and resource utilization. This tool provides cluster-wide configuration and status information that is essential for understanding the overall state of the OpenShift cluster before analyzing performance metrics.

Parameters:
- namespace (optional): Specific namespace to query. If not provided, queries all namespaces for comprehensive cluster information.

Returns detailed cluster information including:
- NetworkPolicy counts and configurations
- AdminNetworkPolicy (ANP) counts and status  
- EgressFirewall policy counts
- Node status, roles, and capacity information
- Resource utilization across the cluster
- OVN-Kubernetes component status

Use this tool when you need to understand the overall cluster state, network policy configurations, or before performing detailed performance analysis."""
)
async def get_openshift_general_info(request: GeneralInfoRequest) -> Dict[str, Any]:
    """
    Get comprehensive OpenShift cluster information including NetworkPolicy, 
    AdminNetworkPolicy, and EgressFirewall counts, node status, and resource utilization.
    
    Returns cluster-wide configuration and status information.
    """
    try:
        general_info = OpenShiftGeneralInfo()
        await general_info.initialize()
        
        # Add timeout to prevent hanging
        cluster_info = await asyncio.wait_for(
            # general_info.collect_cluster_info(),
            collect_cluster_information(),
            timeout=30.0
        )
        cluster_info={"cluster_info": cluster_info}
        print("#-"*35)
        print("cluster_info is:\n",cluster_info,type(cluster_info))
        return cluster_info
    except asyncio.TimeoutError:
        return {"error": "Timeout collecting cluster information", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error collecting general info: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}


@app.tool(
    name="get_cluster_node_usage",
    description="""Get cluster node resource usage metrics including CPU utilization, memory consumption, disk usage, and network utilization across all worker and control plane nodes. This tool provides both instant snapshots and duration-based node performance metrics for capacity planning and performance analysis.

Parameters:
- duration (default: "5m"): Query duration using Prometheus time format (e.g., "5m", "1h", "1d", "7d")
- start_time (optional): Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ) for historical queries
- end_time (optional): End time in ISO format (YYYY-MM-DDTHH:MM:SSZ) for historical queries

Returns comprehensive node metrics including:
- CPU usage percentages per node with min/avg/max statistics
- Memory utilization including available, used, and cache/buffer metrics
- Disk I/O operations and space utilization
- Network bandwidth usage and packet rates
- Node health status and capacity information
- Role-based node grouping (master/worker nodes)

Use this tool to identify node-level bottlenecks, capacity issues, or uneven resource distribution across the cluster."""
)
async def get_cluster_node_usage(request: MetricsRequest) -> Dict[str, Any]:
    """
    Get cluster node resource usage metrics including CPU, memory, disk, and network utilization
    across all worker and control plane nodes.
    
    Provides both instant and duration-based node performance metrics.
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
            
        cluster_nodes_usage = NodeUsageQuery(prometheus_client)
        
        # Add timeout to prevent hanging
        cluster_info = await asyncio.wait_for(
            cluster_nodes_usage.query_node_usage(request.duration),
            timeout=30.0
        )
        return cluster_info
    except asyncio.TimeoutError:
        return {"error": "Timeout querying node usage metrics", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error querying node usage: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}


@app.tool(
    name="analyze_cluster_status",
    description="""Perform comprehensive OpenShift cluster status analysis including node health, cluster operator status, and machine config pool (MCP) status with detailed health scoring and actionable recommendations. This tool provides complete cluster health assessment for operational monitoring and troubleshooting.

Parameters:
- include_detailed_analysis (default: true): Whether to include detailed component analysis with health scoring, issue identification, and recommendations
- save_report (default: false): Whether to save the analysis report to files for documentation and reporting purposes
- output_format (default: "json"): Report output format - "json" for structured data, "text" for human-readable summary, or "both" for complete documentation
- output_dir (default: "."): Directory path where report files will be saved if save_report is enabled

Returns comprehensive cluster analysis including:
- Overall cluster health score (0-100) with status classification (excellent/good/fair/poor/critical)
- Node status analysis with ready/not-ready counts, role distribution, and node condition assessment
- Cluster operator health with availability, degraded, and progressing status for all operators
- Machine config pool status with update progress, degraded pools, and configuration sync status
- Critical issues requiring immediate attention with severity classification
- Warning issues for monitoring and preventive maintenance
- Prioritized recommendations for cluster optimization and issue resolution
- Executive summary with key findings and immediate action indicators
- Component-specific health scores and detailed status breakdowns

Use this tool for:
- Regular cluster health monitoring and operational dashboards
- Troubleshooting cluster issues and performance problems  
- Pre-maintenance cluster health verification
- Post-deployment validation and health checks
- Capacity planning and resource optimization analysis
- Compliance and audit reporting for cluster operations
- Root cause analysis for cluster performance degradation
- Executive reporting on infrastructure health status

The tool automatically detects critical infrastructure issues, provides actionable insights for cluster administrators, and generates comprehensive reports suitable for both technical teams and management reporting."""
)
async def analyze_cluster_status(request: ClusterStatusRequest) -> Dict[str, Any]:
    """
    Perform comprehensive/overal OpenShift cluster status analysis including node health,
    cluster operator status, and machine config pool status with detailed health scoring
    and actionable recommendations.
    
    Provides complete cluster health assessment for operational monitoring and troubleshooting.
    """
    global cluster_collector, cluster_analyzer
    try:
        if not cluster_collector or not cluster_analyzer:
            await initialize_components()
        
        logger.info("Starting comprehensive cluster status analysis...")
        
        # Collect cluster status data with timeout
        cluster_data = await asyncio.wait_for(
            cluster_collector.collect_comprehensive_cluster_status(),
            timeout=60.0
        )
        
        if 'error' in cluster_data:
            return {
                'error': f"Failed to collect cluster data: {cluster_data['error']}",
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        
        # Perform analysis if requested
        if request.include_detailed_analysis:
            analysis_result = cluster_analyzer.analyze_cluster_status(cluster_data)
            
            if 'error' in analysis_result:
                return {
                    'error': f"Analysis failed: {analysis_result['error']}",
                    'cluster_data': cluster_data,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
        else:
            # Return basic status without detailed analysis
            analysis_result = {
                'basic_analysis': True,
                'cluster_data': cluster_data,
                'overall_summary': cluster_data.get('overall_summary', {}),
                'analysis_timestamp': datetime.now(timezone.utc).isoformat()
            }
                
        # Add collection metadata
        analysis_result['collection_metadata'] = {
            'collection_timestamp': cluster_data.get('collection_timestamp'),
            'analysis_duration': request.include_detailed_analysis,
            'components_analyzed': list(cluster_data.keys()),
        }
        
        logger.info("Cluster status analysis completed successfully")
        logger.info(f"analysis_result is {type(analysis_result)}")
        return analysis_result
        
    except asyncio.TimeoutError:
        return {
            'error': 'Timeout during cluster status collection - cluster may be experiencing issues',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error in cluster status analysis: {e}")
        return {
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }


@app.tool(
    name="query_ovn_basic_info",
    description="""Query fundamental OVN (Open Virtual Network) database information and cluster-wide pod status metrics for baseline monitoring and health assessment. This tool provides essential OVN infrastructure metrics including database sizes and overall pod health status across the cluster.

Parameters:
- include_pod_status (default: true): Whether to include cluster-wide pod phase counts and status distribution (Running, Pending, Failed, Succeeded, Unknown)
- include_db_metrics (default: true): Whether to include OVN database size metrics for both Northbound and Southbound databases in bytes
- custom_metrics (optional): Dictionary of additional custom metric_name -> prometheus_query pairs for extended data collection beyond default OVN metrics

Returns comprehensive OVN basic information including:
- OVN Northbound database size with maximum values across all instances
- OVN Southbound database size with maximum values across all instances  
- Cluster-wide pod phase distribution (Running, Pending, Failed, Succeeded, Unknown pods)
- Total pod count across all namespaces for capacity monitoring
- Database size trends and storage utilization indicators
- Custom metric results if additional queries were specified
- Timestamp information for all collected metrics
- Error details for any failed metric collection attempts

Use this tool for:
- Baseline OVN database health monitoring and size tracking
- Overall cluster pod health assessment and capacity planning
- Initial cluster state verification before detailed performance analysis
- OVN database growth monitoring and storage capacity planning
- Quick cluster health status checks and operational dashboards
- Pre-troubleshooting baseline data collection
- Establishing baseline metrics for performance comparison

This tool provides foundational OVN infrastructure data that is essential for understanding cluster health before diving into detailed performance metrics."""
)
async def query_ovn_basic_info(request: OVNBasicInfoRequest) -> Dict[str, Any]:
    """
    Query fundamental OVN database information and cluster-wide pod status metrics
    for baseline monitoring and health assessment.
    
    Provides essential OVN infrastructure metrics including database sizes and pod health status.
    """
    global prometheus_client, auth_manager
    try:
        if not prometheus_client or not auth_manager:
            await initialize_components()
        
        result = {
            "collection_timestamp": datetime.now(timezone.utc).isoformat(),
            "metrics_collected": []
        }
        
        # Collect OVN database metrics if requested
        if request.include_db_metrics:
            try:
                logger.info("Collecting OVN database metrics...")
                collector = ovnBasicInfoCollector(
                    auth_manager.prometheus_url, 
                    auth_manager.prometheus_token
                )
                
                # Use custom metrics if provided, otherwise use defaults
                metrics_to_collect = request.custom_metrics
                
                db_metrics = await asyncio.wait_for(
                    collector.collect_max_values(metrics_to_collect),
                    timeout=30.0
                )
                
                result["ovn_database_metrics"] = db_metrics
                result["metrics_collected"].append("ovn_database_metrics")
                logger.info("OVN database metrics collected successfully")
                
            except asyncio.TimeoutError:
                result["ovn_database_metrics"] = {
                    "error": "Timeout collecting OVN database metrics"
                }
                logger.warning("Timeout collecting OVN database metrics")
            except Exception as e:
                result["ovn_database_metrics"] = {
                    "error": f"Failed to collect OVN database metrics: {str(e)}"
                }
                logger.error(f"Error collecting OVN database metrics: {e}")
        
        # Collect pod status information if requested
        if request.include_pod_status:
            try:
                logger.info("Collecting cluster-wide pod status...")
                
                pod_status = await asyncio.wait_for(
                    get_pod_phase_counts(
                        auth_manager.prometheus_url,
                        auth_manager.prometheus_token
                    ),
                    timeout=30.0
                )
                
                result["pod_status_metrics"] = pod_status
                result["metrics_collected"].append("pod_status_metrics")
                logger.info("Pod status metrics collected successfully")
                
            except asyncio.TimeoutError:
                result["pod_status_metrics"] = {
                    "error": "Timeout collecting pod status metrics"
                }
                logger.warning("Timeout collecting pod status metrics")
            except Exception as e:
                result["pod_status_metrics"] = {
                    "error": f"Failed to collect pod status metrics: {str(e)}"
                }
                logger.error(f"Error collecting pod status metrics: {e}")
        
        # Add summary information
        result["summary"] = {
            "total_metrics_types": len(result["metrics_collected"]),
            "collection_success": len([k for k in result.keys() if k.endswith("_metrics") and "error" not in result[k]]),
            "collection_errors": len([k for k in result.keys() if k.endswith("_metrics") and "error" in result[k]]),
        }
        
        logger.info(f"OVN basic info collection completed: {result['summary']}")
        return result
        
    except Exception as e:
        logger.error(f"Error in OVN basic info collection: {e}")
        return {
            "error": str(e), 
            "timestamp": datetime.now(timezone.utc).isoformat()
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
        
        kube_api_metrics = KubeAPIMetrics(prometheus_client)
        
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
async def query_ovnk_containers_metrics(request: PODsRequest) -> Dict[str, Any]:
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
async def query_multus_metrics(request: PODsRequest) -> Dict[str, Any]:
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
    description="""Query OVN-Kubernetes synchronization duration metrics including controller sync times, resource reconciliation durations, and performance bottlenecks. This tool is critical for identifying performance issues in OVN-K control plane operations.

Parameters:
- duration (default: "5m"): Analysis time window using Prometheus format (e.g., "5m", "1h", "1d", "7d")
- start_time (optional): Historical query start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
- end_time (optional): Historical query end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)

Returns comprehensive sync duration analysis including:
- ovnkube_controller_ready_duration_seconds metrics with top 10 longest durations
- ovnkube_node_ready_duration_seconds for node-level sync performance
- ovnkube_controller_sync_duration_seconds with resource-specific breakdown
- Per-resource synchronization times (pods, services, network policies)
- Resource type and namespace-specific sync performance
- Sync duration trends and statistical analysis (min/avg/max)
- Performance bottleneck identification by resource type

Use this tool to diagnose OVN-K control plane performance issues, identify slow resource synchronization, troubleshoot network policy application delays, or monitor overall controller responsiveness."""
)
async def query_ovnk_sync_duration_seconds_metrics(request: MetricsRequest) -> Dict[str, Any]:
    """
    Query OVN-Kubernetes synchronization duration metrics including controller sync times,
    resource reconciliation durations, and performance bottlenecks.
    
    Critical for identifying performance issues in OVN-K control plane operations.
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
        
        collector = OVNSyncDurationCollector(prometheus_client)
        
        # Add timeout to prevent hanging
        result = await asyncio.wait_for(
            collector.collect_sync_duration_seconds_metrics(request.duration),
            timeout=30.0
        )
        return result
    except asyncio.TimeoutError:
        return {"error": "Timeout collecting sync duration metrics", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error querying sync duration metrics: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

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