#!/usr/bin/env python3
"""
OpenShift OVN-Kubernetes Benchmark MCP Server
Main server entry point using FastMCP with streamable-http transport
Updated with comprehensive analysis tools based on unified performance analyzer
"""

import asyncio
import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict

import fastmcp
from fastmcp.server import FastMCP

from tools.ovnk_benchmark_openshift_general_info import OpenShiftGeneralInfo
from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery
from tools.ovnk_benchmark_prometheus_kubeapi import KubeAPIMetrics
from tools.ovnk_benchmark_prometheus_pods_usage import PodsUsageCollector, collect_ovn_duration_usage
from tools.ovnk_benchmark_prometheus_ovnk_sync import OVNSyncDurationCollector
from tools.ovnk_benchmark_prometheus_ovnk_ovs import OVSUsageCollector
from tools.ovnk_benchmark_prometheus_nodes_usage import NodeUsageQuery
from ocauth.ovnk_benchmark_auth import OpenShiftAuth
from config.ovnk_benchmark_config import Config
from elt.ovnk_benchmark_performance_elt import PerformanceELT
from storage.ovnk_benchmark_storage_ovnk import PrometheusStorage

# Import analysis modules
try:
    from analysis.ovnk_benchmark_performance_unified import UnifiedPerformanceAnalyzer
    UNIFIED_ANALYZER_AVAILABLE = True
except ImportError:
    UNIFIED_ANALYZER_AVAILABLE = False
    print("Warning: Unified analyzer not available")

# Configure timezone
os.environ['TZ'] = 'UTC'

class MetricsRequest(BaseModel):
    """Request model for basic metrics queries"""
    duration: str = Field(
        default="5m", 
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

class AnalysisRequest(BaseModel):
    """Request model for performance analysis operations"""
    component: str = Field(
        description="Component to analyze: 'pods', 'ovs', 'sync_duration', 'multus', or 'all'"
    )
    metrics_data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Metrics data to analyze (if not provided, will be collected automatically)"
    )
    duration: str = Field(
        default="1h",
        description="Duration for data collection if metrics_data not provided"
    )
    save_reports: bool = Field(
        default=True,
        description="Whether to save analysis reports to files"
    )
    output_dir: str = Field(
        default=".",
        description="Directory to save analysis reports"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

# Initialize FastMCP app
app = FastMCP("ocp-benchmark-mcp")

# Global components
auth_manager: Optional[OpenShiftAuth] = None
config: Optional[Config] = None
prometheus_client: Optional[PrometheusBaseQuery] = None
storage: Optional[PrometheusStorage] = None
unified_analyzer: Optional[UnifiedPerformanceAnalyzer] = None

async def initialize_components():
    """Initialize global components"""
    global auth_manager, config, prometheus_client, storage, unified_analyzer
    
    config = Config()
    auth_manager = OpenShiftAuth(config.kubeconfig_path)
    await auth_manager.initialize()
    
    prometheus_client = PrometheusBaseQuery(
        auth_manager.prometheus_url,
        auth_manager.prometheus_token
    )
    
    storage = PrometheusStorage()
    await storage.initialize()
    
    if UNIFIED_ANALYZER_AVAILABLE:
        unified_analyzer = UnifiedPerformanceAnalyzer()

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
        cluster_info = await general_info.collect_cluster_info()
        return general_info.to_dict(cluster_info)
    except Exception as e:
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
        cluster_info = await cluster_nodes_usage.query_node_usage(request.duration)
        return cluster_info
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
    
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
        return await kube_api_metrics.get_metrics(request.duration, request.start_time, request.end_time)
    except Exception as e:
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

        pods_duration_summary = await collect_ovn_duration_usage(
            prometheus_client,
            duration=request.duration
        )
        return pods_duration_summary
    
    except Exception as e:
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
        
        collector = PodsUsageCollector(prometheus_client)
        return await collector.collect_duration_usage(
            request.duration, 
            request.pod_pattern, 
            request.container_pattern, 
            request.namespace_pattern
        )
    
    except Exception as e:
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
        range_results = await collector.collect_all_ovs_metrics(request.duration)
        return range_results
    except Exception as e:
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
                
        collector = PodsUsageCollector(prometheus_client)
        return await collector.collect_duration_usage(
            request.duration, 
            request.pod_pattern, 
            request.container_pattern, 
            request.namespace_pattern
        )

    except Exception as e:
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
        return await collector.collect_sync_duration_seconds_metrics(request.duration)
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="analyze_pods_performance",
    description="""Perform comprehensive performance analysis of OVN-Kubernetes pods including resource utilization patterns, performance bottlenecks, health scoring, and actionable recommendations. This tool generates detailed analysis reports with insights for pod performance optimization.

Parameters:
- component: Must be set to "pods" for pod analysis
- metrics_data (optional): Pre-collected metrics data to analyze. If not provided, will collect automatically
- duration (default: "1h"): Data collection duration if metrics_data not provided (e.g., "5m", "1h", "1d")
- save_reports (default: true): Whether to save analysis reports to files (JSON and text formats)
- output_dir (default: "."): Directory to save analysis reports

Returns comprehensive pod analysis including:
- Overall pod health scoring and status assessment
- Resource utilization efficiency analysis 
- Performance bottleneck identification
- Pod placement and node distribution analysis
- Resource limit and request optimization recommendations
- Comparative performance analysis across pods
- Critical issues requiring immediate attention
- Performance improvement recommendations

Use this tool for in-depth pod performance analysis, optimization planning, capacity management, or troubleshooting resource-related issues in OVN-K deployments."""
)
async def analyze_pods_performance(request: AnalysisRequest) -> Dict[str, Any]:
    """
    Perform comprehensive performance analysis of OVN-Kubernetes pods including
    resource utilization patterns, performance bottlenecks, health scoring, and recommendations.
    
    Generates detailed analysis reports with actionable insights for pod performance optimization.
    """
    global prometheus_client, unified_analyzer
    try:
        if not UNIFIED_ANALYZER_AVAILABLE:
            return {"error": "Unified analyzer not available", "timestamp": datetime.now(timezone.utc).isoformat()}
        
        if not prometheus_client:
            await initialize_components()
        
        # Get metrics data if not provided
        metrics_data = request.metrics_data
        if not metrics_data:
            metrics_data = await collect_ovn_duration_usage(
                prometheus_client,
                duration=request.duration
            )
        
        # Perform analysis
        if not unified_analyzer:
            unified_analyzer = UnifiedPerformanceAnalyzer()
        
        analysis_result = unified_analyzer.analyze_component("pods", metrics_data)
        
        # Save reports if requested
        if request.save_reports:
            try:
                report_files = unified_analyzer.generate_comprehensive_report(
                    {"pods": analysis_result},
                    save_json=True,
                    save_text=True,
                    output_dir=request.output_dir
                )
                analysis_result["saved_reports"] = report_files
            except Exception as e:
                analysis_result["report_save_error"] = str(e)
        
        return {
            "analysis_result": analysis_result,
            "metrics_data_summary": {
                "collection_type": metrics_data.get("collection_type", "unknown"),
                "total_pods": len(metrics_data.get("top_10_pods", [])),
                "timestamp": metrics_data.get("collection_timestamp", datetime.now(timezone.utc).isoformat())
            }
        }
    
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="analyze_ovs_performance",
    description="""Perform comprehensive performance analysis of Open vSwitch (OVS) components including CPU/memory usage patterns, flow table efficiency, bridge performance, and health assessment. This tool provides detailed OVS performance insights with optimization recommendations.

Parameters:
- component: Must be set to "ovs" for OVS analysis
- metrics_data (optional): Pre-collected OVS metrics data to analyze. If not provided, will collect automatically
- duration (default: "1h"): Data collection duration if metrics_data not provided
- save_reports (default: true): Whether to save analysis reports to files
- output_dir (default: "."): Directory to save analysis reports

Returns comprehensive OVS analysis including:
- OVS component health scoring (ovs-vswitchd, ovsdb-server)
- CPU and memory utilization efficiency analysis
- Flow table performance and optimization recommendations  
- Bridge-specific performance analysis (br-int, br-ex)
- Connection metrics analysis (stream_open, rconn issues)
- Per-node OVS performance comparison
- Resource consumption trends and patterns
- Critical performance issues and alerts
- Optimization recommendations for flow processing

Use this tool for OVS performance tuning, troubleshooting dataplane issues, flow table optimization, or comprehensive OVS health assessment."""
)
async def analyze_ovs_performance(request: AnalysisRequest) -> Dict[str, Any]:
    """
    Perform comprehensive performance analysis of Open vSwitch (OVS) components including
    CPU/memory usage patterns, flow table efficiency, bridge performance, and health assessment.
    
    Provides detailed OVS performance insights with optimization recommendations.
    """
    global prometheus_client, auth_manager, unified_analyzer
    try:
        if not UNIFIED_ANALYZER_AVAILABLE:
            return {"error": "Unified analyzer not available", "timestamp": datetime.now(timezone.utc).isoformat()}
        
        if not prometheus_client or not auth_manager:
            await initialize_components()
        
        # Get metrics data if not provided
        metrics_data = request.metrics_data
        if not metrics_data:
            collector = OVSUsageCollector(prometheus_client, auth_manager)
            metrics_data = await collector.collect_all_ovs_metrics(request.duration)
        
        # Perform analysis
        if not unified_analyzer:
            unified_analyzer = UnifiedPerformanceAnalyzer()
        
        analysis_result = unified_analyzer.analyze_component("ovs", metrics_data)
        
        # Save reports if requested
        if request.save_reports:
            try:
                report_files = unified_analyzer.generate_comprehensive_report(
                    {"ovs": analysis_result},
                    save_json=True,
                    save_text=True,
                    output_dir=request.output_dir
                )
                analysis_result["saved_reports"] = report_files
            except Exception as e:
                analysis_result["report_save_error"] = str(e)
        
        return {
            "analysis_result": analysis_result,
            "metrics_data_summary": {
                "collection_type": metrics_data.get("collection_type", "unknown"),
                "timestamp": metrics_data.get("timestamp", datetime.now(timezone.utc).isoformat())
            }
        }
    
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="analyze_sync_duration_performance",
    description="""Perform comprehensive analysis of OVN-Kubernetes synchronization duration metrics including sync time patterns, performance bottlenecks, and controller efficiency. This tool identifies critical sync performance issues and provides optimization guidance.

Parameters:
- component: Must be set to "sync_duration" for synchronization analysis
- metrics_data (optional): Pre-collected sync duration metrics. If not provided, will collect automatically
- duration (default: "1h"): Data collection duration if metrics_data not provided
- save_reports (default: true): Whether to save analysis reports to files
- output_dir (default: "."): Directory to save analysis reports

Returns comprehensive sync duration analysis including:
- Controller sync performance health scoring
- Resource-specific synchronization bottleneck identification
- Sync duration trend analysis and statistical breakdown
- Per-resource-type performance analysis (pods, services, network policies)
- Controller responsiveness assessment
- Resource reconciliation efficiency metrics
- Critical sync delays requiring attention
- Performance optimization recommendations for controllers

Use this tool to diagnose control plane performance issues, optimize resource synchronization, troubleshoot slow network policy application, or assess overall OVN-K controller efficiency."""
)
async def analyze_sync_duration_performance(request: AnalysisRequest) -> Dict[str, Any]:
    """
    Perform comprehensive analysis of OVN-Kubernetes synchronization duration metrics
    including sync time patterns, performance bottlenecks, and controller efficiency.
    
    Identifies critical sync performance issues and provides optimization guidance.
    """
    global prometheus_client, unified_analyzer
    try:
        if not UNIFIED_ANALYZER_AVAILABLE:
            return {"error": "Unified analyzer not available", "timestamp": datetime.now(timezone.utc).isoformat()}
        
        if not prometheus_client:
            await initialize_components()
        
        # Get metrics data if not provided
        metrics_data = request.metrics_data
        if not metrics_data:
            collector = OVNSyncDurationCollector(prometheus_client)
            metrics_data = await collector.collect_sync_duration_seconds_metrics(request.duration)
        
        # Perform analysis
        if not unified_analyzer:
            unified_analyzer = UnifiedPerformanceAnalyzer()
        
        analysis_result = unified_analyzer.analyze_component("sync_duration", metrics_data)
        
        # Save reports if requested
        if request.save_reports:
            try:
                report_files = unified_analyzer.generate_comprehensive_report(
                    {"sync_duration": analysis_result},
                    save_json=True,
                    save_text=True,
                    output_dir=request.output_dir
                )
                analysis_result["saved_reports"] = report_files
            except Exception as e:
                analysis_result["report_save_error"] = str(e)
        
        return {
            "analysis_result": analysis_result,
            "metrics_data_summary": {
                "collection_type": metrics_data.get("collection_type", "unknown"),
                "duration": metrics_data.get("duration", request.duration),
                "timestamp": metrics_data.get("timestamp", datetime.now(timezone.utc).isoformat())
            }
        }
    
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="analyze_multus_performance",
    description="""Perform comprehensive performance analysis of Multus CNI components including network attachment processing efficiency, resource usage patterns, and multi-network performance characteristics. This tool provides insights into secondary network interface provisioning performance.

Parameters:
- component: Must be set to "multus" for Multus CNI analysis
- metrics_data (optional): Pre-collected Multus metrics data to analyze. If not provided, will collect automatically
- duration (default: "1h"): Data collection duration if metrics_data not provided
- save_reports (default: true): Whether to save analysis reports to files
- output_dir (default: "."): Directory to save analysis reports

Returns comprehensive Multus analysis including:
- Multus daemon performance health scoring
- Network attachment processing efficiency analysis
- Secondary network interface provisioning latency assessment
- Multi-network pod resource consumption patterns
- CNI plugin performance and error rate analysis
- Network attachment definition processing bottlenecks
- Resource utilization optimization recommendations
- Multi-network configuration efficiency insights

Use this tool for Multus performance optimization, troubleshooting multi-network issues, analyzing secondary interface provisioning performance, or validating multi-network deployment efficiency."""
)
async def analyze_multus_performance(request: AnalysisRequest) -> Dict[str, Any]:
    """
    Perform comprehensive performance analysis of Multus CNI components including
    network attachment processing efficiency, resource usage, and performance patterns.
    
    Provides insights into secondary network interface provisioning performance.
    """
    global prometheus_client, unified_analyzer
    try:
        if not UNIFIED_ANALYZER_AVAILABLE:
            return {"error": "Unified analyzer not available", "timestamp": datetime.now(timezone.utc).isoformat()}
        
        if not prometheus_client:
            await initialize_components()
        
        # Get metrics data if not provided
        metrics_data = request.metrics_data
        if not metrics_data:
            # Use pod collector with Multus-specific patterns
            collector = PodsUsageCollector(prometheus_client)
            metrics_data = await collector.collect_duration_usage(
                request.duration,
                pod_pattern="multus.*|network-metrics.*",
                namespace_pattern="openshift-multus"
            )
        
        # Perform analysis
        if not unified_analyzer:
            unified_analyzer = UnifiedPerformanceAnalyzer()
        
        analysis_result = unified_analyzer.analyze_component("multus", metrics_data)
        
        # Save reports if requested
        if request.save_reports:
            try:
                report_files = unified_analyzer.generate_comprehensive_report(
                    {"multus": analysis_result},
                    save_json=True,
                    save_text=True,
                    output_dir=request.output_dir
                )
                analysis_result["saved_reports"] = report_files
            except Exception as e:
                analysis_result["report_save_error"] = str(e)
        
        return {
            "analysis_result": analysis_result,
            "metrics_data_summary": {
                "collection_type": metrics_data.get("collection_type", "unknown"),
                "total_pods": len(metrics_data.get("top_10_pods", [])),
                "timestamp": metrics_data.get("collection_timestamp", datetime.now(timezone.utc).isoformat())
            }
        }
    
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="analyze_unified_performance",
    description="""Perform comprehensive unified performance analysis across all OVNK components including cross-component correlation analysis, unified health scoring, and comprehensive optimization recommendations. This tool generates executive summary and comprehensive reports covering the entire OVNK stack.

Parameters:
- component: Must be set to "all" for unified cross-component analysis
- metrics_data (optional): Pre-collected metrics for all components. If not provided, will collect automatically for all components
- duration (default: "1h"): Data collection duration if metrics_data not provided
- save_reports (default: true): Whether to save comprehensive analysis reports to files
- output_dir (default: "."): Directory to save analysis reports

Returns unified analysis including:
- Executive summary with overall cluster health assessment
- Cross-component performance correlation analysis
- Unified health scoring across all OVNK components (pods, OVS, sync duration, Multus)
- Critical issue identification with prioritized action items
- Performance bottleneck root cause analysis across components
- Comprehensive optimization recommendations
- Resource allocation and capacity planning insights
- System-wide performance trends and patterns
- Integration between networking components analysis

Use this tool for comprehensive OVNK performance assessment, executive reporting, system-wide optimization planning, or holistic troubleshooting of complex performance issues spanning multiple components."""
)
async def analyze_unified_performance(request: AnalysisRequest) -> Dict[str, Any]:
    """
    Perform comprehensive unified performance analysis across all OVNK components
    including cross-component correlation analysis, unified health scoring, and
    comprehensive optimization recommendations.
    
    Generates executive summary and comprehensive reports covering the entire OVNK stack.
    """
    global prometheus_client, auth_manager, unified_analyzer
    try:
        if not UNIFIED_ANALYZER_AVAILABLE:
            return {"error": "Unified analyzer not available", "timestamp": datetime.now(timezone.utc).isoformat()}
        
        if not prometheus_client or not auth_manager:
            await initialize_components()
        
        if not unified_analyzer:
            unified_analyzer = UnifiedPerformanceAnalyzer()
        
        # Collect data for all components if not provided
        components_data = request.metrics_data or {}
        
        if not components_data:
            print("Collecting metrics for all components...")
            
            # Collect pods metrics
            try:
                components_data["pods"] = await collect_ovn_duration_usage(
                    prometheus_client, duration=request.duration
                )
            except Exception as e:
                components_data["pods"] = {"error": str(e)}
            
            # Collect OVS metrics
            try:
                ovs_collector = OVSUsageCollector(prometheus_client, auth_manager)
                components_data["ovs"] = await ovs_collector.collect_all_ovs_metrics(request.duration)
            except Exception as e:
                components_data["ovs"] = {"error": str(e)}
            
            # Collect sync duration metrics
            try:
                sync_collector = OVNSyncDurationCollector(prometheus_client)
                components_data["sync_duration"] = await sync_collector.collect_sync_duration_seconds_metrics(request.duration)
            except Exception as e:
                components_data["sync_duration"] = {"error": str(e)}
            
            # Collect Multus metrics
            try:
                multus_collector = PodsUsageCollector(prometheus_client)
                components_data["multus"] = await multus_collector.collect_duration_usage(
                    request.duration,
                    pod_pattern="multus.*|network-metrics.*",
                    namespace_pattern="openshift-multus"
                )
            except Exception as e:
                components_data["multus"] = {"error": str(e)}
        
        # Perform unified analysis
        analysis_results = unified_analyzer.analyze_all_components(components_data)
        
        # Generate comprehensive reports
        if request.save_reports:
            try:
                report_files = unified_analyzer.generate_comprehensive_report(
                    analysis_results,
                    save_json=True,
                    save_text=True,
                    output_dir=request.output_dir
                )
                return {
                    "analysis_results": analysis_results,
                    "saved_reports": report_files,
                    "executive_summary": unified_analyzer._generate_executive_summary(analysis_results),
                    "cross_component_insights": unified_analyzer._generate_cross_component_insights(analysis_results)
                }
            except Exception as e:
                return {
                    "analysis_results": analysis_results,
                    "report_save_error": str(e),
                    "executive_summary": unified_analyzer._generate_executive_summary(analysis_results)
                }
        
        return {
            "analysis_results": analysis_results,
            "executive_summary": unified_analyzer._generate_executive_summary(analysis_results),
            "cross_component_insights": unified_analyzer._generate_cross_component_insights(analysis_results)
        }
    
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="get_performance_health_check",
    description="""Perform a quick health check across all OVNK components providing a high-level status overview with health scores and immediate action items. This tool is ideal for monitoring dashboards and quick system status assessment.

Parameters:
- pod_pattern (default: "ovnkube.*"): Pod pattern for health check scope
- container_pattern (default: ".*"): Container pattern for analysis
- label_selector (default: ".*"): Label selector pattern
- namespace_pattern (default: "openshift-ovn-kubernetes"): Target namespace pattern
- top_n (default: 10): Number of top issues to highlight
- duration (default: "1h"): Analysis duration for health assessment
- start_time (optional): Historical health check start time
- end_time (optional): Historical health check end time

Returns quick health assessment including:
- Overall cluster health status (EXCELLENT/GOOD/MODERATE/POOR/CRITICAL)
- Per-component health scores and status indicators
- Critical issues count requiring immediate attention  
- Warning issues count for monitoring
- Key findings summary with actionable insights
- Component health status breakdown (pods, OVS, sync duration, etc.)
- Immediate action required flag for urgent issues
- Quick performance overview without deep analysis

Use this tool for monitoring dashboards, quick status checks, health monitoring alerts, or rapid assessment of OVNK system health before deeper analysis."""
)
async def get_performance_health_check(request: PODsRequest) -> Dict[str, Any]:
    """
    Perform a quick health check across all OVNK components providing
    a high-level status overview with health scores and immediate action items.
    
    Ideal for monitoring dashboards and quick system status assessment.
    """
    global prometheus_client, auth_manager, unified_analyzer
    try:
        if not UNIFIED_ANALYZER_AVAILABLE:
            return {"error": "Unified analyzer not available", "timestamp": datetime.now(timezone.utc).isoformat()}
        
        if not prometheus_client or not auth_manager:
            await initialize_components()
        
        if not unified_analyzer:
            unified_analyzer = UnifiedPerformanceAnalyzer()
        
        # Quick data collection (shorter duration for health check)
        health_duration = "5m"  # Quick health check
        components_data = {}
        
        # Collect minimal data for quick assessment
        try:
            components_data["pods"] = await collect_ovn_duration_usage(
                prometheus_client, duration=health_duration
            )
        except Exception as e:
            components_data["pods"] = {"error": str(e)}
        
        try:
            ovs_collector = OVSUsageCollector(prometheus_client, auth_manager)
            components_data["ovs"] = await ovs_collector.collect_all_ovs_metrics(health_duration)
        except Exception as e:
            components_data["ovs"] = {"error": str(e)}
        
        # Quick analysis
        analysis_results = unified_analyzer.analyze_all_components(components_data)
        executive_summary = unified_analyzer._generate_executive_summary(analysis_results)
        
        # Generate health status
        health_status = {}
        for component, result in analysis_results.items():
            if "error" in result:
                health_status[component] = "ERROR"
            else:
                health_score = unified_analyzer._extract_health_score(component, result)
                if health_score is None:
                    health_status[component] = "UNKNOWN"
                elif health_score >= 85:
                    health_status[component] = "EXCELLENT"
                elif health_score >= 70:
                    health_status[component] = "GOOD"
                elif health_score >= 50:
                    health_status[component] = "MODERATE"
                elif health_score >= 30:
                    health_status[component] = "POOR"
                else:
                    health_status[component] = "CRITICAL"
        
        return {
            "health_check_timestamp": datetime.now(timezone.utc).isoformat(),
            "overall_cluster_health": executive_summary.get("overall_cluster_health", "unknown"),
            "component_health_status": health_status,
            "critical_issues": executive_summary.get("critical_issues_count", 0),
            "warning_issues": executive_summary.get("warning_issues_count", 0),
            "immediate_action_required": executive_summary.get("immediate_action_required", False),
            "key_findings": executive_summary.get("key_findings", []),
            "components_analyzed": list(health_status.keys()),
            "health_check_duration": health_duration
        }
    
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="store_performance_data",
    description="""Store performance metrics data in the DuckDB database for historical analysis and trend monitoring. This tool enables long-term performance tracking and historical comparison capabilities.

Parameters:
- metrics_data: Dictionary containing performance metrics data to store in database (required)
- timestamp (optional): ISO timestamp for the data (defaults to current time if not provided)

Returns storage confirmation including:
- Success/failure status of data storage operation
- Timestamp of stored data
- Storage location and database information

Use this tool to persist performance data for historical analysis, trend monitoring, capacity planning, or building performance baselines over time."""
)
async def store_performance_data(request: PerformanceDataRequest) -> Dict[str, Any]:
    """
    Store performance metrics data in the DuckDB database for historical analysis
    and trend monitoring.
    
    Enables long-term performance tracking and historical comparison capabilities.
    """
    global storage
    try:
        if not storage:
            await initialize_components()
        elt = PerformanceELT(storage.db_path)
        timestamp = request.timestamp or datetime.now(timezone.utc).isoformat()
        await elt.store_performance_data(request.metrics_data, timestamp)
        return {
            "status": "success", 
            "message": "Performance data stored successfully",
            "timestamp": timestamp
        }
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="get_performance_history", 
    description="""Retrieve historical performance data from the database for trend analysis and performance tracking over time. This tool supports filtering by metric type and configurable time ranges for comprehensive historical analysis.

Parameters:
- days (default: 7): Number of days of historical data to retrieve (1-365)
- metric_type (optional): Filter by specific metric type (e.g., "pods", "ovs", "sync_duration", "multus")

Returns historical performance data including:
- Time-series performance data over specified period
- Trend analysis and performance patterns
- Historical comparison points for baseline analysis
- Performance degradation or improvement indicators
- Long-term capacity utilization trends

Use this tool for performance trend analysis, capacity planning, identifying performance degradation over time, or establishing performance baselines for comparison."""
)
async def get_performance_history(days: int = 7, metric_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Retrieve historical performance data from the database for trend analysis
    and performance tracking over time.
    
    Supports filtering by metric type and configurable time ranges for analysis.
    """
    global storage
    try:
        if not storage:
            await initialize_components()
        elt = PerformanceELT(storage.db_path)
        result = await elt.get_performance_history(days, metric_type)
        return result
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

async def main():
    """Main entry point"""
    try:
        await initialize_components()
        await app.run_async(
            transport="streamable-http",
            host="0.0.0.0",
            port=8000
        )
    except Exception as e:
        print(f"Error starting server: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
