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

@app.tool()
async def get_openshift_general_info(request: GeneralInfoRequest) -> Dict[str, Any]:
    """
    Get comprehensive OpenShift cluster information and configuration details.
    
    Collects detailed cluster information including:
    - Cluster name, version, and platform (AWS, Azure, GCP, etc.)
    - Node inventory categorized by role (master/control-plane, infra, worker)
    - Node specifications: CPU capacity, memory, architecture, kernel version
    - Node status: ready/not ready, schedulable status, creation timestamps
    - Resource counts: namespaces, pods, services, secrets, configmaps
    - Network policy counts: NetworkPolicy, AdminNetworkPolicy, EgressFirewall
    - Cluster operators status and health
    - API server URL and cluster infrastructure details
    
    Parameters:
    - namespace (optional): Specific namespace to focus on, defaults to cluster-wide collection
    
    Returns comprehensive cluster inventory and status information suitable for:
    - Infrastructure auditing and capacity planning
    - Compliance reporting and documentation
    - Troubleshooting cluster issues
    - Monitoring cluster health and configuration drift
    
    No authentication parameters needed - uses auto-discovered OpenShift credentials.
    """
    try:
        general_info = OpenShiftGeneralInfo()
        await general_info.initialize()
        cluster_info = await general_info.collect_cluster_info()
        return general_info.to_dict(cluster_info)
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool()
async def get_cluster_node_usage(request: MetricsRequest) -> Dict[str, Any]:
    """
    Get comprehensive cluster node resource usage and performance metrics.
    
    Queries and analyzes node-level resource utilization across the entire cluster:
    - CPU usage statistics (min/avg/max) per node and grouped by role
    - Memory utilization in MB with detailed statistics
    - Network I/O metrics (receive/transmit rates in bytes/s)
    - Node role-based grouping (master/control-plane, infra, worker)
    - Performance statistics over specified time duration
    
    Uses Kubernetes node role labels for accurate categorization:
    - node-role.kubernetes.io/control-plane
    - node-role.kubernetes.io/master  
    - node-role.kubernetes.io/infra
    - node-role.kubernetes.io/worker
    
    Parameters:
    - duration: Time range for metrics collection (e.g., "5m", "1h", "6h", "1d", "7d")
    - start_time: Optional ISO timestamp for historical analysis (YYYY-MM-DDTHH:MM:SSZ)
    - end_time: Optional ISO timestamp for historical analysis (YYYY-MM-DDTHH:MM:SSZ)
    
    Returns detailed node resource utilization data including:
    - Per-node metrics with role classification
    - Group summaries by node role
    - Statistical analysis (min/avg/max values)
    - Proper units for all metrics (%, MB, bytes/s)
    
    Essential for:
    - Capacity planning and resource optimization
    - Identifying performance bottlenecks at node level
    - Monitoring cluster resource consumption patterns
    - Infrastructure cost optimization analysis
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
    
@app.tool()
async def query_kube_api_metrics(request: MetricsRequest) -> Dict[str, Any]:
    """
    Query comprehensive Kubernetes API server performance and health metrics.
    
    Monitors critical API server performance indicators including:
    - Read-only API call latencies (GET, LIST operations) with P99 percentiles
    - Mutating API call latencies (POST, PUT, DELETE, PATCH) with detailed statistics
    - API request rates and throughput analysis by verb and resource
    - Error rates and HTTP response codes distribution
    - Current inflight requests and concurrency metrics
    - etcd request duration and backend performance
    
    Provides health scoring and performance assessment:
    - Latency evaluation (excellent < 100ms, good < 500ms, warning < 1s, critical > 1s)
    - Request rate analysis and trend identification
    - Error rate monitoring and alert generation
    - Overall API server health score calculation
    
    Parameters:
    - duration: Metrics collection timespan (e.g., "5m", "15m", "1h", "6h")
    - start_time: Optional historical start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    - end_time: Optional historical end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    
    Returns comprehensive API server analytics including:
    - Detailed latency statistics with percentile analysis
    - Request rate and error rate trends
    - Performance health assessment and alerts
    - Historical data for trend analysis when time range specified
    
    Critical for:
    - API server performance monitoring and optimization
    - Identifying control plane bottlenecks
    - Troubleshooting cluster responsiveness issues
    - Capacity planning for API server scaling
    - SLA monitoring and performance reporting
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
        kube_api_metrics = KubeAPIMetrics(prometheus_client)
        return await kube_api_metrics.get_metrics(request.duration, request.start_time, request.end_time)
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool()
async def query_ovnk_pods_metrics(request: PODsRequest) -> Dict[str, Any]:
    """
    Query detailed OVN-Kubernetes pod resource usage and performance metrics.
    
    Analyzes resource consumption and performance characteristics of OVN-K components:
    - ovnkube-controller pods: CPU, memory usage, and performance patterns
    - ovnkube-node pods: Per-node resource utilization and efficiency
    - ovnkube-master pods: Control plane resource consumption
    - Associated network components and sidecars (kube-rbac-proxy, etc.)
    
    Provides comprehensive pod-level analysis:
    - Top resource consumers identification and ranking
    - Duration-based usage patterns and trends
    - Container-level resource breakdown within pods
    - Memory and CPU utilization statistics over time
    - Performance bottleneck identification
    
    Parameters:
    - pod_pattern: Regex pattern for pod name matching (default: "ovnkube.*")
      Examples: "ovnkube-controller.*", "ovnkube-node.*", "ovnkube.*"
    - container_pattern: Container name regex filter (default: ".*" for all)
      Examples: "ovnkube-controller", "kube-rbac-proxy.*", ".*proxy.*"
    - namespace_pattern: Namespace regex pattern (default: "openshift-ovn-kubernetes")
    - label_selector: Pod label selector pattern for advanced filtering
    - duration: Time range for metrics analysis (default: "1h")
      Examples: "15m", "1h", "6h", "24h"
    - top_n: Number of top consumers to return (1-50, default: 10)
    - start_time: Historical analysis start time (ISO format)
    - end_time: Historical analysis end time (ISO format)
    
    Returns detailed pod performance analysis including:
    - Ranked list of resource consumers
    - Statistical analysis of usage patterns
    - Performance trends and anomaly detection
    - Resource efficiency metrics and recommendations
    
    Essential for:
    - OVN-K component performance optimization
    - Resource allocation tuning and rightsizing
    - Identifying memory leaks or CPU hotspots
    - Capacity planning for network infrastructure
    - Troubleshooting OVN-K performance issues
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

@app.tool()
async def query_ovnk_containers_metrics(request: PODsRequest) -> Dict[str, Any]:
    """
    Query granular container-level metrics within OVN-Kubernetes pods for detailed performance analysis.
    
    Provides fine-grained analysis of individual containers within OVN-K pods:
    - Per-container CPU usage, memory consumption, and resource limits utilization
    - Container-specific performance patterns and resource efficiency
    - Sidecar container analysis (kube-rbac-proxy, monitoring agents)
    - Resource allocation vs actual usage comparison
    - Container restart patterns and stability metrics
    
    Enables deep-dive analysis for:
    - Individual container optimization and tuning
    - Resource limit adjustment recommendations  
    - Multi-container pod resource distribution analysis
    - Container-level troubleshooting and debugging
    - Precise resource allocation for cost optimization
    
    Parameters:
    - pod_pattern: Regex for targeting specific pods (default: "ovnkube.*")
      Examples: "ovnkube-controller.*", "ovnkube-node.*", specific pod names
    - container_pattern: Fine-grained container name filtering (default: ".*")
      Examples: "ovnkube-controller", "kube-rbac-proxy", "northd", "nbdb"
    - namespace_pattern: Namespace targeting (default: "openshift-ovn-kubernetes")
    - label_selector: Advanced pod label-based filtering
    - duration: Analysis time window (default: "1h")
      Recommended: "30m" for detailed analysis, "6h" for trend analysis
    - top_n: Number of top resource-consuming containers (1-50, default: 10)
    - start_time: Historical analysis start point (ISO timestamp)
    - end_time: Historical analysis end point (ISO timestamp)
    
    Returns comprehensive container-level metrics including:
    - Individual container resource usage statistics
    - Resource limit utilization percentages
    - Container efficiency scores and optimization opportunities
    - Multi-container pod resource distribution analysis
    - Historical trends and performance patterns
    
    Ideal for:
    - Container rightsizing and resource optimization
    - Identifying inefficient containers within pods
    - Resource limit tuning and optimization
    - Container-level performance troubleshooting
    - Cost optimization through precise resource allocation
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

@app.tool()
async def query_ovnk_ovs_metrics(request: PODsRequest) -> Dict[str, Any]:
    """
    Query comprehensive Open vSwitch (OVS) performance metrics and dataplane health indicators.
    
    Monitors critical OVS dataplane performance and operational metrics:
    - OVS daemon CPU usage and memory consumption patterns
    - Flow table statistics: flow count, flow processing rates, table utilization
    - Bridge statistics: packet forwarding rates, drop counters, error rates  
    - Connection metrics: OpenFlow connections, database connections
    - Datapath performance: packet processing latency, throughput statistics
    - OVS database (ovsdb-server) performance and replication health
    
    Provides deep visibility into:
    - Network dataplane performance bottlenecks
    - Flow table efficiency and optimization opportunities
    - OVS resource consumption and scaling patterns
    - Network packet processing performance
    - Database synchronization and consistency metrics
    
    Parameters:
    - pod_pattern: Target OVS-related pods (default: "ovnkube.*")
      Examples: ".*ovs.*", "ovnkube-node.*", specific OVS pod patterns
    - container_pattern: OVS container filtering (default: ".*")
      Examples: "ovs-vswitchd", "ovsdb-server", "ovn-controller"
    - namespace_pattern: Namespace scope (default: "openshift-ovn-kubernetes")  
    - duration: Metrics collection period (default: "1h")
      Recommended: "15m" for immediate issues, "4h" for trend analysis
    - top_n: Top performing/consuming entities to analyze (default: 10)
    - start_time: Historical analysis start time (ISO format)
    - end_time: Historical analysis end time (ISO format)
    
    Returns comprehensive OVS performance data including:
    - Flow table statistics and efficiency metrics
    - Bridge performance and packet processing rates
    - OVS daemon resource consumption analysis
    - Connection health and database synchronization status
    - Performance trends and anomaly identification
    - Optimization recommendations for flow management
    
    Critical for:
    - Network dataplane performance optimization
    - OVS flow table tuning and management
    - Troubleshooting packet forwarding issues
    - Network performance bottleneck identification
    - Capacity planning for network infrastructure
    - OVS configuration optimization and tuning
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

@app.tool()
async def query_multus_metrics(request: PODsRequest) -> Dict[str, Any]:
    """
    Query Multus CNI performance metrics and secondary network interface management statistics.
    
    Monitors Multus CNI operational performance and network attachment efficiency:
    - Network attachment definition (NAD) processing times and success rates
    - Secondary network interface provisioning latency and throughput
    - Multus daemon resource usage (CPU, memory) and performance patterns
    - Network attachment request queuing and processing statistics
    - CNI plugin invocation times and error rates
    - Multi-network pod connectivity health and performance
    
    Provides insights into:
    - Secondary network provisioning efficiency
    - CNI plugin performance and optimization opportunities
    - Network attachment processing bottlenecks  
    - Multus daemon scaling and resource requirements
    - Multi-network connectivity troubleshooting data
    
    Parameters:
    - pod_pattern: Multus pod targeting (default: "ovnkube.*", recommend: "multus.*")
      Examples: "multus.*", "network-metrics.*", ".*multus.*"
    - container_pattern: Multus container filtering (default: ".*")
      Examples: "multus", "kube-multus", "multus-daemon"
    - namespace_pattern: Multus namespace scope (default: "openshift-ovn-kubernetes")
      Recommend: "openshift-multus" for dedicated Multus monitoring
    - duration: Analysis time window (default: "1h")
      Recommended: "30m" for real-time issues, "4h" for trend analysis
    - top_n: Top performers/consumers to analyze (default: 10)
    - start_time: Historical analysis start timestamp (ISO format)
    - end_time: Historical analysis end timestamp (ISO format)
    
    Returns detailed Multus performance analysis including:
    - Network attachment provisioning performance statistics
    - CNI plugin execution times and efficiency metrics
    - Multus daemon resource consumption patterns
    - Secondary network connectivity health indicators
    - Error rates and failure pattern analysis
    - Performance optimization recommendations
    
    Essential for:
    - Secondary network performance optimization
    - Multus CNI configuration tuning
    - Network attachment troubleshooting
    - Multi-network pod connectivity analysis
    - CNI plugin performance evaluation
    - Network infrastructure capacity planning
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
    
@app.tool()
async def query_ovnk_sync_duration_seconds_metrics(request: MetricsRequest) -> Dict[str, Any]:
    """
    Query OVN-Kubernetes synchronization duration metrics and control plane performance indicators.
    
    Monitors critical OVN-K controller synchronization performance:
    - Resource reconciliation loop timing and efficiency
    - Network policy synchronization duration and patterns
    - Pod network setup and teardown timing statistics
    - Service endpoint synchronization performance
    - Load balancer configuration sync duration
    - Ingress and egress rule processing times
    
    Analyzes control plane performance bottlenecks:
    - Sync duration trends and anomaly detection
    - Resource type-specific synchronization patterns
    - Performance degradation identification
    - Synchronization queue depth and processing rates
    - Error rates and retry pattern analysis
    
    Parameters:
    - duration: Time window for sync metrics analysis (default: "5m")
      Recommended: "15m" for immediate issues, "2h" for trend analysis
    - start_time: Historical analysis start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    - end_time: Historical analysis end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    
    Returns comprehensive synchronization performance data:
    - Sync duration statistics with percentile analysis (P50, P90, P99)
    - Resource-specific synchronization performance breakdown
    - Trend analysis and performance pattern identification
    - Control plane efficiency metrics and health indicators
    - Performance bottleneck identification and recommendations
    - Historical comparison and regression analysis
    
    Critical for:
    - OVN-K control plane performance optimization
    - Network policy synchronization troubleshooting  
    - Resource reconciliation efficiency analysis
    - Performance regression detection and monitoring
    - Control plane capacity planning and scaling
    - Network configuration deployment performance analysis
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
        collector = OVNSyncDurationCollector(prometheus_client)
        return await collector.collect_sync_duration_seconds_metrics(request.duration)
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool()
async def analyze_pods_performance(request: AnalysisRequest) -> Dict[str, Any]:
    """
    Perform comprehensive performance analysis of OVN-Kubernetes pods with intelligent insights and optimization recommendations.
    
    Conducts advanced analysis of OVN-K pod performance including:
    - Resource utilization pattern analysis and efficiency scoring
    - Performance bottleneck identification and root cause analysis
    - Memory leak detection and CPU spike pattern recognition
    - Resource allocation optimization recommendations
    - Health scoring based on multiple performance indicators
    - Historical trend analysis and performance regression detection
    
    Provides actionable insights for:
    - Pod rightsizing and resource optimization strategies
    - Performance anomaly detection and alerting
    - Resource allocation efficiency improvements
    - Capacity planning and scaling recommendations
    - Performance troubleshooting guidance and solutions
    
    Parameters:
    - component: Must be "pods" for pod-specific analysis
    - metrics_data: Pre-collected pod metrics (optional, will auto-collect if not provided)
    - duration: Data collection timespan if metrics_data not provided (default: "1h")
      Recommended: "2h" for comprehensive analysis, "6h" for trend analysis
    - save_reports: Whether to generate and save analysis reports (default: true)
    - output_dir: Directory for saving analysis reports (default: ".")
    
    Returns intelligent performance analysis including:
    - Detailed performance assessment with health scoring (0-100 scale)
    - Resource utilization efficiency analysis and optimization recommendations
    - Performance bottleneck identification with specific remediation steps
    - Anomaly detection results and trend analysis insights
    - Executive summary with key findings and action items
    - Generated reports (JSON and text formats) if save_reports enabled
    
    Essential for:
    - Proactive OVN-K pod performance management
    - Resource optimization and cost reduction initiatives
    - Performance troubleshooting and issue resolution
    - Capacity planning and infrastructure scaling decisions
    - SLA compliance monitoring and performance reporting
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
            }}
    
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool()
async def analyze_ovs_performance(request: AnalysisRequest) -> Dict[str, Any]:
    """
    Perform comprehensive performance analysis of Open vSwitch (OVS) components with intelligent dataplane optimization insights.
    
    Conducts advanced OVS performance analysis including:
    - Flow table efficiency analysis and optimization recommendations
    - Bridge performance assessment and bottleneck identification
    - OVS daemon resource consumption pattern analysis
    - Packet processing latency and throughput optimization
    - Flow management efficiency and rule optimization suggestions
    - Database synchronization health and performance analysis
    
    Provides intelligent dataplane insights for:
    - Flow table optimization and rule consolidation strategies
    - OVS configuration tuning recommendations
    - Network performance bottleneck resolution
    - Dataplane scaling and capacity planning
    - Packet forwarding efficiency improvements
    
    Parameters:
    - component: Must be "ovs" for OVS-specific analysis
    - metrics_data: Pre-collected OVS metrics (optional, will auto-collect if not provided)
    - duration: Data collection period if metrics_data not provided (default: "1h")
      Recommended: "1h" for current analysis, "4h" for comprehensive trend analysis
    - save_reports: Enable analysis report generation and saving (default: true)
    - output_dir: Report output directory (default: ".")
    
    Returns comprehensive OVS performance analysis including:
    - Flow table efficiency scores and optimization recommendations
    - Bridge performance metrics with bottleneck identification
    - OVS daemon health assessment and resource optimization guidance
    - Packet processing performance analysis and tuning suggestions
    - Database synchronization health indicators and improvement recommendations
    - Executive summary with actionable OVS optimization strategies
    
    Critical for:
    - Network dataplane performance optimization
    - OVS configuration tuning and management
    - Flow table efficiency improvement initiatives
    - Network troubleshooting and performance issue resolution
    - Dataplane capacity planning and scaling strategies
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

@app.tool()
async def analyze_sync_duration_performance(request: AnalysisRequest) -> Dict[str, Any]:
    """
    Perform comprehensive analysis of OVN-Kubernetes synchronization duration metrics and control plane efficiency assessment.
    
    Conducts advanced synchronization performance analysis including:
    - Resource reconciliation loop efficiency analysis and optimization recommendations
    - Network policy synchronization performance pattern recognition
    - Controller sync timing anomaly detection and trend analysis
    - Resource-specific synchronization bottleneck identification
    - Control plane performance health scoring and assessment
    - Sync duration optimization strategies and recommendations
    
    Provides intelligent control plane insights for:
    - Synchronization performance optimization and tuning
    - Resource reconciliation efficiency improvements
    - Controller scaling and performance optimization
    - Network policy deployment performance enhancement
    - Control plane bottleneck resolution strategies
    
    Parameters:
    - component: Must be "sync_duration" for synchronization analysis
    - metrics_data: Pre-collected sync metrics (optional, will auto-collect if not provided)
    - duration: Data collection timespan if metrics_data not provided (default: "1h")
      Recommended: "30m" for real-time analysis, "3h" for comprehensive trend analysis
    - save_reports: Enable detailed report generation and saving (default: true)
    - output_dir: Directory for analysis report output (default: ".")
    
    Returns comprehensive synchronization performance analysis including:
    - Sync duration efficiency scores with percentile analysis (P50, P90, P99)
    - Resource reconciliation performance assessment and optimization guidance
    - Control plane health indicators with actionable recommendations
    - Synchronization bottleneck identification and resolution strategies
    - Performance trend analysis and regression detection
    - Executive summary with critical findings and improvement actions
    
    Essential for:
    - OVN-K control plane performance optimization
    - Resource synchronization troubleshooting and tuning
    - Network policy deployment performance improvement
    - Controller efficiency monitoring and optimization
    - Control plane capacity planning and scaling decisions
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

@app.tool()
async def analyze_multus_performance(request: AnalysisRequest) -> Dict[str, Any]:
    """
    Perform comprehensive performance analysis of Multus CNI components with secondary network optimization insights.
    
    Conducts advanced Multus CNI performance analysis including:
    - Network attachment definition (NAD) processing efficiency analysis
    - Secondary network interface provisioning performance assessment
    - CNI plugin execution timing and optimization recommendations
    - Multi-network connectivity health scoring and troubleshooting
    - Resource consumption pattern analysis for Multus daemons
    - Network attachment request processing bottleneck identification
    
    Provides intelligent multi-network insights for:
    - CNI plugin performance optimization strategies
    - Network attachment provisioning efficiency improvements
    - Secondary network configuration tuning recommendations
    - Multi-network pod connectivity optimization
    - Multus daemon scaling and resource allocation guidance
    
    Parameters:
    - component: Must be "multus" for Multus-specific analysis
    - metrics_data: Pre-collected Multus metrics (optional, will auto-collect if not provided)
    - duration: Data collection period if metrics_data not provided (default: "1h")
      Recommended: "1h" for standard analysis, "3h" for comprehensive trend analysis
    - save_reports: Enable analysis report generation and saving (default: true)
    - output_dir: Output directory for analysis reports (default: ".")
    
    Returns comprehensive Multus performance analysis including:
    - Network attachment provisioning efficiency scores and optimization recommendations
    - CNI plugin performance metrics with execution timing analysis
    - Secondary network connectivity health assessment and troubleshooting guidance
    - Multi-network resource consumption analysis and optimization strategies
    - Performance bottleneck identification with specific remediation steps
    - Executive summary with key findings and actionable improvement recommendations
    
    Critical for:
    - Multi-network performance optimization and tuning
    - Secondary network interface provisioning troubleshooting
    - CNI plugin efficiency monitoring and improvement
    - Network attachment performance optimization
    - Multi-network infrastructure capacity planning
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

@app.tool()
async def analyze_unified_performance(request: AnalysisRequest) -> Dict[str, Any]:
    """
    Perform comprehensive unified performance analysis across all OVN-Kubernetes components with holistic optimization insights.
    
    Conducts enterprise-grade, cross-component performance analysis including:
    - Holistic OVNK stack performance assessment with component interdependency analysis
    - Cross-component correlation analysis identifying system-wide bottlenecks
    - Unified health scoring with weighted component importance calculations
    - Comprehensive optimization recommendations prioritized by impact and effort
    - Executive-level performance summary with business impact assessment
    - System-wide trend analysis and performance regression detection
    
    Provides comprehensive infrastructure insights for:
    - Enterprise-wide OVNK performance optimization strategies
    - Cross-component bottleneck identification and resolution
    - Infrastructure capacity planning with component scaling recommendations
    - Performance SLA compliance monitoring and improvement
    - Cost optimization through intelligent resource allocation
    - Executive reporting with actionable infrastructure improvements
    
    Analyzes all OVNK components:
    - OVN-K pods: Resource utilization, efficiency, and scaling recommendations
    - Open vSwitch: Dataplane performance, flow optimization, and network efficiency
    - Synchronization: Control plane performance, reconciliation efficiency
    - Multus CNI: Multi-network performance, secondary interface provisioning
    - Cross-component interactions: Dependencies, conflicts, and optimization opportunities
    
    Parameters:
    - component: Must be "all" for unified cross-component analysis
    - metrics_data: Complete component metrics (optional, will auto-collect comprehensive data)
      If provided, should include: {"pods": {...}, "ovs": {...}, "sync_duration": {...}, "multus": {...}}
    - duration: Data collection timespan for comprehensive analysis (default: "1h")
      Recommended: "2h" for thorough analysis, "6h" for trend analysis, "24h" for capacity planning
    - save_reports: Generate comprehensive analysis reports (default: true)
      Creates executive summary, technical analysis, and component-specific reports
    - output_dir: Report output directory with structured file organization (default: ".")
    
    Returns enterprise-grade unified analysis including:
    - Executive summary with business impact assessment and priority recommendations
    - Cross-component correlation analysis with system-wide optimization strategies
    - Unified health score (0-100) with component breakdown and improvement roadmap
    - Comprehensive performance assessment with bottleneck prioritization matrix
    - Resource optimization recommendations with cost impact analysis
    - Performance trend analysis with predictive capacity planning insights
    - Action-oriented improvement plan with implementation priority and effort estimation
    
    Essential for:
    - Enterprise OVNK infrastructure optimization and strategic planning
    - Executive-level performance reporting and business impact assessment
    - Cross-team collaboration with comprehensive technical and business insights
    - Infrastructure investment planning with data-driven recommendations
    - Performance SLA management and compliance monitoring
    - Large-scale deployment optimization and capacity planning
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

@app.tool()
async def get_performance_health_check(request: PODsRequest) -> Dict[str, Any]:
    """
    Perform rapid health assessment across all OVNK components for immediate status visibility and alert generation.
    
    Provides fast, comprehensive health checking including:
    - Component-level health status with color-coded severity levels
    - Immediate action items for critical issues requiring urgent attention
    - High-level performance indicators suitable for dashboard monitoring
    - Quick anomaly detection with trend-based alerting
    - Resource utilization red-flag identification
    - System-wide health score calculation with component impact weighting
    
    Designed for operational monitoring use cases:
    - Real-time infrastructure health monitoring dashboards
    - Automated alerting and notification systems
    - Quick triage and incident response support
    - SLA compliance monitoring and reporting
    - Proactive issue identification before service impact
    
    Health Status Levels:
    - EXCELLENT: Component performing optimally (health score ≥ 85)
    - GOOD: Component performing within acceptable parameters (70-84)
    - MODERATE: Some performance concerns identified (50-69) 
    - POOR: Significant performance issues requiring attention (30-49)
    - CRITICAL: Severe performance problems requiring immediate action (< 30)
    - ERROR: Component metrics collection failed or unavailable
    - UNKNOWN: Insufficient data for health assessment
    
    Parameters:
    - pod_pattern: Pod targeting for health assessment (default: "ovnkube.*")
    - container_pattern: Container filtering for focused health checking (default: ".*")
    - namespace_pattern: Namespace scope for health monitoring (default: "openshift-ovn-kubernetes")
    - duration: Quick health check time window - optimized for fast response (fixed: 5 minutes)
      Note: Uses shorter duration for rapid assessment vs comprehensive analysis
    - Additional parameters inherited but optimized for speed over depth
    
    Returns rapid health assessment including:
    - overall_cluster_health: System-wide health status summary
    - component_health_status: Per-component health with status levels
    - critical_issues: Count of critical problems requiring immediate action
    - warning_issues: Count of warning-level issues for proactive attention
    - immediate_action_required: Boolean flag for urgent intervention needs
    - key_findings: Prioritized list of most important health insights
    - components_analyzed: List of components successfully assessed
    - health_check_timestamp: UTC timestamp of assessment execution
    
    Ideal for:
    - Operations team dashboards and monitoring systems
    - Automated health checking and alerting workflows
    - Quick infrastructure status assessment and reporting
    - Pre-maintenance health validation and post-change verification
    - Executive-level infrastructure health visibility
    - Integration with external monitoring and alerting platforms
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

@app.tool()
async def store_performance_data(request: PerformanceDataRequest) -> Dict[str, Any]:
    """
    Store performance metrics data in DuckDB database for historical analysis and long-term trend monitoring.
    
    Enables persistent storage of performance metrics for:
    - Historical performance tracking and trend analysis over time
    - Performance regression detection through baseline comparisons
    - Capacity planning with historical growth pattern analysis
    - SLA compliance reporting with historical performance data
    - Long-term optimization effectiveness measurement
    - Executive reporting with performance trend visualization
    
    Storage Features:
    - High-performance DuckDB columnar storage optimized for analytical queries
    - Structured data organization with indexed timestamps for efficient retrieval
    - Automatic data validation and consistency checking
    - Metadata preservation for comprehensive context tracking
    - Efficient compression and storage optimization for large datasets
    
    Parameters:
    - metrics_data: Complete metrics dictionary to store in database
      Expected structure: Performance metrics from any collection tool
      Examples: Pod metrics, OVS metrics, sync duration data, analysis results
    - timestamp: Optional ISO timestamp for the data (defaults to current UTC time)
      Format: YYYY-MM-DDTHH:MM:SSZ (e.g., "2024-01-15T10:30:00Z")
      If not provided, uses current timestamp for data point
    
    Returns storage confirmation including:
    - status: "success" for successful storage operation
    - message: Confirmation message with storage details
    - timestamp: Actual timestamp used for data storage
    - Error details if storage operation fails
    
    Essential for:
    - Building historical performance baselines and trend analysis
    - Long-term capacity planning and infrastructure growth modeling
    - Performance regression detection and alerting
    - Executive performance reporting with historical context
    - SLA compliance monitoring and historical performance verification
    - Integration with business intelligence and reporting systems
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

@app.tool()
async def get_performance_history(days: int = 7, metric_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Retrieve historical performance data from database for comprehensive trend analysis and reporting.
    
    Provides powerful historical analysis capabilities including:
    - Time-series performance data retrieval with flexible time ranges
    - Metric type filtering for focused analysis and reporting
    - Historical trend identification and pattern recognition
    - Performance baseline establishment for regression detection
    - Long-term capacity planning data with growth trend analysis
    - Executive reporting data with historical context and insights
    
    Historical Analysis Features:
    - Flexible time range selection from days to months of historical data
    - Metric type filtering for component-specific trend analysis  
    - Automatic data aggregation and statistical analysis
    - Performance trend calculation with growth rate analysis
    - Anomaly detection based on historical patterns
    - Baseline performance establishment for comparison analysis
    
    Parameters:
    - days: Number of days of historical data to retrieve (default: 7 days)
      Examples: 1 (daily), 7 (weekly), 30 (monthly), 90 (quarterly), 365 (yearly)
      Recommended ranges:
      - 1-7 days: Short-term performance analysis and troubleshooting
      - 7-30 days: Monthly trend analysis and optimization planning
      - 30-90 days: Quarterly capacity planning and performance assessment
      - 90+ days: Long-term trend analysis and strategic planning
    - metric_type: Optional metric type filter for focused analysis
      Examples: "pods", "ovs", "sync_duration", "multus", "api_server", "nodes"
      If not specified, returns all metric types for comprehensive analysis
    
    Returns comprehensive historical analysis including:
    - Historical performance data organized by timestamp and metric type
    - Trend analysis with performance direction indicators (improving/degrading)
    - Statistical analysis including averages, minimums, maximums over time period
    - Data point counts and coverage analysis for data quality assessment
    - Performance baseline calculations for regression detection
    - Growth rate analysis and capacity planning insights
    
    Essential for:
    - Long-term performance trend monitoring and analysis
    - Capacity planning with data-driven growth projections
    - Performance regression detection and root cause analysis
    - Executive performance reporting with historical context
    - SLA compliance verification with historical performance data
    - Strategic infrastructure planning and optimization roadmaps
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
