#!/usr/bin/env python3
"""
OpenShift OVN-Kubernetes Benchmark MCP API Wrapper
Exposes MCP tools as FastAPI endpoints using lifespan management
Updated to use MCP ClientSession and streamable HTTP client
"""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ConfigDict
import uvicorn

# MCP imports for client session and HTTP streaming
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

# Import all the components from the original MCP server
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

# Pydantic models (reusing from original MCP server)
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

class PerformanceHistoryRequest(BaseModel):
    """Request model for performance history queries"""
    days: int = Field(
        default=7,
        description="Number of days of historical data to retrieve (1-365)"
    )
    metric_type: Optional[str] = Field(
        default=None,
        description="Filter by specific metric type (e.g., 'pods', 'ovs', 'sync_duration', 'multus')"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

# Global components
auth_manager: Optional[OpenShiftAuth] = None
config: Optional[Config] = None
prometheus_client: Optional[PrometheusBaseQuery] = None
storage: Optional[PrometheusStorage] = None
unified_analyzer: Optional[UnifiedPerformanceAnalyzer] = None
mcp_client_session: Optional[ClientSession] = None
http_client: Optional[Any] = None

async def initialize_mcp_client():
    """Initialize MCP client session and HTTP client"""
    global mcp_client_session, http_client
    try:
        # Initialize MCP streamable HTTP client
        http_client = streamablehttp_client()
        
        # Initialize MCP ClientSession (you may need to adjust parameters based on your specific MCP setup)
        mcp_client_session = ClientSession()
        await mcp_client_session.__aenter__()
        
        print("MCP client components initialized successfully")
    except Exception as e:
        print(f"Error initializing MCP client: {e}")
        raise

async def cleanup_mcp_client():
    """Cleanup MCP client components"""
    global mcp_client_session, http_client
    try:
        if mcp_client_session:
            await mcp_client_session.__aexit__(None, None, None)
        if http_client and hasattr(http_client, 'close'):
            await http_client.close()
        print("MCP client components cleaned up successfully")
    except Exception as e:
        print(f"Error cleaning up MCP client: {e}")

async def initialize_components():
    """Initialize global components including MCP client"""
    global auth_manager, config, prometheus_client, storage, unified_analyzer
    
    try:
        # Initialize MCP client first
        await initialize_mcp_client()
        
        config = Config()
        auth_manager = OpenShiftAuth(config.kubeconfig_path)
        await auth_manager.initialize()
        
        # Update PrometheusBaseQuery to use MCP HTTP client if it supports it
        # Note: You may need to modify PrometheusBaseQuery to accept the MCP HTTP client
        prometheus_client = PrometheusBaseQuery(
            auth_manager.prometheus_url,
            auth_manager.prometheus_token,
            http_client=http_client  # Pass MCP HTTP client if supported
        )
        
        storage = PrometheusStorage()
        await storage.initialize()
        
        if UNIFIED_ANALYZER_AVAILABLE:
            unified_analyzer = UnifiedPerformanceAnalyzer()
        
        print("All components initialized successfully")
    except Exception as e:
        print(f"Error initializing components: {e}")
        raise

async def cleanup_components():
    """Cleanup global components including MCP client"""
    global auth_manager, storage
    try:
        if storage:
            await storage.close()
        if auth_manager:
            await auth_manager.cleanup()
        
        # Cleanup MCP client last
        await cleanup_mcp_client()
        
        print("All components cleaned up successfully")
    except Exception as e:
        print(f"Error cleaning up components: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan management for proper startup and shutdown"""
    # Startup
    print("Starting OCP Benchmark MCP API with MCP Client...")
    await initialize_components()
    yield
    # Shutdown
    print("Shutting down OCP Benchmark MCP API...")
    await cleanup_components()

# Create FastAPI app with lifespan
app = FastAPI(
    title="OpenShift OVN-Kubernetes Benchmark API",
    description="REST API wrapper for OCP Benchmark MCP Server tools with MCP Client integration",
    version="1.1.0",
    lifespan=lifespan
)

@app.get("/", summary="Health Check")
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "OCP Benchmark MCP API",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "unified_analyzer_available": UNIFIED_ANALYZER_AVAILABLE,
        "mcp_client_initialized": mcp_client_session is not None,
        "http_client_initialized": http_client is not None
    }

@app.post("/api/v1/openshift/general-info", summary="Get OpenShift General Information")
async def get_openshift_general_info(request: GeneralInfoRequest = GeneralInfoRequest()):
    """Get comprehensive OpenShift cluster information including NetworkPolicy counts, AdminNetworkPolicy counts, EgressFirewall counts, node status, and resource utilization."""
    try:
        general_info = OpenShiftGeneralInfo(http_client=http_client)  # Pass MCP HTTP client
        await general_info.initialize()
        cluster_info = await general_info.collect_cluster_info()
        return general_info.to_dict(cluster_info)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/cluster/node-usage", summary="Get Cluster Node Usage Metrics")
async def get_cluster_node_usage(request: MetricsRequest = MetricsRequest()):
    """Get cluster node resource usage metrics including CPU utilization, memory consumption, disk usage, and network utilization."""
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
            
        cluster_nodes_usage = NodeUsageQuery(prometheus_client)
        cluster_info = await cluster_nodes_usage.query_node_usage(request.duration)
        return cluster_info
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/kube-api/metrics", summary="Query Kubernetes API Metrics")
async def query_kube_api_metrics(request: MetricsRequest = MetricsRequest()):
    """Query Kubernetes API server performance metrics including request rates, response times, error rates, and resource consumption."""
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
        kube_api_metrics = KubeAPIMetrics(prometheus_client)
        return await kube_api_metrics.get_metrics(request.duration, request.start_time, request.end_time)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/ovnk/pods-metrics", summary="Query OVN-K Pod Metrics")
async def query_ovnk_pods_metrics(request: PODsRequest = PODsRequest()):
    """Query OVN-Kubernetes pod resource usage metrics including CPU and memory consumption."""
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
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/ovnk/containers-metrics", summary="Query OVN-K Container Metrics")
async def query_ovnk_containers_metrics(request: PODsRequest = PODsRequest()):
    """Query detailed container-level metrics within OVN-Kubernetes pods including per-container CPU and memory usage."""
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
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/ovnk/ovs-metrics", summary="Query OVS Metrics")
async def query_ovnk_ovs_metrics(request: PODsRequest = PODsRequest()):
    """Query Open vSwitch (OVS) performance metrics including CPU usage, memory consumption, flow table statistics."""
    global prometheus_client, auth_manager
    try:
        if not prometheus_client or not auth_manager:
            await initialize_components()
        
        collector = OVSUsageCollector(prometheus_client, auth_manager, http_client=http_client)  # Pass MCP HTTP client
        range_results = await collector.collect_all_ovs_metrics(request.duration)
        return range_results
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/multus/metrics", summary="Query Multus Metrics")
async def query_multus_metrics(request: PODsRequest = PODsRequest(pod_pattern="multus.*", namespace_pattern="openshift-multus")):
    """Query Multus CNI metrics including network attachment processing times, resource usage."""
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
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/ovnk/sync-duration-metrics", summary="Query OVN-K Sync Duration Metrics")
async def query_ovnk_sync_duration_seconds_metrics(request: MetricsRequest = MetricsRequest()):
    """Query OVN-Kubernetes synchronization duration metrics including controller sync times, resource reconciliation durations."""
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
        collector = OVNSyncDurationCollector(prometheus_client)
        return await collector.collect_sync_duration_seconds_metrics(request.duration)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/analysis/pods", summary="Analyze Pod Performance")
async def analyze_pods_performance(request: AnalysisRequest):
    """Perform comprehensive performance analysis of OVN-Kubernetes pods."""
    global prometheus_client, unified_analyzer
    try:
        if not UNIFIED_ANALYZER_AVAILABLE:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail={"error": "Unified analyzer not available", "timestamp": datetime.now(timezone.utc).isoformat()}
            )
        
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
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/analysis/ovs", summary="Analyze OVS Performance")
async def analyze_ovs_performance(request: AnalysisRequest):
    """Perform comprehensive performance analysis of Open vSwitch (OVS) components."""
    global prometheus_client, auth_manager, unified_analyzer
    try:
        if not UNIFIED_ANALYZER_AVAILABLE:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail={"error": "Unified analyzer not available", "timestamp": datetime.now(timezone.utc).isoformat()}
            )
        
        if not prometheus_client or not auth_manager:
            await initialize_components()
        
        # Get metrics data if not provided
        metrics_data = request.metrics_data
        if not metrics_data:
            collector = OVSUsageCollector(prometheus_client, auth_manager, http_client=http_client)
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
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/analysis/sync-duration", summary="Analyze Sync Duration Performance")
async def analyze_sync_duration_performance(request: AnalysisRequest):
    """Perform comprehensive analysis of OVN-Kubernetes synchronization duration metrics."""
    global prometheus_client, unified_analyzer
    try:
        if not UNIFIED_ANALYZER_AVAILABLE:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail={"error": "Unified analyzer not available", "timestamp": datetime.now(timezone.utc).isoformat()}
            )
        
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
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/analysis/multus", summary="Analyze Multus Performance")
async def analyze_multus_performance(request: AnalysisRequest):
    """Perform comprehensive performance analysis of Multus CNI components."""
    global prometheus_client, unified_analyzer
    try:
        if not UNIFIED_ANALYZER_AVAILABLE:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail={"error": "Unified analyzer not available", "timestamp": datetime.now(timezone.utc).isoformat()}
            )
        
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
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/analysis/unified", summary="Analyze Unified Performance")
async def analyze_unified_performance(request: AnalysisRequest):
    """Perform comprehensive unified performance analysis across all OVNK components."""
    global prometheus_client, auth_manager, unified_analyzer
    try:
        if not UNIFIED_ANALYZER_AVAILABLE:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail={"error": "Unified analyzer not available", "timestamp": datetime.now(timezone.utc).isoformat()}
            )
        
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
                ovs_collector = OVSUsageCollector(prometheus_client, auth_manager, http_client=http_client)
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
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/health-check", summary="Performance Health Check")
async def get_performance_health_check(request: PODsRequest = PODsRequest()):
    """Perform a quick health check across all OVNK components providing a high-level status overview."""
    global prometheus_client, auth_manager, unified_analyzer
    try:
        if not UNIFIED_ANALYZER_AVAILABLE:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail={"error": "Unified analyzer not available", "timestamp": datetime.now(timezone.utc).isoformat()}
            )
        
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
            ovs_collector = OVSUsageCollector(prometheus_client, auth_manager, http_client=http_client)
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
            "health_check_duration": health_duration,
            "mcp_client_status": "active" if mcp_client_session else "inactive"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/storage/performance-data", summary="Store Performance Data")
async def store_performance_data(request: PerformanceDataRequest):
    """Store performance metrics data in the DuckDB database for historical analysis and trend monitoring."""
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
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

@app.post("/api/v1/storage/performance-history", summary="Get Performance History")
async def get_performance_history(request: PerformanceHistoryRequest = PerformanceHistoryRequest()):
    """Retrieve historical performance data from the database for trend analysis and performance tracking over time."""
    global storage
    try:
        if not storage:
            await initialize_components()
        elt = PerformanceELT(storage.db_path)
        result = await elt.get_performance_history(request.days, request.metric_type)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

# MCP Client specific endpoints
@app.get("/api/v1/mcp/client-status", summary="Get MCP Client Status")
async def get_mcp_client_status():
    """Get detailed status of MCP client components"""
    global mcp_client_session, http_client
    return {
        "mcp_client_session": {
            "initialized": mcp_client_session is not None,
            "status": "active" if mcp_client_session else "inactive"
        },
        "http_client": {
            "initialized": http_client is not None,
            "type": type(http_client).__name__ if http_client else None
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.post("/api/v1/mcp/reinitialize", summary="Reinitialize MCP Client")
async def reinitialize_mcp_client():
    """Reinitialize MCP client components"""
    try:
        await cleanup_mcp_client()
        await initialize_mcp_client()
        return {
            "status": "success",
            "message": "MCP client reinitialized successfully",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
        )

# Additional API endpoints for convenience
@app.get("/api/v1/components/status", summary="Get Component Status")
async def get_component_status():
    """Get status of all initialized components"""
    return {
        "auth_manager": auth_manager is not None,
        "config": config is not None,
        "prometheus_client": prometheus_client is not None,
        "storage": storage is not None,
        "unified_analyzer": unified_analyzer is not None,
        "unified_analyzer_available": UNIFIED_ANALYZER_AVAILABLE,
        "mcp_client_session": mcp_client_session is not None,
        "http_client": http_client is not None,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/api/v1/metrics", summary="List Available Metrics")
async def list_available_metrics():
    """List all available metric endpoints and their descriptions"""
    return {
        "metrics_endpoints": {
            "/api/v1/openshift/general-info": "OpenShift cluster general information",
            "/api/v1/cluster/node-usage": "Cluster node resource usage metrics",
            "/api/v1/kube-api/metrics": "Kubernetes API server performance metrics",
            "/api/v1/ovnk/pods-metrics": "OVN-K pod resource usage metrics",
            "/api/v1/ovnk/containers-metrics": "OVN-K container-level metrics",
            "/api/v1/ovnk/ovs-metrics": "Open vSwitch performance metrics",
            "/api/v1/multus/metrics": "Multus CNI metrics",
            "/api/v1/ovnk/sync-duration-metrics": "OVN-K synchronization duration metrics"
        },
        "analysis_endpoints": {
            "/api/v1/analysis/pods": "Pod performance analysis",
            "/api/v1/analysis/ovs": "OVS performance analysis",
            "/api/v1/analysis/sync-duration": "Sync duration performance analysis",
            "/api/v1/analysis/multus": "Multus performance analysis",
            "/api/v1/analysis/unified": "Unified cross-component analysis",
            "/api/v1/health-check": "Quick health check across all components"
        },
        "storage_endpoints": {
            "/api/v1/storage/performance-data": "Store performance data",
            "/api/v1/storage/performance-history": "Retrieve historical performance data"
        },
        "mcp_endpoints": {
            "/api/v1/mcp/client-status": "Get MCP client status",
            "/api/v1/mcp/reinitialize": "Reinitialize MCP client components"
        }
    }

def create_app():
    """Factory function to create the FastAPI app (useful for testing)"""
    return app

if __name__ == "__main__":
    # Run the server
    uvicorn.run(
        "ovnk_benchmark_mcp_api:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
        log_level="info"
    )