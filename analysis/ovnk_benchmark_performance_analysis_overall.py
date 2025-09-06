#!/usr/bin/env python3
"""
OVNK Benchmark Overall Performance Analysis
Comprehensive analysis module that combines cluster, API, OVNK, and node performance analysis
File: analysis/ovnk_benchmark_performance_analysis_overall.py
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict

# Import required analysis modules
from analysis.ovnk_benchmark_performance_analysis_allovnk import OVNKPerformanceAnalyzer
from analysis.ovnk_benchmark_performance_analysis_clusters_api import ClusterAPIPerformanceAnalyzer
from tools.ovnk_benchmark_openshift_cluster_info import ClusterInfoCollector,ClusterInfo
from tools.ovnk_benchmark_prometheus_nodes_usage import nodeUsageCollector
from tools.ovnk_benchmark_prometheus_kubeapi import kubeAPICollector
from tools.ovnk_benchmark_prometheus_pods_usage import PodsUsageCollector, collect_ovn_duration_usage
from tools.ovnk_benchmark_prometheus_ovnk_latency import OVNLatencyCollector
from tools.ovnk_benchmark_prometheus_ovnk_ovs import OVSUsageCollector
from tools.ovnk_benchmark_prometheus_basicinfo import ovnBasicInfoCollector, get_pod_phase_counts
from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery
from ocauth.ovnk_benchmark_auth import OpenShiftAuth

logger = logging.getLogger(__name__)


@dataclass
class OverallHealthScore:
    """Overall cluster health scoring"""
    total_score: float
    cluster_score: float
    api_score: float
    ovnk_score: float
    nodes_score: float
    health_level: str
    critical_issues_count: int
    warning_issues_count: int


@dataclass
class ComponentPerformanceResults:
    """Performance results for each component"""
    cluster_general_info: Dict[str, Any]
    node_usage_analysis: Dict[str, Any]
    ovnk_pods_analysis: Dict[str, Any]
    ovnk_containers_analysis: Dict[str, Any]
    multus_pods_analysis: Dict[str, Any]
    ovs_metrics_analysis: Dict[str, Any]
    ovnk_sync_analysis: Dict[str, Any]
    api_latency_analysis: Dict[str, Any]
    ovn_basic_info: Dict[str, Any]


@dataclass
class OverallAnalysisResult:
    """Complete overall analysis result"""
    analysis_metadata: Dict[str, Any]
    overall_health: OverallHealthScore
    component_results: ComponentPerformanceResults
    performance_summary: Dict[str, Any]
    critical_issues: List[str]
    warning_issues: List[str]
    recommendations: List[str]
    execution_metrics: Dict[str, Any]


class OverallPerformanceAnalyzer:
    """Comprehensive overall performance analyzer for OCP/OVNK clusters"""
    
    def __init__(self, prometheus_url: str, token: Optional[str] = None, auth_client: Optional[OpenShiftAuth] = None):
        """
        Initialize the Overall Performance Analyzer
        
        Args:
            prometheus_url: Prometheus server URL
            token: Optional authentication token
            auth_client: Optional OpenShift authentication client
        """
        self.prometheus_url = prometheus_url
        self.token = token
        self.auth_client = auth_client
        
        # Initialize sub-analyzers
        self.ovnk_analyzer = OVNKPerformanceAnalyzer(prometheus_url, token, auth_client)
        self.cluster_api_analyzer = ClusterAPIPerformanceAnalyzer()
        
        # Performance scoring weights
        self.component_weights = {
            'cluster': 0.25,  # 25% - cluster basic health
            'api': 0.25,      # 25% - API server performance
            'ovnk': 0.30,     # 30% - OVNK components (most critical)
            'nodes': 0.20     # 20% - node resource utilization
        }
        
        logger.info(f"Initialized OverallPerformanceAnalyzer with URL={prometheus_url}")

    def _to_number(self, value: Any, default: float = 0.0) -> float:
        """Safely coerce a value to a float number, returning default on None or invalid."""
        try:
            if value is None:
                return default
            # Handle strings like "123" or "123.45"
            return float(value)
        except (TypeError, ValueError):
            return default
    
    async def collect_all_component_data(self, duration: str = "1h", 
                                       focus_areas: Optional[List[str]] = None) -> ComponentPerformanceResults:
        """
        Collect performance data from all components
        
        Args:
            duration: Analysis duration (e.g., "1h", "5m", "1d")
            focus_areas: Optional list of areas to focus on (e.g., ['cluster', 'api', 'ovnk'])
            
        Returns:
            Component performance results
        """
        logger.info(f"Collecting performance data for duration: {duration}")
        
        # Determine which components to collect based on focus areas
        collect_all = focus_areas is None
        collect_cluster = collect_all or 'cluster' in focus_areas
        collect_api = collect_all or 'api' in focus_areas
        collect_ovnk = collect_all or 'ovnk' in focus_areas
        collect_nodes = collect_all or 'nodes' in focus_areas
        collect_databases = collect_all or 'databases' in focus_areas
        collect_sync = collect_all or 'sync' in focus_areas
        
        tasks = []
        task_names = []
        
        # Cluster general information
        if collect_cluster:
            tasks.append(self._collect_cluster_general_info())
            task_names.append('cluster_general_info')
        
        # Node usage analysis
        if collect_nodes:
            tasks.append(self._collect_node_usage_analysis(duration))
            task_names.append('node_usage_analysis')
        
        # OVNK pods analysis
        if collect_ovnk:
            tasks.append(self._collect_ovnk_pods_analysis(duration))
            task_names.append('ovnk_pods_analysis')
            
            # OVNK containers analysis
            tasks.append(self._collect_ovnk_containers_analysis(duration))
            task_names.append('ovnk_containers_analysis')
        
        # Multus pods analysis
        if collect_ovnk:
            tasks.append(self._collect_multus_pods_analysis(duration))
            task_names.append('multus_pods_analysis')
        
        # OVS metrics analysis
        if collect_ovnk:
            tasks.append(self._collect_ovs_metrics_analysis(duration))
            task_names.append('ovs_metrics_analysis')
        
        # OVNK sync analysis
        if collect_sync or collect_ovnk:
            tasks.append(self._collect_ovnk_sync_analysis(duration))
            task_names.append('ovnk_sync_analysis')
        
        # API latency analysis
        if collect_api:
            tasks.append(self._collect_api_latency_analysis(duration))
            task_names.append('api_latency_analysis')
        
        # OVN basic info
        if collect_databases or collect_ovnk:
            tasks.append(self._collect_ovn_basic_info())
            task_names.append('ovn_basic_info')
        
        # Execute all collection tasks concurrently
        logger.info(f"Executing {len(tasks)} collection tasks concurrently...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle exceptions
        component_data = {}
        
        for i, (result, task_name) in enumerate(zip(results, task_names)):
            if isinstance(result, Exception):
                logger.error(f"Error collecting {task_name}: {result}")
                component_data[task_name] = {'error': str(result), 'collection_failed': True}
            else:
                component_data[task_name] = result
                logger.info(f"Successfully collected {task_name}")
        
        # Create ComponentPerformanceResults with all data (filling missing with empty dicts)
        return ComponentPerformanceResults(
            cluster_general_info=component_data.get('cluster_general_info', {}),
            node_usage_analysis=component_data.get('node_usage_analysis', {}),
            ovnk_pods_analysis=component_data.get('ovnk_pods_analysis', {}),
            ovnk_containers_analysis=component_data.get('ovnk_containers_analysis', {}),
            multus_pods_analysis=component_data.get('multus_pods_analysis', {}),
            ovs_metrics_analysis=component_data.get('ovs_metrics_analysis', {}),
            ovnk_sync_analysis=component_data.get('ovnk_sync_analysis', {}),
            api_latency_analysis=component_data.get('api_latency_analysis', {}),
            ovn_basic_info=component_data.get('ovn_basic_info', {})
        )
    
    async def _collect_cluster_general_info(self) -> Dict[str, Any]:
        """Collect cluster general information"""
        try:
            return await collect_cluster_information()
        except Exception as e:
            logger.error(f"Failed to collect cluster general info: {e}")
            return {'error': str(e)}
    
    async def _collect_node_usage_analysis(self, duration: str) -> Dict[str, Any]:
        """Collect node usage analysis"""
        try:
            async with PrometheusBaseQuery(self.prometheus_url, self.token) as client:
                collector = nodeUsageCollector(client, self.auth_client)
                return await collector.collect_usage_data(duration)
        except Exception as e:
            logger.error(f"Failed to collect node usage: {e}")
            return {'error': str(e)}
    
    async def _collect_ovnk_pods_analysis(self, duration: str) -> Dict[str, Any]:
        """Collect OVNK pods analysis"""
        try:
            async with PrometheusBaseQuery(self.prometheus_url, self.token) as client:
                return await collect_ovn_duration_usage(client, duration, self.auth_client)
        except Exception as e:
            logger.error(f"Failed to collect OVNK pods: {e}")
            return {'error': str(e)}
    
    async def _collect_ovnk_containers_analysis(self, duration: str) -> Dict[str, Any]:
        """Collect OVNK containers analysis"""
        try:
            async with PrometheusBaseQuery(self.prometheus_url, self.token) as client:
                collector = PodsUsageCollector(client, self.auth_client)
                return await collector.collect_duration_usage(
                    duration, 
                    pod_pattern="ovnkube.*",
                    container_pattern=".*",
                    namespace_pattern="openshift-ovn-kubernetes"
                )
        except Exception as e:
            logger.error(f"Failed to collect OVNK containers: {e}")
            return {'error': str(e)}
    
    async def _collect_multus_pods_analysis(self, duration: str) -> Dict[str, Any]:
        """Collect Multus pods analysis"""
        try:
            async with PrometheusBaseQuery(self.prometheus_url, self.token) as client:
                collector = PodsUsageCollector(client, self.auth_client)
                return await collector.collect_duration_usage(
                    duration,
                    pod_pattern="multus.*",
                    container_pattern=".*",
                    namespace_pattern="openshift-multus"
                )
        except Exception as e:
            logger.error(f"Failed to collect Multus pods: {e}")
            return {'error': str(e)}
    
    async def _collect_ovs_metrics_analysis(self, duration: str) -> Dict[str, Any]:
        """Collect OVS metrics analysis"""
        try:
            async with PrometheusBaseQuery(self.prometheus_url, self.token) as client:
                collector = OVSUsageCollector(client, self.auth_client)
                return await collector.collect_all_ovs_metrics(duration)
        except Exception as e:
            logger.error(f"Failed to collect OVS metrics: {e}")
            return {'error': str(e)}
    
    async def _collect_ovnk_sync_analysis(self, duration: str) -> Dict[str, Any]:
        """Collect OVNK sync analysis"""
        try:
            async with PrometheusBaseQuery(self.prometheus_url, self.token) as client:
                collector = OVNLatencyCollector(client)
                return await collector.collect_sync_duration_seconds_metrics(duration)
        except Exception as e:
            logger.error(f"Failed to collect OVNK sync: {e}")
            return {'error': str(e)}
    
    async def _collect_api_latency_analysis(self, duration: str) -> Dict[str, Any]:
        """Collect API latency analysis"""
        try:
            async with PrometheusBaseQuery(self.prometheus_url, self.token) as client:
                api_metrics = kubeAPICollector(client)
                return await api_metrics.get_metrics(duration)
        except Exception as e:
            logger.error(f"Failed to collect API latency: {e}")
            return {'error': str(e)}
    
    async def _collect_ovn_basic_info(self) -> Dict[str, Any]:
        """Collect OVN basic information"""
        try:
            result = {}
            
            # Collect database sizes
            collector = ovnBasicInfoCollector(self.prometheus_url, self.token)
            db_metrics = await collector.collect_max_values()
            result['database_metrics'] = db_metrics
            
            # Collect pod phase counts
            pod_status = await get_pod_phase_counts(self.prometheus_url, self.token)
            result['pod_status'] = pod_status
            
            return result
        except Exception as e:
            logger.error(f"Failed to collect OVN basic info: {e}")
            return {'error': str(e)}
    
    def _calculate_component_scores(self, component_results: ComponentPerformanceResults) -> Dict[str, float]:
        """Calculate health scores for each component"""
        scores = {}
        
        # Cluster score based on general info and basic health
        cluster_score = 100.0
        cluster_info_obj = component_results.cluster_general_info
        if (isinstance(cluster_info_obj, dict) and 'error' in cluster_info_obj) or not isinstance(cluster_info_obj, dict):
            cluster_score = 0.0
        else:
            # Check for cluster operators issues
            operators_status = cluster_info_obj.get('cluster_operators_status', {}) if isinstance(cluster_info_obj, dict) else {}
            if operators_status:
                degraded_count = len([op for op in operators_status.values() if isinstance(op, dict) and op.get('degraded')])
                unavailable_count = len([op for op in operators_status.values() if isinstance(op, dict) and not op.get('available')])
                total_operators = len(operators_status)
                
                if total_operators > 0:
                    cluster_score -= (degraded_count / total_operators) * 30  # 30 points for degraded
                    cluster_score -= (unavailable_count / total_operators) * 50  # 50 points for unavailable
        
        scores['cluster'] = max(0.0, cluster_score)
        
        # API score based on latency and error rates
        api_score = 100.0
        if isinstance(component_results.api_latency_analysis, dict) and 'error' in component_results.api_latency_analysis:
            api_score = 0.0
        else:
            api_metrics = component_results.api_latency_analysis.get('metrics', {}) if isinstance(component_results.api_latency_analysis, dict) else {}
            
            # Check read-only API latency
            if isinstance(api_metrics, dict) and 'avg_ro_apicalls_latency' in api_metrics and isinstance(api_metrics.get('avg_ro_apicalls_latency'), dict):
                ro_stats = api_metrics['avg_ro_apicalls_latency'].get('statistics', {}) if isinstance(api_metrics['avg_ro_apicalls_latency'], dict) else {}
                p99_latency = self._to_number(ro_stats.get('p99', 0), 0.0)
                if p99_latency > 2.0:  # 2 seconds
                    api_score -= 40
                elif p99_latency > 1.0:  # 1 second
                    api_score -= 20
            
            # Check mutating API latency
            if isinstance(api_metrics, dict) and 'avg_mutating_apicalls_latency' in api_metrics and isinstance(api_metrics.get('avg_mutating_apicalls_latency'), dict):
                mut_stats = api_metrics['avg_mutating_apicalls_latency'].get('statistics', {}) if isinstance(api_metrics['avg_mutating_apicalls_latency'], dict) else {}
                p99_latency = self._to_number(mut_stats.get('p99', 0), 0.0)
                if p99_latency > 2.0:
                    api_score -= 40
                elif p99_latency > 1.0:
                    api_score -= 20
        
        scores['api'] = max(0.0, api_score)
        
        # OVNK score based on pods, sync, and OVS performance
        ovnk_score = 100.0
        ovnk_issues = 0
        
        # Check OVNK pods
        if 'error' in component_results.ovnk_pods_analysis:
            ovnk_issues += 1
        
        # Check sync performance
        if isinstance(component_results.ovnk_sync_analysis, dict) and 'error' in component_results.ovnk_sync_analysis:
            ovnk_issues += 1
        else:
            sync_data = component_results.ovnk_sync_analysis if isinstance(component_results.ovnk_sync_analysis, dict) else {}
            top_10_overall = sync_data.get('top_10_overall', []) if isinstance(sync_data, dict) else []
            slow_syncs = [s for s in top_10_overall if self._to_number(s.get('max_value', 0), 0.0) > 5.0]
            if len(slow_syncs) > 3:
                ovnk_score -= 30
        
        # Check OVS metrics
        if 'error' in component_results.ovs_metrics_analysis:
            ovnk_issues += 1
        
        # Apply penalties for component issues
        ovnk_score -= ovnk_issues * 25
        scores['ovnk'] = max(0.0, ovnk_score)
        
        # Nodes score based on resource utilization
        nodes_score = 100.0
        if isinstance(component_results.node_usage_analysis, dict) and 'error' in component_results.node_usage_analysis:
            nodes_score = 0.0
        else:
            node_groups = component_results.node_usage_analysis.get('node_groups', {}) if isinstance(component_results.node_usage_analysis, dict) else {}
            total_nodes = 0
            high_usage_nodes = 0
            
            for group_name, group_data in (node_groups.items() if isinstance(node_groups, dict) else []):
                nodes = group_data.get('nodes', []) if isinstance(group_data, dict) else []
                total_nodes += len(nodes)
                
                for node in nodes:
                    metrics = node.get('metrics', {}) if isinstance(node, dict) else {}
                    cpu_usage = self._to_number((metrics.get('cpu_usage', {}).get('avg', 0) if isinstance(metrics.get('cpu_usage', {}), dict) else 0), 0.0)
                    memory_usage = self._to_number((metrics.get('memory_usage', {}).get('avg', 0) if isinstance(metrics.get('memory_usage', {}), dict) else 0), 0.0)
                    
                    if cpu_usage > 80 or memory_usage > 8192:  # 80% CPU or 8GB memory
                        high_usage_nodes += 1
            
            if total_nodes > 0:
                usage_ratio = high_usage_nodes / total_nodes
                nodes_score -= usage_ratio * 60  # Penalty based on high usage ratio
        
        scores['nodes'] = max(0.0, nodes_score)
        
        return scores
    
    def _calculate_overall_health_score(self, component_scores: Dict[str, float]) -> OverallHealthScore:
        """Calculate overall health score from component scores"""
        
        # Calculate weighted total score
        total_score = sum(
            component_scores.get(component, 0) * weight
            for component, weight in self.component_weights.items()
        )
        
        # Determine health level
        if total_score >= 90:
            health_level = "excellent"
        elif total_score >= 75:
            health_level = "good"
        elif total_score >= 60:
            health_level = "moderate"
        elif total_score >= 40:
            health_level = "poor"
        else:
            health_level = "critical"
        
        return OverallHealthScore(
            total_score=round(total_score, 2),
            cluster_score=round(component_scores.get('cluster', 0), 2),
            api_score=round(component_scores.get('api', 0), 2),
            ovnk_score=round(component_scores.get('ovnk', 0), 2),
            nodes_score=round(component_scores.get('nodes', 0), 2),
            health_level=health_level,
            critical_issues_count=0,  # Will be updated by analysis
            warning_issues_count=0    # Will be updated by analysis
        )
    
    def _identify_critical_issues(self, component_results: ComponentPerformanceResults, 
                                component_scores: Dict[str, float]) -> List[str]:
        """Identify critical issues requiring immediate attention"""
        critical_issues = []
        
        # Cluster critical issues
        if component_scores.get('cluster', 0) < 50:
            critical_issues.append("CRITICAL: Cluster health score below 50% - multiple operators may be degraded")
        
        # API critical issues
        if component_scores.get('api', 0) < 50:
            api_metrics = component_results.api_latency_analysis.get('metrics', {}) if isinstance(component_results.api_latency_analysis, dict) else {}
            if isinstance(api_metrics, dict) and 'avg_ro_apicalls_latency' in api_metrics and isinstance(api_metrics.get('avg_ro_apicalls_latency'), dict):
                p99 = api_metrics['avg_ro_apicalls_latency'].get('statistics', {}).get('p99', 0) if isinstance(api_metrics['avg_ro_apicalls_latency'].get('statistics', {}), dict) else 0
                if p99 > 2.0:
                    critical_issues.append(f"CRITICAL: API read-only latency P99 is {p99:.3f}s (threshold: 2.0s)")
            
            if isinstance(api_metrics, dict) and 'avg_mutating_apicalls_latency' in api_metrics and isinstance(api_metrics.get('avg_mutating_apicalls_latency'), dict):
                p99 = api_metrics['avg_mutating_apicalls_latency'].get('statistics', {}).get('p99', 0) if isinstance(api_metrics['avg_mutating_apicalls_latency'].get('statistics', {}), dict) else 0
                if p99 > 2.0:
                    critical_issues.append(f"CRITICAL: API mutating latency P99 is {p99:.3f}s (threshold: 2.0s)")
        
        # OVNK critical issues
        if component_scores.get('ovnk', 0) < 50:
            critical_issues.append("CRITICAL: OVNK components health below 50% - network performance severely impacted")
            
            # Check database sizes
            db_metrics = component_results.ovn_basic_info.get('database_metrics', {}) if isinstance(component_results.ovn_basic_info, dict) else {}
            for db_name, db_info in (db_metrics.items() if isinstance(db_metrics, dict) else []):
                if isinstance(db_info, dict) and 'error' not in db_info and db_info.get('max_value', 0) > 500 * 1024 * 1024:  # 500MB
                    size_mb = db_info.get('max_value', 0) / (1024 * 1024)
                    critical_issues.append(f"CRITICAL: {db_name} database size ({size_mb:.1f}MB) exceeds 500MB threshold")
        
        # Node critical issues
        if component_scores.get('nodes', 0) < 50:
            critical_issues.append("CRITICAL: Multiple nodes under severe resource pressure")
        
        return critical_issues
    
    def _identify_warning_issues(self, component_results: ComponentPerformanceResults, 
                               component_scores: Dict[str, float]) -> List[str]:
        """Identify warning issues requiring attention"""
        warning_issues = []
        
        # Cluster warning issues
        if 50 <= component_scores.get('cluster', 100) < 75:
            warning_issues.append("WARNING: Cluster health needs attention - some operators may be degraded")
        
        # API warning issues
        if 50 <= component_scores.get('api', 100) < 75:
            warning_issues.append("WARNING: API server performance is below optimal levels")
        
        # OVNK warning issues
        if 50 <= component_scores.get('ovnk', 100) < 75:
            warning_issues.append("WARNING: OVNK components showing performance degradation")
        
        # Node warning issues
        if 50 <= component_scores.get('nodes', 100) < 75:
            warning_issues.append("WARNING: Some nodes experiencing elevated resource usage")
        
        # Check for specific warning conditions
        pod_status = component_results.ovn_basic_info.get('pod_status', {}) if isinstance(component_results.ovn_basic_info, dict) else {}
        if isinstance(pod_status, dict) and 'error' not in pod_status:
            pending_pods = (pod_status.get('Pending', {}).get('count', 0) if isinstance(pod_status.get('Pending', {}), dict) else 0)
            failed_pods = (pod_status.get('Failed', {}).get('count', 0) if isinstance(pod_status.get('Failed', {}), dict) else 0)
            
            if pending_pods > 10:
                warning_issues.append(f"WARNING: {pending_pods} pods in Pending state")
            if failed_pods > 5:
                warning_issues.append(f"WARNING: {failed_pods} pods in Failed state")
        
        return warning_issues
    
    def _generate_overall_recommendations(self, component_results: ComponentPerformanceResults,
                                        component_scores: Dict[str, float],
                                        critical_issues: List[str],
                                        warning_issues: List[str]) -> List[str]:
        """Generate overall recommendations based on analysis"""
        recommendations = []
        
        # Critical recommendations
        if critical_issues:
            recommendations.append("IMMEDIATE ACTION REQUIRED: Address critical issues before they impact cluster stability")
            
            if any("API" in issue for issue in critical_issues):
                recommendations.append("Investigate API server and etcd performance immediately")
            
            if any("OVNK" in issue for issue in critical_issues):
                recommendations.append("Check OVNK pod resource limits and node capacity")
                recommendations.append("Review network policy complexity and OVN database optimization")
            
            if any("database size" in issue for issue in critical_issues):
                recommendations.append("Plan OVN database maintenance and cleanup procedures")
        
        # Performance optimization recommendations
        if component_scores.get('ovnk', 100) < 80:
            recommendations.append("Consider OVNK performance tuning and resource optimization")
        
        if component_scores.get('nodes', 100) < 80:
            recommendations.append("Review node resource allocation and consider cluster scaling")
        
        if component_scores.get('api', 100) < 80:
            recommendations.append("Monitor API server performance and consider optimizing client request patterns")
        
        # Proactive recommendations
        if not critical_issues and not warning_issues:
            recommendations.append("Cluster health is good - continue regular monitoring")
            recommendations.append("Consider establishing baseline performance metrics for trend analysis")
        elif warning_issues and not critical_issues:
            recommendations.append("Address warning issues during next maintenance window")
        
        # General best practices
        recommendations.append("Monitor trends over time to identify performance degradation early")
        recommendations.append("Ensure resource requests and limits are properly configured for OVNK pods")
        
        return recommendations[:8]  # Limit to top 8 recommendations
    
    def _create_performance_summary(self, component_results: ComponentPerformanceResults,
                                  component_scores: Dict[str, float],
                                  overall_health: OverallHealthScore) -> Dict[str, Any]:
        """Create performance summary"""
        
        # Extract key metrics
        summary = {
            'overall_health_score': overall_health.total_score,
            'health_level': overall_health.health_level,
            'component_scores': {
                'cluster': component_scores.get('cluster', 0),
                'api': component_scores.get('api', 0),
                'ovnk': component_scores.get('ovnk', 0),
                'nodes': component_scores.get('nodes', 0)
            },
            'key_metrics': {},
            'resource_utilization': {},
            'network_performance': {}
        }
        
        # Extract key cluster metrics
        cluster_info = component_results.cluster_general_info
        if isinstance(cluster_info, dict) and 'error' not in cluster_info:
            summary['key_metrics']['total_nodes'] = cluster_info.get('total_nodes', 0)
            summary['key_metrics']['total_pods'] = cluster_info.get('pods_count', 0)
            summary['key_metrics']['networkpolicies_count'] = cluster_info.get('networkpolicies_count', 0)
            summary['key_metrics']['adminnetworkpolicies_count'] = cluster_info.get('adminnetworkpolicies_count', 0)
        
        # Extract node utilization
        node_analysis = component_results.node_usage_analysis
        if isinstance(node_analysis, dict) and 'error' not in node_analysis:
            node_groups = node_analysis.get('node_groups', {}) if isinstance(node_analysis, dict) else {}
            total_cpu_avg = 0
            total_memory_avg = 0
            node_count = 0
            
            for group_data in (node_groups.values() if isinstance(node_groups, dict) else []):
                for node in (group_data.get('nodes', []) if isinstance(group_data, dict) else []):
                    metrics = node.get('metrics', {}) if isinstance(node, dict) else {}
                    cpu_avg = self._to_number((metrics.get('cpu_usage', {}).get('avg', 0) if isinstance(metrics.get('cpu_usage', {}), dict) else 0), 0.0)
                    memory_avg = self._to_number((metrics.get('memory_usage', {}).get('avg', 0) if isinstance(metrics.get('memory_usage', {}), dict) else 0), 0.0)
                    
                    total_cpu_avg = self._to_number(total_cpu_avg, 0.0) + cpu_avg
                    total_memory_avg = self._to_number(total_memory_avg, 0.0) + memory_avg
                    node_count = int(self._to_number(node_count, 0.0) + 1)
            
            if node_count > 0:
                summary['resource_utilization']['avg_cpu_percent'] = round(total_cpu_avg / node_count, 2)
                summary['resource_utilization']['avg_memory_gb'] = round((total_memory_avg / node_count) / 1024, 2)
        
        # Extract API performance
        api_analysis = component_results.api_latency_analysis
        if isinstance(api_analysis, dict) and 'error' not in api_analysis:
            api_metrics = api_analysis.get('metrics', {}) if isinstance(api_analysis, dict) else {}
            
            if isinstance(api_metrics, dict) and 'avg_ro_apicalls_latency' in api_metrics and isinstance(api_metrics.get('avg_ro_apicalls_latency'), dict):
                ro_stats = api_metrics['avg_ro_apicalls_latency'].get('statistics', {}) if isinstance(api_metrics['avg_ro_apicalls_latency'], dict) else {}
                summary['network_performance']['readonly_api_p99_ms'] = round(ro_stats.get('p99', 0) * 1000, 2)
            
            if isinstance(api_metrics, dict) and 'avg_mutating_apicalls_latency' in api_metrics and isinstance(api_metrics.get('avg_mutating_apicalls_latency'), dict):
                mut_stats = api_metrics['avg_mutating_apicalls_latency'].get('statistics', {}) if isinstance(api_metrics['avg_mutating_apicalls_latency'], dict) else {}
                summary['network_performance']['mutating_api_p99_ms'] = round(mut_stats.get('p99', 0) * 1000, 2)
        
        # Extract OVN database sizes
        ovn_basic = component_results.ovn_basic_info
        if isinstance(ovn_basic, dict) and 'error' not in ovn_basic and 'database_metrics' in ovn_basic:
            db_metrics = ovn_basic['database_metrics'] if isinstance(ovn_basic.get('database_metrics'), dict) else {}
            
            for db_name, db_info in (db_metrics.items() if isinstance(db_metrics, dict) else []):
                if isinstance(db_info, dict) and 'error' not in db_info and db_info.get('max_value') is not None:
                    size_mb = db_info.get('max_value', 0) / (1024 * 1024)
                    summary['network_performance'][f'{db_name}_db_size_mb'] = round(size_mb, 2)
        
        return summary
    
    async def analyze_overall_performance(self, duration: str = "1h", 
                                        include_detailed_analysis: bool = True,
                                        focus_areas: Optional[List[str]] = None) -> OverallAnalysisResult:
        """
        Perform comprehensive overall performance analysis of the entire OCP/OVNK cluster
        
        Args:
            duration: Analysis duration (e.g., "1h", "5m", "1d")
            include_detailed_analysis: Whether to include detailed component analysis
            focus_areas: Optional list of focus areas to analyze
            
        Returns:
            Complete overall analysis result
        """
        analysis_start_time = datetime.now(timezone.utc)
        
        logger.info(f"Starting overall performance analysis for duration: {duration}")
        
        try:
            # Collect all component data
            component_results = await self.collect_all_component_data(duration, focus_areas)
            
            # Calculate component scores
            component_scores = self._calculate_component_scores(component_results)
            
            # Calculate overall health score
            overall_health = self._calculate_overall_health_score(component_scores)
            
            # Identify issues
            critical_issues = self._identify_critical_issues(component_results, component_scores)
            warning_issues = self._identify_warning_issues(component_results, component_scores)
            
            # Update health score with issue counts
            overall_health.critical_issues_count = len(critical_issues)
            overall_health.warning_issues_count = len(warning_issues)
            
            # Generate recommendations
            recommendations = self._generate_overall_recommendations(
                component_results, component_scores, critical_issues, warning_issues
            )
            
            # Create performance summary
            performance_summary = self._create_performance_summary(
                component_results, component_scores, overall_health
            )
            
            # Calculate execution metrics
            analysis_end_time = datetime.now(timezone.utc)
            execution_duration = (analysis_end_time - analysis_start_time).total_seconds()
            
            execution_metrics = {
                'total_duration_seconds': round(execution_duration, 2),
                'collection_start_time': analysis_start_time.isoformat(),
                'collection_end_time': analysis_end_time.isoformat(),
                'components_collected': len([
                    name for name, data in asdict(component_results).items() 
                    if data and 'error' not in data
                ]),
                'components_failed': len([
                    name for name, data in asdict(component_results).items() 
                    if data and 'error' in data
                ])
            }
            
            # Create analysis metadata
            analysis_metadata = {
                'analysis_type': 'overall_performance',
                'analysis_timestamp': analysis_start_time.isoformat(),
                'duration': duration,
                'focus_areas': focus_areas,
                'include_detailed_analysis': include_detailed_analysis,
                'prometheus_url_masked': self.prometheus_url.replace(self.token or '', '***') if self.token else self.prometheus_url
            }
            
            result = OverallAnalysisResult(
                analysis_metadata=analysis_metadata,
                overall_health=overall_health,
                component_results=component_results,
                performance_summary=performance_summary,
                critical_issues=critical_issues,
                warning_issues=warning_issues,
                recommendations=recommendations,
                execution_metrics=execution_metrics
            )
            
            logger.info(f"Overall analysis completed in {execution_duration:.2f}s. Health: {overall_health.health_level} ({overall_health.total_score}%)")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in overall performance analysis: {e}")
            raise
    
    def to_json_result(self, analysis_result: OverallAnalysisResult) -> Dict[str, Any]:
        """Convert analysis result to JSON-serializable dictionary"""
        return {
            'analysis_metadata': analysis_result.analysis_metadata,
            'overall_health': asdict(analysis_result.overall_health),
            'performance_summary': analysis_result.performance_summary,
            'critical_issues': analysis_result.critical_issues,
            'warning_issues': analysis_result.warning_issues,
            'recommendations': analysis_result.recommendations,
            'execution_metrics': analysis_result.execution_metrics,
            'detailed_component_results': asdict(analysis_result.component_results) if analysis_result.analysis_metadata.get('include_detailed_analysis', True) else None
        }


# Convenience function for direct usage with authentication
async def analyze_overall_performance_with_auth(duration: str = "1h",
                                              include_detailed_analysis: bool = True,
                                              focus_areas: Optional[List[str]] = None,
                                              auth_client: Optional[OpenShiftAuth] = None) -> Dict[str, Any]:
    """
    Analyze overall performance with automatic authentication setup
    
    Args:
        duration: Analysis duration
        include_detailed_analysis: Whether to include detailed analysis
        focus_areas: Optional focus areas list
        auth_client: Optional authentication client
        
    Returns:
        JSON-serializable analysis results
    """
    if not auth_client:
        auth_client = OpenShiftAuth()
        await auth_client.initialize()
    
    analyzer = OverallPerformanceAnalyzer(
        prometheus_url=auth_client.prometheus_url,
        token=auth_client.prometheus_token,
        auth_client=auth_client
    )
    
    analysis_result = await analyzer.analyze_overall_performance(
        duration=duration,
        include_detailed_analysis=include_detailed_analysis,
        focus_areas=focus_areas
    )
    
    return analyzer.to_json_result(analysis_result)


# Example usage and testing
async def main():
    """Main function for testing the overall performance analyzer"""
    try:
        # Initialize authentication
        auth = OpenShiftAuth()
        await auth.initialize()
        
        # Test connection
        if not await auth.test_prometheus_connection():
            print("Cannot connect to Prometheus")
            return
        
        print("Starting Overall Performance Analysis...")
        
        # Test with different configurations
        test_configs = [
            {"duration": "5m", "include_detailed_analysis": True, "focus_areas": None},
            {"duration": "1h", "include_detailed_analysis": True, "focus_areas": ["cluster", "api", "ovnk"]},
            {"duration": "30m", "include_detailed_analysis": False, "focus_areas": ["nodes", "ovnk"]}
        ]
        
        for i, config in enumerate(test_configs, 1):
            print(f"\n=== TEST {i}: {config} ===")
            
            result = await analyze_overall_performance_with_auth(**config)
            
            # Print summary
            print(f"Overall Health: {result['overall_health']['health_level'].upper()} ({result['overall_health']['total_score']}%)")
            print(f"Critical Issues: {len(result['critical_issues'])}")
            print(f"Warning Issues: {len(result['warning_issues'])}")
            print(f"Analysis Duration: {result['execution_metrics']['total_duration_seconds']}s")
            
            if result['critical_issues']:
                print("Critical Issues:")
                for issue in result['critical_issues'][:3]:
                    print(f"  - {issue}")
            
            print("Top Recommendations:")
            for rec in result['recommendations'][:3]:
                print(f"  - {rec}")
        
        print("\nOverall Performance Analysis testing completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        print(f"Error in analysis: {e}")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    asyncio.run(main())