#!/usr/bin/env python3
"""
OVNK Benchmark Performance Analysis - Clusters & API
Comprehensive analysis module for OpenShift cluster performance including:
- Cluster basic information and health
- Node performance analysis grouped by role (master/worker/infra)
- API server latency and performance analysis
- Integrated recommendations and alerts
"""

import asyncio
import json
import logging
import statistics
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict

# Import required modules
from tools.ovnk_benchmark_openshift_cluster_info import OpenShiftGeneralInfo, NodeInfo, ClusterInfo
from tools.ovnk_benchmark_prometheus_nodes_usage import nodeUsageCollector
from tools.ovnk_benchmark_prometheus_kubeapi import kubeAPICollector
from analysis.ovnk_benchmark_performance_utility import (
    BasePerformanceAnalyzer, PerformanceLevel, AlertLevel, ResourceType,
    PerformanceThreshold, PerformanceAlert, AnalysisMetadata, ClusterHealth,
    ThresholdClassifier, RecommendationEngine, StatisticsCalculator,
    MemoryConverter
)

logger = logging.getLogger(__name__)


@dataclass
class NodeGroupAnalysis:
    """Analysis results for a node group (master/worker/infra)"""
    role: str
    node_count: int
    nodes: List[Dict[str, Any]]
    performance_summary: Dict[str, Any]
    alerts: List[PerformanceAlert]
    recommendations: List[str]
    health_score: float


@dataclass
class APIPerformanceAnalysis:
    """API server performance analysis results"""
    latency_analysis: Dict[str, Any]
    request_patterns: Dict[str, Any]
    performance_alerts: List[PerformanceAlert]
    recommendations: List[str]
    health_score: float


@dataclass
class ClusterAnalysisResult:
    """Complete cluster analysis result"""
    metadata: AnalysisMetadata
    cluster_basic_info: Dict[str, Any]
    node_groups_analysis: Dict[str, NodeGroupAnalysis]
    api_performance_analysis: APIPerformanceAnalysis
    overall_health: ClusterHealth
    critical_issues: List[str]
    recommendations: List[str]


class ClusterAPIPerformanceAnalyzer(BasePerformanceAnalyzer):
    """Main analyzer for cluster and API performance"""
    
    def __init__(self):
        super().__init__("Cluster_API_Performance")
        
        # Performance thresholds
        self.cpu_threshold = ThresholdClassifier.get_default_cpu_threshold()
        self.memory_threshold = ThresholdClassifier.get_default_memory_threshold()
        
        # API latency thresholds (in seconds)
        self.api_latency_threshold = PerformanceThreshold(
            excellent_max=0.1,    # 100ms
            good_max=0.5,         # 500ms
            moderate_max=1.0,     # 1s
            poor_max=2.0,         # 2s
            unit="seconds",
            component_type="api_latency"
        )
        
        # Request rate thresholds (requests/second)
        self.request_rate_threshold = PerformanceThreshold(
            excellent_max=100,
            good_max=500,
            moderate_max=1000,
            poor_max=2000,
            unit="requests/second",
            component_type="request_rate"
        )

    def analyze_metrics_data(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Satisfy abstract base API with a quick, synchronous metrics analysis.

        This method provides a lightweight summary that can be used by generic
        callers of the base analyzer API. For comprehensive analysis, prefer
        the async analyze_cluster_performance entry point.
        """
        alerts: List[PerformanceAlert] = []
        summary: Dict[str, Any] = {
            'analysis_type': 'generic_metrics_quick_analysis',
            'findings': {},
        }

        try:
            # Basic API latency quick check if present
            metrics = metrics_data.get('metrics', {}) if isinstance(metrics_data, dict) else {}

            # Read-only API latency
            ro_stats = metrics.get('avg_ro_apicalls_latency', {}).get('statistics', {})
            if ro_stats:
                p99 = ro_stats.get('p99', 0)
                level, severity = ThresholdClassifier.classify_performance(p99, self.api_latency_threshold)
                summary['findings']['readonly_api_p99_seconds'] = round(p99, 4)
                summary['findings']['readonly_api_level'] = level.value
                if level in [PerformanceLevel.CRITICAL, PerformanceLevel.POOR]:
                    alerts.append(PerformanceAlert(
                        severity=AlertLevel.CRITICAL if level == PerformanceLevel.CRITICAL else AlertLevel.HIGH,
                        resource_type=ResourceType.NETWORK,
                        pod_name='kube-apiserver',
                        message=f'Read-only API P99 latency high: {p99:.3f}s',
                        current_value=p99,
                        threshold_value=self.api_latency_threshold.good_max,
                        unit='seconds',
                        timestamp=datetime.now(timezone.utc).isoformat()
                    ))

            # Mutating API latency
            mut_stats = metrics.get('avg_mutating_apicalls_latency', {}).get('statistics', {})
            if mut_stats:
                p99m = mut_stats.get('p99', 0)
                levelm, severitym = ThresholdClassifier.classify_performance(p99m, self.api_latency_threshold)
                summary['findings']['mutating_api_p99_seconds'] = round(p99m, 4)
                summary['findings']['mutating_api_level'] = levelm.value
                if levelm in [PerformanceLevel.CRITICAL, PerformanceLevel.POOR]:
                    alerts.append(PerformanceAlert(
                        severity=AlertLevel.CRITICAL if levelm == PerformanceLevel.CRITICAL else AlertLevel.HIGH,
                        resource_type=ResourceType.NETWORK,
                        pod_name='kube-apiserver',
                        message=f'Mutating API P99 latency high: {p99m:.3f}s',
                        current_value=p99m,
                        threshold_value=self.api_latency_threshold.good_max,
                        unit='seconds',
                        timestamp=datetime.now(timezone.utc).isoformat()
                    ))

            # Compute a simple health score based on alerts (use base implementation)
            health = super().calculate_cluster_health_score(alerts, max(1, len(alerts)))
            return {
                'summary': summary,
                'alerts': [asdict(a) for a in alerts],
                'health': asdict(health)
            }
        except Exception:
            # Fail-safe minimal response
            return {
                'summary': summary,
                'alerts': [],
                'health': asdict(super().calculate_cluster_health_score([], 1))
            }
    
    async def analyze_cluster_performance(self, 
                                        cluster_info: ClusterInfo,
                                        node_usage_data: Dict[str, Any],
                                        api_metrics_data: Dict[str, Any],
                                        duration: str = "1h") -> ClusterAnalysisResult:
        """
        Comprehensive cluster performance analysis
        
        Args:
            cluster_info: Basic cluster information
            node_usage_data: Node usage metrics from Prometheus
            api_metrics_data: API server metrics from Prometheus
            duration: Analysis duration
            
        Returns:
            Complete analysis result
        """
        try:
            logger.info("Starting comprehensive cluster performance analysis...")
            
            # Generate metadata
            total_nodes = cluster_info.total_nodes
            metadata = self.generate_metadata("cluster_api_performance", total_nodes, duration)
            
            # Extract basic cluster information
            cluster_basic_info = self._extract_cluster_basic_info(cluster_info)
            
            # Analyze node groups performance
            node_groups_analysis = await self._analyze_node_groups(node_usage_data, cluster_info)
            
            # Analyze API performance
            api_performance_analysis = await self._analyze_api_performance(api_metrics_data)
            
            # Collect all alerts
            all_alerts = []
            for group_analysis in node_groups_analysis.values():
                all_alerts.extend(group_analysis.alerts)
            all_alerts.extend(api_performance_analysis.performance_alerts)
            
            # Calculate overall health
            overall_health = self.calculate_cluster_health_score(all_alerts, total_nodes)
            
            # Generate critical issues and recommendations
            critical_issues = self._extract_critical_issues(all_alerts, node_groups_analysis, api_performance_analysis)
            recommendations = self._generate_comprehensive_recommendations(
                node_groups_analysis, api_performance_analysis, overall_health
            )
            
            result = ClusterAnalysisResult(
                metadata=metadata,
                cluster_basic_info=cluster_basic_info,
                node_groups_analysis=node_groups_analysis,
                api_performance_analysis=api_performance_analysis,
                overall_health=overall_health,
                critical_issues=critical_issues,
                recommendations=recommendations
            )
            
            logger.info("Cluster performance analysis completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Failed to analyze cluster performance: {e}")
            raise
    
    def _extract_cluster_basic_info(self, cluster_info: ClusterInfo) -> Dict[str, Any]:
        """Extract basic cluster information"""
        return {
            'cluster_name': cluster_info.cluster_name,
            'cluster_version': cluster_info.cluster_version,
            'platform': cluster_info.platform,
            'api_server_url': cluster_info.api_server_url,
            'total_nodes': cluster_info.total_nodes,
            'node_distribution': {
                'master_nodes': len(cluster_info.master_nodes),
                'infra_nodes': len(cluster_info.infra_nodes),
                'worker_nodes': len(cluster_info.worker_nodes)
            },
            'resource_counts': {
                'namespaces': cluster_info.namespaces_count,
                'pods': cluster_info.pods_count,
                'services': cluster_info.services_count,
                'secrets': cluster_info.secrets_count,
                'configmaps': cluster_info.configmaps_count,
                'networkpolicies': cluster_info.networkpolicies_count,
                'adminnetworkpolicies': cluster_info.adminnetworkpolicies_count,
                'egressfirewalls': cluster_info.egressfirewalls_count
            },
            'cluster_operators_status': cluster_info.cluster_operators_status,
            'collection_timestamp': cluster_info.collection_timestamp
        }
    
    async def _analyze_node_groups(self, node_usage_data: Dict[str, Any], cluster_info: ClusterInfo) -> Dict[str, NodeGroupAnalysis]:
        """Analyze performance for each node group"""
        node_groups_analysis = {}
        
        if 'node_groups' not in node_usage_data:
            logger.warning("No node groups data found in usage data")
            return node_groups_analysis
        
        for role, group_data in node_usage_data['node_groups'].items():
            if not group_data.get('nodes'):
                continue
                
            logger.info(f"Analyzing {role} nodes ({len(group_data['nodes'])} nodes)")
            
            # Analyze each node in the group
            analyzed_nodes = []
            group_alerts = []
            
            for node_data in group_data['nodes']:
                node_analysis = self._analyze_single_node(node_data, role)
                analyzed_nodes.append(node_analysis['analysis'])
                group_alerts.extend(node_analysis['alerts'])
            
            # Calculate group performance summary
            performance_summary = self._calculate_group_performance_summary(group_data, analyzed_nodes)
            
            # Generate group recommendations
            recommendations = self._generate_node_group_recommendations(role, performance_summary, group_alerts)
            
            # Calculate group health score
            health_score = self._calculate_group_health_score(group_alerts, len(group_data['nodes']))
            
            node_groups_analysis[role] = NodeGroupAnalysis(
                role=role,
                node_count=len(group_data['nodes']),
                nodes=analyzed_nodes,
                performance_summary=performance_summary,
                alerts=group_alerts,
                recommendations=recommendations,
                health_score=health_score
            )
        
        return node_groups_analysis
    
    def _analyze_single_node(self, node_data: Dict[str, Any], role: str) -> Dict[str, Any]:
        """Analyze performance of a single node"""
        node_name = node_data.get('node_name', node_data.get('instance', 'unknown'))
        metrics = node_data.get('metrics', {})
        
        analysis = {
            'node_name': node_name,
            'instance': node_data.get('instance', ''),
            'role': role,
            'performance_levels': {},
            'resource_usage': {},
            'kubernetes_labels': node_data.get('kubernetes_labels', {})
        }
        
        alerts = []
        
        # Analyze CPU usage
        if 'cpu_usage' in metrics and metrics['cpu_usage'].get('avg') is not None:
            cpu_avg = metrics['cpu_usage']['avg']
            cpu_max = metrics['cpu_usage']['max']
            
            cpu_level, cpu_severity = ThresholdClassifier.classify_performance(cpu_avg, self.cpu_threshold)
            
            analysis['performance_levels']['cpu'] = {
                'level': cpu_level.value,
                'severity_score': round(cpu_severity, 2)
            }
            
            analysis['resource_usage']['cpu'] = {
                'avg_percent': cpu_avg,
                'max_percent': cpu_max,
                'unit': '%'
            }
            
            # Generate CPU alerts
            if cpu_level in [PerformanceLevel.CRITICAL, PerformanceLevel.POOR]:
                severity = AlertLevel.CRITICAL if cpu_level == PerformanceLevel.CRITICAL else AlertLevel.HIGH
                alerts.append(PerformanceAlert(
                    severity=severity,
                    resource_type=ResourceType.CPU,
                    pod_name=node_name,
                    node_name=node_name,
                    message=f"High CPU usage: {cpu_avg}%",
                    current_value=cpu_avg,
                    threshold_value=self.cpu_threshold.poor_max if cpu_level == PerformanceLevel.CRITICAL else self.cpu_threshold.moderate_max,
                    unit="%",
                    timestamp=datetime.now(timezone.utc).isoformat()
                ))
        
        # Analyze Memory usage
        if 'memory_usage' in metrics and metrics['memory_usage'].get('avg') is not None:
            memory_avg = metrics['memory_usage']['avg']
            memory_max = metrics['memory_usage']['max']
            
            memory_level, memory_severity = ThresholdClassifier.classify_performance(memory_avg, self.memory_threshold)
            
            analysis['performance_levels']['memory'] = {
                'level': memory_level.value,
                'severity_score': round(memory_severity, 2)
            }
            
            analysis['resource_usage']['memory'] = {
                'avg_mb': memory_avg,
                'max_mb': memory_max,
                'avg_gb': round(memory_avg / 1024, 2),
                'max_gb': round(memory_max / 1024, 2),
                'unit': 'MB'
            }
            
            # Generate memory alerts
            if memory_level in [PerformanceLevel.CRITICAL, PerformanceLevel.POOR]:
                severity = AlertLevel.CRITICAL if memory_level == PerformanceLevel.CRITICAL else AlertLevel.HIGH
                alerts.append(PerformanceAlert(
                    severity=severity,
                    resource_type=ResourceType.MEMORY,
                    pod_name=node_name,
                    node_name=node_name,
                    message=f"High memory usage: {memory_avg:.0f}MB ({memory_avg/1024:.1f}GB)",
                    current_value=memory_avg,
                    threshold_value=self.memory_threshold.poor_max if memory_level == PerformanceLevel.CRITICAL else self.memory_threshold.moderate_max,
                    unit="MB",
                    timestamp=datetime.now(timezone.utc).isoformat()
                ))
        
        # Analyze Network usage
        if 'network_rx' in metrics and 'network_tx' in metrics:
            rx_avg = metrics['network_rx'].get('avg', 0)
            tx_avg = metrics['network_tx'].get('avg', 0)
            rx_max = metrics['network_rx'].get('max', 0)
            tx_max = metrics['network_tx'].get('max', 0)
            
            analysis['resource_usage']['network'] = {
                'rx_avg_bytes_per_sec': rx_avg,
                'tx_avg_bytes_per_sec': tx_avg,
                'rx_max_bytes_per_sec': rx_max,
                'tx_max_bytes_per_sec': tx_max,
                'rx_avg_mbps': round(rx_avg * 8 / 1000000, 2),
                'tx_avg_mbps': round(tx_avg * 8 / 1000000, 2),
                'unit': 'bytes/s'
            }
        
        return {
            'analysis': analysis,
            'alerts': alerts
        }
    
    def _calculate_group_performance_summary(self, group_data: Dict[str, Any], analyzed_nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate performance summary for a node group"""
        summary = {
            'role': group_data.get('node_count', 0),
            'node_count': len(analyzed_nodes),
            'resource_averages': {},
            'resource_peaks': {},
            'performance_distribution': {},
            'problematic_nodes': []
        }
        
        # Extract values for statistics
        cpu_values = []
        memory_values = []
        network_rx_values = []
        network_tx_values = []
        
        performance_counts = {
            'excellent': 0,
            'good': 0, 
            'moderate': 0,
            'poor': 0,
            'critical': 0
        }
        
        for node in analyzed_nodes:
            # Collect CPU values
            if 'cpu' in node.get('resource_usage', {}):
                cpu_avg = node['resource_usage']['cpu']['avg_percent']
                cpu_values.append(cpu_avg)
                
                # Count performance levels
                cpu_level = node.get('performance_levels', {}).get('cpu', {}).get('level', 'unknown')
                if cpu_level in performance_counts:
                    performance_counts[cpu_level] += 1
            
            # Collect memory values
            if 'memory' in node.get('resource_usage', {}):
                memory_avg = node['resource_usage']['memory']['avg_mb']
                memory_values.append(memory_avg)
            
            # Collect network values
            if 'network' in node.get('resource_usage', {}):
                network_rx_values.append(node['resource_usage']['network']['rx_avg_bytes_per_sec'])
                network_tx_values.append(node['resource_usage']['network']['tx_avg_bytes_per_sec'])
            
            # Track problematic nodes
            cpu_level = node.get('performance_levels', {}).get('cpu', {}).get('level')
            memory_level = node.get('performance_levels', {}).get('memory', {}).get('level')
            
            if cpu_level in ['critical', 'poor'] or memory_level in ['critical', 'poor']:
                summary['problematic_nodes'].append({
                    'node_name': node['node_name'],
                    'cpu_level': cpu_level,
                    'memory_level': memory_level,
                    'cpu_usage': node.get('resource_usage', {}).get('cpu', {}).get('avg_percent'),
                    'memory_usage_mb': node.get('resource_usage', {}).get('memory', {}).get('avg_mb')
                })
        
        # Calculate resource averages and peaks
        if cpu_values:
            summary['resource_averages']['cpu'] = {
                'avg_percent': round(statistics.mean(cpu_values), 2),
                'median_percent': round(statistics.median(cpu_values), 2),
                'std_dev': round(statistics.stdev(cpu_values) if len(cpu_values) > 1 else 0, 2)
            }
            summary['resource_peaks']['cpu'] = {
                'max_percent': round(max(cpu_values), 2),
                'min_percent': round(min(cpu_values), 2)
            }
        
        if memory_values:
            summary['resource_averages']['memory'] = {
                'avg_mb': round(statistics.mean(memory_values), 2),
                'avg_gb': round(statistics.mean(memory_values) / 1024, 2),
                'median_mb': round(statistics.median(memory_values), 2),
                'std_dev_mb': round(statistics.stdev(memory_values) if len(memory_values) > 1 else 0, 2)
            }
            summary['resource_peaks']['memory'] = {
                'max_mb': round(max(memory_values), 2),
                'max_gb': round(max(memory_values) / 1024, 2),
                'min_mb': round(min(memory_values), 2)
            }
        
        if network_rx_values:
            summary['resource_averages']['network'] = {
                'avg_rx_bytes_per_sec': round(statistics.mean(network_rx_values), 2),
                'avg_tx_bytes_per_sec': round(statistics.mean(network_tx_values), 2),
                'avg_rx_mbps': round(statistics.mean(network_rx_values) * 8 / 1000000, 2),
                'avg_tx_mbps': round(statistics.mean(network_tx_values) * 8 / 1000000, 2)
            }
            summary['resource_peaks']['network'] = {
                'max_rx_bytes_per_sec': round(max(network_rx_values), 2),
                'max_tx_bytes_per_sec': round(max(network_tx_values), 2)
            }
        
        # Performance distribution
        summary['performance_distribution'] = performance_counts
        
        return summary
    
    async def _analyze_api_performance(self, api_metrics_data: Dict[str, Any]) -> APIPerformanceAnalysis:
        """Analyze API server performance metrics"""
        latency_analysis = {}
        request_patterns = {}
        performance_alerts = []
        recommendations = []
        
        if 'metrics' not in api_metrics_data:
            logger.warning("No metrics data found in API metrics")
            return APIPerformanceAnalysis(
                latency_analysis={},
                request_patterns={},
                performance_alerts=[],
                recommendations=["Unable to analyze API performance - no metrics data available"],
                health_score=0.0
            )
        
        metrics = api_metrics_data['metrics']
        
        # Analyze read-only API latency
        if 'avg_ro_apicalls_latency' in metrics:
            ro_latency_data = metrics['avg_ro_apicalls_latency']
            if 'statistics' in ro_latency_data:
                stats = ro_latency_data['statistics']
                avg_latency = stats.get('avg', 0)
                max_latency = stats.get('max', 0)
                p99_latency = stats.get('p99', 0)
                
                level, severity = ThresholdClassifier.classify_performance(p99_latency, self.api_latency_threshold)
                
                latency_analysis['readonly'] = {
                    'avg_seconds': round(avg_latency, 4),
                    'max_seconds': round(max_latency, 4),
                    'p99_seconds': round(p99_latency, 4),
                    'performance_level': level.value,
                    'severity_score': round(severity, 2)
                }
                
                # Generate latency alerts
                if level in [PerformanceLevel.CRITICAL, PerformanceLevel.POOR]:
                    severity_level = AlertLevel.CRITICAL if level == PerformanceLevel.CRITICAL else AlertLevel.HIGH
                    performance_alerts.append(PerformanceAlert(
                        severity=severity_level,
                        resource_type=ResourceType.NETWORK,  # Using NETWORK as closest match for API latency
                        pod_name="kube-apiserver",
                        message=f"High read-only API latency (P99): {p99_latency:.3f}s",
                        current_value=p99_latency,
                        threshold_value=self.api_latency_threshold.good_max,
                        unit="seconds",
                        timestamp=datetime.now(timezone.utc).isoformat()
                    ))
        
        # Analyze mutating API latency
        if 'avg_mutating_apicalls_latency' in metrics:
            mut_latency_data = metrics['avg_mutating_apicalls_latency']
            if 'statistics' in mut_latency_data:
                stats = mut_latency_data['statistics']
                avg_latency = stats.get('avg', 0)
                max_latency = stats.get('max', 0)
                p99_latency = stats.get('p99', 0)
                
                level, severity = ThresholdClassifier.classify_performance(p99_latency, self.api_latency_threshold)
                
                latency_analysis['mutating'] = {
                    'avg_seconds': round(avg_latency, 4),
                    'max_seconds': round(max_latency, 4),
                    'p99_seconds': round(p99_latency, 4),
                    'performance_level': level.value,
                    'severity_score': round(severity, 2)
                }
                
                # Generate latency alerts
                if level in [PerformanceLevel.CRITICAL, PerformanceLevel.POOR]:
                    severity_level = AlertLevel.CRITICAL if level == PerformanceLevel.CRITICAL else AlertLevel.HIGH
                    performance_alerts.append(PerformanceAlert(
                        severity=severity_level,
                        resource_type=ResourceType.NETWORK,
                        pod_name="kube-apiserver",
                        message=f"High mutating API latency (P99): {p99_latency:.3f}s",
                        current_value=p99_latency,
                        threshold_value=self.api_latency_threshold.good_max,
                        unit="seconds",
                        timestamp=datetime.now(timezone.utc).isoformat()
                    ))
        
        # Analyze request rates
        if 'api_request_rate' in metrics:
            rate_data = metrics['api_request_rate']
            if 'values' in rate_data:
                total_rate = 0
                verb_breakdown = {}
                
                for item in rate_data['values']:
                    if item.get('values'):
                        last_value = item['values'][-1]['value']
                        total_rate += last_value
                        
                        # Track by verb
                        verb = item.get('labels', {}).get('verb', 'unknown')
                        if verb not in verb_breakdown:
                            verb_breakdown[verb] = 0
                        verb_breakdown[verb] += last_value
                
                request_patterns['total_rate'] = {
                    'requests_per_second': round(total_rate, 2),
                    'verb_breakdown': {k: round(v, 2) for k, v in verb_breakdown.items()}
                }
        
        # Analyze errors
        if 'api_request_errors' in metrics:
            error_data = metrics['api_request_errors']
            if 'values' in error_data:
                total_errors = sum(
                    item['values'][-1]['value'] if item.get('values') else 0 
                    for item in error_data['values']
                )
                
                request_patterns['error_rate'] = {
                    'errors_per_second': round(total_errors, 2)
                }
                
                # Alert on high error rates
                if total_errors > 1.0:  # More than 1 error per second
                    performance_alerts.append(PerformanceAlert(
                        severity=AlertLevel.HIGH,
                        resource_type=ResourceType.NETWORK,
                        pod_name="kube-apiserver",
                        message=f"High API error rate: {total_errors:.2f} errors/sec",
                        current_value=total_errors,
                        threshold_value=1.0,
                        unit="errors/sec",
                        timestamp=datetime.now(timezone.utc).isoformat()
                    ))
        
        # Generate API recommendations
        recommendations = self._generate_api_recommendations(latency_analysis, request_patterns, performance_alerts)
        
        # Calculate health score
        health_score = self._calculate_api_health_score(performance_alerts, latency_analysis)
        
        return APIPerformanceAnalysis(
            latency_analysis=latency_analysis,
            request_patterns=request_patterns,
            performance_alerts=performance_alerts,
            recommendations=recommendations,
            health_score=health_score
        )
    
    def _generate_node_group_recommendations(self, role: str, performance_summary: Dict[str, Any], alerts: List[PerformanceAlert]) -> List[str]:
        """Generate recommendations for a node group"""
        recommendations = []
        
        critical_alerts = [a for a in alerts if a.severity == AlertLevel.CRITICAL]
        high_alerts = [a for a in alerts if a.severity == AlertLevel.HIGH]
        
        if critical_alerts:
            recommendations.append(f"URGENT: {len(critical_alerts)} {role} nodes have critical performance issues")
            
            # Specific recommendations based on alert types
            cpu_alerts = [a for a in critical_alerts if a.resource_type == ResourceType.CPU]
            memory_alerts = [a for a in critical_alerts if a.resource_type == ResourceType.MEMORY]
            
            if cpu_alerts:
                recommendations.append(f"High CPU usage on {len(cpu_alerts)} {role} nodes - consider workload redistribution")
            if memory_alerts:
                recommendations.append(f"High memory usage on {len(memory_alerts)} {role} nodes - check for memory leaks or increase node capacity")
        
        elif high_alerts:
            recommendations.append(f"Warning: {len(high_alerts)} {role} nodes need attention")
        
        # Role-specific recommendations
        if role == "master":
            if critical_alerts or high_alerts:
                recommendations.extend([
                    "Master node performance issues can affect entire cluster stability",
                    "Consider checking etcd performance and API server resource usage"
                ])
        elif role == "worker":
            if len(alerts) > performance_summary.get('node_count', 1) * 0.3:
                recommendations.append("High percentage of worker nodes under stress - consider cluster scaling")
        
        # Resource-specific recommendations
        cpu_avg = performance_summary.get('resource_averages', {}).get('cpu', {}).get('avg_percent', 0)
        memory_avg = performance_summary.get('resource_averages', {}).get('memory', {}).get('avg_mb', 0)
        
        if cpu_avg > 70:
            recommendations.append(f"Group CPU average ({cpu_avg:.1f}%) is high - monitor for sustained load")
        if memory_avg > 4096:  # 4GB
            recommendations.append(f"Group memory average ({memory_avg/1024:.1f}GB) is high - review memory allocation")
        
        if not recommendations:
            recommendations.append(f"{role.title()} nodes are performing within acceptable parameters")
        
        return recommendations
    
    def _generate_api_recommendations(self, latency_analysis: Dict[str, Any], request_patterns: Dict[str, Any], alerts: List[PerformanceAlert]) -> List[str]:
        """Generate API server recommendations"""
        recommendations = []
        
        # Latency-based recommendations
        if 'readonly' in latency_analysis:
            ro_data = latency_analysis['readonly']
            if ro_data.get('performance_level') in ['critical', 'poor']:
                recommendations.append(f"Read-only API latency is {ro_data.get('performance_level')} ({ro_data.get('p99_seconds', 0):.3f}s)")
                recommendations.append("Consider checking etcd performance and master node resources")
        
        if 'mutating' in latency_analysis:
            mut_data = latency_analysis['mutating']
            if mut_data.get('performance_level') in ['critical', 'poor']:
                recommendations.append(f"Mutating API latency is {mut_data.get('performance_level')} ({mut_data.get('p99_seconds', 0):.3f}s)")
                recommendations.append("Review cluster admission controllers and webhooks")
        
        # Request pattern recommendations
        if 'total_rate' in request_patterns:
            total_rate = request_patterns['total_rate']['requests_per_second']
            if total_rate > 1000:
                recommendations.append(f"High API request rate ({total_rate:.1f} req/s) - monitor for potential throttling")
        
        if 'error_rate' in request_patterns:
            error_rate = request_patterns['error_rate']['errors_per_second']
            if error_rate > 0.1:
                recommendations.append(f"API errors detected ({error_rate:.2f} errors/s) - investigate error patterns")
        
        # Critical alerts recommendations
        critical_alerts = [a for a in alerts if a.severity == AlertLevel.CRITICAL]
        if critical_alerts:
            recommendations.append("URGENT: Critical API performance issues detected")
            recommendations.append("Immediate investigation of API server and etcd health required")
        
        if not recommendations:
            recommendations.append("API server performance is within acceptable parameters")
        
        return recommendations
    
    def _calculate_group_health_score(self, alerts: List[PerformanceAlert], node_count: int) -> float:
        """Calculate health score for a node group"""
        if node_count == 0:
            return 0.0
        
        critical_count = sum(1 for a in alerts if a.severity == AlertLevel.CRITICAL)
        high_count = sum(1 for a in alerts if a.severity == AlertLevel.HIGH)
        medium_count = sum(1 for a in alerts if a.severity == AlertLevel.MEDIUM)
        
        # Calculate penalty based on alert severity and count
        total_penalty = (critical_count * 0.4) + (high_count * 0.2) + (medium_count * 0.1)
        
        # Normalize by node count and calculate health score (0-100)
        penalty_per_node = total_penalty / node_count
        health_score = max(0.0, 100.0 - (penalty_per_node * 100))
        
        return round(health_score, 2)
    
    def _calculate_api_health_score(self, alerts: List[PerformanceAlert], latency_analysis: Dict[str, Any]) -> float:
        """Calculate health score for API server"""
        base_score = 100.0
        
        # Penalty for alerts
        for alert in alerts:
            if alert.severity == AlertLevel.CRITICAL:
                base_score -= 40
            elif alert.severity == AlertLevel.HIGH:
                base_score -= 20
            elif alert.severity == AlertLevel.MEDIUM:
                base_score -= 10
        
        # Additional penalty for poor latency performance
        for api_type, data in latency_analysis.items():
            if data.get('performance_level') == 'critical':
                base_score -= 30
            elif data.get('performance_level') == 'poor':
                base_score -= 15
        
        return round(max(0.0, base_score), 2)
    
    def _extract_critical_issues(self, all_alerts: List[PerformanceAlert], 
                                node_groups_analysis: Dict[str, NodeGroupAnalysis],
                                api_performance_analysis: APIPerformanceAnalysis) -> List[str]:
        """Extract critical issues from analysis results"""
        critical_issues = []
        
        # Critical alerts
        critical_alerts = [a for a in all_alerts if a.severity == AlertLevel.CRITICAL]
        if critical_alerts:
            critical_issues.append(f"CRITICAL: {len(critical_alerts)} critical performance alerts detected")
        
        # Node group issues
        for role, analysis in node_groups_analysis.items():
            if analysis.health_score < 50:
                critical_issues.append(f"CRITICAL: {role} nodes health score is critically low ({analysis.health_score}%)")
            
            problematic_nodes = analysis.performance_summary.get('problematic_nodes', [])
            if len(problematic_nodes) > analysis.node_count * 0.5:
                critical_issues.append(f"CRITICAL: Majority of {role} nodes ({len(problematic_nodes)}/{analysis.node_count}) have performance issues")
        
        # API issues
        if api_performance_analysis.health_score < 50:
            critical_issues.append(f"CRITICAL: API server health score is critically low ({api_performance_analysis.health_score}%)")
        
        # Latency issues
        for api_type, data in api_performance_analysis.latency_analysis.items():
            if data.get('performance_level') == 'critical':
                critical_issues.append(f"CRITICAL: {api_type} API latency is critically high ({data.get('p99_seconds', 0):.3f}s)")
        
        return critical_issues
    
    def _generate_comprehensive_recommendations(self, 
                                              node_groups_analysis: Dict[str, NodeGroupAnalysis],
                                              api_performance_analysis: APIPerformanceAnalysis,
                                              overall_health: ClusterHealth) -> List[str]:
        """Generate comprehensive recommendations for the entire cluster"""
        recommendations = []
        
        # Overall health recommendations
        if overall_health.health_score < 50:
            recommendations.append("URGENT: Cluster health is critically low - immediate action required")
        elif overall_health.health_score < 70:
            recommendations.append("WARNING: Cluster health needs attention")
        
        # Collect all node group recommendations
        for role, analysis in node_groups_analysis.items():
            if analysis.health_score < 70:
                recommendations.extend([f"[{role.upper()}] " + rec for rec in analysis.recommendations[:2]])
        
        # API recommendations
        if api_performance_analysis.health_score < 70:
            recommendations.extend(["[API] " + rec for rec in api_performance_analysis.recommendations[:2]])
        
        # Strategic recommendations based on overall cluster state
        total_critical_alerts = sum(
            len([a for a in analysis.alerts if a.severity == AlertLevel.CRITICAL])
            for analysis in node_groups_analysis.values()
        ) + len([a for a in api_performance_analysis.performance_alerts if a.severity == AlertLevel.CRITICAL])
        
        if total_critical_alerts > 5:
            recommendations.append("Consider cluster maintenance window to address multiple critical issues")
        
        # Scaling recommendations
        worker_analysis = node_groups_analysis.get('worker')
        if worker_analysis and len(worker_analysis.performance_summary.get('problematic_nodes', [])) > worker_analysis.node_count * 0.4:
            recommendations.append("Consider adding more worker nodes to distribute load")
        
        if not recommendations:
            recommendations.append("Cluster is performing well - continue monitoring")
        
        return recommendations[:10]  # Limit to top 10 recommendations
    
    def calculate_cluster_health_score(self, all_alerts: List[PerformanceAlert], total_nodes: int) -> ClusterHealth:
        """Calculate overall cluster health score"""
        base_score = 100.0
        
        # Count alerts by severity
        critical_count = sum(1 for a in all_alerts if a.severity == AlertLevel.CRITICAL)
        high_count = sum(1 for a in all_alerts if a.severity == AlertLevel.HIGH)
        medium_count = sum(1 for a in all_alerts if a.severity == AlertLevel.MEDIUM)
        low_count = sum(1 for a in all_alerts if a.severity == AlertLevel.LOW)
        
        # Calculate penalties
        total_penalty = (critical_count * 15) + (high_count * 8) + (medium_count * 4) + (low_count * 1)
        
        # Apply penalty
        health_score = max(0.0, base_score - total_penalty)
        
        # Determine health level
        if health_score >= 90:
            health_level = "excellent"
        elif health_score >= 75:
            health_level = "good"
        elif health_score >= 60:
            health_level = "moderate"
        elif health_score >= 40:
            health_level = "poor"
        else:
            health_level = "critical"
        
        return ClusterHealth(
            health_score=round(health_score, 2),
            health_level=health_level,
            total_alerts=len(all_alerts),
            critical_alerts=critical_count,
            high_alerts=high_count,
            medium_alerts=medium_count,
            low_alerts=low_count
        )
    
    def generate_report(self, analysis_result: ClusterAnalysisResult) -> Dict[str, Any]:
        """Generate a comprehensive performance report"""
        return {
            'analysis_metadata': asdict(analysis_result.metadata),
            'cluster_overview': {
                'basic_info': analysis_result.cluster_basic_info,
                'overall_health': asdict(analysis_result.overall_health),
                'critical_issues_count': len(analysis_result.critical_issues),
                'recommendations_count': len(analysis_result.recommendations)
            },
            'node_groups_summary': {
                role: {
                    'node_count': analysis.node_count,
                    'health_score': analysis.health_score,
                    'alerts_count': len(analysis.alerts),
                    'critical_alerts_count': len([a for a in analysis.alerts if a.severity == AlertLevel.CRITICAL]),
                    'problematic_nodes_count': len(analysis.performance_summary.get('problematic_nodes', [])),
                    'performance_distribution': analysis.performance_summary.get('performance_distribution', {}),
                    'resource_averages': analysis.performance_summary.get('resource_averages', {})
                }
                for role, analysis in analysis_result.node_groups_analysis.items()
            },
            'api_performance_summary': {
                'health_score': analysis_result.api_performance_analysis.health_score,
                'latency_analysis': analysis_result.api_performance_analysis.latency_analysis,
                'request_patterns': analysis_result.api_performance_analysis.request_patterns,
                'alerts_count': len(analysis_result.api_performance_analysis.performance_alerts)
            },
            'critical_issues': analysis_result.critical_issues,
            'top_recommendations': analysis_result.recommendations,
            'detailed_analysis': {
                'node_groups': {
                    role: asdict(analysis) for role, analysis in analysis_result.node_groups_analysis.items()
                },
                'api_performance': asdict(analysis_result.api_performance_analysis)
            }
        }


# Usage example and main execution
async def main():
    """Example usage of the ClusterAPIPerformanceAnalyzer"""
    try:
        # Initialize the analyzer
        analyzer = ClusterAPIPerformanceAnalyzer()
        
        # Initialize data collectors (these would be imported from your modules)
        general_info = OpenShiftGeneralInfo()
        node_usage_query = NodeUsageQuery()
        api_metrics = kubeAPICollector()
        
        # Collect cluster information
        print("Collecting cluster information...")
        cluster_info = await general_info.get_cluster_info()
        
        # Collect node usage data
        print("Collecting node usage metrics...")
        node_usage_data = await node_usage_query.get_node_usage_by_role(duration="1h")
        
        # Collect API metrics
        print("Collecting API metrics...")
        api_metrics_data = await api_metrics.get_api_performance_metrics(duration="1h")
        
        # Perform comprehensive analysis
        print("Analyzing cluster performance...")
        analysis_result = await analyzer.analyze_cluster_performance(
            cluster_info=cluster_info,
            node_usage_data=node_usage_data,
            api_metrics_data=api_metrics_data,
            duration="1h"
        )
        
        # Generate report
        report = analyzer.generate_report(analysis_result)
        
        # Output results
        print("\n" + "="*80)
        print("OVNK CLUSTER PERFORMANCE ANALYSIS REPORT")
        print("="*80)
        
        print(f"\nCluster: {analysis_result.cluster_basic_info['cluster_name']}")
        print(f"Version: {analysis_result.cluster_basic_info['cluster_version']}")
        print(f"Total Nodes: {analysis_result.cluster_basic_info['total_nodes']}")
        print(f"Overall Health Score: {analysis_result.overall_health.health_score}% ({analysis_result.overall_health.health_level.upper()})")
        
        print(f"\nNode Groups Analysis:")
        for role, analysis in analysis_result.node_groups_analysis.items():
            print(f"  {role.upper()}: {analysis.node_count} nodes, Health: {analysis.health_score}%")
        
        print(f"\nAPI Performance: Health Score {analysis_result.api_performance_analysis.health_score}%")
        
        if analysis_result.critical_issues:
            print(f"\nCRITICAL ISSUES ({len(analysis_result.critical_issues)}):")
            for issue in analysis_result.critical_issues:
                print(f"  - {issue}")
        
        print(f"\nTOP RECOMMENDATIONS ({len(analysis_result.recommendations)}):")
        for i, rec in enumerate(analysis_result.recommendations[:5], 1):
            print(f"  {i}. {rec}")
        
        # Save detailed report to file
        with open(f"cluster_performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\nDetailed report saved to cluster_performance_report_*.json")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the analysis
    asyncio.run(main())