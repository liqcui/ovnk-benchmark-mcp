"""
OVNK Benchmark Pod Usage Performance Analysis Module
Analyzes pod usage metrics collected from Prometheus and generates performance insights
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, asdict

# Import from the performance utility module
from ovnk_benchmark_performance_utility import (
    BasePerformanceAnalyzer,
    PerformanceLevel,
    AlertLevel,
    ResourceType,
    PerformanceThreshold,
    PerformanceAlert,
    AnalysisMetadata,
    ClusterHealth,
    ThresholdClassifier,
    MemoryConverter,
    StatisticsCalculator,
    RecommendationEngine,
    create_performance_alert,
    format_performance_summary_for_json
)


@dataclass
class PodPerformanceMetrics:
    """Pod-specific performance metrics"""
    pod_name: str
    node_name: str
    namespace: str
    cpu_usage: float
    memory_usage_mb: float
    cpu_level: PerformanceLevel
    memory_level: PerformanceLevel
    cpu_severity: float
    memory_severity: float
    overall_severity: float
    recommendations: List[str]


@dataclass
class NodePerformanceSummary:
    """Node-level performance summary"""
    node_name: str
    node_roles: List[str]
    total_pods: int
    total_cpu_usage: float
    total_memory_usage_mb: float
    critical_pods: int
    warning_pods: int
    healthy_pods: int
    node_efficiency: Dict[str, Any]


class PodUsageAnalyzer(BasePerformanceAnalyzer):
    """Analyzes pod usage metrics and generates performance insights"""
    
    def __init__(self):
        super().__init__("Pod Usage Analyzer")
        self.cpu_threshold = ThresholdClassifier.get_default_cpu_threshold()
        self.memory_threshold = ThresholdClassifier.get_default_memory_threshold()
    
    def analyze_metrics_data(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze pod usage metrics data and generate performance insights
        
        Args:
            metrics_data: Pod usage data from PodsUsageCollector
            
        Returns:
            Dictionary containing comprehensive analysis results
        """
        try:
            # Extract basic information
            collection_type = metrics_data.get('collection_type', 'unknown')
            total_pods = metrics_data.get('total_pods_analyzed', 0)
            top_pods_data = metrics_data.get('top_10_pods', [])
            
            # Generate metadata
            duration = None
            if 'query_info' in metrics_data:
                duration = metrics_data['query_info'].get('duration')
            
            metadata = self.generate_metadata(collection_type, total_pods, duration)
            
            # Analyze individual pods
            pod_analyses = []
            alerts = []
            node_summaries = {}
            
            for pod_data in top_pods_data:
                pod_analysis = self._analyze_single_pod(pod_data, collection_type == 'instant')
                pod_analyses.append(pod_analysis)
                
                # Generate alerts for problematic pods
                pod_alerts = self._generate_pod_alerts(pod_analysis)
                alerts.extend(pod_alerts)
                
                # Aggregate node-level data
                self._aggregate_node_data(pod_analysis, node_summaries)
            
            # Calculate cluster health
            cluster_health = self.calculate_cluster_health_score(alerts, total_pods)
            
            # Generate recommendations
            cluster_recommendations = self._generate_cluster_recommendations(
                alerts, node_summaries, cluster_health
            )
            
            # Create comprehensive analysis result
            analysis_result = {
                'metadata': asdict(metadata),
                'cluster_health': asdict(cluster_health),
                'pod_analysis': {
                    'total_analyzed': len(pod_analyses),
                    'performance_distribution': self._calculate_performance_distribution(pod_analyses),
                    'top_resource_consumers': self._get_top_consumers(pod_analyses)
                },
                'node_analysis': {
                    'total_nodes': len(node_summaries),
                    'node_summaries': [asdict(summary) for summary in node_summaries.values()]
                },
                'alerts': {
                    'total_alerts': len(alerts),
                    'by_severity': self._group_alerts_by_severity(alerts),
                    'critical_alerts': [self._format_alert(a) for a in alerts if a.severity == AlertLevel.CRITICAL][:5]
                },
                'recommendations': cluster_recommendations
            }
            
            return format_performance_summary_for_json(analysis_result)
            
        except Exception as e:
            return {
                'error': f"Analysis failed: {str(e)}",
                'metadata': {
                    'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
                    'analyzer_type': self.analyzer_type
                }
            }
    
    def _analyze_single_pod(self, pod_data: Dict[str, Any], is_instant: bool) -> PodPerformanceMetrics:
        """Analyze a single pod's performance metrics"""
        pod_name = pod_data.get('pod_name', 'unknown')
        node_name = pod_data.get('node_name', 'unknown')
        
        # Extract namespace from pod_info if available
        namespace = 'unknown'
        if 'pod_info' in pod_data:
            namespace = pod_data['pod_info'].get('namespace', 'unknown')
        
        # Extract CPU usage
        cpu_usage = self._extract_cpu_usage(pod_data.get('usage_metrics', {}), is_instant)
        
        # Extract memory usage
        memory_usage_bytes = self._extract_memory_usage(pod_data.get('usage_metrics', {}), is_instant)
        memory_usage_mb = memory_usage_bytes / (1024 * 1024) if memory_usage_bytes > 0 else 0.0
        
        # Classify performance levels
        cpu_level, cpu_severity = ThresholdClassifier.classify_performance(cpu_usage, self.cpu_threshold)
        memory_level, memory_severity = ThresholdClassifier.classify_performance(
            memory_usage_mb, self.memory_threshold
        )
        
        # Calculate overall severity (weighted average)
        overall_severity = (cpu_severity * 0.6 + memory_severity * 0.4)
        
        # Generate recommendations
        recommendations = []
        recommendations.extend(
            RecommendationEngine.generate_resource_recommendations(
                ResourceType.CPU, cpu_level, cpu_usage, "%"
            )
        )
        recommendations.extend(
            RecommendationEngine.generate_resource_recommendations(
                ResourceType.MEMORY, memory_level, memory_usage_mb, "MB"
            )
        )
        
        return PodPerformanceMetrics(
            pod_name=pod_name,
            node_name=node_name,
            namespace=namespace,
            cpu_usage=cpu_usage,
            memory_usage_mb=memory_usage_mb,
            cpu_level=cpu_level,
            memory_level=memory_level,
            cpu_severity=cpu_severity,
            memory_severity=memory_severity,
            overall_severity=overall_severity,
            recommendations=recommendations
        )
    
    def _extract_cpu_usage(self, usage_metrics: Dict[str, Any], is_instant: bool) -> float:
        """Extract CPU usage from metrics"""
        cpu_keys = ['cpu_usage', 'cpu_ovnkube_node_pods', 'cpu-ovnkube-node-pods']
        
        for key in cpu_keys:
            if key in usage_metrics:
                metric_data = usage_metrics[key]
                if is_instant:
                    return metric_data.get('value', 0.0)
                else:
                    return metric_data.get('avg', 0.0)
        
        return 0.0
    
    def _extract_memory_usage(self, usage_metrics: Dict[str, Any], is_instant: bool) -> float:
        """Extract memory usage from metrics (returns bytes)"""
        memory_keys = ['memory_usage', 'memory_working_set', 'memory_ovnkube_node_pods', 'memory-ovnkube-node-pods']
        
        for key in memory_keys:
            if key in usage_metrics:
                metric_data = usage_metrics[key]
                if is_instant:
                    value = metric_data.get('value', 0.0)
                    unit = metric_data.get('unit', 'bytes')
                else:
                    value = metric_data.get('avg', 0.0)
                    unit = metric_data.get('unit', 'bytes')
                
                # Convert to bytes if needed
                if unit.lower() in ['mb', 'gb', 'kb']:
                    return MemoryConverter.to_bytes(value, unit)
                else:
                    return value
        
        return 0.0
    
    def _generate_pod_alerts(self, pod_analysis: PodPerformanceMetrics) -> List[PerformanceAlert]:
        """Generate alerts for a pod based on its performance analysis"""
        alerts = []
        
        # CPU alerts
        if pod_analysis.cpu_level in [PerformanceLevel.POOR, PerformanceLevel.CRITICAL]:
            severity = AlertLevel.CRITICAL if pod_analysis.cpu_level == PerformanceLevel.CRITICAL else AlertLevel.HIGH
            
            alert = create_performance_alert(
                severity=severity.value,
                resource_type="cpu",
                component_name=pod_analysis.pod_name,
                message=f"High CPU usage: {pod_analysis.cpu_usage:.1f}%",
                current_value=pod_analysis.cpu_usage,
                threshold_value=self.cpu_threshold.moderate_max,
                unit="%",
                node_name=pod_analysis.node_name
            )
            alerts.append(alert)
        
        # Memory alerts
        if pod_analysis.memory_level in [PerformanceLevel.POOR, PerformanceLevel.CRITICAL]:
            severity = AlertLevel.CRITICAL if pod_analysis.memory_level == PerformanceLevel.CRITICAL else AlertLevel.HIGH
            
            alert = create_performance_alert(
                severity=severity.value,
                resource_type="memory",
                component_name=pod_analysis.pod_name,
                message=f"High memory usage: {pod_analysis.memory_usage_mb:.1f}MB",
                current_value=pod_analysis.memory_usage_mb,
                threshold_value=self.memory_threshold.moderate_max,
                unit="MB",
                node_name=pod_analysis.node_name
            )
            alerts.append(alert)
        
        return alerts
    
    def _aggregate_node_data(self, pod_analysis: PodPerformanceMetrics, 
                           node_summaries: Dict[str, NodePerformanceSummary]) -> None:
        """Aggregate pod data by node"""
        node_name = pod_analysis.node_name
        
        if node_name not in node_summaries:
            node_summaries[node_name] = NodePerformanceSummary(
                node_name=node_name,
                node_roles=[],
                total_pods=0,
                total_cpu_usage=0.0,
                total_memory_usage_mb=0.0,
                critical_pods=0,
                warning_pods=0,
                healthy_pods=0,
                node_efficiency={}
            )
        
        summary = node_summaries[node_name]
        summary.total_pods += 1
        summary.total_cpu_usage += pod_analysis.cpu_usage
        summary.total_memory_usage_mb += pod_analysis.memory_usage_mb
        
        # Count pod health levels
        if pod_analysis.overall_severity >= 80:
            summary.critical_pods += 1
        elif pod_analysis.overall_severity >= 40:
            summary.warning_pods += 1
        else:
            summary.healthy_pods += 1
    
    def _calculate_performance_distribution(self, pod_analyses: List[PodPerformanceMetrics]) -> Dict[str, Any]:
        """Calculate performance level distribution across pods"""
        cpu_levels = [p.cpu_level for p in pod_analyses]
        memory_levels = [p.memory_level for p in pod_analyses]
        
        def count_levels(levels: List[PerformanceLevel]) -> Dict[str, int]:
            counts = {level.value: 0 for level in PerformanceLevel}
            for level in levels:
                counts[level.value] += 1
            return counts
        
        cpu_distribution = count_levels(cpu_levels)
        memory_distribution = count_levels(memory_levels)
        
        # Calculate overall health percentages
        total_pods = len(pod_analyses)
        if total_pods == 0:
            return {'cpu_distribution': cpu_distribution, 'memory_distribution': memory_distribution}
        
        critical_pods = sum(1 for p in pod_analyses if p.overall_severity >= 80)
        warning_pods = sum(1 for p in pod_analyses if 40 <= p.overall_severity < 80)
        healthy_pods = total_pods - critical_pods - warning_pods
        
        return {
            'cpu_distribution': cpu_distribution,
            'memory_distribution': memory_distribution,
            'overall_health': {
                'healthy_pods': healthy_pods,
                'warning_pods': warning_pods,
                'critical_pods': critical_pods,
                'healthy_percentage': round((healthy_pods / total_pods) * 100, 1),
                'warning_percentage': round((warning_pods / total_pods) * 100, 1),
                'critical_percentage': round((critical_pods / total_pods) * 100, 1)
            }
        }
    
    def _get_top_consumers(self, pod_analyses: List[PodPerformanceMetrics], top_n: int = 5) -> Dict[str, Any]:
        """Get top resource consuming pods"""
        # Sort by CPU usage
        top_cpu = sorted(pod_analyses, key=lambda p: p.cpu_usage, reverse=True)[:top_n]
        
        # Sort by memory usage
        top_memory = sorted(pod_analyses, key=lambda p: p.memory_usage_mb, reverse=True)[:top_n]
        
        # Sort by overall severity
        top_severity = sorted(pod_analyses, key=lambda p: p.overall_severity, reverse=True)[:top_n]
        
        return {
            'top_cpu_consumers': [
                {
                    'pod_name': p.pod_name,
                    'node_name': p.node_name,
                    'cpu_usage_percent': round(p.cpu_usage, 2),
                    'performance_level': p.cpu_level.value
                } for p in top_cpu
            ],
            'top_memory_consumers': [
                {
                    'pod_name': p.pod_name,
                    'node_name': p.node_name,
                    'memory_usage_mb': round(p.memory_usage_mb, 2),
                    'performance_level': p.memory_level.value
                } for p in top_memory
            ],
            'highest_severity_pods': [
                {
                    'pod_name': p.pod_name,
                    'node_name': p.node_name,
                    'overall_severity': round(p.overall_severity, 1),
                    'cpu_percent': round(p.cpu_usage, 2),
                    'memory_mb': round(p.memory_usage_mb, 2)
                } for p in top_severity
            ]
        }
    
    def _group_alerts_by_severity(self, alerts: List[PerformanceAlert]) -> Dict[str, int]:
        """Group alerts by severity level"""
        severity_counts = {level.value: 0 for level in AlertLevel}
        
        for alert in alerts:
            severity_counts[alert.severity.value] += 1
        
        return severity_counts
    
    def _format_alert(self, alert: PerformanceAlert) -> Dict[str, Any]:
        """Format alert for JSON output"""
        return {
            'severity': alert.severity.value,
            'resource_type': alert.resource_type.value,
            'pod_name': alert.pod_name,
            'node_name': alert.node_name,
            'message': alert.message,
            'current_value': round(alert.current_value, 2),
            'threshold_value': round(alert.threshold_value, 2),
            'unit': alert.unit,
            'timestamp': alert.timestamp
        }
    
    def _generate_cluster_recommendations(self, alerts: List[PerformanceAlert], 
                                        node_summaries: Dict[str, NodePerformanceSummary],
                                        cluster_health: ClusterHealth) -> List[str]:
        """Generate cluster-level recommendations"""
        recommendations = []
        
        # Health-based recommendations
        if cluster_health.health_level == "critical":
            recommendations.append("URGENT: Cluster health is critical - immediate action required")
            recommendations.append("Multiple pods showing poor performance - investigate resource constraints")
        elif cluster_health.health_level == "poor":
            recommendations.append("Cluster performance issues detected - review resource allocation")
        
        # Alert-based recommendations
        critical_alerts = [a for a in alerts if a.severity == AlertLevel.CRITICAL]
        high_alerts = [a for a in alerts if a.severity == AlertLevel.HIGH]
        
        if len(critical_alerts) > 0:
            recommendations.append(f"{len(critical_alerts)} pods in critical state - prioritize immediate optimization")
        
        if len(high_alerts) > len(node_summaries) * 2:  # More than 2 high alerts per node on average
            recommendations.append("High alert density suggests cluster capacity issues")
        
        # Node-specific recommendations
        problematic_nodes = [n for n in node_summaries.values() if n.critical_pods > 0]
        if problematic_nodes:
            recommendations.append(f"Review {len(problematic_nodes)} nodes with critical pods")
        
        # Resource pattern recommendations
        cpu_alerts = [a for a in alerts if a.resource_type == ResourceType.CPU]
        memory_alerts = [a for a in alerts if a.resource_type == ResourceType.MEMORY]
        
        if len(cpu_alerts) > len(memory_alerts) * 2:
            recommendations.append("CPU-heavy workload pattern detected - consider CPU optimization")
        elif len(memory_alerts) > len(cpu_alerts) * 2:
            recommendations.append("Memory-heavy workload pattern detected - consider memory optimization")
        
        # Add general recommendations if no specific issues
        if not recommendations:
            recommendations.append("Overall cluster performance is within acceptable parameters")
            recommendations.append("Continue monitoring for any emerging performance patterns")
        
        return recommendations[:10]  # Limit to top 10 recommendations
    
    def analyze_pod_trends(self, historical_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze pod performance trends over time"""
        if len(historical_data) < 2:
            return {'error': 'Insufficient data for trend analysis'}
        
        trends = {}
        
        # Extract pod names that appear in multiple collections
        common_pods = set(historical_data[0].get('top_10_pods', [{}])[0].get('pod_name', ''))
        for data in historical_data[1:]:
            current_pods = {pod.get('pod_name', '') for pod in data.get('top_10_pods', [])}
            common_pods = common_pods.intersection(current_pods)
        
        # Analyze trends for common pods
        for pod_name in common_pods:
            if not pod_name:
                continue
                
            pod_timeline = []
            for data in historical_data:
                for pod in data.get('top_10_pods', []):
                    if pod.get('pod_name') == pod_name:
                        cpu_usage = self._extract_cpu_usage(pod.get('usage_metrics', {}), 
                                                           data.get('collection_type') == 'instant')
                        memory_usage = self._extract_memory_usage(pod.get('usage_metrics', {}), 
                                                                data.get('collection_type') == 'instant')
                        
                        pod_timeline.append({
                            'timestamp': data.get('collection_timestamp'),
                            'cpu_usage': cpu_usage,
                            'memory_usage_mb': memory_usage / (1024 * 1024)
                        })
                        break
            
            if len(pod_timeline) >= 2:
                trends[pod_name] = self._calculate_trend_metrics(pod_timeline)
        
        return {
            'trend_analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            'pods_analyzed': len(trends),
            'trends': trends
        }
    
    def _calculate_trend_metrics(self, timeline: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate trend metrics for a pod"""
        if len(timeline) < 2:
            return {}
        
        cpu_values = [t['cpu_usage'] for t in timeline]
        memory_values = [t['memory_usage_mb'] for t in timeline]
        
        # Calculate simple trend direction
        cpu_trend = "stable"
        memory_trend = "stable"
        
        if len(cpu_values) >= 2:
            cpu_change = (cpu_values[-1] - cpu_values[0]) / cpu_values[0] * 100 if cpu_values[0] > 0 else 0
            if cpu_change > 20:
                cpu_trend = "increasing"
            elif cpu_change < -20:
                cpu_trend = "decreasing"
        
        if len(memory_values) >= 2:
            memory_change = (memory_values[-1] - memory_values[0]) / memory_values[0] * 100 if memory_values[0] > 0 else 0
            if memory_change > 20:
                memory_trend = "increasing"
            elif memory_change < -20:
                memory_trend = "decreasing"
        
        return {
            'cpu_trend': cpu_trend,
            'memory_trend': memory_trend,
            'data_points': len(timeline),
            'time_span': {
                'start': timeline[0]['timestamp'],
                'end': timeline[-1]['timestamp']
            },
            'latest_values': {
                'cpu_percent': round(cpu_values[-1], 2),
                'memory_mb': round(memory_values[-1], 2)
            }
        }


# Convenience functions for direct usage
async def analyze_instant_pod_usage(metrics_data: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze instant pod usage metrics"""
    analyzer = PodUsageAnalyzer()
    return analyzer.analyze_metrics_data(metrics_data)


async def analyze_duration_pod_usage(metrics_data: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze duration pod usage metrics"""
    analyzer = PodUsageAnalyzer()
    return analyzer.analyze_metrics_data(metrics_data)


def compare_pod_usage_analyses(analysis1: Dict[str, Any], analysis2: Dict[str, Any]) -> Dict[str, Any]:
    """Compare two pod usage analyses"""
    comparison = {
        'comparison_timestamp': datetime.now(timezone.utc).isoformat(),
        'analysis1_timestamp': analysis1.get('metadata', {}).get('analysis_timestamp'),
        'analysis2_timestamp': analysis2.get('metadata', {}).get('analysis_timestamp'),
        'health_comparison': {
            'analysis1_score': analysis1.get('cluster_health', {}).get('overall_score', 0),
            'analysis2_score': analysis2.get('cluster_health', {}).get('overall_score', 0),
            'score_change': 0.0
        },
        'alert_comparison': {
            'analysis1_total': analysis1.get('alerts', {}).get('total_alerts', 0),
            'analysis2_total': analysis2.get('alerts', {}).get('total_alerts', 0),
            'alert_change': 0
        }
    }
    
    # Calculate changes
    score1 = comparison['health_comparison']['analysis1_score']
    score2 = comparison['health_comparison']['analysis2_score']
    comparison['health_comparison']['score_change'] = round(score2 - score1, 2)
    
    alerts1 = comparison['alert_comparison']['analysis1_total']
    alerts2 = comparison['alert_comparison']['analysis2_total']
    comparison['alert_comparison']['alert_change'] = alerts2 - alerts1
    
    return comparison


# Example usage
if __name__ == "__main__":
    print("OVNK Pod Usage Performance Analyzer")
    print("=" * 50)
    
    # Example metrics data structure (for testing)
    example_metrics = {
        'collection_type': 'instant',
        'total_pods_analyzed': 5,
        'top_10_pods': [
            {
                'pod_name': 'ovnkube-node-abc123',
                'node_name': 'worker-1',
                'usage_metrics': {
                    'cpu_usage': {'value': 85.5, 'unit': '%'},
                    'memory_usage': {'value': 2048.0, 'unit': 'MB'}
                },
                'pod_info': {'namespace': 'openshift-ovn-kubernetes'}
            }
        ]
    }
    
    # Create analyzer and test
    analyzer = PodUsageAnalyzer()
    print(f"Analyzer type: {analyzer.analyzer_type}")
    print(f"CPU threshold: {analyzer.cpu_threshold.good_max}% (good max)")
    print(f"Memory threshold: {analyzer.memory_threshold.good_max} MB (good max)")
    
    print("\nModule loaded successfully!")