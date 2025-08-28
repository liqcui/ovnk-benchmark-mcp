"""
Performance Analysis Module - Updated with Utility Classes
Provides analysis capabilities for pod usage metrics collected from Prometheus
"""

import json
import statistics
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass

from .ovnk_benchmark_performance_utility import (
    BasePerformanceAnalyzer, PerformanceLevel, AlertLevel, ResourceType,
    PerformanceThreshold, PerformanceAlert, AnalysisMetadata, ClusterHealth,
    MemoryConverter, StatisticsCalculator, ThresholdClassifier, 
    RecommendationEngine, ReportGenerator
)


@dataclass
class AnalysisResult:
    """Analysis result data structure"""
    pod_name: str
    node_name: str
    resource_type: ResourceType
    performance_level: PerformanceLevel
    current_usage: float
    threshold_info: PerformanceThreshold
    recommendations: List[str]
    severity_score: float  # 0-100, higher is worse


class PODsPerformanceAnalyzer(BasePerformanceAnalyzer):
    """Analyzes pod performance metrics and provides insights using utility base class"""
    
    def __init__(self):
        super().__init__("pods_performance")
        
        # Initialize thresholds using utility functions
        self.cpu_thresholds = ThresholdClassifier.get_default_cpu_threshold()
        self.memory_thresholds = ThresholdClassifier.get_default_memory_threshold()
    
    def analyze_metrics_data(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Main entry point for analyzing pod metrics data"""
        return self.analyze_usage_summary(metrics_data)
    
    def analyze_pod_usage(self, pod_summary: Dict[str, Any]) -> List[AnalysisResult]:
        """Analyze individual pod usage metrics"""
        results = []
        pod_name = pod_summary.get('pod_name', 'unknown')
        node_name = pod_summary.get('node_name', 'unknown')
        usage_metrics = pod_summary.get('usage_metrics', {})
        
        for metric_name, metric_data in usage_metrics.items():
            # Determine resource type
            resource_type = None
            if 'cpu' in metric_name.lower():
                resource_type = ResourceType.CPU
            elif 'memory' in metric_name.lower():
                resource_type = ResourceType.MEMORY
            else:
                continue  # Skip non-CPU/memory metrics for now
            
            # Extract usage value (prefer avg for duration queries, value for instant)
            usage_value = metric_data.get('avg') or metric_data.get('value', 0.0)
            unit = metric_data.get('unit', '')
            
            if resource_type == ResourceType.CPU:
                level, severity = ThresholdClassifier.classify_performance(usage_value, self.cpu_thresholds)
                recommendations = RecommendationEngine.generate_resource_recommendations(
                    resource_type, level, usage_value, unit
                )
                
                results.append(AnalysisResult(
                    pod_name=pod_name,
                    node_name=node_name,
                    resource_type=resource_type,
                    performance_level=level,
                    current_usage=usage_value,
                    threshold_info=self.cpu_thresholds,
                    recommendations=recommendations,
                    severity_score=severity
                ))
                
            elif resource_type == ResourceType.MEMORY:
                # Convert to MB for consistent analysis using utility
                usage_mb = MemoryConverter.to_mb(usage_value, unit)
                level, severity = ThresholdClassifier.classify_performance(usage_mb, self.memory_thresholds)
                recommendations = RecommendationEngine.generate_resource_recommendations(
                    resource_type, level, usage_mb, "MB"
                )
                
                results.append(AnalysisResult(
                    pod_name=pod_name,
                    node_name=node_name,
                    resource_type=resource_type,
                    performance_level=level,
                    current_usage=usage_mb,
                    threshold_info=self.memory_thresholds,
                    recommendations=recommendations,
                    severity_score=severity
                ))
        
        return results
    
    def analyze_usage_summary(self, usage_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze complete usage summary and provide comprehensive insights"""
        collection_type = usage_summary.get('collection_type', 'unknown')
        top_pods = usage_summary.get('top_10_pods', [])
        
        # Generate metadata using base class
        metadata = self.generate_metadata(collection_type, len(top_pods))
        
        all_analyses = []
        all_alerts = []
        critical_pods = []
        poor_performance_pods = []
        node_analysis = {}
        resource_analysis = {'cpu': [], 'memory': []}
        
        # Analyze each pod
        for pod in top_pods:
            pod_analyses = self.analyze_pod_usage(pod)
            all_analyses.extend(pod_analyses)
            
            # Convert analyses to alerts and track performance issues
            for analysis in pod_analyses:
                # Create alert if performance is poor or critical
                if analysis.performance_level in [PerformanceLevel.CRITICAL, PerformanceLevel.POOR]:
                    severity = AlertLevel.CRITICAL if analysis.performance_level == PerformanceLevel.CRITICAL else AlertLevel.HIGH
                    
                    alert = PerformanceAlert(
                        severity=severity,
                        resource_type=analysis.resource_type,
                        pod_name=analysis.pod_name,
                        container_name='',
                        node_name=analysis.node_name,
                        message=f"{analysis.resource_type.value.upper()} usage {analysis.performance_level.value}: {analysis.current_usage:.2f}",
                        current_value=analysis.current_usage,
                        threshold_value=self._get_threshold_for_level(analysis.threshold_info, analysis.performance_level),
                        unit=self._get_unit_for_resource(analysis.resource_type),
                        timestamp=self.analysis_timestamp.isoformat()
                    )
                    all_alerts.append(alert)
                
                # Track critical and poor performance pods
                if analysis.performance_level == PerformanceLevel.CRITICAL:
                    critical_pods.append(analysis)
                elif analysis.performance_level == PerformanceLevel.POOR:
                    poor_performance_pods.append(analysis)
                
                # Aggregate by node
                node = analysis.node_name
                if node not in node_analysis:
                    node_analysis[node] = {'pods': [], 'severities': [], 'issues': []}
                node_analysis[node]['pods'].append(analysis.pod_name)
                node_analysis[node]['severities'].append(analysis.severity_score)
                
                # Aggregate by resource type
                if analysis.resource_type == ResourceType.CPU:
                    resource_analysis['cpu'].append(analysis.current_usage)
                elif analysis.resource_type == ResourceType.MEMORY:
                    resource_analysis['memory'].append(analysis.current_usage)
        
        # Calculate cluster health using base class method
        cluster_health = self.calculate_cluster_health_score(all_alerts, len(top_pods))
        
        # Calculate node-level analysis using utility statistics
        for node, node_data in node_analysis.items():
            if node_data['severities']:
                severity_stats = StatisticsCalculator.calculate_basic_stats(node_data['severities'])
                node_data['avg_severity'] = severity_stats['mean']
                node_data['max_severity'] = severity_stats['max']
                node_data['pod_count'] = len(set(node_data['pods']))
                
                # Generate node-level recommendations
                if node_data['avg_severity'] > 60:
                    node_data['recommendations'] = [
                        "Consider redistributing workloads across nodes",
                        "Monitor node resource capacity",
                        "Check for node-level resource contention"
                    ]
        
        # Calculate resource-level statistics using utility
        cpu_stats = StatisticsCalculator.calculate_basic_stats(resource_analysis['cpu']) if resource_analysis['cpu'] else {}
        memory_stats = StatisticsCalculator.calculate_basic_stats(resource_analysis['memory']) if resource_analysis['memory'] else {}
        
        if cpu_stats:
            cpu_stats['unit'] = '%'
        if memory_stats:
            memory_stats['unit'] = 'MB'
        
        # Generate cluster-level recommendations using utility
        cluster_recommendations = RecommendationEngine.generate_cluster_recommendations(
            cluster_health.critical_issues_count,
            cluster_health.warning_issues_count,
            len(top_pods),
            "pods"
        )
        
        return {
            'analysis_metadata': metadata.__dict__,
            'cluster_health': cluster_health.__dict__,
            'resource_statistics': {
                'cpu': cpu_stats,
                'memory': memory_stats
            },
            'node_analysis': dict(sorted(node_analysis.items(), 
                                       key=lambda x: x[1].get('avg_severity', 0), 
                                       reverse=True)),
            'critical_pods': [self._format_analysis_result(a) for a in critical_pods],
            'poor_performance_pods': [self._format_analysis_result(a) for a in poor_performance_pods],
            'cluster_recommendations': cluster_recommendations,
            'alerts': [alert.__dict__ for alert in all_alerts],
            'detailed_analyses': [self._format_analysis_result(a) for a in all_analyses]
        }
    
    def _get_threshold_for_level(self, threshold: PerformanceThreshold, level: PerformanceLevel) -> float:
        """Get threshold value for a performance level"""
        if level == PerformanceLevel.CRITICAL:
            return threshold.poor_max
        elif level == PerformanceLevel.POOR:
            return threshold.moderate_max
        elif level == PerformanceLevel.MODERATE:
            return threshold.good_max
        else:
            return threshold.excellent_max
    
    
    def _format_analysis_result(self, analysis: AnalysisResult) -> Dict[str, Any]:
        """Format analysis result for JSON serialization"""
        return {
            'pod_name': analysis.pod_name,
            'node_name': analysis.node_name,
            'resource_type': analysis.resource_type.value,
            'performance_level': analysis.performance_level.value,
            'current_usage': round(analysis.current_usage, 2),
            'severity_score': round(analysis.severity_score, 2),
            'unit': self._get_unit_for_resource(analysis.resource_type),
            'thresholds': {
                'excellent_max': analysis.threshold_info.excellent_max,
                'good_max': analysis.threshold_info.good_max,
                'moderate_max': analysis.threshold_info.moderate_max,
                'poor_max': analysis.threshold_info.poor_max
            },
            'recommendations': analysis.recommendations
        }
    
    def generate_performance_report(self, analysis: Dict[str, Any], output_file: Optional[str] = None) -> str:
        """Generate a human-readable performance report using utility report generator"""
        report_lines = []
        
        # Use ReportGenerator for summary section
        metadata_dict = analysis['analysis_metadata']
        metadata = AnalysisMetadata(**metadata_dict)
        
        cluster_health_dict = analysis['cluster_health']
        cluster_health = ClusterHealth(**cluster_health_dict)
        
        summary_lines = ReportGenerator.generate_summary_section(metadata, cluster_health)
        report_lines.extend(summary_lines)
        
        # Resource Statistics
        report_lines.append("RESOURCE STATISTICS")
        report_lines.append("-" * 40)
        
        cpu_stats = analysis['resource_statistics'].get('cpu', {})
        if cpu_stats:
            report_lines.extend([
                f"CPU Usage Statistics:",
                f"  Average: {cpu_stats.get('mean', 0):.2f}%",
                f"  Minimum: {cpu_stats.get('min', 0):.2f}%",
                f"  Maximum: {cpu_stats.get('max', 0):.2f}%",
                f"  Median: {cpu_stats.get('median', 0):.2f}%",
                ""
            ])
        
        memory_stats = analysis['resource_statistics'].get('memory', {})
        if memory_stats:
            report_lines.extend([
                f"Memory Usage Statistics:",
                f"  Average: {memory_stats.get('mean', 0):.2f} MB",
                f"  Minimum: {memory_stats.get('min', 0):.2f} MB", 
                f"  Maximum: {memory_stats.get('max', 0):.2f} MB",
                f"  Median: {memory_stats.get('median', 0):.2f} MB",
                ""
            ])
        
        # Use ReportGenerator for alerts section
        alerts = [PerformanceAlert(**alert_dict) for alert_dict in analysis.get('alerts', [])]
        alert_lines = ReportGenerator.generate_alerts_section(alerts)
        report_lines.extend(alert_lines)
        
        # Poor Performance Pods
        poor_pods = analysis.get('poor_performance_pods', [])
        if poor_pods:
            report_lines.extend([
                "POOR PERFORMANCE PODS",
                "-" * 40
            ])
            for pod in poor_pods[:5]:  # Show top 5 poor performance pods
                report_lines.extend([
                    f"Pod: {pod['pod_name']} (Node: {pod['node_name']})",
                    f"  Resource: {pod['resource_type'].upper()}",
                    f"  Current Usage: {pod['current_usage']:.2f} {pod['unit']}",
                    f"  Severity Score: {pod['severity_score']:.1f}/100",
                    ""
                ])
        
        # Cluster Recommendations
        recommendations = analysis.get('cluster_recommendations', [])
        if recommendations:
            report_lines.extend([
                "CLUSTER RECOMMENDATIONS", 
                "-" * 40
            ])
            for i, rec in enumerate(recommendations, 1):
                report_lines.append(f"{i}. {rec}")
            report_lines.append("")
        
        # Node Analysis Summary
        node_analysis = analysis.get('node_analysis', {})
        if node_analysis:
            report_lines.extend([
                "NODE ANALYSIS SUMMARY",
                "-" * 40
            ])
            for node, data in list(node_analysis.items())[:5]:  # Top 5 nodes by severity
                report_lines.extend([
                    f"Node: {node}",
                    f"  Average Severity: {data.get('avg_severity', 0):.1f}/100",
                    f"  Pod Count: {data.get('pod_count', 0)}",
                    f"  Max Severity: {data.get('max_severity', 0):.1f}/100",
                    ""
                ])
        
        report_lines.extend([
            "=" * 80,
            "END OF REPORT",
            "=" * 80
        ])
        
        report_text = "\n".join(report_lines)
        
        # Save to file if requested using utility
        if output_file:
            if ReportGenerator.save_text_report(report_text, output_file):
                print(f"Performance report saved to {output_file}")
        
        return report_text
    
    def export_analysis_json(self, analysis: Dict[str, Any], output_file: str) -> None:
        """Export analysis results to JSON file using utility"""
        if ReportGenerator.save_json_report(analysis, output_file):
            print(f"Analysis exported to {output_file}")


# Convenience functions
def analyze_pod_performance(usage_summary: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze pod performance from usage summary"""
    analyzer = PODsPerformanceAnalyzer()
    return analyzer.analyze_usage_summary(usage_summary)


def generate_performance_report(usage_summary: Dict[str, Any], output_file: Optional[str] = None) -> str:
    """Generate performance report from usage summary"""
    analyzer = PODsPerformanceAnalyzer()
    analysis = analyzer.analyze_usage_summary(usage_summary)
    return analyzer.generate_performance_report(analysis, output_file)


def analyze_and_export(usage_summary: Dict[str, Any], 
                      json_output: Optional[str] = None, 
                      report_output: Optional[str] = None) -> Dict[str, Any]:
    """Analyze performance and export results"""
    analyzer = PODsPerformanceAnalyzer()
    analysis = analyzer.analyze_usage_summary(usage_summary)
    
    if json_output:
        analyzer.export_analysis_json(analysis, json_output)
    
    if report_output:
        analyzer.generate_performance_report(analysis, report_output)
    
    return analysis


# Example usage
def main():
    """Example usage of updated performance analyzer"""
    # Sample usage summary data
    sample_data = {
        'collection_type': 'instant',
        'top_10_pods': [
            {
                'pod_name': 'test-pod-1',
                'node_name': 'worker-1',
                'usage_metrics': {
                    'cpu_usage': {'value': 85.5, 'unit': '%'},
                    'memory_usage': {'value': 2048, 'unit': 'MB'}
                }
            },
            {
                'pod_name': 'test-pod-2',
                'node_name': 'worker-2',
                'usage_metrics': {
                    'cpu_usage': {'value': 25.0, 'unit': '%'},
                    'memory_usage': {'value': 512, 'unit': 'MB'}
                }
            }
        ]
    }
    
    analyzer = PODsPerformanceAnalyzer()
    analysis = analyzer.analyze_usage_summary(sample_data)
    report = analyzer.generate_performance_report(analysis)
    
    print("Analysis completed!")
    print(f"Cluster health score: {analysis['cluster_health']['overall_score']:.1f}/100")
    print(f"Health level: {analysis['cluster_health']['health_level']}")


if __name__ == "__main__":
    main()