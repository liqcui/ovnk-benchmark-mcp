"""
OVN-Kubernetes Sync Duration Performance Analysis
Optimized version using common utilities
"""

from .ovnk_benchmark_performance_utility import (
    BasePerformanceAnalyzer, PerformanceLevel, ResourceType, PerformanceThreshold,
    StatisticsCalculator, ThresholdClassifier, RecommendationEngine, ReportGenerator,
    MemoryConverter, AnalysisMetadata, ClusterHealth, PerformanceAlert, AlertLevel
)

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import json


@dataclass
class SyncDurationAnalysis:
    """Analysis results for sync duration"""
    metric_name: str
    total_samples: int
    performance_level: PerformanceLevel
    statistics: Dict[str, float]
    percentiles: Dict[str, float]
    outliers: List[Dict[str, Any]]
    performance_distribution: Dict[str, int]
    recommendations: List[str]
    anomalies: List[Dict[str, Any]]


class OVNKubeSyncDurationAnalyzer(BasePerformanceAnalyzer):
    """Analyzer for OVN-Kubernetes sync duration performance"""
    
    def __init__(self, custom_thresholds: Optional[Dict[str, PerformanceThreshold]] = None):
        super().__init__("OVN-Kubernetes Sync Duration")
        self.component_thresholds = custom_thresholds or self._get_default_component_thresholds()
    
    def _get_default_component_thresholds(self) -> Dict[str, PerformanceThreshold]:
        """Get component-specific thresholds"""
        return {
            'ovnkube_controller_ready_duration_seconds': PerformanceThreshold(
                excellent_max=1.0, good_max=5.0, moderate_max=15.0, poor_max=30.0,
                unit="seconds", component_type="controller_ready"
            ),
            'ovnkube_node_ready_duration_seconds': PerformanceThreshold(
                excellent_max=0.5, good_max=2.0, moderate_max=10.0, poor_max=20.0,
                unit="seconds", component_type="node_ready"
            ),
            'ovnkube_controller_sync_duration_seconds': PerformanceThreshold(
                excellent_max=0.05, good_max=0.2, moderate_max=1.0, poor_max=5.0,
                unit="seconds", component_type="controller_sync"
            ),
            'default': ThresholdClassifier.get_default_sync_duration_threshold()
        }
    
    def _get_thresholds_for_metric(self, metric_name: str) -> PerformanceThreshold:
        """Get appropriate thresholds for a specific metric"""
        return self.component_thresholds.get(metric_name, self.component_thresholds['default'])
    
    def analyze_metrics_data(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze sync duration metrics"""
        collection_type = results.get('collection_type', 'unknown')
        
        if collection_type == 'instant':
            return self.analyze_instant_metrics(results)
        else:
            return self.analyze_duration_metrics(results)
    
    def analyze_instant_metrics(self, results: Dict[str, Any]) -> Dict[str, SyncDurationAnalysis]:
        """Analyze instant metrics collection results"""
        analyses = {}
        
        for metric_name, metric_data in results.get('metrics', {}).items():
            if 'error' in metric_data:
                continue
            
            # Extract values and labels
            values = []
            labels_list = []
            
            for item in metric_data.get('data', []):
                if item.get('value') is not None:
                    values.append(item['value'])
                    labels_list.append(item.get('labels', {}))
            
            if not values:
                continue
            
            # Get appropriate thresholds and calculate statistics
            thresholds = self._get_thresholds_for_metric(metric_name)
            stats = StatisticsCalculator.calculate_basic_stats(values)
            percentiles = StatisticsCalculator.calculate_percentiles(values)
            
            # Classify overall performance based on 95th percentile
            p95 = percentiles.get('p95', stats.get('max', 0))
            performance_level, _ = ThresholdClassifier.classify_performance(p95, thresholds)
            
            # Detect outliers
            outliers = StatisticsCalculator.detect_outliers_iqr(values, labels_list)
            
            # Calculate performance distribution
            perf_dist = {level.value: 0 for level in PerformanceLevel}
            for value in values:
                level, _ = ThresholdClassifier.classify_performance(value, thresholds)
                perf_dist[level.value] += 1
            
            # Create analysis
            analysis = SyncDurationAnalysis(
                metric_name=metric_name,
                total_samples=len(values),
                performance_level=performance_level,
                statistics=stats,
                percentiles=percentiles,
                outliers=outliers,
                performance_distribution=perf_dist,
                recommendations=self._generate_sync_recommendations(metric_name, performance_level, p95),
                anomalies=[]
            )
            
            analyses[metric_name] = analysis
        
        return analyses
    
    def analyze_duration_metrics(self, results: Dict[str, Any]) -> Dict[str, SyncDurationAnalysis]:
        """Analyze duration metrics collection results"""
        analyses = {}
        
        for metric_name, metric_data in results.get('metrics', {}).items():
            if 'error' in metric_data:
                continue
            
            # Get time series data
            time_series = results.get('time_series_summary', {}).get(metric_name, [])
            
            # Extract max values from time series summary
            values = []
            labels_list = []
            
            for series_info in time_series:
                if series_info.get('max_value') is not None:
                    values.append(series_info['max_value'])
                    labels_list.append({'pod': series_info.get('pod_name', 'unknown')})
            
            if not values:
                continue
            
            # Get appropriate thresholds and calculate statistics
            thresholds = self._get_thresholds_for_metric(metric_name)
            stats = StatisticsCalculator.calculate_basic_stats(values)
            percentiles = StatisticsCalculator.calculate_percentiles(values)
            
            # Classify overall performance
            p95 = percentiles.get('p95', stats.get('max', 0))
            performance_level, _ = ThresholdClassifier.classify_performance(p95, thresholds)
            
            # Detect outliers and anomalies
            outliers = StatisticsCalculator.detect_outliers_iqr(values, labels_list)
            anomalies = self._detect_time_series_anomalies(time_series, thresholds)
            
            # Calculate performance distribution
            perf_dist = {level.value: 0 for level in PerformanceLevel}
            for value in values:
                level, _ = ThresholdClassifier.classify_performance(value, thresholds)
                perf_dist[level.value] += 1
            
            # Create analysis
            analysis = SyncDurationAnalysis(
                metric_name=metric_name,
                total_samples=len(values),
                performance_level=performance_level,
                statistics=stats,
                percentiles=percentiles,
                outliers=outliers,
                performance_distribution=perf_dist,
                recommendations=self._generate_sync_recommendations(metric_name, performance_level, p95),
                anomalies=anomalies
            )
            
            analyses[metric_name] = analysis
        
        return analyses
    
    def _detect_time_series_anomalies(self, time_series: List[Dict[str, Any]], 
                                    thresholds: PerformanceThreshold) -> List[Dict[str, Any]]:
        """Detect anomalies in time series data"""
        anomalies = []
        
        if not time_series:
            return anomalies
        
        # Extract all max values to establish baseline
        all_max_values = [ts.get('max_value', 0) for ts in time_series if ts.get('max_value')]
        
        if len(all_max_values) < 3:
            return anomalies
        
        mean_max = StatisticsCalculator.calculate_basic_stats(all_max_values)['mean']
        
        # Define anomaly threshold
        anomaly_threshold = max(
            mean_max * 3,  # 3x average
            thresholds.poor_max * 2  # 2x poor threshold
        )
        
        for ts in time_series:
            max_val = ts.get('max_value')
            if max_val and max_val > anomaly_threshold:
                anomalies.append({
                    'pod_name': ts.get('pod_name', 'unknown'),
                    'max_value': max_val,
                    'threshold_exceeded': max_val / anomaly_threshold,
                    'data_points': ts.get('data_points', 0),
                    'type': 'sustained_high_duration'
                })
        
        return anomalies
    
    def _generate_sync_recommendations(self, metric_name: str, 
                                     performance_level: PerformanceLevel, 
                                     current_value: float) -> List[str]:
        """Generate sync duration specific recommendations"""
        # Get base recommendations
        recommendations = RecommendationEngine.generate_resource_recommendations(
            ResourceType.SYNC_DURATION, performance_level, current_value, "seconds"
        )
        
        # Add component-specific recommendations
        if 'controller' in metric_name.lower():
            if performance_level in [PerformanceLevel.POOR, PerformanceLevel.CRITICAL]:
                recommendations.extend([
                    "Check OVN database performance and connections",
                    "Consider controller High Availability configuration",
                    "Monitor OVN northbound/southbound database load"
                ])
        elif 'node' in metric_name.lower():
            if performance_level in [PerformanceLevel.POOR, PerformanceLevel.CRITICAL]:
                recommendations.extend([
                    "Check CNI plugin configuration on affected nodes",
                    "Monitor node resource utilization",
                    "Verify OVS daemon performance on nodes"
                ])
        
        return recommendations
    
    def generate_comprehensive_report(self, analyses: Dict[str, SyncDurationAnalysis], 
                                    collection_info: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        # Convert analyses to alerts for unified reporting
        alerts = []
        total_items = 0
        
        for metric_name, analysis in analyses.items():
            total_items += analysis.total_samples
            
            # Create alerts for poor/critical performance
            if analysis.performance_level in [PerformanceLevel.POOR, PerformanceLevel.CRITICAL]:
                severity = AlertLevel.CRITICAL if analysis.performance_level == PerformanceLevel.CRITICAL else AlertLevel.HIGH
                
                alert = PerformanceAlert(
                    severity=severity,
                    resource_type=ResourceType.SYNC_DURATION,
                    pod_name=metric_name,
                    message=f"{analysis.performance_level.value.title()} sync duration detected",
                    current_value=analysis.percentiles.get('p95', 0),
                    threshold_value=self._get_thresholds_for_metric(metric_name).poor_max,
                    unit="seconds",
                    timestamp=self.analysis_timestamp.isoformat()
                )
                alerts.append(alert)
        
        # Generate metadata and health assessment
        metadata = self.generate_metadata(
            collection_info.get('collection_type', 'unknown'), 
            len(analyses),
            collection_info.get('duration')
        )
        
        cluster_health = self.calculate_cluster_health_score(alerts, len(analyses))
        
        # Build comprehensive report
        report = {
            'report_metadata': {
                'generated_at': metadata.analysis_timestamp,
                'collection_type': metadata.collection_type,
                'analyzer_type': metadata.analyzer_type,
                'total_metrics': len(analyses),
                'analysis_period': {
                    'start_time': collection_info.get('start_time'),
                    'end_time': collection_info.get('end_time'),
                    'duration': collection_info.get('duration')
                }
            },
            'cluster_health': {
                'overall_score': cluster_health.overall_score,
                'health_level': cluster_health.health_level,
                'critical_issues': cluster_health.critical_issues_count,
                'warning_issues': cluster_health.warning_issues_count
            },
            'metric_analyses': {},
            'performance_summary': self._generate_performance_summary(analyses),
            'recommendations': self._generate_cluster_recommendations(analyses, alerts),
            'component_health': self._analyze_component_health(analyses)
        }
        
        # Add individual metric analyses
        for metric_name, analysis in analyses.items():
            report['metric_analyses'][metric_name] = {
                'metric_name': analysis.metric_name,
                'total_samples': analysis.total_samples,
                'performance_level': analysis.performance_level.value,
                'statistics': analysis.statistics,
                'percentiles': analysis.percentiles,
                'outliers_count': len(analysis.outliers),
                'anomalies_count': len(analysis.anomalies),
                'performance_distribution': analysis.performance_distribution,
                'recommendations': analysis.recommendations
            }
        
        return report
    
    def _generate_performance_summary(self, analyses: Dict[str, SyncDurationAnalysis]) -> Dict[str, Any]:
        """Generate performance summary across all metrics"""
        if not analyses:
            return {}
        
        all_p95_values = []
        performance_counts = {level.value: 0 for level in PerformanceLevel}
        total_outliers = 0
        total_anomalies = 0
        
        for analysis in analyses.values():
            all_p95_values.append(analysis.percentiles.get('p95', 0))
            performance_counts[analysis.performance_level.value] += 1
            total_outliers += len(analysis.outliers)
            total_anomalies += len(analysis.anomalies)
        
        summary_stats = StatisticsCalculator.calculate_basic_stats(all_p95_values)
        
        return {
            'overall_p95_statistics': summary_stats,
            'performance_distribution': performance_counts,
            'total_outliers': total_outliers,
            'total_anomalies': total_anomalies,
            'worst_performing_metric': max(analyses.items(), key=lambda x: x[1].percentiles.get('p95', 0))[0] if analyses else None,
            'best_performing_metric': min(analyses.items(), key=lambda x: x[1].percentiles.get('p95', 0))[0] if analyses else None
        }
    
    def _generate_cluster_recommendations(self, analyses: Dict[str, SyncDurationAnalysis], 
                                        alerts: List[PerformanceAlert]) -> List[str]:
        """Generate cluster-level recommendations"""
        critical_count = sum(1 for a in alerts if a.severity == AlertLevel.CRITICAL)
        warning_count = sum(1 for a in alerts if a.severity == AlertLevel.HIGH)
        
        recommendations = RecommendationEngine.generate_cluster_recommendations(
            critical_count, warning_count, len(analyses), "sync duration"
        )
        
        # Add sync-specific recommendations
        if critical_count > 0:
            recommendations.extend([
                "Scale cluster resources or investigate resource constraints",
                "Review OVN-Kubernetes component logs for errors"
            ])
        
        return recommendations
    
    def _analyze_component_health(self, analyses: Dict[str, SyncDurationAnalysis]) -> Dict[str, Any]:
        """Analyze health by component type"""
        component_health = {}
        
        for metric_name, analysis in analyses.items():
            component = 'general'
            if 'controller' in metric_name.lower():
                component = 'ovnkube-controller'
            elif 'node' in metric_name.lower():
                component = 'ovnkube-node'
            
            if component not in component_health:
                component_health[component] = {
                    'metrics_count': 0,
                    'average_p95': 0,
                    'performance_levels': []
                }
            
            component_health[component]['metrics_count'] += 1
            component_health[component]['average_p95'] += analysis.percentiles.get('p95', 0)
            component_health[component]['performance_levels'].append(analysis.performance_level.value)
        
        # Finalize calculations
        for component, health_data in component_health.items():
            if health_data['metrics_count'] > 0:
                health_data['average_p95'] /= health_data['metrics_count']
            
            # Determine overall health
            levels = health_data['performance_levels']
            if 'critical' in levels:
                health_data['overall_health'] = 'critical'
            elif 'poor' in levels:
                health_data['overall_health'] = 'poor'
            elif 'moderate' in levels:
                health_data['overall_health'] = 'moderate'
            else:
                health_data['overall_health'] = 'good'
        
        return component_health
    
    def save_analysis_report(self, report: Dict[str, Any], filename: Optional[str] = None) -> str:
        """Save analysis report to JSON file"""
        if not filename:
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
            collection_type = report.get('report_metadata', {}).get('collection_type', 'unknown')
            filename = f"ovnk_sync_analysis_{collection_type}_{timestamp}.json"
        
        if ReportGenerator.save_json_report(report, filename):
            print(f"Analysis report saved to: {filename}")
            return filename
        return ""


# Maintain backward compatibility
class OVNKubePerformanceMonitor:
    """High-level monitoring class that combines collection and analysis"""
    
    def __init__(self, prometheus_client, analyzer: Optional[OVNKubeSyncDurationAnalyzer] = None):
        from ovnk_benchmark_prometheus_ovnk_sync import OVNSyncDurationCollector
        
        self.collector = OVNSyncDurationCollector(prometheus_client)
        self.analyzer = analyzer or OVNKubeSyncDurationAnalyzer()
    
    async def monitor_instant_performance(self, save_results: bool = True) -> Dict[str, Any]:
        """Monitor instant performance and generate analysis"""
        print("Starting instant performance monitoring...")
        
        results = await self.collector.collect_instant_metrics()
        analyses = self.analyzer.analyze_instant_metrics(results)
        report = self.analyzer.generate_comprehensive_report(analyses, results)
        
        if save_results:
            self.collector.save_results(results)
            self.analyzer.save_analysis_report(report)
        
        return report
    
    async def monitor_duration_performance(self, duration: str, save_results: bool = True) -> Dict[str, Any]:
        """Monitor performance over duration and generate analysis"""
        print(f"Starting duration performance monitoring ({duration})...")
        
        results = await self.collector.collect_sync_duration_seconds_metrics(duration)
        analyses = self.analyzer.analyze_duration_metrics(results)
        report = self.analyzer.generate_comprehensive_report(analyses, results)
        
        if save_results:
            self.collector.save_results(results)
            self.analyzer.save_analysis_report(report)
        
        return report


def print_analysis_summary(report: Dict[str, Any]) -> None:
    """Print analysis summary to console"""
    metadata = report.get('report_metadata', {})
    health = report.get('cluster_health', {})
    summary = report.get('performance_summary', {})
    
    print("\n" + "=" * 60)
    print("OVN-KUBERNETES SYNC DURATION ANALYSIS SUMMARY")
    print("=" * 60)
    
    health_emoji = {
        'excellent': '✅', 'good': '✅', 'moderate': '⚠️', 
        'poor': '🔶', 'critical': '🚨'
    }.get(health.get('health_level', 'unknown'), '❓')
    
    print(f"\n{health_emoji} Overall Health: {health.get('health_level', 'unknown').upper()}")
    print(f"Health Score: {health.get('overall_score', 0)}/100")
    print(f"Metrics Analyzed: {metadata.get('total_metrics', 0)}")
    print(f"Critical Issues: {health.get('critical_issues', 0)}")
    print(f"Warning Issues: {health.get('warning_issues', 0)}")
    
    if summary:
        print(f"\nOutliers Found: {summary.get('total_outliers', 0)}")
        print(f"Anomalies Detected: {summary.get('total_anomalies', 0)}")
        
        if summary.get('worst_performing_metric'):
            print(f"Worst Performing: {summary['worst_performing_metric']}")
        if summary.get('best_performing_metric'):
            print(f"Best Performing: {summary['best_performing_metric']}")
    
    recommendations = report.get('recommendations', [])
    if recommendations:
        print(f"\nTOP RECOMMENDATIONS:")
        for rec in recommendations[:3]:
            print(f"  • {rec}")
    
    print("\n" + "=" * 60)


# Example usage
async def example_usage():
    """Example usage of the optimized sync duration analyzer"""
    import sys
    sys.path.append('../')
    
    from ovnk_benchmark_auth import auth
    from ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery
    
    # Initialize authentication
    await auth.initialize()
    
    # Create Prometheus client
    prometheus_client = PrometheusBaseQuery(
        prometheus_url=auth.prometheus_url,
        token=auth.prometheus_token
    )
    
    # Create monitor with optimized analyzer
    analyzer = OVNKubeSyncDurationAnalyzer()
    monitor = OVNKubePerformanceMonitor(prometheus_client, analyzer)
    
    try:
        # Test instant monitoring
        print("Testing optimized instant performance monitoring...")
        instant_report = await monitor.monitor_instant_performance()
        print_analysis_summary(instant_report)
        
        # Test duration monitoring
        print("\nTesting optimized duration performance monitoring...")
        duration_report = await monitor.monitor_duration_performance('10m')
        print_analysis_summary(duration_report)
        
    finally:
        await prometheus_client.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(example_usage())