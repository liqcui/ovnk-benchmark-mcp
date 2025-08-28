"""
OVNK Benchmark Performance Analysis Utility Module
Common utilities and base classes for performance analysis across different components
"""

import json
import statistics
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass, asdict
from enum import Enum
from abc import ABC, abstractmethod

class PerformanceLevel(Enum):
    """Unified performance level classification"""
    EXCELLENT = "excellent"
    GOOD = "good"
    MODERATE = "moderate"
    POOR = "poor"
    CRITICAL = "critical"


class AlertLevel(Enum):
    """Unified alert severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ResourceType(Enum):
    """Unified resource type enumeration"""
    CPU = "cpu"
    MEMORY = "memory"
    DB_SIZE = "db_size"
    NETWORK = "network"
    DISK = "disk"
    SYNC_DURATION = "sync_duration"


@dataclass
class PerformanceThreshold:
    """Unified performance threshold structure"""
    excellent_max: float
    good_max: float
    moderate_max: float
    poor_max: float
    unit: str
    component_type: str = "general"


@dataclass
class PerformanceAlert:
    """Unified performance alert structure"""
    severity: AlertLevel
    resource_type: ResourceType
    pod_name: str
    container_name: str = ""
    node_name: str = ""
    message: str = ""
    current_value: float = 0.0
    threshold_value: float = 0.0
    unit: str = ""
    timestamp: str = ""


@dataclass
class AnalysisMetadata:
    """Common metadata for all analyses"""
    analysis_timestamp: str
    analyzer_type: str
    collection_type: str
    total_items_analyzed: int
    duration: Optional[str] = None


@dataclass 
class ClusterHealth:
    """Unified cluster health assessment"""
    overall_score: float  # 0-100
    health_level: str
    critical_issues_count: int
    warning_issues_count: int
    healthy_items_count: int


class BasePerformanceAnalyzer(ABC):
    """Base class for all performance analyzers"""
    
    def __init__(self, analyzer_type: str):
        self.analyzer_type = analyzer_type
        self.analysis_timestamp = datetime.now(timezone.utc)
    
    @abstractmethod
    def analyze_metrics_data(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Abstract method for analyzing metrics data"""
        pass
    
    def generate_metadata(self, collection_type: str, total_items: int, duration: Optional[str] = None) -> AnalysisMetadata:
        """Generate common analysis metadata"""
        return AnalysisMetadata(
            analysis_timestamp=self.analysis_timestamp.isoformat(),
            analyzer_type=self.analyzer_type,
            collection_type=collection_type,
            total_items_analyzed=total_items,
            duration=duration
        )
    
    def calculate_cluster_health_score(self, alerts: List[PerformanceAlert], total_items: int) -> ClusterHealth:
        """Calculate unified cluster health score"""
        if total_items == 0:
            return ClusterHealth(0.0, "unknown", 0, 0, 0)
        
        critical_count = sum(1 for a in alerts if a.severity == AlertLevel.CRITICAL)
        high_count = sum(1 for a in alerts if a.severity == AlertLevel.HIGH)
        medium_count = sum(1 for a in alerts if a.severity == AlertLevel.MEDIUM)
        
        # Start with perfect score and deduct based on issues
        score = 100.0
        score -= (critical_count / total_items) * 40  # Critical: up to 40 points
        score -= (high_count / total_items) * 25      # High: up to 25 points
        score -= (medium_count / total_items) * 15    # Medium: up to 15 points
        
        score = max(0.0, min(100.0, score))
        
        # Determine health level
        if score >= 90:
            health_level = "excellent"
        elif score >= 75:
            health_level = "good"
        elif score >= 60:
            health_level = "moderate"
        elif score >= 40:
            health_level = "poor"
        else:
            health_level = "critical"
        
        return ClusterHealth(
            overall_score=round(score, 2),
            health_level=health_level,
            critical_issues_count=critical_count,
            warning_issues_count=high_count + medium_count,
            healthy_items_count=total_items - len(alerts)
        )


class MemoryConverter:
    """Utility class for memory unit conversions"""
    
    @staticmethod
    def to_bytes(value: float, unit: str) -> float:
        """Convert memory value to bytes"""
        if value is None:
            return 0.0
        
        unit = unit.lower()
        multipliers = {
            'b': 1,
            'bytes': 1,
            'kb': 1024,
            'mb': 1024 * 1024,
            'gb': 1024 * 1024 * 1024,
            'tb': 1024 * 1024 * 1024 * 1024
        }
        return value * multipliers.get(unit, 1)
    
    @staticmethod
    def to_mb(value: float, unit: str) -> float:
        """Convert memory value to MB"""
        return MemoryConverter.to_bytes(value, unit) / (1024 * 1024)
    
    @staticmethod
    def to_gb(value: float, unit: str) -> float:
        """Convert memory value to GB"""
        return MemoryConverter.to_bytes(value, unit) / (1024 * 1024 * 1024)
    
    @staticmethod
    def format_memory(value_bytes: float, target_unit: str = "auto") -> Tuple[float, str]:
        """Format memory value with appropriate unit"""
        if target_unit != "auto":
            if target_unit.lower() == "mb":
                return value_bytes / (1024 * 1024), "MB"
            elif target_unit.lower() == "gb":
                return value_bytes / (1024 * 1024 * 1024), "GB"
            else:
                return value_bytes, "bytes"
        
        # Auto-select appropriate unit
        if value_bytes >= 1024 * 1024 * 1024:
            return value_bytes / (1024 * 1024 * 1024), "GB"
        elif value_bytes >= 1024 * 1024:
            return value_bytes / (1024 * 1024), "MB"
        elif value_bytes >= 1024:
            return value_bytes / 1024, "KB"
        else:
            return value_bytes, "bytes"


class StatisticsCalculator:
    """Utility class for common statistical calculations"""
    
    @staticmethod
    def calculate_basic_stats(values: List[float]) -> Dict[str, float]:
        """Calculate basic statistics for a list of values"""
        if not values:
            return {}
        
        return {
            'count': len(values),
            'min': min(values),
            'max': max(values),
            'mean': statistics.mean(values),
            'median': statistics.median(values),
            'std_dev': statistics.stdev(values) if len(values) > 1 else 0.0,
            'variance': statistics.variance(values) if len(values) > 1 else 0.0
        }
    
    @staticmethod
    def calculate_percentiles(values: List[float], percentiles: List[int] = [50, 75, 90, 95, 99]) -> Dict[str, float]:
        """Calculate percentiles for a list of values"""
        if not values:
            return {}
        
        sorted_values = sorted(values)
        result = {}
        
        for p in percentiles:
            idx = int(len(sorted_values) * p / 100)
            if idx >= len(sorted_values):
                idx = len(sorted_values) - 1
            result[f'p{p}'] = sorted_values[idx]
        
        return result
    
    @staticmethod
    def detect_outliers_iqr(values: List[float], labels: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Detect outliers using IQR method"""
        if len(values) < 4:
            return []
        
        sorted_indices = sorted(range(len(values)), key=lambda i: values[i])
        sorted_values = [values[i] for i in sorted_indices]
        
        q1_idx = len(sorted_values) // 4
        q3_idx = 3 * len(sorted_values) // 4
        
        q1 = sorted_values[q1_idx]
        q3 = sorted_values[q3_idx]
        iqr = q3 - q1
        
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        outliers = []
        for i, value in enumerate(values):
            if value < lower_bound or value > upper_bound:
                outlier_info = {
                    'value': value,
                    'index': i,
                    'type': 'low' if value < lower_bound else 'high',
                    'bounds': {'lower': lower_bound, 'upper': upper_bound}
                }
                if labels and i < len(labels):
                    outlier_info['label'] = labels[i]
                outliers.append(outlier_info)
        
        return outliers


class ThresholdClassifier:
    """Utility class for performance classification based on thresholds"""
    
    @staticmethod
    def classify_performance(value: float, thresholds: PerformanceThreshold) -> Tuple[PerformanceLevel, float]:
        """Classify performance level and calculate severity score (0-100)"""
        if value <= thresholds.excellent_max:
            severity = min(value / thresholds.excellent_max * 20, 20)
            return PerformanceLevel.EXCELLENT, severity
        elif value <= thresholds.good_max:
            severity = 20 + (value - thresholds.excellent_max) / (thresholds.good_max - thresholds.excellent_max) * 20
            return PerformanceLevel.GOOD, severity
        elif value <= thresholds.moderate_max:
            severity = 40 + (value - thresholds.good_max) / (thresholds.moderate_max - thresholds.good_max) * 20
            return PerformanceLevel.MODERATE, severity
        elif value <= thresholds.poor_max:
            severity = 60 + (value - thresholds.moderate_max) / (thresholds.poor_max - thresholds.moderate_max) * 20
            return PerformanceLevel.POOR, severity
        else:
            severity = min(80 + (value - thresholds.poor_max) / thresholds.poor_max * 20, 100)
            return PerformanceLevel.CRITICAL, severity
    
    @staticmethod
    def get_default_cpu_threshold() -> PerformanceThreshold:
        """Get default CPU performance threshold"""
        return PerformanceThreshold(
            excellent_max=20.0,
            good_max=50.0,
            moderate_max=75.0,
            poor_max=90.0,
            unit="%",
            component_type="cpu"
        )
    
    @staticmethod
    def get_default_memory_threshold() -> PerformanceThreshold:
        """Get default memory performance threshold (in MB)"""
        return PerformanceThreshold(
            excellent_max=1024.0,    # 1GB
            good_max=2048.0,         # 2GB
            moderate_max=4096.0,     # 4GB
            poor_max=8192.0,         # 8GB
            unit="MB",
            component_type="memory"
        )
    
    @staticmethod
    def get_default_sync_duration_threshold() -> PerformanceThreshold:
        """Get default sync duration threshold (in seconds)"""
        return PerformanceThreshold(
            excellent_max=0.1,       # 100ms
            good_max=0.5,           # 500ms
            moderate_max=2.0,       # 2s
            poor_max=10.0,          # 10s
            unit="seconds",
            component_type="sync_duration"
        )


class RecommendationEngine:
    """Utility class for generating common recommendations"""
    
    @staticmethod
    def generate_resource_recommendations(resource_type: ResourceType, 
                                        performance_level: PerformanceLevel,
                                        current_value: float,
                                        unit: str) -> List[str]:
        """Generate recommendations based on resource type and performance level"""
        recommendations = []
        
        if resource_type == ResourceType.CPU:
            if performance_level == PerformanceLevel.CRITICAL:
                recommendations.extend([
                    "URGENT: CPU usage is critically high, immediate action required",
                    "Consider scaling up the pod or increasing CPU limits",
                    "Check for CPU-intensive processes and optimize code paths"
                ])
            elif performance_level == PerformanceLevel.POOR:
                recommendations.extend([
                    "CPU usage is concerning, consider optimization",
                    "Review application performance and potential bottlenecks",
                    "Consider increasing CPU requests and limits"
                ])
            elif performance_level == PerformanceLevel.MODERATE:
                recommendations.extend([
                    "CPU usage is moderate, monitor for trends",
                    "Consider profiling application for optimization opportunities"
                ])
            else:
                recommendations.append("CPU usage is within acceptable range")
        
        elif resource_type == ResourceType.MEMORY:
            if performance_level == PerformanceLevel.CRITICAL:
                recommendations.extend([
                    "URGENT: Memory usage is critically high, pod may be at risk of OOM kill",
                    "Immediately increase memory limits or scale up",
                    "Check for memory leaks in the application"
                ])
            elif performance_level == PerformanceLevel.POOR:
                recommendations.extend([
                    "High memory usage detected, increase memory limits",
                    "Review application for memory optimization opportunities",
                    f"Current usage: {current_value:.1f}{unit} - consider increasing limits"
                ])
            elif performance_level == PerformanceLevel.MODERATE:
                recommendations.extend([
                    "Memory usage is moderate, monitor for growth trends",
                    "Ensure memory limits are set appropriately"
                ])
            else:
                recommendations.append("Memory usage is within acceptable range")
        
        elif resource_type == ResourceType.SYNC_DURATION:
            if performance_level == PerformanceLevel.CRITICAL:
                recommendations.extend([
                    "CRITICAL: Sync durations are extremely high, immediate investigation required",
                    "Check for resource constraints (CPU, memory) on affected nodes",
                    "Review OVN-Kubernetes controller logs for errors"
                ])
            elif performance_level == PerformanceLevel.POOR:
                recommendations.extend([
                    "Sync durations are higher than acceptable thresholds",
                    "Monitor resource utilization on nodes with high sync times",
                    "Check network connectivity between OVN components"
                ])
            elif performance_level == PerformanceLevel.MODERATE:
                recommendations.extend([
                    "Performance is moderate, consider optimization",
                    "Monitor trends to ensure performance doesn't degrade"
                ])
        
        return recommendations
    
    @staticmethod
    def generate_cluster_recommendations(critical_count: int, 
                                       warning_count: int,
                                       total_items: int,
                                       component_type: str = "general") -> List[str]:
        """Generate cluster-level recommendations"""
        recommendations = []
        
        if critical_count > 0:
            recommendations.append(f"URGENT: {critical_count} {component_type} component(s) in critical state")
            recommendations.append("Immediate attention required for critical components")
        
        if warning_count > total_items * 0.3:  # More than 30% in warning
            recommendations.append(f"High number of {component_type} components ({warning_count}) in warning state")
            recommendations.append("Consider cluster capacity review or optimization")
        
        if critical_count == 0 and warning_count == 0:
            recommendations.append(f"All {component_type} components are operating within normal parameters")
        
        return recommendations


class ReportGenerator:
    """Utility class for generating performance reports"""
    
    @staticmethod
    def generate_summary_section(metadata: AnalysisMetadata, health: ClusterHealth) -> List[str]:
        """Generate summary section for reports"""
        return [
            "=" * 80,
            f"{metadata.analyzer_type.upper()} PERFORMANCE ANALYSIS REPORT",
            "=" * 80,
            f"Analysis Timestamp: {metadata.analysis_timestamp}",
            f"Collection Type: {metadata.collection_type.title()}",
            f"Total Items Analyzed: {metadata.total_items_analyzed}",
            f"Duration: {metadata.duration or 'N/A'}",
            "",
            "OVERALL HEALTH ASSESSMENT",
            "-" * 40,
            f"Health Score: {health.overall_score}/100",
            f"Health Level: {health.health_level.title()}",
            f"Critical Issues: {health.critical_issues_count}",
            f"Warning Issues: {health.warning_issues_count}",
            f"Healthy Items: {health.healthy_items_count}",
            ""
        ]
    
    @staticmethod
    def generate_alerts_section(alerts: List[PerformanceAlert], max_alerts: int = 10) -> List[str]:
        """Generate alerts section for reports"""
        if not alerts:
            return ["No performance alerts detected.", ""]
        
        lines = ["PERFORMANCE ALERTS", "-" * 40]
        
        # Group by severity
        critical_alerts = [a for a in alerts if a.severity == AlertLevel.CRITICAL]
        high_alerts = [a for a in alerts if a.severity == AlertLevel.HIGH]
        medium_alerts = [a for a in alerts if a.severity == AlertLevel.MEDIUM]
        
        for severity_name, alert_list in [("CRITICAL", critical_alerts), ("HIGH", high_alerts), ("MEDIUM", medium_alerts)]:
            if alert_list:
                lines.append(f"{severity_name} ({len(alert_list)}):")
                for alert in alert_list[:max_alerts]:
                    lines.append(f"  • {alert.pod_name}: {alert.message}")
                    if alert.current_value and alert.unit:
                        lines.append(f"    Current: {alert.current_value:.2f} {alert.unit}")
                lines.append("")
        
        return lines
    
    @staticmethod
    def save_json_report(data: Dict[str, Any], filename: str) -> bool:
        """Save analysis data to JSON file"""
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            return True
        except Exception as e:
            print(f"Error saving JSON report: {e}")
            return False
    
    @staticmethod
    def save_text_report(report_text: str, filename: str) -> bool:
        """Save report text to file"""
        try:
            with open(filename, 'w') as f:
                f.write(report_text)
            return True
        except Exception as e:
            print(f"Error saving text report: {e}")
            return False

# Example usage and testing utilities
def create_sample_metrics_data(component_type: str = "general") -> Dict[str, Any]:
    """Create sample metrics data for testing"""
    if component_type == "ovn_sync":
        return {
            "collection_type": "instant",
            "metrics": {
                "ovnkube_controller_sync_duration_seconds": {
                    "data": [
                        {"pod_name": "ovnkube-controller-1", "value": 0.15, "labels": {}},
                        {"pod_name": "ovnkube-controller-2", "value": 2.5, "labels": {}}
                    ]
                }
            }
        }
    elif component_type == "pod_usage":
        return {
            "collection_type": "instant",
            "top_10_pods": [
                {
                    "pod_name": "test-pod-1",
                    "node_name": "worker-1",
                    "usage_metrics": {
                        "cpu_usage": {"value": 85.5, "unit": "%"},
                        "memory_usage": {"value": 2048, "unit": "MB"}
                    }
                }
            ]
        }
    else:
        return {
            "collection_type": "instant",
            "metrics": {
                "container_cpu_usage": {
                    "data": [
                        {"pod_name": "test-pod", "container_name": "test-container", "value": 75.0, "unit": "%"}
                    ]
                }
            }
        }


# Export utility functions for backward compatibility
def calculate_basic_statistics(values: List[float]) -> Dict[str, float]:
    """Calculate basic statistics - backward compatibility function"""
    return StatisticsCalculator.calculate_basic_stats(values)


def convert_memory_to_mb(value: float, unit: str) -> float:
    """Convert memory to MB - backward compatibility function"""
    return MemoryConverter.to_mb(value, unit)


def classify_performance_level(value: float, thresholds: PerformanceThreshold) -> Tuple[PerformanceLevel, float]:
    """Classify performance level - backward compatibility function"""
    return ThresholdClassifier.classify_performance(value, thresholds)

"""
Additional utility functions for ovnk_benchmark_performance_utility.py
These functions should be added to the existing utility module
"""

from typing import Dict, List, Any, Optional
import statistics
from datetime import datetime, timezone
from .ovnk_benchmark_performance_utility import (
    PerformanceThreshold, PerformanceLevel, AlertLevel, 
    ResourceType, PerformanceAlert
)


class ClusterResourceAnalyzer:
    """Additional utility class for cluster-level resource analysis"""
    
    @staticmethod
    def analyze_cluster_resource_distribution(node_groups: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze resource distribution across cluster node groups"""
        distribution_analysis = {
            'total_capacity': {'cpu_cores': 0, 'memory_gb': 0},
            'usage_patterns': {},
            'resource_imbalance': {},
            'efficiency_metrics': {}
        }
        
        total_cpu_capacity = 0
        total_memory_capacity = 0
        role_resources = {}
        
        for role, group_data in node_groups.items():
            if not group_data.get('nodes'):
                continue
                
            role_cpu = 0
            role_memory = 0
            role_usage = {'cpu': [], 'memory': []}
            
            for node in group_data['nodes']:
                # Extract capacity (would need to be added to node data structure)
                # For now, estimate based on usage patterns
                if 'metrics' in node:
                    cpu_usage = node['metrics'].get('cpu_usage', {}).get('avg', 0)
                    memory_usage = node['metrics'].get('memory_usage', {}).get('avg', 0)
                    
                    role_usage['cpu'].append(cpu_usage)
                    role_usage['memory'].append(memory_usage)
            
            if role_usage['cpu']:
                role_resources[role] = {
                    'avg_cpu_usage': round(statistics.mean(role_usage['cpu']), 2),
                    'avg_memory_usage_gb': round(statistics.mean(role_usage['memory']) / 1024, 2),
                    'cpu_efficiency': ClusterResourceAnalyzer._calculate_efficiency(role_usage['cpu']),
                    'memory_efficiency': ClusterResourceAnalyzer._calculate_efficiency(role_usage['memory'])
                }
        
        distribution_analysis['usage_patterns'] = role_resources
        return distribution_analysis
    
    @staticmethod
    def _calculate_efficiency(usage_values: List[float]) -> str:
        """Calculate resource efficiency based on usage distribution"""
        if not usage_values:
            return "unknown"
        
        avg_usage = statistics.mean(usage_values)
        std_dev = statistics.stdev(usage_values) if len(usage_values) > 1 else 0
        
        # Efficiency based on utilization and consistency
        if avg_usage < 30:
            return "underutilized"
        elif avg_usage > 80:
            return "overutilized"
        elif std_dev > 20:
            return "inconsistent"
        else:
            return "optimal"


class APILatencyAnalyzer:
    """Specialized analyzer for API server latency patterns"""
    
    @staticmethod
    def analyze_latency_trends(latency_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze API latency trends and patterns"""
        analysis = {
            'latency_comparison': {},
            'performance_trends': {},
            'bottleneck_indicators': []
        }
        
        # Compare read-only vs mutating latencies
        if 'readonly' in latency_data and 'mutating' in latency_data:
            ro_p99 = latency_data['readonly'].get('p99_seconds', 0)
            mut_p99 = latency_data['mutating'].get('p99_seconds', 0)
            
            analysis['latency_comparison'] = {
                'readonly_vs_mutating_ratio': round(ro_p99 / mut_p99 if mut_p99 > 0 else 0, 2),
                'difference_seconds': round(abs(mut_p99 - ro_p99), 4),
                'pattern': APILatencyAnalyzer._classify_latency_pattern(ro_p99, mut_p99)
            }
            
            # Identify potential bottlenecks
            if mut_p99 > ro_p99 * 3:
                analysis['bottleneck_indicators'].append("Mutating operations significantly slower - possible admission controller issues")
            elif ro_p99 > 0.5:
                analysis['bottleneck_indicators'].append("High read latency - possible etcd performance issues")
        
        return analysis
    
    @staticmethod
    def _classify_latency_pattern(ro_latency: float, mut_latency: float) -> str:
        """Classify the latency pattern between read-only and mutating operations"""
        if ro_latency < 0.1 and mut_latency < 0.2:
            return "optimal"
        elif ro_latency < 0.5 and mut_latency < 1.0:
            return "acceptable"
        elif mut_latency > ro_latency * 5:
            return "mutating_bottleneck"
        elif ro_latency > 1.0:
            return "read_bottleneck"
        else:
            return "degraded"


class NetworkPolicyImpactAnalyzer:
    """Analyzer for network policy impact on cluster performance"""
    
    @staticmethod
    def analyze_network_policy_impact(cluster_info: Dict[str, Any], 
                                    network_usage: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze potential impact of network policies on performance"""
        analysis = {
            'policy_density': {},
            'potential_impact': {},
            'recommendations': []
        }
        
        # Calculate policy density
        resource_counts = cluster_info.get('resource_counts', {})
        total_nodes = cluster_info.get('total_nodes', 1)
        total_pods = resource_counts.get('pods', 0)
        
        networkpolicies = resource_counts.get('networkpolicies', 0)
        adminnetworkpolicies = resource_counts.get('adminnetworkpolicies', 0)
        egressfirewalls = resource_counts.get('egressfirewalls', 0)
        
        analysis['policy_density'] = {
            'networkpolicies_per_pod': round(networkpolicies / max(total_pods, 1), 4),
            'adminnetworkpolicies_per_cluster': adminnetworkpolicies,
            'egressfirewalls_per_cluster': egressfirewalls,
            'total_network_policies': networkpolicies + adminnetworkpolicies + egressfirewalls
        }
        
        # Assess potential impact
        total_policies = networkpolicies + adminnetworkpolicies + egressfirewalls
        
        if total_policies > 100:
            analysis['potential_impact']['level'] = 'high'
            analysis['recommendations'].append("High number of network policies may impact performance")
        elif total_policies > 50:
            analysis['potential_impact']['level'] = 'medium'
            analysis['recommendations'].append("Moderate network policy usage - monitor for performance impact")
        else:
            analysis['potential_impact']['level'] = 'low'
        
        # Check for excessive egress firewalls
        if egressfirewalls > 10:
            analysis['recommendations'].append("High number of egress firewalls detected - may impact outbound network performance")
        
        return analysis


class HealthScoreCalculator:
    """Enhanced health score calculator with component weighting"""
    
    @staticmethod
    def calculate_weighted_cluster_health(node_groups_health: Dict[str, float],
                                        api_health: float,
                                        cluster_operators_available: int,
                                        cluster_operators_total: int) -> Dict[str, Any]:
        """Calculate weighted cluster health considering component importance"""
        
        # Component weights (master nodes and API are more critical)
        weights = {
            'master': 0.4,   # Master nodes are most critical
            'api': 0.3,      # API server is very critical
            'worker': 0.2,   # Worker nodes are important for workloads
            'infra': 0.1,    # Infra nodes are least critical
            'operators': 0.1 # Cluster operators baseline health
        }
        
        # Calculate weighted score
        total_weight = 0
        weighted_score = 0
        
        # Node groups
        for role, health_score in node_groups_health.items():
            if role in weights:
                weight = weights[role]
                weighted_score += health_score * weight
                total_weight += weight
        
        # API health
        weighted_score += api_health * weights['api']
        total_weight += weights['api']
        
        # Cluster operators health
        if cluster_operators_total > 0:
            operator_health = (cluster_operators_available / cluster_operators_total) * 100
            weighted_score += operator_health * weights['operators']
            total_weight += weights['operators']
        
        # Normalize score
        final_score = weighted_score / total_weight if total_weight > 0 else 0
        
        # Determine health level
        if final_score >= 90:
            health_level = "excellent"
        elif final_score >= 75:
            health_level = "good"
        elif final_score >= 60:
            health_level = "moderate"
        elif final_score >= 40:
            health_level = "poor"
        else:
            health_level = "critical"
        
        return {
            'weighted_score': round(final_score, 2),
            'health_level': health_level,
            'component_weights': weights,
            'component_scores': {
                'nodes': node_groups_health,
                'api': api_health,
                'operators': (cluster_operators_available / cluster_operators_total) * 100 if cluster_operators_total > 0 else 0
            }
        }


class AlertSeverityProcessor:
    """Enhanced alert processing and categorization"""
    
    @staticmethod
    def categorize_alerts_by_impact(alerts: List[PerformanceAlert]) -> Dict[str, Any]:
        """Categorize alerts by their potential cluster impact"""
        categorization = {
            'cluster_wide_impact': [],
            'node_specific': [],
            'service_degradation': [],
            'capacity_warnings': []
        }
        
        for alert in alerts:
            # Cluster-wide impact alerts
            if alert.pod_name == "kube-apiserver" or "master" in alert.node_name.lower():
                categorization['cluster_wide_impact'].append(alert)
            
            # Service degradation
            elif alert.severity == AlertLevel.CRITICAL:
                categorization['service_degradation'].append(alert)
            
            # Capacity warnings
            elif alert.resource_type in [ResourceType.CPU, ResourceType.MEMORY]:
                if alert.current_value > alert.threshold_value * 0.8:  # 80% of threshold
                    categorization['capacity_warnings'].append(alert)
                else:
                    categorization['node_specific'].append(alert)
            
            # Default to node-specific
            else:
                categorization['node_specific'].append(alert)
        
        return categorization
    
    @staticmethod
    def generate_prioritized_action_items(alert_categorization: Dict[str, List[PerformanceAlert]]) -> List[Dict[str, Any]]:
        """Generate prioritized action items from categorized alerts"""
        action_items = []
        
        # Priority 1: Cluster-wide impact
        if alert_categorization['cluster_wide_impact']:
            action_items.append({
                'priority': 1,
                'category': 'cluster_stability',
                'title': 'Address cluster-wide performance issues',
                'alerts_count': len(alert_categorization['cluster_wide_impact']),
                'urgency': 'immediate',
                'estimated_impact': 'high'
            })
        
        # Priority 2: Service degradation
        if alert_categorization['service_degradation']:
            action_items.append({
                'priority': 2,
                'category': 'service_recovery',
                'title': 'Resolve critical service performance issues',
                'alerts_count': len(alert_categorization['service_degradation']),
                'urgency': 'high',
                'estimated_impact': 'medium'
            })
        
        # Priority 3: Capacity warnings
        if alert_categorization['capacity_warnings']:
            action_items.append({
                'priority': 3,
                'category': 'capacity_planning',
                'title': 'Address capacity constraints',
                'alerts_count': len(alert_categorization['capacity_warnings']),
                'urgency': 'medium',
                'estimated_impact': 'medium'
            })
        
        # Priority 4: Node-specific issues
        if alert_categorization['node_specific']:
            action_items.append({
                'priority': 4,
                'category': 'node_optimization',
                'title': 'Optimize individual node performance',
                'alerts_count': len(alert_categorization['node_specific']),
                'urgency': 'low',
                'estimated_impact': 'low'
            })
        
        return action_items


def get_api_latency_threshold() -> PerformanceThreshold:
    """Get API latency performance threshold"""
    return PerformanceThreshold(
        excellent_max=0.1,    # 100ms
        good_max=0.5,         # 500ms
        moderate_max=1.0,     # 1s
        poor_max=2.0,         # 2s
        unit="seconds",
        component_type="api_latency"
    )


def get_network_throughput_threshold() -> PerformanceThreshold:
    """Get network throughput performance threshold (in Mbps)"""
    return PerformanceThreshold(
        excellent_max=100,     # 100 Mbps
        good_max=500,          # 500 Mbps
        moderate_max=1000,     # 1 Gbps
        poor_max=5000,         # 5 Gbps
        unit="Mbps",
        component_type="network_throughput"
    )


def calculate_node_resource_efficiency(cpu_usage: float, memory_usage_mb: float, 
                                     estimated_cpu_capacity: float = 100.0,
                                     estimated_memory_capacity_mb: float = 16384) -> Dict[str, Any]:
    """Calculate resource efficiency metrics for a node"""
    cpu_efficiency = (cpu_usage / estimated_cpu_capacity) * 100 if estimated_cpu_capacity > 0 else 0
    memory_efficiency = (memory_usage_mb / estimated_memory_capacity_mb) * 100 if estimated_memory_capacity_mb > 0 else 0
    
    # Overall efficiency score (balanced between CPU and memory)
    overall_efficiency = (cpu_efficiency + memory_efficiency) / 2
    
    return {
        'cpu_efficiency_percent': round(cpu_efficiency, 2),
        'memory_efficiency_percent': round(memory_efficiency, 2),
        'overall_efficiency_percent': round(overall_efficiency, 2),
        'efficiency_level': _classify_efficiency_level(overall_efficiency),
        'recommendations': _generate_efficiency_recommendations(cpu_efficiency, memory_efficiency)
    }


def _classify_efficiency_level(efficiency_percent: float) -> str:
    """Classify resource efficiency level"""
    if efficiency_percent >= 90:
        return "critical_utilization"
    elif efficiency_percent >= 75:
        return "high_utilization"
    elif efficiency_percent >= 50:
        return "moderate_utilization"
    elif efficiency_percent >= 25:
        return "low_utilization"
    else:
        return "very_low_utilization"


def _generate_efficiency_recommendations(cpu_efficiency: float, memory_efficiency: float) -> List[str]:
    """Generate recommendations based on resource efficiency"""
    recommendations = []
    
    if cpu_efficiency > 90:
        recommendations.append("CPU utilization is critically high - consider scaling or optimization")
    elif cpu_efficiency < 25:
        recommendations.append("CPU is underutilized - consider workload consolidation")
    
    if memory_efficiency > 90:
        recommendations.append("Memory utilization is critically high - risk of OOM conditions")
    elif memory_efficiency < 25:
        recommendations.append("Memory is underutilized - consider reducing memory allocation")
    
    # Balanced utilization recommendations
    efficiency_diff = abs(cpu_efficiency - memory_efficiency)
    if efficiency_diff > 30:
        if cpu_efficiency > memory_efficiency:
            recommendations.append("CPU-bound workload detected - consider CPU optimization")
        else:
            recommendations.append("Memory-bound workload detected - consider memory optimization")
    
    return recommendations


def generate_cluster_capacity_recommendations(node_groups_analysis: Dict[str, Any],
                                            total_nodes: int) -> List[str]:
    """Generate cluster capacity planning recommendations"""
    recommendations = []
    
    high_usage_nodes = 0
    critical_usage_nodes = 0
    
    for role, group_data in node_groups_analysis.items():
        if 'performance_summary' not in group_data:
            continue
            
        problematic_nodes = group_data['performance_summary'].get('problematic_nodes', [])
        high_usage_nodes += len(problematic_nodes)
        
        # Count critical nodes
        critical_nodes = sum(1 for node in problematic_nodes 
                           if node.get('cpu_usage', 0) > 90 or node.get('memory_usage_mb', 0) > 8192)
        critical_usage_nodes += critical_nodes
    
    # Generate recommendations based on usage patterns
    high_usage_percentage = (high_usage_nodes / total_nodes) * 100 if total_nodes > 0 else 0
    critical_usage_percentage = (critical_usage_nodes / total_nodes) * 100 if total_nodes > 0 else 0
    
    if critical_usage_percentage > 20:
        recommendations.append("URGENT: More than 20% of nodes are in critical resource usage state")
        recommendations.append("Immediate cluster scaling or workload redistribution required")
    elif high_usage_percentage > 50:
        recommendations.append("More than 50% of nodes show high resource usage")
        recommendations.append("Plan for cluster capacity expansion in the near term")
    elif high_usage_percentage > 30:
        recommendations.append("Monitor cluster capacity - approaching resource constraints")
    
    # Role-specific capacity recommendations
    master_issues = len(node_groups_analysis.get('master', {}).get('performance_summary', {}).get('problematic_nodes', []))
    if master_issues > 0:
        recommendations.append("Master node performance issues detected - critical for cluster stability")
    
    return recommendations


def format_performance_summary_for_json(analysis_result: Any) -> Dict[str, Any]:
    """Format analysis result for clean JSON output"""
    if hasattr(analysis_result, '__dict__'):
        # Convert dataclass to dict if needed
        result_dict = asdict(analysis_result) if hasattr(analysis_result, '__dataclass_fields__') else analysis_result.__dict__
    else:
        result_dict = analysis_result
    
    # Clean up the dictionary for JSON serialization
    def clean_dict(obj):
        if isinstance(obj, dict):
            return {k: clean_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_dict(item) for item in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, 'value'):  # Handle Enum objects
            return obj.value
        else:
            return obj
    
    return clean_dict(result_dict)


def create_performance_alert(severity: str, resource_type: str, component_name: str,
                           message: str, current_value: float, threshold_value: float,
                           unit: str, node_name: str = "") -> PerformanceAlert:
    """Convenience function to create performance alerts"""
    severity_map = {
        'low': AlertLevel.LOW,
        'medium': AlertLevel.MEDIUM,
        'high': AlertLevel.HIGH,
        'critical': AlertLevel.CRITICAL
    }
    
    resource_map = {
        'cpu': ResourceType.CPU,
        'memory': ResourceType.MEMORY,
        'network': ResourceType.NETWORK,
        'disk': ResourceType.DISK,
        'sync_duration': ResourceType.SYNC_DURATION
    }
    
    return PerformanceAlert(
        severity=severity_map.get(severity.lower(), AlertLevel.MEDIUM),
        resource_type=resource_map.get(resource_type.lower(), ResourceType.CPU),
        pod_name=component_name,
        node_name=node_name,
        message=message,
        current_value=current_value,
        threshold_value=threshold_value,
        unit=unit,
        timestamp=datetime.now(timezone.utc).isoformat()
    )

# Main example
if __name__ == "__main__":
    # Example of using the utility module
    print("OVNK Performance Analysis Utility Module Example")
    print("=" * 50)
    
    # Test memory conversion
    print("Memory Conversion Test:")
    converter = MemoryConverter()
    print(f"2048 MB to bytes: {converter.to_bytes(2048, 'MB'):,.0f}")
    print(f"1.5 GB to MB: {converter.to_mb(1.5, 'GB'):.1f}")
    
    # Test statistics calculation
    print("\nStatistics Calculation Test:")
    values = [10.5, 25.2, 30.1, 45.8, 60.3, 75.9, 90.1]
    stats = StatisticsCalculator.calculate_basic_stats(values)
    print(f"Values: {values}")
    print(f"Mean: {stats['mean']:.2f}, Std Dev: {stats['std_dev']:.2f}")
    
    # Test performance classification
    print("\nPerformance Classification Test:")
    cpu_threshold = ThresholdClassifier.get_default_cpu_threshold()
    level, severity = ThresholdClassifier.classify_performance(85.0, cpu_threshold)
    print(f"CPU usage 85%: {level.value}, Severity: {severity:.1f}")
    
    print("\nUtility module loaded successfully!")