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


class UnifiedAnalysisInterface:
    """Unified interface for all performance analyzers"""
    
    def __init__(self):
        self.analyzers = {}
    
    def register_analyzer(self, name: str, analyzer: BasePerformanceAnalyzer):
        """Register a performance analyzer"""
        self.analyzers[name] = analyzer
    
    def analyze(self, analyzer_name: str, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze using specified analyzer"""
        if analyzer_name not in self.analyzers:
            raise ValueError(f"Analyzer '{analyzer_name}' not found")
        
        return self.analyzers[analyzer_name].analyze_metrics_data(metrics_data)
    
    def analyze_all(self, metrics_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Analyze using all registered analyzers"""
        results = {}
        for name, analyzer in self.analyzers.items():
            try:
                results[name] = analyzer.analyze_metrics_data(metrics_data)
            except Exception as e:
                results[name] = {"error": str(e)}
        return results
    
    def get_available_analyzers(self) -> List[str]:
        """Get list of available analyzers"""
        return list(self.analyzers.keys())


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