"""
OVS Performance Analysis Module
Analyzes OVS metrics for performance insights and recommendations
File: /analysis/ovnk_benchmark_performance_analysis_ovs.py
"""

import json
import statistics
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Union, Tuple
from enum import Enum
from dataclasses import dataclass


class AlertLevel(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class PerformanceStatus(Enum):
    """Performance status categories"""
    EXCELLENT = "excellent"
    GOOD = "good"
    DEGRADED = "degraded"
    CRITICAL = "critical"


@dataclass
class PerformanceAlert:
    """Performance alert data structure"""
    level: AlertLevel
    category: str
    message: str
    metric_name: str
    current_value: float
    threshold: float
    recommendation: str
    affected_components: List[str]


@dataclass
class PerformanceInsight:
    """Performance insight data structure"""
    category: str
    title: str
    description: str
    impact: str
    confidence: float  # 0.0 to 1.0
    recommendations: List[str]


class OVSPerformanceAnalyzer:
    """Analyze OVS performance metrics and provide insights"""
    
    def __init__(self):
        # Performance thresholds
        self.cpu_thresholds = {
            'warning': 70.0,  # 70% CPU usage
            'critical': 85.0  # 85% CPU usage
        }
        
        self.memory_thresholds = {
            'warning_mb': 500.0,    # 500MB memory usage
            'critical_mb': 1000.0,  # 1GB memory usage
            'warning_gb': 0.5,      # 0.5GB memory usage
            'critical_gb': 1.0      # 1GB memory usage
        }
        
        self.flow_thresholds = {
            'high_flows_warning': 10000,   # 10K flows
            'high_flows_critical': 50000,  # 50K flows
            'dp_flows_warning': 5000,      # 5K datapath flows
            'dp_flows_critical': 20000     # 20K datapath flows
        }
        
        self.connection_thresholds = {
            'overflow_warning': 100,     # 100 overflow events
            'overflow_critical': 1000,   # 1000 overflow events
            'discarded_warning': 50,     # 50 discarded connections
            'discarded_critical': 500    # 500 discarded connections
        }
    
    def analyze_cpu_usage(self, cpu_data: Dict[str, Any]) -> Tuple[List[PerformanceAlert], List[PerformanceInsight]]:
        """Analyze CPU usage data"""
        alerts = []
        insights = []
        
        if 'error' in cpu_data:
            return alerts, insights
        
        # Analyze OVS vswitchd CPU usage
        if 'ovs_vswitchd_cpu' in cpu_data:
            vswitchd_data = cpu_data['ovs_vswitchd_cpu']
            
            for node_data in vswitchd_data:
                node_name = node_data.get('node_name', 'unknown')
                max_cpu = node_data.get('max', 0)
                avg_cpu = node_data.get('avg', 0)
                
                # Check for CPU alerts
                if max_cpu >= self.cpu_thresholds['critical']:
                    alerts.append(PerformanceAlert(
                        level=AlertLevel.CRITICAL,
                        category="cpu_usage",
                        message=f"Critical CPU usage on node {node_name}: {max_cpu}%",
                        metric_name="ovs_vswitchd_cpu_max",
                        current_value=max_cpu,
                        threshold=self.cpu_thresholds['critical'],
                        recommendation="Consider scaling OVS vswitchd or optimizing flow rules",
                        affected_components=[f"ovs-vswitchd@{node_name}"]
                    ))
                elif max_cpu >= self.cpu_thresholds['warning']:
                    alerts.append(PerformanceAlert(
                        level=AlertLevel.WARNING,
                        category="cpu_usage",
                        message=f"High CPU usage on node {node_name}: {max_cpu}%",
                        metric_name="ovs_vswitchd_cpu_max",
                        current_value=max_cpu,
                        threshold=self.cpu_thresholds['warning'],
                        recommendation="Monitor closely and consider optimization if trend continues",
                        affected_components=[f"ovs-vswitchd@{node_name}"]
                    ))
                
                # Generate insights for high variance
                min_cpu = node_data.get('min', 0)
                if max_cpu - min_cpu > 30:  # High variance
                    insights.append(PerformanceInsight(
                        category="cpu_variance",
                        title=f"High CPU variance on {node_name}",
                        description=f"CPU usage varies significantly (min: {min_cpu}%, max: {max_cpu}%)",
                        impact="May indicate bursty traffic patterns or inefficient flow processing",
                        confidence=0.8,
                        recommendations=[
                            "Review traffic patterns for this node",
                            "Consider flow table optimization",
                            "Monitor for traffic spikes"
                        ]
                    ))
        
        # Analyze OVSDB server CPU usage
        if 'ovsdb_server_cpu' in cpu_data:
            ovsdb_data = cpu_data['ovsdb_server_cpu']
            
            for node_data in ovsdb_data:
                node_name = node_data.get('node_name', 'unknown')
                max_cpu = node_data.get('max', 0)
                
                if max_cpu >= self.cpu_thresholds['critical']:
                    alerts.append(PerformanceAlert(
                        level=AlertLevel.CRITICAL,
                        category="cpu_usage",
                        message=f"Critical OVSDB server CPU usage on node {node_name}: {max_cpu}%",
                        metric_name="ovsdb_server_cpu_max",
                        current_value=max_cpu,
                        threshold=self.cpu_thresholds['critical'],
                        recommendation="OVSDB server overloaded - check database size and client connections",
                        affected_components=[f"ovsdb-server@{node_name}"]
                    ))
                elif max_cpu >= self.cpu_thresholds['warning']:
                    alerts.append(PerformanceAlert(
                        level=AlertLevel.WARNING,
                        category="cpu_usage",
                        message=f"High OVSDB server CPU usage on node {node_name}: {max_cpu}%",
                        metric_name="ovsdb_server_cpu_max",
                        current_value=max_cpu,
                        threshold=self.cpu_thresholds['warning'],
                        recommendation="Monitor OVSDB database size and client connection patterns",
                        affected_components=[f"ovsdb-server@{node_name}"]
                    ))
        
        return alerts, insights
    
    def analyze_memory_usage(self, memory_data: Dict[str, Any]) -> Tuple[List[PerformanceAlert], List[PerformanceInsight]]:
        """Analyze memory usage data"""
        alerts = []
        insights = []
        
        if 'error' in memory_data:
            return alerts, insights
        
        # Analyze OVS DB memory usage
        if 'ovs_db_memory' in memory_data:
            db_memory_data = memory_data['ovs_db_memory']
            
            for pod_data in db_memory_data:
                pod_name = pod_data.get('pod_name', 'unknown')
                max_memory = pod_data.get('max', 0)
                unit = pod_data.get('unit', 'bytes')
                
                # Convert to MB for comparison
                max_memory_mb = self._convert_to_mb(max_memory, unit)
                
                if max_memory_mb >= self.memory_thresholds['critical_mb']:
                    alerts.append(PerformanceAlert(
                        level=AlertLevel.CRITICAL,
                        category="memory_usage",
                        message=f"Critical OVSDB memory usage in pod {pod_name}: {max_memory} {unit}",
                        metric_name="ovs_db_memory_max",
                        current_value=max_memory,
                        threshold=self.memory_thresholds['critical_mb'],
                        recommendation="OVSDB using excessive memory - check for memory leaks or large database",
                        affected_components=[pod_name]
                    ))
                elif max_memory_mb >= self.memory_thresholds['warning_mb']:
                    alerts.append(PerformanceAlert(
                        level=AlertLevel.WARNING,
                        category="memory_usage",
                        message=f"High OVSDB memory usage in pod {pod_name}: {max_memory} {unit}",
                        metric_name="ovs_db_memory_max",
                        current_value=max_memory,
                        threshold=self.memory_thresholds['warning_mb'],
                        recommendation="Monitor OVSDB memory growth and consider cleanup if needed",
                        affected_components=[pod_name]
                    ))
        
        # Analyze OVS vswitchd memory usage
        if 'ovs_vswitchd_memory' in memory_data:
            vswitchd_memory_data = memory_data['ovs_vswitchd_memory']
            
            for pod_data in vswitchd_memory_data:
                pod_name = pod_data.get('pod_name', 'unknown')
                max_memory = pod_data.get('max', 0)
                unit = pod_data.get('unit', 'bytes')
                
                max_memory_mb = self._convert_to_mb(max_memory, unit)
                
                if max_memory_mb >= self.memory_thresholds['critical_mb']:
                    alerts.append(PerformanceAlert(
                        level=AlertLevel.CRITICAL,
                        category="memory_usage",
                        message=f"Critical OVS vswitchd memory usage in pod {pod_name}: {max_memory} {unit}",
                        metric_name="ovs_vswitchd_memory_max",
                        current_value=max_memory,
                        threshold=self.memory_thresholds['critical_mb'],
                        recommendation="OVS vswitchd using excessive memory - check flow table size and optimization",
                        affected_components=[pod_name]
                    ))
                elif max_memory_mb >= self.memory_thresholds['warning_mb']:
                    alerts.append(PerformanceAlert(
                        level=AlertLevel.WARNING,
                        category="memory_usage",
                        message=f"High OVS vswitchd memory usage in pod {pod_name}: {max_memory} {unit}",
                        metric_name="ovs_vswitchd_memory_max",
                        current_value=max_memory,
                        threshold=self.memory_thresholds['warning_mb'],
                        recommendation="Monitor memory growth and review flow table efficiency",
                        affected_components=[pod_name]
                    ))
                
                # Memory growth insight
                min_memory = pod_data.get('min', 0)
                if max_memory > min_memory * 2:  # Memory doubled
                    insights.append(PerformanceInsight(
                        category="memory_growth",
                        title=f"Significant memory growth in {pod_name}",
                        description=f"Memory usage increased from {min_memory} to {max_memory} {unit}",
                        impact="May indicate memory leak or increasing flow table size",
                        confidence=0.7,
                        recommendations=[
                            "Monitor memory usage trends over time",
                            "Check flow table size and cleanup policies",
                            "Review for potential memory leaks"
                        ]
                    ))
        
        return alerts, insights
    
    def analyze_flow_metrics(self, dp_flows_data: Dict[str, Any], bridge_flows_data: Dict[str, Any]) -> Tuple[List[PerformanceAlert], List[PerformanceInsight]]:
        """Analyze flow table metrics"""
        alerts = []
        insights = []
        
        # Analyze datapath flows
        if 'error' not in dp_flows_data and 'data' in dp_flows_data:
            for flow_data in dp_flows_data['data']:
                instance = flow_data.get('instance', 'unknown')
                max_flows = flow_data.get('max', 0)
                avg_flows = flow_data.get('avg', 0)
                
                if max_flows >= self.flow_thresholds['dp_flows_critical']:
                    alerts.append(PerformanceAlert(
                        level=AlertLevel.CRITICAL,
                        category="flow_table",
                        message=f"Critical datapath flow count on {instance}: {max_flows}",
                        metric_name="dp_flows_total",
                        current_value=max_flows,
                        threshold=self.flow_thresholds['dp_flows_critical'],
                        recommendation="Datapath flow table near capacity - review flow rules and cleanup",
                        affected_components=[instance]
                    ))
                elif max_flows >= self.flow_thresholds['dp_flows_warning']:
                    alerts.append(PerformanceAlert(
                        level=AlertLevel.WARNING,
                        category="flow_table",
                        message=f"High datapath flow count on {instance}: {max_flows}",
                        metric_name="dp_flows_total",
                        current_value=max_flows,
                        threshold=self.flow_thresholds['dp_flows_warning'],
                        recommendation="Monitor datapath flow growth and consider optimization",
                        affected_components=[instance]
                    ))
        
        # Analyze bridge flows
        if 'error' not in bridge_flows_data:
            # Check br-int flows
            if 'br_int_flows' in bridge_flows_data:
                for flow_data in bridge_flows_data['br_int_flows']:
                    instance = flow_data.get('instance', 'unknown')
                    max_flows = flow_data.get('max', 0)
                    
                    if max_flows >= self.flow_thresholds['high_flows_critical']:
                        alerts.append(PerformanceAlert(
                            level=AlertLevel.CRITICAL,
                            category="bridge_flows",
                            message=f"Critical br-int flow count on {instance}: {max_flows}",
                            metric_name="bridge_flows_br_int",
                            current_value=max_flows,
                            threshold=self.flow_thresholds['high_flows_critical'],
                            recommendation="br-int bridge has too many flows - review network policies and segmentation",
                            affected_components=[f"br-int@{instance}"]
                        ))
                    elif max_flows >= self.flow_thresholds['high_flows_warning']:
                        alerts.append(PerformanceAlert(
                            level=AlertLevel.WARNING,
                            category="bridge_flows",
                            message=f"High br-int flow count on {instance}: {max_flows}",
                            metric_name="bridge_flows_br_int",
                            current_value=max_flows,
                            threshold=self.flow_thresholds['high_flows_warning'],
                            recommendation="Monitor br-int flow growth and review network policies",
                            affected_components=[f"br-int@{instance}"]
                        ))
            
            # Check br-ex flows
            if 'br_ex_flows' in bridge_flows_data:
                for flow_data in bridge_flows_data['br_ex_flows']:
                    instance = flow_data.get('instance', 'unknown')
                    max_flows = flow_data.get('max', 0)
                    
                    if max_flows >= self.flow_thresholds['high_flows_warning']:
                        # br-ex typically has fewer flows, so use lower threshold
                        level = AlertLevel.CRITICAL if max_flows >= 1000 else AlertLevel.WARNING
                        alerts.append(PerformanceAlert(
                            level=level,
                            category="bridge_flows",
                            message=f"High br-ex flow count on {instance}: {max_flows}",
                            metric_name="bridge_flows_br_ex",
                            current_value=max_flows,
                            threshold=1000,
                            recommendation="br-ex bridge has many flows - review external network configuration",
                            affected_components=[f"br-ex@{instance}"]
                        ))
        
        # Generate flow efficiency insights
        if ('error' not in dp_flows_data and 'data' in dp_flows_data and 
            'error' not in bridge_flows_data):
            
            total_dp_flows = sum(flow['max'] for flow in dp_flows_data['data'])
            total_bridge_flows = (
                sum(flow['max'] for flow in bridge_flows_data.get('br_int_flows', [])) +
                sum(flow['max'] for flow in bridge_flows_data.get('br_ex_flows', []))
            )
            
            if total_bridge_flows > 0:
                dp_to_bridge_ratio = total_dp_flows / total_bridge_flows
                
                if dp_to_bridge_ratio > 2.0:
                    insights.append(PerformanceInsight(
                        category="flow_efficiency",
                        title="High datapath to bridge flow ratio",
                        description=f"Datapath flows ({total_dp_flows}) significantly exceed bridge flows ({total_bridge_flows})",
                        impact="May indicate inefficient flow processing or megaflow issues",
                        confidence=0.6,
                        recommendations=[
                            "Review megaflow cache efficiency",
                            "Check for flow rule conflicts",
                            "Consider flow table optimization"
                        ]
                    ))
        
        return alerts, insights
    
    def analyze_connection_metrics(self, connection_data: Dict[str, Any]) -> Tuple[List[PerformanceAlert], List[PerformanceInsight]]:
        """Analyze OVS connection metrics"""
        alerts = []
        insights = []
        
        if 'error' in connection_data or 'connection_metrics' not in connection_data:
            return alerts, insights
        
        metrics = connection_data['connection_metrics']
        
        # Check for connection overflows
        if 'rconn_overflow' in metrics and 'error' not in metrics['rconn_overflow']:
            overflow_max = metrics['rconn_overflow'].get('max', 0)
            
            if overflow_max >= self.connection_thresholds['overflow_critical']:
                alerts.append(PerformanceAlert(
                    level=AlertLevel.CRITICAL,
                    category="connection_health",
                    message=f"Critical connection overflow count: {overflow_max}",
                    metric_name="rconn_overflow",
                    current_value=overflow_max,
                    threshold=self.connection_thresholds['overflow_critical'],
                    recommendation="OVS connection buffer overflowing - check controller connectivity and load",
                    affected_components=["ovs-vswitchd"]
                ))
            elif overflow_max >= self.connection_thresholds['overflow_warning']:
                alerts.append(PerformanceAlert(
                    level=AlertLevel.WARNING,
                    category="connection_health",
                    message=f"High connection overflow count: {overflow_max}",
                    metric_name="rconn_overflow",
                    current_value=overflow_max,
                    threshold=self.connection_thresholds['overflow_warning'],
                    recommendation="Monitor controller connectivity and reduce message load if possible",
                    affected_components=["ovs-vswitchd"]
                ))
        
        # Check for discarded connections
        if 'rconn_discarded' in metrics and 'error' not in metrics['rconn_discarded']:
            discarded_max = metrics['rconn_discarded'].get('max', 0)
            
            if discarded_max >= self.connection_thresholds['discarded_critical']:
                alerts.append(PerformanceAlert(
                    level=AlertLevel.CRITICAL,
                    category="connection_health",
                    message=f"Critical discarded connection count: {discarded_max}",
                    metric_name="rconn_discarded",
                    current_value=discarded_max,
                    threshold=self.connection_thresholds['discarded_critical'],
                    recommendation="Many connections being discarded - check network connectivity and controller health",
                    affected_components=["ovs-vswitchd"]
                ))
            elif discarded_max >= self.connection_thresholds['discarded_warning']:
                alerts.append(PerformanceAlert(
                    level=AlertLevel.WARNING,
                    category="connection_health",
                    message=f"High discarded connection count: {discarded_max}",
                    metric_name="rconn_discarded",
                    current_value=discarded_max,
                    threshold=self.connection_thresholds['discarded_warning'],
                    recommendation="Monitor connection stability and controller responsiveness",
                    affected_components=["ovs-vswitchd"]
                ))
        
        # Generate connection health insights
        if ('stream_open' in metrics and 'rconn_overflow' in metrics and 
            'error' not in metrics['stream_open'] and 'error' not in metrics['rconn_overflow']):
            
            stream_open = metrics['stream_open'].get('max', 0)
            overflow = metrics['rconn_overflow'].get('max', 0)
            
            if stream_open > 0 and overflow / stream_open > 0.1:  # >10% overflow rate
                insights.append(PerformanceInsight(
                    category="connection_efficiency",
                    title="High connection overflow rate",
                    description=f"Connection overflow rate: {(overflow/stream_open)*100:.1f}%",
                    impact="High overflow rate may indicate controller communication issues",
                    confidence=0.8,
                    recommendations=[
                        "Check controller health and connectivity",
                        "Review message processing capacity",
                        "Consider connection buffer tuning"
                    ]
                ))
        
        return alerts, insights
    
    def determine_overall_status(self, alerts: List[PerformanceAlert]) -> PerformanceStatus:
        """Determine overall OVS performance status based on alerts"""
        if not alerts:
            return PerformanceStatus.EXCELLENT
        
        critical_count = sum(1 for alert in alerts if alert.level == AlertLevel.CRITICAL)
        warning_count = sum(1 for alert in alerts if alert.level == AlertLevel.WARNING)
        
        if critical_count > 0:
            return PerformanceStatus.CRITICAL
        elif warning_count >= 3:
            return PerformanceStatus.DEGRADED
        elif warning_count > 0:
            return PerformanceStatus.GOOD
        else:
            return PerformanceStatus.EXCELLENT
    
    def generate_performance_summary(self, alerts: List[PerformanceAlert], insights: List[PerformanceInsight]) -> Dict[str, Any]:
        """Generate a comprehensive performance summary"""
        overall_status = self.determine_overall_status(alerts)
        
        # Categorize alerts
        alert_categories = {}
        for alert in alerts:
            category = alert.category
            if category not in alert_categories:
                alert_categories[category] = {"critical": 0, "warning": 0, "info": 0}
            alert_categories[category][alert.level.value] += 1
        
        # Categorize insights
        insight_categories = {}
        for insight in insights:
            category = insight.category
            if category not in insight_categories:
                insight_categories[category] = 0
            insight_categories[category] += 1
        
        return {
            'overall_status': overall_status.value,
            'summary_metrics': {
                'total_alerts': len(alerts),
                'critical_alerts': sum(1 for a in alerts if a.level == AlertLevel.CRITICAL),
                'warning_alerts': sum(1 for a in alerts if a.level == AlertLevel.WARNING),
                'total_insights': len(insights),
                'high_confidence_insights': sum(1 for i in insights if i.confidence >= 0.8)
            },
            'alert_breakdown': alert_categories,
            'insight_breakdown': insight_categories,
            'top_issues': [
                {
                    'category': alert.category,
                    'level': alert.level.value,
                    'message': alert.message,
                    'recommendation': alert.recommendation
                }
                for alert in sorted(alerts, key=lambda x: (x.level == AlertLevel.CRITICAL, x.current_value), reverse=True)[:5]
            ],
            'key_insights': [
                {
                    'category': insight.category,
                    'title': insight.title,
                    'impact': insight.impact,
                    'confidence': insight.confidence
                }
                for insight in sorted(insights, key=lambda x: x.confidence, reverse=True)[:3]
            ]
        }
    
    def _convert_to_mb(self, value: float, unit: str) -> float:
        """Convert memory value to MB"""
        unit = unit.lower()
        if unit in ['gb', 'g']:
            return value * 1024
        elif unit in ['kb', 'k']:
            return value / 1024
        elif unit in ['bytes', 'b']:
            return value / (1024 * 1024)
        else:  # assume MB
            return value
    
    def analyze_comprehensive_ovs_metrics(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Perform comprehensive analysis of all OVS metrics"""
        all_alerts = []
        all_insights = []
        
        try:
            # Analyze CPU usage
            if 'cpu_usage' in metrics_data:
                cpu_alerts, cpu_insights = self.analyze_cpu_usage(metrics_data['cpu_usage'])
                all_alerts.extend(cpu_alerts)
                all_insights.extend(cpu_insights)
            
            # Analyze memory usage
            if 'memory_usage' in metrics_data:
                mem_alerts, mem_insights = self.analyze_memory_usage(metrics_data['memory_usage'])
                all_alerts.extend(mem_alerts)
                all_insights.extend(mem_insights)
            
            # Analyze flow metrics
            if 'dp_flows' in metrics_data and 'bridge_flows' in metrics_data:
                flow_alerts, flow_insights = self.analyze_flow_metrics(
                    metrics_data['dp_flows'], 
                    metrics_data['bridge_flows']
                )
                all_alerts.extend(flow_alerts)
                all_insights.extend(flow_insights)
            
            # Analyze connection metrics
            if 'connection_metrics' in metrics_data:
                conn_alerts, conn_insights = self.analyze_connection_metrics(metrics_data['connection_metrics'])
                all_alerts.extend(conn_alerts)
                all_insights.extend(conn_insights)
            
            # Generate comprehensive summary
            performance_summary = self.generate_performance_summary(all_alerts, all_insights)
            
            return {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'analysis_version': '1.0.0',
                'performance_summary': performance_summary,
                'detailed_alerts': [
                    {
                        'level': alert.level.value,
                        'category': alert.category,
                        'message': alert.message,
                        'metric_name': alert.metric_name,
                        'current_value': alert.current_value,
                        'threshold': alert.threshold,
                        'recommendation': alert.recommendation,
                        'affected_components': alert.affected_components
                    }
                    for alert in all_alerts
                ],
                'detailed_insights': [
                    {
                        'category': insight.category,
                        'title': insight.title,
                        'description': insight.description,
                        'impact': insight.impact,
                        'confidence': insight.confidence,
                        'recommendations': insight.recommendations
                    }
                    for insight in all_insights
                ]
            }
            
        except Exception as e:
            return {
                'error': f'Analysis failed: {str(e)}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }


# Example usage and testing
def main():
    """Test the OVS performance analyzer"""
    
    # Sample data for testing
    sample_metrics = {
        'cpu_usage': {
            'ovs_vswitchd_cpu': [
                {'node_name': 'worker-1', 'min': 45.2, 'avg': 67.8, 'max': 89.5, 'unit': '%'},
                {'node_name': 'worker-2', 'min': 12.1, 'avg': 25.3, 'max': 38.7, 'unit': '%'}
            ],
            'ovsdb_server_cpu': [
                {'node_name': 'worker-1', 'min': 5.2, 'avg': 15.8, 'max': 25.1, 'unit': '%'},
                {'node_name': 'worker-2', 'min': 3.1, 'avg': 8.3, 'max': 12.7, 'unit': '%'}
            ]
        },
        'memory_usage': {
            'ovs_db_memory': [
                {'pod_name': 'ovs-db-worker-1', 'min': 256.5, 'avg': 445.2, 'max': 678.9, 'unit': 'MB'},
                {'pod_name': 'ovs-db-worker-2', 'min': 189.3, 'avg': 234.7, 'max': 289.1, 'unit': 'MB'}
            ],
            'ovs_vswitchd_memory': [
                {'pod_name': 'ovs-vswitchd-worker-1', 'min': 512.1, 'avg': 756.3, 'max': 1024.7, 'unit': 'MB'},
                {'pod_name': 'ovs-vswitchd-worker-2', 'min': 298.4, 'avg': 387.6, 'max': 456.8, 'unit': 'MB'}
            ]
        },
        'dp_flows': {
            'data': [
                {'instance': '10.1.1.1:9090', 'min': 1250, 'avg': 2340, 'max': 3450, 'unit': 'flows'},
                {'instance': '10.1.1.2:9090', 'min': 890, 'avg': 1670, 'max': 2280, 'unit': 'flows'}
            ]
        },
        'bridge_flows': {
            'br_int_flows': [
                {'instance': '10.1.1.1:9090', 'min': 450, 'avg': 890, 'max': 1250, 'unit': 'flows'},
                {'instance': '10.1.1.2:9090', 'min': 320, 'avg': 567, 'max': 780, 'unit': 'flows'}
            ],
            'br_ex_flows': [
                {'instance': '10.1.1.1:9090', 'min': 45, 'avg': 89, 'max': 125, 'unit': 'flows'},
                {'instance': '10.1.1.2:9090', 'min': 32, 'avg': 56, 'max': 78, 'unit': 'flows'}
            ]
        },
        'connection_metrics': {
            'stream_open': {'min': 12, 'avg': 18, 'max': 24, 'unit': 'count'},
            'rconn_overflow': {'min': 0, 'avg': 2, 'max': 5, 'unit': 'count'},
            'rconn_discarded': {'min': 0, 'avg': 1, 'max': 3, 'unit': 'count'}
        }
    }
    
    analyzer = OVSPerformanceAnalyzer()
    analysis_result = analyzer.analyze_comprehensive_ovs_metrics(sample_metrics)
    
    print("=== OVS Performance Analysis Results ===")
    print(json.dumps(analysis_result, indent=2))


if __name__ == "__main__":
    main()