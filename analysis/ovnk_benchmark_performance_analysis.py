"""
Performance Analysis Module
Advanced analysis functions for identifying performance issues and bottlenecks
"""

import asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import statistics


class PerformanceIssueType(Enum):
    """Types of performance issues"""
    HIGH_LATENCY = "high_latency"
    HIGH_CPU = "high_cpu"
    HIGH_MEMORY = "high_memory"
    HIGH_ERROR_RATE = "high_error_rate"
    RESOURCE_BOTTLENECK = "resource_bottleneck"
    SYNC_DEGRADATION = "sync_degradation"
    QUEUE_BUILDUP = "queue_buildup"
    DATABASE_GROWTH = "database_growth"


class Severity(Enum):
    """Issue severity levels"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class PerformanceIssue:
    """Represents a performance issue"""
    issue_type: PerformanceIssueType
    severity: Severity
    component: str
    description: str
    affected_metrics: List[str]
    root_cause: Optional[str]
    recommendations: List[str]
    confidence_score: float  # 0.0 to 1.0


@dataclass
class PerformanceTrend:
    """Represents a performance trend"""
    metric_name: str
    component: str
    trend_direction: str  # "improving", "degrading", "stable"
    trend_strength: float  # 0.0 to 1.0
    change_rate: float
    confidence: float


class OVNKPerformanceAnalyzer:
    """Advanced performance analyzer for OVN-Kubernetes metrics"""
    
    def __init__(self):
        self.thresholds = self._get_performance_thresholds()
    
    def _get_performance_thresholds(self) -> Dict[str, Dict[str, float]]:
        """Get performance thresholds for different metrics"""
        return {
            'api_server': {
                'latency_warning': 0.5,      # 500ms
                'latency_critical': 1.0,     # 1s
                'error_rate_warning': 0.01,  # 1%
                'error_rate_critical': 0.05  # 5%
            },
            'multus': {
                'cpu_warning': 0.5,          # 0.5 cores
                'cpu_critical': 1.0,         # 1 core
                'memory_warning': 500,       # 500MB
                'memory_critical': 1024      # 1GB
            },
            'ovnk_pods': {
                'cpu_warning': 1.0,          # 1 core
                'cpu_critical': 2.0,         # 2 cores
                'memory_warning': 1024,      # 1GB
                'memory_critical': 2048      # 2GB
            },
            'ovnk_containers': {
                'nbdb_size_warning': 100,    # 100MB
                'nbdb_size_critical': 500,   # 500MB
                'sbdb_size_warning': 500,    # 500MB
                'sbdb_size_critical': 1024   # 1GB
            },
            'ovnk_sync': {
                'sync_duration_warning': 5.0,    # 5s
                'sync_duration_critical': 15.0,  # 15s
                'error_rate_warning': 0.1,       # 0.1/s
                'error_rate_critical': 1.0       # 1.0/s
            }
        }
    
    async def analyze_performance_data(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform comprehensive performance analysis
        
        Args:
            metrics_data: Complete metrics data from all categories
            
        Returns:
            Analysis results with issues, trends, and recommendations
        """
        analysis = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'issues': [],
            'trends': [],
            'bottlenecks': [],
            'recommendations': [],
            'health_assessment': {},
            'risk_analysis': {}
        }
        
        try:
            # Analyze each category
            for category, data in metrics_data.items():
                if category == 'general_info':
                    continue
                
                category_analysis = await self._analyze_category(category, data)
                analysis['issues'].extend(category_analysis.get('issues', []))
                analysis['trends'].extend(category_analysis.get('trends', []))
                analysis['bottlenecks'].extend(category_analysis.get('bottlenecks', []))
            
            # Cross-category analysis
            cross_analysis = await self._analyze_cross_category_patterns(metrics_data)
            analysis['issues'].extend(cross_analysis.get('issues', []))
            analysis['recommendations'].extend(cross_analysis.get('recommendations', []))
            
            # Generate health assessment
            analysis['health_assessment'] = self._generate_health_assessment(analysis['issues'])
            
            # Risk analysis
            analysis['risk_analysis'] = self._analyze_risks(analysis['issues'], analysis['trends'])
            
            # Generate recommendations
            analysis['recommendations'].extend(self._generate_recommendations(analysis['issues'], analysis['trends']))
            
        except Exception as e:
            analysis['error'] = str(e)
        
        return analysis
    
    async def _analyze_category(self, category: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze performance data for a specific category"""
        analysis = {
            'issues': [],
            'trends': [],
            'bottlenecks': []
        }
        
        if 'metrics' not in data:
            return analysis
        
        metrics = data['metrics']
        
        # Category-specific analysis
        if category == 'api_server':
            analysis.update(await self._analyze_api_server(metrics))
        elif category == 'multus':
            analysis.update(await self._analyze_multus(metrics))
        elif category == 'ovnk_pods':
            analysis.update(await self._analyze_ovnk_pods(metrics))
        elif category == 'ovnk_containers':
            analysis.update(await self._analyze_ovnk_containers(metrics))
        elif category == 'ovnk_sync':
            analysis.update(await self._analyze_ovnk_sync(metrics))
        
        return analysis
    
    async def _analyze_api_server(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze API server performance"""
        issues = []
        trends = []
        bottlenecks = []
        
        # Analyze read-only API latency
        if 'avg_ro_apicalls_latency' in metrics:
            latency_data = metrics['avg_ro_apicalls_latency']
            if 'statistics' in latency_data:
                p99_latency = latency_data['statistics'].get('p99', 0)
                
                if p99_latency > self.thresholds['api_server']['latency_critical']:
                    issues.append(PerformanceIssue(
                        issue_type=PerformanceIssueType.HIGH_LATENCY,
                        severity=Severity.CRITICAL,
                        component='api_server',
                        description=f'Critical API read latency: {p99_latency:.3f}s (p99)',
                        affected_metrics=['avg_ro_apicalls_latency'],
                        root_cause='High cluster load or etcd performance issues',
                        recommendations=[
                            'Check etcd performance and health',
                            'Review API server resource allocation',
                            'Investigate high-traffic API clients'
                        ],
                        confidence_score=0.9
                    ))
                elif p99_latency > self.thresholds['api_server']['latency_warning']:
                    issues.append(PerformanceIssue(
                        issue_type=PerformanceIssueType.HIGH_LATENCY,
                        severity=Severity.MEDIUM,
                        component='api_server',
                        description=f'Elevated API read latency: {p99_latency:.3f}s (p99)',
                        affected_metrics=['avg_ro_apicalls_latency'],
                        root_cause='Increased cluster activity or resource constraints',
                        recommendations=[
                            'Monitor API server CPU and memory usage',
                            'Check for inefficient API queries',
                            'Consider API server scaling'
                        ],
                        confidence_score=0.7
                    ))
        
        # Analyze mutating API latency
        if 'avg_mutating_apicalls_latency' in metrics:
            latency_data = metrics['avg_mutating_apicalls_latency']
            if 'statistics' in latency_data:
                p99_latency = latency_data['statistics'].get('p99', 0)
                
                if p99_latency > self.thresholds['api_server']['latency_critical'] * 2:  # Higher threshold for mutations
                    issues.append(PerformanceIssue(
                        issue_type=PerformanceIssueType.HIGH_LATENCY,
                        severity=Severity.HIGH,
                        component='api_server',
                        description=f'High API mutation latency: {p99_latency:.3f}s (p99)',
                        affected_metrics=['avg_mutating_apicalls_latency'],
                        root_cause='Validation overhead, admission controllers, or storage issues',
                        recommendations=[
                            'Review admission controller performance',
                            'Check etcd write performance',
                            'Analyze validation and webhook latency'
                        ],
                        confidence_score=0.8
                    ))
        
        # Analyze API request patterns for bottlenecks
        if 'api_request_rate' in metrics and 'api_request_errors' in metrics:
            rate_data = metrics['api_request_rate']
            error_data = metrics['api_request_errors']
            
            # Look for high error rates combined with high request rates
            if 'by_resource' in rate_data and 'by_resource' in error_data:
                for resource in rate_data['by_resource']:
                    if resource in error_data['by_resource']:
                        request_rate = rate_data['by_resource'][resource].get('latest', 0)
                        error_rate = error_data['by_resource'][resource].get('latest', 0)
                        
                        if request_rate > 0:
                            error_percentage = (error_rate / request_rate) * 100
                            
                            if error_percentage > 5:  # > 5% error rate
                                bottlenecks.append({
                                    'type': 'api_resource_errors',
                                    'resource': resource,
                                    'error_percentage': round(error_percentage, 2),
                                    'request_rate': round(request_rate, 2),
                                    'severity': 'high' if error_percentage > 10 else 'medium'
                                })
        
        return {'issues': issues, 'trends': trends, 'bottlenecks': bottlenecks}
    
    async def _analyze_multus(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze Multus CNI performance"""
        issues = []
        trends = []
        bottlenecks = []
        
        # Analyze CPU usage
        if 'max_cpu_multus' in metrics:
            cpu_data = metrics['max_cpu_multus']
            if 'statistics' in cpu_data:
                max_cpu = cpu_data['statistics'].get('max', 0)
                
                if max_cpu > self.thresholds['multus']['cpu_critical']:
                    issues.append(PerformanceIssue(
                        issue_type=PerformanceIssueType.HIGH_CPU,
                        severity=Severity.HIGH,
                        component='multus',
                        description=f'High Multus CPU usage: {max_cpu:.3f} cores',
                        affected_metrics=['max_cpu_multus'],
                        root_cause='High network interface creation/deletion activity',
                        recommendations=[
                            'Check pod creation/deletion patterns',
                            'Review NetworkAttachmentDefinition configurations',
                            'Consider Multus resource limits'
                        ],
                        confidence_score=0.8
                    ))
        
        # Analyze memory usage trends
        if 'max_memory_multus' in metrics:
            memory_data = metrics['max_memory_multus']
            if 'statistics' in memory_data:
                max_memory = memory_data['statistics'].get('max', 0)
                max_memory_mb = max_memory / (1024 * 1024)
                
                if max_memory_mb > self.thresholds['multus']['memory_critical']:
                    issues.append(PerformanceIssue(
                        issue_type=PerformanceIssueType.HIGH_MEMORY,
                        severity=Severity.HIGH,
                        component='multus',
                        description=f'High Multus memory usage: {max_memory_mb:.1f} MB',
                        affected_metrics=['max_memory_multus'],
                        root_cause='Memory leak or excessive network configuration caching',
                        recommendations=[
                            'Check for memory leaks in Multus logs',
                            'Review cache settings and cleanup',
                            'Monitor long-running Multus processes'
                        ],
                        confidence_score=0.7
                    ))
        
        return {'issues': issues, 'trends': trends, 'bottlenecks': bottlenecks}
    
    async def _analyze_ovnk_pods(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze OVN-Kubernetes pods performance"""
        issues = []
        trends = []
        bottlenecks = []
        
        # Analyze control plane performance
        if 'max_cpu_ovn_control_plane' in metrics:
            cpu_data = metrics['max_cpu_ovn_control_plane']
            if 'by_container' in cpu_data:
                for container, stats in cpu_data['by_container'].items():
                    max_cpu = stats.get('max', 0)
                    
                    if max_cpu > self.thresholds['ovnk_pods']['cpu_critical']:
                        issues.append(PerformanceIssue(
                            issue_type=PerformanceIssueType.HIGH_CPU,
                            severity=Severity.CRITICAL,
                            component='ovnk_control_plane',
                            description=f'Critical CPU usage in {container}: {max_cpu:.3f} cores',
                            affected_metrics=['max_cpu_ovn_control_plane'],
                            root_cause='High control plane load or inefficient processing',
                            recommendations=[
                                f'Investigate {container} workload and configuration',
                                'Check for excessive network policy processing',
                                'Review control plane resource allocation'
                            ],
                            confidence_score=0.9
                        ))
        
        # Analyze node performance
        if 'max_memory_ovnkube_node' in metrics:
            memory_data = metrics['max_memory_ovnkube_node']
            if 'by_container' in memory_data:
                for container, stats in memory_data['by_container'].items():
                    max_memory = stats.get('max', 0)
                    max_memory_mb = max_memory / (1024 * 1024)
                    
                    if max_memory_mb > self.thresholds['ovnk_pods']['memory_critical']:
                        issues.append(PerformanceIssue(
                            issue_type=PerformanceIssueType.HIGH_MEMORY,
                            severity=Severity.HIGH,
                            component='ovnk_node',
                            description=f'High memory usage in {container}: {max_memory_mb:.1f} MB',
                            affected_metrics=['max_memory_ovnkube_node'],
                            root_cause='Memory leak, excessive caching, or high node load',
                            recommendations=[
                                f'Monitor {container} memory patterns over time',
                                'Check for memory leaks in logs',
                                'Review node resource allocation'
                            ],
                            confidence_score=0.8
                        ))
        
        return {'issues': issues, 'trends': trends, 'bottlenecks': bottlenecks}
    
    async def _analyze_ovnk_containers(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze OVN-Kubernetes containers performance"""
        issues = []
        trends = []
        bottlenecks = []
        
        # Analyze database sizes
        if 'nbdb_size_bytes' in metrics:
            nbdb_data = metrics['nbdb_size_bytes']
            if 'statistics' in nbdb_data:
                nbdb_size = nbdb_data['statistics'].get('avg', 0)
                nbdb_size_mb = nbdb_size / (1024 * 1024)
                
                if nbdb_size_mb > self.thresholds['ovnk_containers']['nbdb_size_critical']:
                    issues.append(PerformanceIssue(
                        issue_type=PerformanceIssueType.DATABASE_GROWTH,
                        severity=Severity.CRITICAL,
                        component='ovn_northbound_db',
                        description=f'Critical Northbound DB size: {nbdb_size_mb:.1f} MB',
                        affected_metrics=['nbdb_size_bytes'],
                        root_cause='Excessive logical network objects or cleanup issues',
                        recommendations=[
                            'Review NetworkPolicy and logical network object counts',
                            'Check for stale entries in NB database',
                            'Consider database cleanup procedures'
                        ],
                        confidence_score=0.9
                    ))
                elif nbdb_size_mb > self.thresholds['ovnk_containers']['nbdb_size_warning']:
                    issues.append(PerformanceIssue(
                        issue_type=PerformanceIssueType.DATABASE_GROWTH,
                        severity=Severity.MEDIUM,
                        component='ovn_northbound_db',
                        description=f'Growing Northbound DB size: {nbdb_size_mb:.1f} MB',
                        affected_metrics=['nbdb_size_bytes'],
                        root_cause='Increasing network complexity or object accumulation',
                        recommendations=[
                            'Monitor database growth trend',
                            'Audit network policy and object lifecycle',
                            'Plan for database optimization'
                        ],
                        confidence_score=0.7
                    ))
        
        if 'sbdb_size_bytes' in metrics:
            sbdb_data = metrics['sbdb_size_bytes']
            if 'statistics' in sbdb_data:
                sbdb_size = sbdb_data['statistics'].get('avg', 0)
                sbdb_size_mb = sbdb_size / (1024 * 1024)
                
                if sbdb_size_mb > self.thresholds['ovnk_containers']['sbdb_size_critical']:
                    issues.append(PerformanceIssue(
                        issue_type=PerformanceIssueType.DATABASE_GROWTH,
                        severity=Severity.HIGH,
                        component='ovn_southbound_db',
                        description=f'Large Southbound DB size: {sbdb_size_mb:.1f} MB',
                        affected_metrics=['sbdb_size_bytes'],
                        root_cause='High flow table entries or chassis scaling',
                        recommendations=[
                            'Review flow table optimization',
                            'Check chassis and port binding counts',
                            'Monitor southbound database compaction'
                        ],
                        confidence_score=0.8
                    ))
        
        # Analyze OVS performance
        if 'ovs_vswitchd_cpu_usage' in metrics:
            ovs_data = metrics['ovs_vswitchd_cpu_usage']
            if 'statistics' in ovs_data:
                cpu_usage = ovs_data['statistics'].get('avg', 0)
                
                if cpu_usage > 50:  # > 50% CPU
                    issues.append(PerformanceIssue(
                        issue_type=PerformanceIssueType.HIGH_CPU,
                        severity=Severity.HIGH,
                        component='ovs_vswitchd',
                        description=f'High OVS vSwitchd CPU usage: {cpu_usage:.1f}%',
                        affected_metrics=['ovs_vswitchd_cpu_usage'],
                        root_cause='High packet processing load or inefficient flow rules',
                        recommendations=[
                            'Analyze flow table efficiency',
                            'Check for packet processing bottlenecks',
                            'Review network traffic patterns'
                        ],
                        confidence_score=0.8
                    ))
        
        return {'issues': issues, 'trends': trends, 'bottlenecks': bottlenecks}
    
    async def _analyze_ovnk_sync(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze OVN-Kubernetes sync performance"""
        issues = []
        trends = []
        bottlenecks = []
        
        # Analyze controller sync duration
        if 'ovnkube_controller_sync_duration' in metrics:
            sync_data = metrics['ovnkube_controller_sync_duration']
            if 'statistics' in sync_data:
                max_duration = sync_data['statistics'].get('max', 0)
                
                if max_duration > self.thresholds['ovnk_sync']['sync_duration_critical']:
                    issues.append(PerformanceIssue(
                        issue_type=PerformanceIssueType.SYNC_DEGRADATION,
                        severity=Severity.CRITICAL,
                        component='ovnk_controller',
                        description=f'Critical controller sync duration: {max_duration:.1f}s',
                        affected_metrics=['ovnkube_controller_sync_duration'],
                        root_cause='Database locks, high object counts, or processing bottlenecks',
                        recommendations=[
                            'Check for database contention',
                            'Review controller resource allocation',
                            'Analyze sync workload patterns'
                        ],
                        confidence_score=0.9
                    ))
        
        # Analyze sync error rates
        if 'sync_errors_total' in metrics:
            error_data = metrics['sync_errors_total']
            if 'statistics' in error_data:
                error_rate = error_data['statistics'].get('avg', 0)
                
                if error_rate > self.thresholds['ovnk_sync']['error_rate_critical']:
                    issues.append(PerformanceIssue(
                        issue_type=PerformanceIssueType.HIGH_ERROR_RATE,
                        severity=Severity.CRITICAL,
                        component='ovnk_sync',
                        description=f'High sync error rate: {error_rate:.1f}/s',
                        affected_metrics=['sync_errors_total'],
                        root_cause='Configuration conflicts, database issues, or network problems',
                        recommendations=[
                            'Review sync error logs for patterns',
                            'Check network connectivity to databases',
                            'Validate configuration consistency'
                        ],
                        confidence_score=0.9
                    ))
        
        # Analyze queue buildup
        if 'controller_queue_depth' in metrics:
            queue_data = metrics['controller_queue_depth']
            if 'by_controller' in queue_data:
                for controller, stats in queue_data['by_controller'].items():
                    max_depth = stats.get('max', 0)
                    
                    if max_depth > 1000:  # > 1000 items
                        issues.append(PerformanceIssue(
                            issue_type=PerformanceIssueType.QUEUE_BUILDUP,
                            severity=Severity.HIGH,
                            component=f'ovnk_{controller}',
                            description=f'Large queue depth in {controller}: {int(max_depth)} items',
                            affected_metrics=['controller_queue_depth'],
                            root_cause='Processing bottleneck or resource constraints',
                            recommendations=[
                                f'Increase {controller} processing capacity',
                                'Check for blocking operations',
                                'Review queue processing efficiency'
                            ],
                            confidence_score=0.8
                        ))
        
        return {'issues': issues, 'trends': trends, 'bottlenecks': bottlenecks}
    
    async def _analyze_cross_category_patterns(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze patterns across different metric categories"""
        issues = []
        recommendations = []
        
        # Check for correlated high resource usage
        high_cpu_components = []
        high_memory_components = []
        
        for category in ['multus', 'ovnk_pods', 'ovnk_containers']:
            if category in metrics_data and 'summary' in metrics_data[category]:
                summary = metrics_data[category]['summary']
                
                # Check for high CPU alerts
                alerts = summary.get('alerts', [])
                for alert in alerts:
                    if 'cpu' in alert.lower() and ('high' in alert.lower() or 'critical' in alert.lower()):
                        high_cpu_components.append(category)
                        break
                
                # Check for high memory alerts
                for alert in alerts:
                    if 'memory' in alert.lower() and ('high' in alert.lower() or 'critical' in alert.lower()):
                        high_memory_components.append(category)
                        break
        
        # If multiple components show high resource usage, suggest cluster-level issue
        if len(high_cpu_components) >= 2:
            issues.append(PerformanceIssue(
                issue_type=PerformanceIssueType.RESOURCE_BOTTLENECK,
                severity=Severity.HIGH,
                component='cluster',
                description=f'Multiple components showing high CPU usage: {", ".join(high_cpu_components)}',
                affected_metrics=['cpu_usage'],
                root_cause='Cluster-wide resource contention or increased workload',
                recommendations=[
                    'Review overall cluster resource allocation',
                    'Check for workload increases',
                    'Consider node scaling or resource rebalancing'
                ],
                confidence_score=0.8
            ))
        
        if len(high_memory_components) >= 2:
            issues.append(PerformanceIssue(
                issue_type=PerformanceIssueType.RESOURCE_BOTTLENECK,
                severity=Severity.HIGH,
                component='cluster',
                description=f'Multiple components showing high memory usage: {", ".join(high_memory_components)}',
                affected_metrics=['memory_usage'],
                root_cause='Memory pressure or resource allocation issues',
                recommendations=[
                    'Investigate memory allocation patterns',
                    'Check for memory leaks across components',
                    'Review resource limits and requests'
                ],
                confidence_score=0.8
            ))
        
        # Check for API server issues affecting sync performance
        if 'api_server' in metrics_data and 'ovnk_sync' in metrics_data:
            api_health = metrics_data['api_server'].get('summary', {}).get('health_score', 100)
            sync_health = metrics_data['ovnk_sync'].get('summary', {}).get('health_score', 100)
            
            if api_health < 80 and sync_health < 80:
                recommendations.append({
                    'priority': 'high',
                    'category': 'cross_component',
                    'description': 'API server performance issues may be impacting OVN-K sync performance',
                    'action': 'Prioritize API server optimization to improve overall sync performance'
                })
        
        return {'issues': issues, 'recommendations': recommendations}
    
    def _generate_health_assessment(self, issues: List[PerformanceIssue]) -> Dict[str, Any]:
        """Generate overall health assessment based on identified issues"""
        assessment = {
            'overall_health': 'excellent',
            'risk_level': 'low',
            'critical_issues': 0,
            'high_issues': 0,
            'medium_issues': 0,
            'low_issues': 0,
            'affected_components': set(),
            'primary_concerns': []
        }
        
        # Count issues by severity
        for issue in issues:
            assessment['affected_components'].add(issue.component)
            
            if issue.severity == Severity.CRITICAL:
                assessment['critical_issues'] += 1
            elif issue.severity == Severity.HIGH:
                assessment['high_issues'] += 1
            elif issue.severity == Severity.MEDIUM:
                assessment['medium_issues'] += 1
            else:
                assessment['low_issues'] += 1
        
        assessment['affected_components'] = list(assessment['affected_components'])
        
        # Determine overall health
        if assessment['critical_issues'] > 0:
            assessment['overall_health'] = 'critical'
            assessment['risk_level'] = 'high'
        elif assessment['high_issues'] > 2:
            assessment['overall_health'] = 'poor'
            assessment['risk_level'] = 'high'
        elif assessment['high_issues'] > 0 or assessment['medium_issues'] > 3:
            assessment['overall_health'] = 'warning'
            assessment['risk_level'] = 'medium'
        elif assessment['medium_issues'] > 0:
            assessment['overall_health'] = 'good'
            assessment['risk_level'] = 'low'
        
        # Identify primary concerns
        issue_types = {}
        for issue in issues:
            if issue.severity in [Severity.CRITICAL, Severity.HIGH]:
                issue_type = issue.issue_type.value
                if issue_type not in issue_types:
                    issue_types[issue_type] = 0
                issue_types[issue_type] += 1
        
        assessment['primary_concerns'] = sorted(issue_types.items(), key=lambda x: x[1], reverse=True)
        
        return assessment
    
    def _analyze_risks(self, issues: List[PerformanceIssue], trends: List[PerformanceTrend]) -> Dict[str, Any]:
        """Analyze risks based on current issues and trends"""
        risk_analysis = {
            'immediate_risks': [],
            'emerging_risks': [],
            'long_term_risks': [],
            'risk_mitigation_priority': []
        }
        
        # Immediate risks from critical and high severity issues
        for issue in issues:
            if issue.severity == Severity.CRITICAL:
                risk_analysis['immediate_risks'].append({
                    'risk': f'{issue.component} failure or severe degradation',
                    'cause': issue.description,
                    'impact': 'Service disruption, network connectivity issues',
                    'mitigation': issue.recommendations[0] if issue.recommendations else 'Immediate investigation required'
                })
            elif issue.severity == Severity.HIGH:
                risk_analysis['emerging_risks'].append({
                    'risk': f'{issue.component} performance degradation',
                    'cause': issue.description,
                    'impact': 'Reduced performance, potential service impact',
                    'mitigation': issue.recommendations[0] if issue.recommendations else 'Performance optimization needed'
                })
        
        # Long-term risks from trends
        for trend in trends:
            if trend.trend_direction == 'degrading' and trend.confidence > 0.7:
                risk_analysis['long_term_risks'].append({
                    'risk': f'{trend.component} {trend.metric_name} degradation',
                    'trend_strength': trend.trend_strength,
                    'impact': 'Gradual performance decline leading to service issues',
                    'mitigation': 'Monitor trend and plan proactive optimization'
                })
        
        # Priority for risk mitigation
        priority_components = ['api_server', 'ovnk_controller', 'ovn_database']
        for component in priority_components:
            component_issues = [i for i in issues if component in i.component and i.severity in [Severity.CRITICAL, Severity.HIGH]]
            if component_issues:
                risk_analysis['risk_mitigation_priority'].append({
                    'component': component,
                    'priority': 'high',
                    'issues_count': len(component_issues),
                    'rationale': f'Critical component with {len(component_issues)} high-priority issues'
                })
        
        return risk_analysis
    
    def _generate_recommendations(self, issues: List[PerformanceIssue], trends: List[PerformanceTrend]) -> List[Dict[str, Any]]:
        """Generate actionable recommendations based on analysis"""
        recommendations = []
        
        # Group issues by component for targeted recommendations
        component_issues = {}
        for issue in issues:
            if issue.component not in component_issues:
                component_issues[issue.component] = []
            component_issues[issue.component].append(issue)
        
        # Generate component-specific recommendations
        for component, comp_issues in component_issues.items():
            high_priority_issues = [i for i in comp_issues if i.severity in [Severity.CRITICAL, Severity.HIGH]]
            
            if high_priority_issues:
                # Extract most common recommendations
                all_recommendations = []
                for issue in high_priority_issues:
                    all_recommendations.extend(issue.recommendations)
                
                # Count recommendation frequency
                rec_counts = {}
                for rec in all_recommendations:
                    rec_counts[rec] = rec_counts.get(rec, 0) + 1
                
                # Get top recommendations
                top_recs = sorted(rec_counts.items(), key=lambda x: x[1], reverse=True)[:3]
                
                recommendations.append({
                    'priority': 'high' if any(i.severity == Severity.CRITICAL for i in high_priority_issues) else 'medium',
                    'component': component,
                    'title': f'Address {component} performance issues',
                    'description': f'Multiple performance issues detected in {component}',
                    'actions': [rec[0] for rec in top_recs],
                    'expected_impact': 'Improved performance and reduced risk of service disruption'
                })
        
        # Add general recommendations based on patterns
        if len(issues) > 10:
            recommendations.append({
                'priority': 'medium',
                'component': 'cluster',
                'title': 'Implement comprehensive monitoring',
                'description': 'High number of performance issues detected across multiple components',
                'actions': [
                    'Set up automated alerting for key performance metrics',
                    'Implement regular performance baseline reviews',
                    'Create performance dashboards for proactive monitoring'
                ],
                'expected_impact': 'Earlier detection and prevention of performance issues'
            })
        
        # Add capacity planning recommendations for trending issues
        degrading_trends = [t for t in trends if t.trend_direction == 'degrading']
        if len(degrading_trends) > 3:
            recommendations.append({
                'priority': 'medium',
                'component': 'cluster',
                'title': 'Capacity planning and resource optimization',
                'description': 'Multiple degrading performance trends detected',
                'actions': [
                    'Conduct capacity planning analysis',
                    'Review resource allocation and limits',
                    'Plan for infrastructure scaling'
                ],
                'expected_impact': 'Proactive resource management and improved performance stability'
            })
        
        return recommendations