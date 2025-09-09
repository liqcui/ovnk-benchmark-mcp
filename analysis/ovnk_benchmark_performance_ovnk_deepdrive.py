#!/usr/bin/env python3
"""
OVN Deep Drive Performance Analyzer Module
Comprehensive analysis of OVN-Kubernetes performance metrics
File: analysis/ovnk_benchmark_performance_ovnk_deepdrive.py
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Union
import statistics

# Import existing modules
from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery
from tools.ovnk_benchmark_prometheus_basicinfo import (
    ovnBasicInfoCollector, 
    get_pod_phase_counts,
    get_comprehensive_metrics_summary
)
from tools.ovnk_benchmark_prometheus_pods_usage import PodsUsageCollector
from tools.ovnk_benchmark_prometheus_ovnk_latency import OVNLatencyCollector
from tools.ovnk_benchmark_prometheus_ovnk_ovs import OVSUsageCollector
from ocauth.ovnk_benchmark_auth import OpenShiftAuth
from tools.ovnk_benchmark_prometheus_nodes_usage import nodeUsageCollector

# Import utility module
from .ovnk_benchmark_performance_utility import (
    BasePerformanceAnalyzer, PerformanceLevel, AlertLevel, ResourceType,
    PerformanceThreshold, PerformanceAlert, AnalysisMetadata, ClusterHealth,
    MemoryConverter, StatisticsCalculator, ThresholdClassifier, 
    RecommendationEngine, HealthScoreCalculator,
    create_performance_alert, format_performance_summary_for_json,
    ReportGenerator
)

class ovnDeepDriveAnalyzer(BasePerformanceAnalyzer):
    """Comprehensive OVN-Kubernetes performance analyzer with enhanced analysis capabilities"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery, auth: Optional[OpenShiftAuth] = None):
        super().__init__("OVN_DeepDrive_Analyzer")
        self.prometheus_client = prometheus_client
        self.auth = auth
        
        # Initialize collectors
        self.basic_info_collector = ovnBasicInfoCollector(
            prometheus_client.prometheus_url, 
            prometheus_client.token
        )
        self.pods_usage_collector = PodsUsageCollector(prometheus_client, auth)
        self.latency_collector = OVNLatencyCollector(prometheus_client)
        self.ovs_collector = OVSUsageCollector(prometheus_client, auth)
        self.node_usage_collector = nodeUsageCollector(prometheus_client, auth)

    def analyze_metrics_data(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Implement abstract method for comprehensive metrics analysis"""
        return self.analyze_performance_insights(metrics_data)

    def _extract_top_5_from_usage_data(self, usage_data: Dict[str, Any], metric_type: str = "cpu") -> List[Dict[str, Any]]:
        """Extract top 5 entries from usage collector results"""
        if metric_type == "cpu":
            top_list = usage_data.get('top_5_cpu_usage', [])
        else:
            top_list = usage_data.get('top_5_memory_usage', [])
        
        return top_list[:5]
    
    def _calculate_comprehensive_performance_score(self, metrics_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced performance score calculation using utility module components"""
        
        # Initialize performance alerts list
        performance_alerts = []
        
        def _safe_float(val: Any, default: float = 0.0) -> float:
            try:
                return float(val) if val is not None else default
            except (ValueError, TypeError):
                return default

        score_components = {
            'latency_score': 0.0,
            'resource_utilization_score': 0.0,
            'stability_score': 0.0,
            'ovs_performance_score': 0.0,
            'node_health_score': 0.0
        }

        # Latency Analysis using utility thresholds
        latency_data = metrics_summary.get('latency_metrics', {})
        if latency_data and not latency_data.get('error'):
            latency_values = []
            categories = latency_data.get('categories', {})
            
            for category_name, category_metrics in categories.items():
                for metric_name, metric_data in category_metrics.items():
                    avg_val = _safe_float(metric_data.get('avg_value', 0.0))
                    max_val = _safe_float(metric_data.get('max_value', 0.0))
                    
                    if avg_val > 0:
                        latency_values.append(avg_val)
                    
                    # Create alerts for high latency
                    if max_val > 2.0:  # > 2 seconds
                        severity = 'critical' if max_val > 5.0 else 'high'
                        alert = create_performance_alert(
                            severity=severity,
                            resource_type='sync_duration',
                            component_name=metric_data.get('component', 'unknown'),
                            message=f"High {metric_name} latency detected: {max_val:.2f}s",
                            current_value=max_val,
                            threshold_value=2.0,
                            unit='seconds'
                        )
                        performance_alerts.append(alert)

            if latency_values:
                avg_latency = statistics.mean(latency_values)
                # Use threshold-based scoring
                latency_threshold = PerformanceThreshold(
                    excellent_max=0.1, good_max=0.5, moderate_max=1.0, poor_max=2.0,
                    unit='seconds', component_type='latency'
                )
                level, severity = ThresholdClassifier.classify_performance(avg_latency, latency_threshold)
                score_components['latency_score'] = max(0, 100 - severity)

        # Resource Utilization Analysis
        cpu_usage_data = metrics_summary.get('ovnkube_pods_cpu', {})
        if cpu_usage_data and not cpu_usage_data.get('error'):
            cpu_values = []
            memory_values = []
            
            # Analyze node pods
            node_pods = cpu_usage_data.get('ovnkube_node_pods', {})
            for usage_type in ['top_5_cpu', 'top_5_memory']:
                for pod_entry in node_pods.get(usage_type, []):
                    metrics = pod_entry.get('metrics', {})
                    for metric_name, metric_data in metrics.items():
                        if 'cpu' in metric_name.lower() and usage_type == 'top_5_cpu':
                            cpu_val = _safe_float(metric_data.get('avg', 0.0))
                            if cpu_val > 0:
                                cpu_values.append(cpu_val)
                                
                                # Create CPU alerts
                                if cpu_val > 80:
                                    severity = 'critical' if cpu_val > 95 else 'high'
                                    alert = create_performance_alert(
                                        severity=severity,
                                        resource_type='cpu',
                                        component_name=pod_entry.get('pod_name', 'unknown'),
                                        message=f"High CPU usage: {cpu_val:.1f}%",
                                        current_value=cpu_val,
                                        threshold_value=80.0,
                                        unit='%',
                                        node_name=pod_entry.get('node_name', '')
                                    )
                                    performance_alerts.append(alert)
                        
                        elif 'memory' in metric_name.lower() and usage_type == 'top_5_memory':
                            mem_val = _safe_float(metric_data.get('avg', 0.0))
                            if mem_val > 0:
                                # Convert to MB if needed
                                mem_mb = MemoryConverter.to_mb(mem_val, 'MB')
                                memory_values.append(mem_mb)
                                
                                # Create memory alerts
                                if mem_mb > 2048:  # > 2GB
                                    severity = 'critical' if mem_mb > 4096 else 'high'
                                    alert = create_performance_alert(
                                        severity=severity,
                                        resource_type='memory',
                                        component_name=pod_entry.get('pod_name', 'unknown'),
                                        message=f"High memory usage: {mem_mb:.0f} MB",
                                        current_value=mem_mb,
                                        threshold_value=2048.0,
                                        unit='MB',
                                        node_name=pod_entry.get('node_name', '')
                                    )
                                    performance_alerts.append(alert)

            # Calculate resource scores using thresholds
            if cpu_values:
                max_cpu = max(cpu_values)
                cpu_threshold = ThresholdClassifier.get_default_cpu_threshold()
                level, severity = ThresholdClassifier.classify_performance(max_cpu, cpu_threshold)
                score_components['resource_utilization_score'] = max(0, 100 - severity)

        # Stability Analysis (Alerts)
        basic_info = metrics_summary.get('basic_info', {})
        if basic_info and not basic_info.get('error'):
            alerts_data = basic_info.get('alerts_summary', {})
            alert_count = len(alerts_data.get('top_alerts', []) or [])
            
            if alert_count == 0:
                score_components['stability_score'] = 100
            elif alert_count < 3:
                score_components['stability_score'] = 85
            elif alert_count < 6:
                score_components['stability_score'] = 70
            elif alert_count < 10:
                score_components['stability_score'] = 50
            else:
                score_components['stability_score'] = 30

        # OVS Performance Analysis
        ovs_data = metrics_summary.get('ovs_metrics', {})
        if ovs_data and not ovs_data.get('error'):
            ovs_cpu_scores = []
            cpu_usage = ovs_data.get('cpu_usage', {})
            
            if cpu_usage and not cpu_usage.get('error'):
                for component in ['ovs_vswitchd_top5', 'ovsdb_server_top5']:
                    top_entries = cpu_usage.get(component, [])
                    for entry in top_entries:
                        max_val = _safe_float(entry.get('max', 0.0))
                        if max_val > 0:
                            ovs_cpu_scores.append(max_val)

            if ovs_cpu_scores:
                max_ovs_cpu = max(ovs_cpu_scores)
                if max_ovs_cpu < 30:
                    score_components['ovs_performance_score'] = 95
                elif max_ovs_cpu < 50:
                    score_components['ovs_performance_score'] = 80
                elif max_ovs_cpu < 70:
                    score_components['ovs_performance_score'] = 60
                else:
                    score_components['ovs_performance_score'] = 40

        # Node Health Analysis
        nodes_data = metrics_summary.get('nodes_usage', {})
        if nodes_data and not nodes_data.get('error'):
            node_health_scores = []
            
            # Analyze different node types
            for node_type in ['controlplane_nodes', 'infra_nodes', 'top5_worker_nodes']:
                node_group = nodes_data.get(node_type, {})
                if node_group:
                    summary = node_group.get('summary', {})
                    max_cpu = _safe_float(summary.get('cpu_usage', {}).get('max', 0.0))
                    max_mem_mb = _safe_float(summary.get('memory_usage', {}).get('max', 0.0))
                    
                    # Score based on resource utilization
                    if max_cpu < 50 and max_mem_mb < 4096:
                        node_health_scores.append(95)
                    elif max_cpu < 70 and max_mem_mb < 8192:
                        node_health_scores.append(80)
                    elif max_cpu < 85:
                        node_health_scores.append(60)
                    else:
                        node_health_scores.append(40)

            if node_health_scores:
                score_components['node_health_score'] = statistics.mean(node_health_scores)

        # Calculate weighted overall score
        weights = {
            'latency_score': 0.25,
            'resource_utilization_score': 0.25,
            'stability_score': 0.20,
            'ovs_performance_score': 0.15,
            'node_health_score': 0.15
        }
        
        overall_score = sum(score_components[key] * weights[key] for key in weights)
        
        # Calculate cluster health using utility function
        cluster_health = self.calculate_cluster_health_score(
            performance_alerts, 
            self._count_total_components(metrics_summary)
        )
        
        return {
            'overall_score': round(overall_score, 2),
            'component_scores': score_components,
            'performance_grade': self._get_performance_grade(overall_score),
            'cluster_health': cluster_health,
            'performance_alerts': [alert.__dict__ for alert in performance_alerts],
            'recommendations': RecommendationEngine.generate_cluster_recommendations(
                cluster_health.critical_issues_count,
                cluster_health.warning_issues_count,
                self._count_total_components(metrics_summary),
                "OVN-Kubernetes"
            )
        }

    def _get_performance_grade(self, score: float) -> str:
        """Convert numeric score to letter grade"""
        if score >= 90: return 'A+'
        elif score >= 85: return 'A'
        elif score >= 80: return 'B+'
        elif score >= 75: return 'B'
        elif score >= 70: return 'C+'
        elif score >= 65: return 'C'
        elif score >= 60: return 'D+'
        elif score >= 55: return 'D'
        else: return 'F'

    def _count_total_components(self, metrics_summary: Dict[str, Any]) -> int:
        """Count total components for health score calculation"""
        total = 0
        
        # Count pods
        ovnkube_data = metrics_summary.get('ovnkube_pods_cpu', {})
        if ovnkube_data and not ovnkube_data.get('error'):
            for pod_group in ['ovnkube_node_pods', 'ovnkube_control_plane_pods']:
                pods = ovnkube_data.get(pod_group, {})
                total += len(pods.get('top_5_cpu', [])) + len(pods.get('top_5_memory', []))
        
        # Count nodes
        nodes_data = metrics_summary.get('nodes_usage', {})
        if nodes_data and not nodes_data.get('error'):
            for node_type in ['controlplane_nodes', 'infra_nodes', 'top5_worker_nodes']:
                node_group = nodes_data.get(node_type, {})
                total += node_group.get('count', 0)
        
        return max(total, 1)  # Avoid division by zero

    async def collect_prometheus_basic_info(self) -> Dict[str, Any]:
        """Collect basic cluster information (requirement 2.1)"""
        try:
            basic_summary = await self.basic_info_collector.collect_comprehensive_summary()
            
            result = {
                'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                'pod_counts': {},
                'database_sizes': {},
                'alerts_summary': {},
                'pod_distribution': {}
            }
            
            # Extract pod counts
            if 'metrics' in basic_summary and 'pod_status' in basic_summary['metrics']:
                pod_status = basic_summary['metrics']['pod_status']
                if not pod_status.get('error'):
                    result['pod_counts'] = {
                        'total_pods': pod_status.get('total_pods', 0),
                        'phases': pod_status.get('phases', {})
                    }
            
            # Extract database sizes using utility converter
            if 'metrics' in basic_summary and 'ovn_database' in basic_summary['metrics']:
                db_info = basic_summary['metrics']['ovn_database']
                if not db_info.get('error'):
                    for db_name, db_data in db_info.items():
                        if isinstance(db_data, dict) and 'max_value' in db_data:
                            size_bytes = db_data['max_value'] or 0
                            formatted_size, unit = MemoryConverter.format_memory(size_bytes, 'auto')
                            
                            result['database_sizes'][db_name] = {
                                'size_bytes': size_bytes,
                                'size_formatted': round(formatted_size, 2),
                                'unit': unit
                            }
            
            # Extract alerts summary
            if 'metrics' in basic_summary and 'alerts' in basic_summary['metrics']:
                alerts_data = basic_summary['metrics']['alerts']
                if not alerts_data.get('error'):
                    result['alerts_summary'] = {
                        'total_alert_types': alerts_data.get('total_alert_types', 0),
                        'top_alerts': alerts_data.get('alerts', [])[:5],
                        'alertname_statistics': alerts_data.get('alertname_statistics', {})
                    }
            
            # Extract pod distribution
            if 'metrics' in basic_summary and 'pod_distribution' in basic_summary['metrics']:
                pod_dist = basic_summary['metrics']['pod_distribution']
                if not pod_dist.get('error'):
                    result['pod_distribution'] = {
                        'total_nodes': pod_dist.get('total_nodes', 0),
                        'top_nodes': pod_dist.get('top_nodes', [])[:5]
                    }
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to collect basic info: {str(e)}'}

    async def collect_ovnkube_pods_usage(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Collect top 5 OVNKube pods CPU/RAM usage (requirement 2.2)"""
        try:
            # Query for ovnkube-node-* pods
            node_usage = await self.pods_usage_collector.collect_duration_usage(
                duration=duration or "5m",
                pod_pattern="ovnkube-node-.*",
                namespace_pattern="openshift-ovn-kubernetes"
            ) if duration else await self.pods_usage_collector.collect_instant_usage(
                pod_pattern="ovnkube-node-.*",
                namespace_pattern="openshift-ovn-kubernetes"
            )
            
            # Query for ovnkube-control-plane-* pods  
            control_usage = await self.pods_usage_collector.collect_duration_usage(
                duration=duration or "5m",
                pod_pattern="ovnkube-control-plane-.*",
                namespace_pattern="openshift-ovn-kubernetes"
            ) if duration else await self.pods_usage_collector.collect_instant_usage(
                pod_pattern="ovnkube-control-plane-.*", 
                namespace_pattern="openshift-ovn-kubernetes"
            )
            
            result = {
                'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                'query_type': 'duration' if duration else 'instant',
                'ovnkube_node_pods': {
                    'top_5_cpu': self._extract_top_5_from_usage_data(node_usage, 'cpu'),
                    'top_5_memory': self._extract_top_5_from_usage_data(node_usage, 'memory')
                },
                'ovnkube_control_plane_pods': {
                    'top_5_cpu': self._extract_top_5_from_usage_data(control_usage, 'cpu'),
                    'top_5_memory': self._extract_top_5_from_usage_data(control_usage, 'memory')
                }
            }
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to collect OVNKube pods usage: {str(e)}'}

    async def collect_ovn_containers_usage(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Collect top 5 OVN containers CPU/RAM usage (requirement 2.3)"""
        try:
            container_patterns = {
                'sbdb': 'sbdb',
                'nbdb': 'nbdb', 
                'ovnkube_controller': '(ovnkube-controller|ovnkube-controller-.*)',
                'northd': '(northd|ovn-northd)',
                'ovn_controller': '(ovn-controller|ovn-controller-.*)'
            }
            
            result = {
                'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                'query_type': 'duration' if duration else 'instant',
                'containers': {}
            }
            
            for container_name, pattern in container_patterns.items():
                try:
                    if duration:
                        usage_data = await self.pods_usage_collector.collect_duration_usage(
                            duration=duration,
                            container_pattern=pattern,
                            namespace_pattern="openshift-ovn-kubernetes"
                        )
                    else:
                        usage_data = await self.pods_usage_collector.collect_instant_usage(
                            container_pattern=pattern,
                            namespace_pattern="openshift-ovn-kubernetes"
                        )
                    
                    top_cpu = self._extract_top_5_from_usage_data(usage_data, 'cpu')
                    top_mem = self._extract_top_5_from_usage_data(usage_data, 'memory')

                    if not top_cpu and not top_mem and duration:
                        instant_usage = await self.pods_usage_collector.collect_instant_usage(
                            container_pattern=pattern,
                            namespace_pattern="openshift-ovn-kubernetes"
                        )
                        top_cpu = self._extract_top_5_from_usage_data(instant_usage, 'cpu') or top_cpu
                        top_mem = self._extract_top_5_from_usage_data(instant_usage, 'memory') or top_mem

                    result['containers'][container_name] = {
                        'top_5_cpu': top_cpu,
                        'top_5_memory': top_mem
                    }
                    
                except Exception as e:
                    result['containers'][container_name] = {'error': str(e)}
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to collect containers usage: {str(e)}'}

    async def collect_ovs_metrics_summary(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Collect top 5 OVS metrics (requirement 2.4)"""
        try:
            ovs_data = await self.ovs_collector.collect_all_ovs_metrics(duration)
            
            result = {
                'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                'query_type': 'duration' if duration else 'instant',
                'cpu_usage': {},
                'memory_usage': {},
                'flows_metrics': {},
                'connection_metrics': {}
            }
            
            # Extract CPU usage top 5
            if 'cpu_usage' in ovs_data and not ovs_data['cpu_usage'].get('error'):
                cpu_data = ovs_data['cpu_usage']
                result['cpu_usage'] = {
                    'ovs_vswitchd_top5': cpu_data.get('summary', {}).get('ovs_vswitchd_top10', [])[:5],
                    'ovsdb_server_top5': cpu_data.get('summary', {}).get('ovsdb_server_top10', [])[:5]
                }
            
            # Extract memory usage top 5
            if 'memory_usage' in ovs_data and not ovs_data['memory_usage'].get('error'):
                mem_data = ovs_data['memory_usage']
                result['memory_usage'] = {
                    'ovs_db_top5': mem_data.get('summary', {}).get('ovs_db_top10', [])[:5],
                    'ovs_vswitchd_top5': mem_data.get('summary', {}).get('ovs_vswitchd_top10', [])[:5]
                }
            
            # Extract flows metrics top 5
            if 'dp_flows' in ovs_data and not ovs_data['dp_flows'].get('error'):
                result['flows_metrics']['dp_flows_top5'] = ovs_data['dp_flows'].get('top_10', [])[:5]
            
            if 'bridge_flows' in ovs_data and not ovs_data['bridge_flows'].get('error'):
                bridge_data = ovs_data['bridge_flows']
                result['flows_metrics']['br_int_top5'] = bridge_data.get('top_10', {}).get('br_int', [])[:5]
                result['flows_metrics']['br_ex_top5'] = bridge_data.get('top_10', {}).get('br_ex', [])[:5]
            
            # Extract connection metrics
            if 'connection_metrics' in ovs_data and not ovs_data['connection_metrics'].get('error'):
                result['connection_metrics'] = ovs_data['connection_metrics'].get('connection_metrics', {})
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to collect OVS metrics: {str(e)}'}
    
    async def collect_latency_metrics_summary(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect comprehensive latency metrics 
        """
        try:
            # Use duration window to avoid instant-query zeros
            latency_window = duration or "5m"
            
            # Collect comprehensive latency data
            latency_data = await self.latency_collector.collect_comprehensive_enhanced_metrics(
                duration=latency_window,
                top_n_results=5
            )
            
            result = {
                'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                'query_type': 'duration' if duration else 'instant',
                'timezone': 'UTC',
                'categories': {}
            }
            
            # Process standard latency categories, but drop metrics with no data
            categories = [
                'ready_duration_metrics', 'sync_duration_metrics', 
                'percentile_latency_metrics', 'pod_latency_metrics',
                'cni_latency_metrics', 'service_latency_metrics', 
                'network_programming_metrics'
            ]
            
            for category in categories:
                if category in latency_data:
                    category_data = latency_data[category]
                    cleaned: Dict[str, Any] = {}
                    
                    for metric_name, metric_result in category_data.items():
                        if isinstance(metric_result, dict) and 'statistics' in metric_result:
                            stats = metric_result['statistics']
                            # Use StatisticsCalculator for additional stats if needed
                            if stats.get('values'):
                                enhanced_stats = StatisticsCalculator.calculate_basic_stats(stats['values'])
                                stats.update(enhanced_stats)
                            # Keep only if meaningful (non-zero) data exists
                            count = stats.get('count') or 0
                            max_val = stats.get('max_value') or 0
                            avg_val = stats.get('avg_value') or 0
                            top_5 = stats.get('top_5') or []
                            if (count > 0) or (max_val > 0) or (avg_val > 0) or (isinstance(top_5, list) and len(top_5) > 0):
                                cleaned[metric_name] = {
                                    'metric_name': metric_result.get('metric_name', metric_name),
                                    'component': metric_result.get('component', 'unknown'),
                                    'unit': metric_result.get('unit', 'seconds'),
                                    'statistics': {
                                        'count': count,
                                        'max_value': max_val,
                                        'avg_value': avg_val,
                                        'top_5': top_5
                                    }
                                }
                    # Only add non-empty categories
                    if cleaned:
                        result['categories'][category.replace('_metrics', '')] = cleaned
            
            # Add query parameters if duration was specified
            if duration:
                start_time, end_time_actual = self.prometheus_client.get_time_range_from_duration(duration, None)
                result['query_parameters'] = {
                    'duration': duration, 
                    'start_time': start_time, 
                    'end_time': end_time_actual
                }
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to collect latency metrics: {str(e)}'}

    async def collect_nodes_usage_summary(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Collect node usage data for master, infra, and top 5 worker nodes (requirement 2.1)"""
        try:
            node_usage_data = await self.node_usage_collector.collect_usage_data(
                duration=duration or "1h", 
                end_time=None, 
                debug=False
            )
            
            result = {
                'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                'query_duration': duration or "1h",
                'timezone': 'UTC',
                'controlplane_nodes': {
                    'summary': {},
                    'individual_nodes': [],
                    'count': 0
                },
                'infra_nodes': {
                    'summary': {},
                    'individual_nodes': [],
                    'count': 0
                },
                'top5_worker_nodes': {
                    'summary': {},
                    'individual_nodes': [],
                    'count': 0
                }
            }
            
            if 'error' in node_usage_data:
                return {'error': node_usage_data['error']}
            
            # Extract controlplane (master) nodes data
            controlplane_group = node_usage_data.get('groups', {}).get('controlplane', {})
            if controlplane_group:
                result['controlplane_nodes']['summary'] = {
                    'cpu_usage': controlplane_group.get('summary', {}).get('cpu_usage', {}),
                    'memory_usage': controlplane_group.get('summary', {}).get('memory_usage', {}),
                    'network_rx': controlplane_group.get('summary', {}).get('network_rx', {}),
                    'network_tx': controlplane_group.get('summary', {}).get('network_tx', {})
                }
                result['controlplane_nodes']['individual_nodes'] = [
                    {
                        'name': node.get('name'),
                        'instance': node.get('instance'),
                        'cpu_usage': node.get('metrics', {}).get('cpu_usage', {}),
                        'memory_usage': node.get('metrics', {}).get('memory_usage', {}),
                        'network_rx': node.get('metrics', {}).get('network_rx', {}),
                        'network_tx': node.get('metrics', {}).get('network_tx', {})
                    }
                    for node in controlplane_group.get('nodes', [])
                ]
                result['controlplane_nodes']['count'] = controlplane_group.get('count', 0)
            
            # Extract infra nodes data
            infra_group = node_usage_data.get('groups', {}).get('infra', {})
            if infra_group:
                result['infra_nodes']['summary'] = {
                    'cpu_usage': infra_group.get('summary', {}).get('cpu_usage', {}),
                    'memory_usage': infra_group.get('summary', {}).get('memory_usage', {}),
                    'network_rx': infra_group.get('summary', {}).get('network_rx', {}),
                    'network_tx': infra_group.get('summary', {}).get('network_tx', {})
                }
                result['infra_nodes']['individual_nodes'] = [
                    {
                        'name': node.get('name'),
                        'instance': node.get('instance'),
                        'cpu_usage': node.get('metrics', {}).get('cpu_usage', {}),
                        'memory_usage': node.get('metrics', {}).get('memory_usage', {}),
                        'network_rx': node.get('metrics', {}).get('network_rx', {}),
                        'network_tx': node.get('metrics', {}).get('network_tx', {})
                    }
                    for node in infra_group.get('nodes', [])
                ]
                result['infra_nodes']['count'] = infra_group.get('count', 0)
            
            # Extract top 5 worker nodes based on CPU usage
            worker_group = node_usage_data.get('groups', {}).get('worker', {})
            if worker_group:
                worker_nodes = worker_group.get('nodes', [])
                
                # Sort worker nodes by max CPU usage and get top 5
                sorted_workers = sorted(
                    [node for node in worker_nodes 
                     if node.get('metrics', {}).get('cpu_usage', {}).get('max') is not None],
                    key=lambda x: x.get('metrics', {}).get('cpu_usage', {}).get('max', 0),
                    reverse=True
                )[:5]
                
                # Calculate summary for top 5 workers
                if sorted_workers:
                    top5_summary = self.node_usage_collector.calculate_group_summary(sorted_workers)
                    result['top5_worker_nodes']['summary'] = {
                        'cpu_usage': top5_summary.get('cpu_usage', {}),
                        'memory_usage': top5_summary.get('memory_usage', {}),
                        'network_rx': top5_summary.get('network_rx', {}),
                        'network_tx': top5_summary.get('network_tx', {})
                    }
                
                result['top5_worker_nodes']['individual_nodes'] = [
                    {
                        'name': node.get('name'),
                        'instance': node.get('instance'),
                        'rank': idx + 1,
                        'cpu_usage': node.get('metrics', {}).get('cpu_usage', {}),
                        'memory_usage': node.get('metrics', {}).get('memory_usage', {}),
                        'network_rx': node.get('metrics', {}).get('network_rx', {}),
                        'network_tx': node.get('metrics', {}).get('network_tx', {})
                    }
                    for idx, node in enumerate(sorted_workers)
                ]
                result['top5_worker_nodes']['count'] = len(sorted_workers)
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to collect node usage data: {str(e)}'}

    def analyze_performance_insights(self, metrics_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced performance analysis using utility module components"""
        insights = {
            'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            'performance_summary': {},
            'key_findings': [],
            'recommendations': [],
            'resource_hotspots': {},
            'latency_analysis': {},
            'node_analysis': {},
            'controller_sync_analysis': {}
        }
        
        # Use the enhanced performance score calculation
        performance_score = self._calculate_comprehensive_performance_score(metrics_summary)
        insights['performance_summary'] = performance_score
        
        # Enhanced Node analysis with utility functions
        nodes_data = metrics_summary.get('nodes_usage', {})
        if nodes_data and not nodes_data.get('error'):
            node_insights = {
                'high_cpu_nodes': [],
                'high_memory_nodes': [],
                'node_performance_summary': {},
                'resource_efficiency': {}
            }
            
            # Analyze controlplane nodes with efficiency calculation
            controlplane_summary = nodes_data.get('controlplane_nodes', {}).get('summary', {})
            if controlplane_summary:
                cpu_max = controlplane_summary.get('cpu_usage', {}).get('max', 0)
                mem_max = controlplane_summary.get('memory_usage', {}).get('max', 0)
                
                # Use utility function for efficiency calculation
                efficiency = calculate_node_resource_efficiency(
                    cpu_usage=cpu_max,
                    memory_usage_mb=MemoryConverter.to_mb(mem_max, 'MB'),
                    estimated_cpu_capacity=100.0,
                    estimated_memory_capacity_mb=16384  # 16GB default
                )
                
                node_insights['node_performance_summary']['controlplane'] = {
                    'max_cpu_usage': cpu_max,
                    'max_memory_usage_mb': mem_max,
                    'status': 'high' if cpu_max > 70 else 'normal',
                    'efficiency': efficiency
                }
                node_insights['resource_efficiency']['controlplane'] = efficiency
            
            # Analyze infra nodes
            infra_summary = nodes_data.get('infra_nodes', {}).get('summary', {})
            if infra_summary:
                cpu_max = infra_summary.get('cpu_usage', {}).get('max', 0)
                mem_max = infra_summary.get('memory_usage', {}).get('max', 0)
                
                efficiency = calculate_node_resource_efficiency(
                    cpu_usage=cpu_max,
                    memory_usage_mb=MemoryConverter.to_mb(mem_max, 'MB')
                )
                
                node_insights['node_performance_summary']['infra'] = {
                    'max_cpu_usage': cpu_max,
                    'max_memory_usage_mb': mem_max,
                    'status': 'high' if cpu_max > 70 else 'normal',
                    'efficiency': efficiency
                }
                node_insights['resource_efficiency']['infra'] = efficiency
            
            # Analyze top 5 worker nodes
            top5_workers = nodes_data.get('top5_worker_nodes', {}).get('individual_nodes', [])
            for worker in top5_workers:
                cpu_max = worker.get('cpu_usage', {}).get('max', 0)
                mem_max = worker.get('memory_usage', {}).get('max', 0)
                
                if cpu_max > 70:
                    node_insights['high_cpu_nodes'].append({
                        'name': worker.get('name'),
                        'cpu_usage': cpu_max,
                        'rank': worker.get('rank')
                    })
                
                if mem_max > 8192:  # > 8GB
                    node_insights['high_memory_nodes'].append({
                        'name': worker.get('name'),
                        'memory_usage_mb': mem_max,
                        'rank': worker.get('rank')
                    })
            
            insights['node_analysis'] = node_insights
        
        # Controller sync analysis is reported under latency_metrics.sync_duration only
        
        # Enhanced Resource hotspots analysis
        ovnkube_pods = metrics_summary.get('ovnkube_pods_cpu', {})
        if ovnkube_pods and not ovnkube_pods.get('error'):
            high_cpu_pods = []
            high_memory_pods = []
            
            # Analyze both node and control plane pods
            for pod_type, pod_data in [('node', ovnkube_pods.get('ovnkube_node_pods', {})),
                                     ('control_plane', ovnkube_pods.get('ovnkube_control_plane_pods', {}))]:
                
                # CPU analysis
                for pod in pod_data.get('top_5_cpu', []):
                    cpu_metrics = pod.get('metrics', {})
                    for metric_name, metric_data in cpu_metrics.items():
                        if 'cpu' in metric_name.lower():
                            avg_cpu = metric_data.get('avg', 0)
                            cpu_threshold = ThresholdClassifier.get_default_cpu_threshold()
                            level, severity = ThresholdClassifier.classify_performance(avg_cpu, cpu_threshold)
                            
                            if level in [PerformanceLevel.POOR, PerformanceLevel.CRITICAL]:
                                high_cpu_pods.append({
                                    'pod_name': pod.get('pod_name'),
                                    'node_name': pod.get('node_name'),
                                    'cpu_usage': avg_cpu,
                                    'pod_type': pod_type,
                                    'performance_level': level.value,
                                    'severity_score': severity
                                })
                
                # Memory analysis
                for pod in pod_data.get('top_5_memory', []):
                    mem_metrics = pod.get('metrics', {})
                    for metric_name, metric_data in mem_metrics.items():
                        if 'memory' in metric_name.lower():
                            avg_mem = metric_data.get('avg', 0)
                            mem_mb = MemoryConverter.to_mb(avg_mem, 'MB')
                            mem_threshold = ThresholdClassifier.get_default_memory_threshold()
                            level, severity = ThresholdClassifier.classify_performance(mem_mb, mem_threshold)
                            
                            if level in [PerformanceLevel.POOR, PerformanceLevel.CRITICAL]:
                                high_memory_pods.append({
                                    'pod_name': pod.get('pod_name'),
                                    'node_name': pod.get('node_name'),
                                    'memory_usage_mb': mem_mb,
                                    'pod_type': pod_type,
                                    'performance_level': level.value,
                                    'severity_score': severity
                                })
            
            insights['resource_hotspots'] = {
                'high_cpu_pods': sorted(high_cpu_pods, key=lambda x: x['severity_score'], reverse=True)[:5],
                'high_memory_pods': sorted(high_memory_pods, key=lambda x: x['severity_score'], reverse=True)[:5]
            }
        
        # Enhanced Latency analysis
        latency_data = metrics_summary.get('latency_metrics', {})
        if latency_data and not latency_data.get('error'):
            high_latency_metrics = []
            for category, metrics in latency_data.get('categories', {}).items():
                for metric_name, metric_info in metrics.items():
                    stats = metric_info.get('statistics', {})
                    max_val = stats.get('max_value', 0)
                    avg_val = stats.get('avg_value', 0)
                    
                    # Use threshold classification
                    latency_threshold = PerformanceThreshold(
                        excellent_max=0.1, good_max=0.5, moderate_max=1.0, poor_max=2.0,
                        unit='seconds', component_type='latency'
                    )
                    level, severity = ThresholdClassifier.classify_performance(max_val, latency_threshold)
                    
                    if level in [PerformanceLevel.MODERATE, PerformanceLevel.POOR, PerformanceLevel.CRITICAL]:
                        high_latency_metrics.append({
                            'category': category,
                            'metric': metric_name,
                            'max_latency': max_val,
                            'avg_latency': avg_val,
                            'component': metric_info.get('component'),
                            'performance_level': level.value,
                            'severity_score': severity
                        })
            
            insights['latency_analysis']['high_latency_metrics'] = sorted(
                high_latency_metrics, 
                key=lambda x: x['severity_score'], 
                reverse=True
            )[:10]  # Top 10 problematic metrics
        
        # Generate enhanced key findings
        findings = []
        perf_summary = performance_score
        
        if perf_summary['overall_score'] >= 90:
            findings.append("Excellent overall cluster performance with minimal issues")
        elif perf_summary['overall_score'] >= 75:
            findings.append("Good cluster performance with some optimization opportunities")
        elif perf_summary['overall_score'] >= 60:
            findings.append("Moderate cluster performance requiring attention")
        else:
            findings.append("Poor cluster performance requiring immediate attention")
        
        # Component-specific findings
        for component, score in perf_summary['component_scores'].items():
            component_name = component.replace('_score', '').replace('_', ' ').title()
            if score < 60:
                findings.append(f"{component_name} performance is concerning (Score: {score:.1f})")
        
        # Node-specific findings
        if insights.get('node_analysis', {}).get('high_cpu_nodes'):
            findings.append(f"Found {len(insights['node_analysis']['high_cpu_nodes'])} worker nodes with high CPU usage")
        
        # Controller sync findings are covered by latency analysis
        
        insights['key_findings'] = findings
        
        # Generate enhanced recommendations using utility functions
        recommendations = []
        
        # Use RecommendationEngine from utility module
        cluster_health = perf_summary.get('cluster_health', {})
        if cluster_health:
            cluster_recs = RecommendationEngine.generate_cluster_recommendations(
                cluster_health.critical_issues_count,
                cluster_health.warning_issues_count,
                self._count_total_components(metrics_summary),
                "OVN-Kubernetes"
            )
            recommendations.extend(cluster_recs)
        
        # Component-specific recommendations
        if perf_summary['component_scores'].get('latency_score', 0) < 70:
            recommendations.append("Investigate network latency and optimize OVN controller configuration")
            recommendations.append("Consider tuning ovnkube-controller sync intervals")
        
        if perf_summary['component_scores'].get('resource_utilization_score', 0) < 70:
            recommendations.append("Review and optimize resource limits for OVNKube pods")
            recommendations.append("Consider horizontal scaling for high-utilization components")
        
        if perf_summary['component_scores'].get('node_health_score', 0) < 70:
            recommendations.append("Investigate node resource constraints and consider cluster capacity expansion")
        
        # Controller sync specific recommendations
        if insights.get('controller_sync_analysis', {}).get('high_latency_controllers'):
            recommendations.append("Optimize controller sync performance - consider resource limits and node placement")
            recommendations.append("Review controller configuration and potential resource contention")
        
        insights['recommendations'] = recommendations
        
        return insights

    async def run_comprehensive_analysis(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Run complete deep drive analysis with enhanced utility integration"""
        print(f"Running comprehensive OVN analysis {'with duration: ' + duration if duration else 'instant'}...")

        # Generate metadata using utility function
        metadata = self.generate_metadata(
            collection_type='comprehensive_deep_drive',
            total_items=0,  # Will be updated after collection
            duration=duration
        )
        
        analysis_result = {
            'analysis_metadata': metadata.__dict__,
            'analysis_type': 'comprehensive_deep_drive',
            'query_duration': duration or 'instant',
            'timezone': 'UTC'
        }
        
        try:
            # Collect all metrics concurrently
            tasks = [
                self.collect_prometheus_basic_info(),
                self.collect_ovnkube_pods_usage(duration),
                self.collect_ovn_containers_usage(duration), 
                self.collect_ovs_metrics_summary(duration),
                self.collect_latency_metrics_summary(duration),
                self.collect_nodes_usage_summary(duration)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Organize results
            analysis_result.update({
                'basic_info': results[0] if not isinstance(results[0], Exception) else {'error': str(results[0])},
                'ovnkube_pods_cpu': results[1] if not isinstance(results[1], Exception) else {'error': str(results[1])},
                'ovn_containers': results[2] if not isinstance(results[2], Exception) else {'error': str(results[2])},
                'ovs_metrics': results[3] if not isinstance(results[3], Exception) else {'error': str(results[3])},
                'latency_metrics': results[4] if not isinstance(results[4], Exception) else {'error': str(results[4])},
                'nodes_usage': results[5] if not isinstance(results[5], Exception) else {'error': str(results[5])}
            })
            
            # Update metadata with actual component count
            metadata.total_items_analyzed = self._count_total_components(analysis_result)
            analysis_result['analysis_metadata'] = metadata.__dict__
            
            # Perform enhanced analysis
            analysis_result['performance_analysis'] = self.analyze_performance_insights(analysis_result)
            
            # Generate formatted summary for JSON output
            analysis_result['formatted_summary'] = format_performance_summary_for_json(analysis_result)
            
            print("Comprehensive OVN analysis completed successfully")
            return analysis_result
            
        except Exception as e:
            analysis_result['error'] = str(e)
            print(f"Error in comprehensive analysis: {e}")
            return analysis_result



async def run_ovn_deep_drive_analysis(prometheus_client: PrometheusBaseQuery, 
                                    auth: Optional[OpenShiftAuth] = None,
                                    duration: Optional[str] = None) -> Dict[str, Any]:
    """Run comprehensive OVN deep drive analysis with utility integration"""
    analyzer = ovnDeepDriveAnalyzer(prometheus_client, auth)
    return await analyzer.run_comprehensive_analysis(duration)

async def get_ovn_performance_json(prometheus_client: PrometheusBaseQuery,
                                 auth: Optional[OpenShiftAuth] = None, 
                                 duration: Optional[str] = None) -> str:
    """Get OVN performance analysis as JSON string with enhanced formatting"""
    results = await run_ovn_deep_drive_analysis(prometheus_client, auth, duration)
    return json.dumps(results, indent=2, default=str)


# Import utility function for backward compatibility
from .ovnk_benchmark_performance_utility import calculate_node_resource_efficiency

# Example usage
async def main():
    """Example usage of enhanced OVN Deep Drive Analyzer"""
    try:
        # Initialize authentication
        from ocauth.ovnk_benchmark_auth import auth
        await auth.initialize()
        
        # Create Prometheus client
        prometheus_client = PrometheusBaseQuery(
            prometheus_url=auth.prometheus_url,
            token=auth.prometheus_token
        )
        
        # Run comprehensive analysis
        analyzer = ovnDeepDriveAnalyzer(prometheus_client, auth)
        
        # Test instant analysis
        print("Running enhanced instant analysis...")
        instant_results = await analyzer.run_comprehensive_analysis()
        
        # Generate report using utility functions
        if 'performance_analysis' in instant_results:
            perf_analysis = instant_results['performance_analysis']
            cluster_health = perf_analysis.get('performance_summary', {}).get('cluster_health', {})
            
            if cluster_health:
                report_generator = ReportGenerator()
                metadata = AnalysisMetadata(**instant_results['analysis_metadata'])
                health_obj = ClusterHealth(**cluster_health)
                
                summary_lines = report_generator.generate_summary_section(metadata, health_obj)
                print("\n".join(summary_lines))
        
        print(json.dumps(instant_results, indent=2, default=str))
        
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        await prometheus_client.close()


if __name__ == "__main__":
    asyncio.run(main())

