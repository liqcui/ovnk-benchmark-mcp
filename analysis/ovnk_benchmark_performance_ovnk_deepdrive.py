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

class ovnDeepDriveAnalyzer:
    """Comprehensive OVN-Kubernetes performance analyzer"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery, auth: Optional[OpenShiftAuth] = None):
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

    def _convert_bytes_to_mb(self, bytes_value: float) -> float:
        """Convert bytes to MB"""
        return round(bytes_value / (1024 * 1024), 2)
    
    def _extract_top_5_from_usage_data(self, usage_data: Dict[str, Any], metric_type: str = "cpu") -> List[Dict[str, Any]]:
        """Extract top 5 entries from usage collector results"""
        if metric_type == "cpu":
            top_list = usage_data.get('top_5_cpu_usage', [])
        else:
            top_list = usage_data.get('top_5_memory_usage', [])
        
        return top_list[:5]
    
    def _calculate_performance_score(self, metrics_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate overall performance score based on collected metrics"""
        score_factors = {
            'latency_score': 0,
            'resource_score': 0, 
            'stability_score': 0,
            'ovs_score': 0
        }
        
        # Latency scoring (lower is better)
        latency_data = metrics_summary.get('latency_metrics', {})
        if latency_data and not latency_data.get('error'):
            avg_latencies = []
            for category, data in latency_data.items():
                if isinstance(data, dict) and 'statistics' in data:
                    avg_val = data['statistics'].get('avg_value', 0)
                    if avg_val > 0:
                        avg_latencies.append(avg_val)
            
            if avg_latencies:
                avg_latency = statistics.mean(avg_latencies)
                if avg_latency < 0.1:  # < 100ms
                    score_factors['latency_score'] = 90
                elif avg_latency < 0.5:  # < 500ms
                    score_factors['latency_score'] = 70
                elif avg_latency < 1.0:  # < 1s
                    score_factors['latency_score'] = 50
                else:
                    score_factors['latency_score'] = 30
        
        # Resource utilization scoring
        cpu_usage = metrics_summary.get('ovnkube_pods_cpu', {})
        if cpu_usage and not cpu_usage.get('error'):
            top_cpu = cpu_usage.get('top_5_cpu_usage', [])
            if top_cpu:
                max_cpu = max([entry.get('metrics', {}).get('cpu_usage', {}).get('avg', 0) 
                              for entry in top_cpu[:3]])  # Top 3
                if max_cpu < 50:  # < 50%
                    score_factors['resource_score'] = 90
                elif max_cpu < 80:  # < 80%
                    score_factors['resource_score'] = 70
                else:
                    score_factors['resource_score'] = 40
        
        # Stability scoring (based on alerts and pod status)
        basic_info = metrics_summary.get('basic_info', {})
        if basic_info and 'metrics' in basic_info:
            alerts = basic_info['metrics'].get('alerts', {})
            if alerts and not alerts.get('error'):
                alert_count = len(alerts.get('top_alerts', []))
                if alert_count == 0:
                    score_factors['stability_score'] = 100
                elif alert_count < 3:
                    score_factors['stability_score'] = 80
                elif alert_count < 6:
                    score_factors['stability_score'] = 60
                else:
                    score_factors['stability_score'] = 40
        
        # OVS performance scoring
        ovs_data = metrics_summary.get('ovs_metrics', {})
        if ovs_data and not ovs_data.get('error'):
            cpu_data = ovs_data.get('cpu_usage', {})
            if cpu_data and not cpu_data.get('error'):
                ovs_top = cpu_data.get('summary', {}).get('ovs_vswitchd_top10', [])
                if ovs_top:
                    max_ovs_cpu = max([entry.get('max', 0) for entry in ovs_top[:3]])
                    if max_ovs_cpu < 30:  # < 30%
                        score_factors['ovs_score'] = 90
                    elif max_ovs_cpu < 60:  # < 60%
                        score_factors['ovs_score'] = 70
                    else:
                        score_factors['ovs_score'] = 50
        
        # Calculate overall score (weighted average)
        weights = {'latency_score': 0.3, 'resource_score': 0.3, 'stability_score': 0.2, 'ovs_score': 0.2}
        overall_score = sum(score_factors[key] * weights[key] for key in weights)
        
        return {
            'overall_score': round(overall_score, 1),
            'component_scores': score_factors,
            'performance_grade': 'A' if overall_score >= 80 else 'B' if overall_score >= 60 else 'C' if overall_score >= 40 else 'D'
        }
    
    async def collect_basic_cluster_info(self) -> Dict[str, Any]:
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
            
            # Extract database sizes
            if 'metrics' in basic_summary and 'ovn_database' in basic_summary['metrics']:
                db_info = basic_summary['metrics']['ovn_database']
                if not db_info.get('error'):
                    for db_name, db_data in db_info.items():
                        if isinstance(db_data, dict) and 'max_value' in db_data:
                            result['database_sizes'][db_name] = {
                                'size_bytes': db_data['max_value'],
                                'size_mb': self._convert_bytes_to_mb(db_data['max_value']) if db_data['max_value'] else 0,
                                'unit': 'MB'
                            }
            
            # Extract alerts summary
            if 'metrics' in basic_summary and 'alerts' in basic_summary['metrics']:
                alerts = basic_summary['metrics']['alerts']
                if not alerts.get('error'):
                    result['alerts_summary'] = {
                        'total_alert_types': alerts.get('total_alert_types', 0),
                        'top_alerts': alerts.get('top_alerts', [])[:5]  # Top 5
                    }
            
            # Extract pod distribution
            if 'metrics' in basic_summary and 'pod_distribution' in basic_summary['metrics']:
                pod_dist = basic_summary['metrics']['pod_distribution']
                if not pod_dist.get('error'):
                    result['pod_distribution'] = {
                        'total_nodes': pod_dist.get('total_nodes', 0),
                        'top_nodes': pod_dist.get('top_nodes', [])[:5]  # Top 5
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
                'sbdb': 'sb-ovsdb',
                'nbdb': 'nb-ovsdb', 
                'ovnkube_controller': 'ovnkube-controller',
                'northd': 'northd',
                'ovn_controller': 'ovn-controller'
            }
            
            result = {
                'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                'query_type': 'duration' if duration else 'instant',
                'containers': {}
            }
            
            for container_name, pattern in container_patterns.items():
                try:
                    # Remove the include_containers parameter - it's determined automatically by container_pattern
                    if duration:
                        usage_data = await self.pods_usage_collector.collect_duration_usage(
                            duration=duration,
                            container_pattern=f".*{pattern}.*",
                            namespace_pattern="openshift-ovn-kubernetes"
                        )
                    else:
                        usage_data = await self.pods_usage_collector.collect_instant_usage(
                            container_pattern=f".*{pattern}.*",
                            namespace_pattern="openshift-ovn-kubernetes"
                        )
                    
                    result['containers'][container_name] = {
                        'top_5_cpu': self._extract_top_5_from_usage_data(usage_data, 'cpu'),
                        'top_5_memory': self._extract_top_5_from_usage_data(usage_data, 'memory')
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
        """Collect top 5 latency metrics (requirement 2.5)"""
        try:
            latency_data = await self.latency_collector.collect_comprehensive_enhanced_metrics(
                duration=duration or "5m" if duration else None,
                top_n_results=5
            )
            
            result = {
                'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                'query_type': 'duration' if duration else 'instant',
                'categories': {}
            }
            
            # Process each category and extract top 5
            categories = [
                'ready_duration_metrics', 'sync_duration_metrics', 
                'percentile_latency_metrics', 'pod_latency_metrics',
                'cni_latency_metrics', 'service_latency_metrics', 
                'network_programming_metrics'
            ]
            
            for category in categories:
                if category in latency_data:
                    category_data = latency_data[category]
                    result['categories'][category.replace('_metrics', '')] = {}
                    
                    for metric_name, metric_result in category_data.items():
                        if isinstance(metric_result, dict) and 'statistics' in metric_result:
                            stats = metric_result['statistics']
                            result['categories'][category.replace('_metrics', '')][metric_name] = {
                                'metric_name': metric_result.get('metric_name', metric_name),
                                'component': metric_result.get('component', 'unknown'),
                                'unit': metric_result.get('unit', 'seconds'),
                                'count': stats.get('count', 0),
                                'max_value': stats.get('max_value', 0),
                                'avg_value': stats.get('avg_value', 0),
                                'top_5': stats.get('top_5', [])
                            }
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to collect latency metrics: {str(e)}'}

    async def collect_nodes_usage_summary(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Collect node usage data for master, infra, and top 5 worker nodes (requirement 2.1)"""
        try:
            # Collect node usage data with specified duration
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
        """Analyze collected metrics and provide performance insights (requirement 2.6)"""
        insights = {
            'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            'performance_summary': {},
            'key_findings': [],
            'recommendations': [],
            'resource_hotspots': {},
            'latency_analysis': {},
            'node_analysis': {}  # New addition
        }
        
        # Performance score calculation (existing code unchanged)
        performance_score = self._calculate_performance_score(metrics_summary)
        insights['performance_summary'] = performance_score
        
        # Node analysis (new addition)
        nodes_data = metrics_summary.get('nodes_usage', {})
        if nodes_data and not nodes_data.get('error'):
            node_insights = {
                'high_cpu_nodes': [],
                'high_memory_nodes': [],
                'node_performance_summary': {}
            }
            
            # Analyze controlplane nodes
            controlplane_summary = nodes_data.get('controlplane_nodes', {}).get('summary', {})
            if controlplane_summary:
                cpu_max = controlplane_summary.get('cpu_usage', {}).get('max', 0)
                mem_max = controlplane_summary.get('memory_usage', {}).get('max', 0)
                node_insights['node_performance_summary']['controlplane'] = {
                    'max_cpu_usage': cpu_max,
                    'max_memory_usage_mb': mem_max,
                    'status': 'high' if cpu_max > 70 else 'normal'
                }
            
            # Analyze infra nodes
            infra_summary = nodes_data.get('infra_nodes', {}).get('summary', {})
            if infra_summary:
                cpu_max = infra_summary.get('cpu_usage', {}).get('max', 0)
                mem_max = infra_summary.get('memory_usage', {}).get('max', 0)
                node_insights['node_performance_summary']['infra'] = {
                    'max_cpu_usage': cpu_max,
                    'max_memory_usage_mb': mem_max,
                    'status': 'high' if cpu_max > 70 else 'normal'
                }
            
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
        
        # Resource hotspots analysis (existing code unchanged)
        ovnkube_pods = metrics_summary.get('ovnkube_pods_cpu', {})
        if ovnkube_pods and not ovnkube_pods.get('error'):
            # Find high CPU usage pods
            node_cpu = ovnkube_pods.get('ovnkube_node_pods', {}).get('top_5_cpu', [])
            control_cpu = ovnkube_pods.get('ovnkube_control_plane_pods', {}).get('top_5_cpu', [])
            
            high_cpu_pods = []
            for pod_list in [node_cpu, control_cpu]:
                for pod in pod_list:
                    cpu_metrics = pod.get('metrics', {})
                    for metric_name, metric_data in cpu_metrics.items():
                        if 'cpu' in metric_name.lower():
                            avg_cpu = metric_data.get('avg', 0)
                            if avg_cpu > 50:  # > 50% CPU
                                high_cpu_pods.append({
                                    'pod_name': pod.get('pod_name'),
                                    'node_name': pod.get('node_name'),
                                    'cpu_usage': avg_cpu
                                })
            
            insights['resource_hotspots']['high_cpu_pods'] = high_cpu_pods[:5]
        
        # Latency analysis (existing code unchanged)
        latency_data = metrics_summary.get('latency_metrics', {})
        if latency_data and not latency_data.get('error'):
            high_latency_metrics = []
            for category, metrics in latency_data.get('categories', {}).items():
                for metric_name, metric_info in metrics.items():
                    max_val = metric_info.get('max_value', 0)
                    if max_val > 1.0:  # > 1 second
                        high_latency_metrics.append({
                            'category': category,
                            'metric': metric_name,
                            'max_latency': max_val,
                            'component': metric_info.get('component')
                        })
            
            insights['latency_analysis']['high_latency_metrics'] = sorted(
                high_latency_metrics, 
                key=lambda x: x['max_latency'], 
                reverse=True
            )[:5]
        
        # Generate key findings (existing code unchanged)
        if performance_score['overall_score'] >= 80:
            insights['key_findings'].append("Overall cluster performance is excellent")
        elif performance_score['overall_score'] >= 60:
            insights['key_findings'].append("Cluster performance is good with room for optimization")
        else:
            insights['key_findings'].append("Cluster performance needs attention")
        
        # Add node-specific findings
        if insights.get('node_analysis', {}).get('high_cpu_nodes'):
            insights['key_findings'].append(f"Found {len(insights['node_analysis']['high_cpu_nodes'])} worker nodes with high CPU usage")
        
        # Generate recommendations based on scores (existing code unchanged)
        if performance_score['component_scores']['latency_score'] < 60:
            insights['recommendations'].append("Consider investigating network latency and optimizing OVN configuration")
        
        if performance_score['component_scores']['resource_score'] < 60:
            insights['recommendations'].append("CPU/Memory usage is high - consider resource limits or scaling")
        
        if performance_score['component_scores']['stability_score'] < 60:
            insights['recommendations'].append("Multiple alerts detected - investigate cluster stability")
        
        # Add node-specific recommendations
        if insights.get('node_analysis', {}).get('node_performance_summary', {}).get('controlplane', {}).get('status') == 'high':
            insights['recommendations'].append("Control plane nodes showing high resource usage - consider scaling or optimization")
        
        return insights

    async def run_comprehensive_analysis(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Run complete deep drive analysis"""
        print(f"Running comprehensive OVN analysis {'with duration: ' + duration if duration else 'instant'}...")
        
        analysis_result = {
            'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            'analysis_type': 'comprehensive_deep_drive',
            'query_duration': duration or 'instant',
            'timezone': 'UTC'
        }
        
        try:
            # Collect all metrics concurrently
            tasks = [
                self.collect_basic_cluster_info(),
                self.collect_ovnkube_pods_usage(duration),
                self.collect_ovn_containers_usage(duration), 
                self.collect_ovs_metrics_summary(duration),
                self.collect_latency_metrics_summary(duration),
                self.collect_nodes_usage_summary(duration)  # New addition
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Organize results
            analysis_result.update({
                'basic_info': results[0] if not isinstance(results[0], Exception) else {'error': str(results[0])},
                'ovnkube_pods_cpu': results[1] if not isinstance(results[1], Exception) else {'error': str(results[1])},
                'ovn_containers': results[2] if not isinstance(results[2], Exception) else {'error': str(results[2])},
                'ovs_metrics': results[3] if not isinstance(results[3], Exception) else {'error': str(results[3])},
                'latency_metrics': results[4] if not isinstance(results[4], Exception) else {'error': str(results[4])},
                 'nodes_usage': results[5] if not isinstance(results[5], Exception) else {'error': str(results[5])}  # New addition
            })
            
            # Perform analysis
            analysis_result['performance_analysis'] = self.analyze_performance_insights(analysis_result)
            
            print("Comprehensive OVN analysis completed successfully")
            return analysis_result
            
        except Exception as e:
            analysis_result['error'] = str(e)
            print(f"Error in comprehensive analysis: {e}")
            return analysis_result


# Convenience functions
async def run_ovn_deep_drive_analysis(prometheus_client: PrometheusBaseQuery, 
                                    auth: Optional[OpenShiftAuth] = None,
                                    duration: Optional[str] = None) -> Dict[str, Any]:
    """Run comprehensive OVN deep drive analysis"""
    analyzer = ovnDeepDriveAnalyzer(prometheus_client, auth)
    return await analyzer.run_comprehensive_analysis(duration)


async def get_ovn_performance_json(prometheus_client: PrometheusBaseQuery,
                                 auth: Optional[OpenShiftAuth] = None, 
                                 duration: Optional[str] = None) -> str:
    """Get OVN performance analysis as JSON string"""
    results = await run_ovn_deep_drive_analysis(prometheus_client, auth, duration)
    return json.dumps(results, indent=2, default=str)


# Example usage
async def main():
    """Example usage of OVN Deep Drive Analyzer"""
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
        print("Running instant analysis...")
        instant_results = await analyzer.run_comprehensive_analysis()
        print(json.dumps(instant_results, indent=2, default=str))
        
        # Test duration analysis
        print("\nRunning 5-minute duration analysis...")
        duration_results = await analyzer.run_comprehensive_analysis("5m")
        print(json.dumps(duration_results, indent=2, default=str))
        
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        await prometheus_client.close()


if __name__ == "__main__":
    asyncio.run(main())
