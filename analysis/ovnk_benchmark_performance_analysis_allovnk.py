"""
OVN-Kubernetes Performance Analysis Module
Comprehensive analysis of OVN-Kubernetes cluster performance metrics
File: analysis/ovnk_benchmark_performance_analysis_allovnk.py
"""

import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import statistics
import sys

# Import the collector modules
sys.path.append('/tools')
from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError
from tools.ovnk_benchmark_prometheus_ovnk_basicinfo import ovnBasicInfoCollector, get_pod_phase_counts
from tools.ovnk_benchmark_prometheus_ovnk_ovs import OVSUsageCollector
from tools.ovnk_benchmark_prometheus_pods_usage import PodsUsageCollector
from tools.ovnk_benchmark_prometheus_ovnk_sync import OVNSyncDurationCollector
from ocauth.ovnk_benchmark_auth import OpenShiftAuth

logger = logging.getLogger(__name__)


class OVNKPerformanceAnalyzer:
    """Comprehensive OVN-Kubernetes performance analyzer"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery, auth_client: OpenShiftAuth):
        self.prometheus_client = prometheus_client
        self.auth_client = auth_client
        
        # Initialize collectors
        self.basic_info_collector = ovnBasicInfoCollector(
            prometheus_url=auth_client.prometheus_url,
            token=auth_client.prometheus_token
        )
        self.ovs_collector = OVSUsageCollector(prometheus_client, auth_client)
        self.pods_collector = PodsUsageCollector(prometheus_client, auth_client)
        self.sync_collector = OVNSyncDurationCollector(prometheus_client)
        
        # Risk thresholds
        self.risk_thresholds = {
            'cpu_high': 80.0,  # CPU usage > 80%
            'cpu_critical': 95.0,  # CPU usage > 95%
            'memory_high_mb': 1000,  # Memory > 1GB
            'memory_critical_mb': 2000,  # Memory > 2GB
            'sync_duration_high': 10.0,  # Sync duration > 10s
            'sync_duration_critical': 30.0,  # Sync duration > 30s
            'db_size_large_mb': 100,  # DB size > 100MB
            'db_size_critical_mb': 500,  # DB size > 500MB
            'flows_high': 10000,  # Flow count > 10k
            'flows_critical': 50000  # Flow count > 50k
        }
    
    def _convert_bytes_to_mb(self, bytes_value: float) -> float:
        """Convert bytes to megabytes"""
        return bytes_value / (1024 * 1024)
    
    def _assess_risk_level(self, value: float, threshold_high: float, threshold_critical: float) -> str:
        """Assess risk level based on thresholds"""
        if value >= threshold_critical:
            return 'critical'
        elif value >= threshold_high:
            return 'high'
        else:
            return 'normal'
    
    def _extract_container_metrics(self, pod_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Extract and organize metrics by container"""
        containers = {}
        
        # Define OVN container patterns
        ovn_containers = {
            'ovnkube-controller': ['ovnkube-controller'],
            'ovnkube-node': ['ovnkube-node'],
            'nbdb': ['nbdb', 'northd'],
            'sbdb': ['sbdb'],
            'ovn-controller': ['ovn-controller'],
            'kube-rbac-proxy': ['kube-rbac-proxy-node', 'kube-rbac-proxy-ovn-metrics']
        }
        
        # Try to match pod to container type
        pod_name = pod_data.get('pod_name', '')
        container_type = 'unknown'
        
        for container_key, patterns in ovn_containers.items():
            if any(pattern in pod_name for pattern in patterns):
                container_type = container_key
                break
        
        # Extract metrics from pod data
        metrics = pod_data.get('metrics', {})
        cpu_usage = 0.0
        memory_usage = 0.0
        
        # Look for CPU metrics
        for metric_name, metric_data in metrics.items():
            if 'cpu' in metric_name.lower():
                if isinstance(metric_data, dict):
                    cpu_usage = metric_data.get('value', metric_data.get('avg', 0.0))
                else:
                    cpu_usage = metric_data
                break
        
        # Look for memory metrics
        for metric_name, metric_data in metrics.items():
            if 'memory' in metric_name.lower():
                if isinstance(metric_data, dict):
                    memory_usage = metric_data.get('value', metric_data.get('avg', 0.0))
                else:
                    memory_usage = metric_data
                break
        
        containers[container_type] = {
            'pod_name': pod_name,
            'cpu_usage_percent': cpu_usage,
            'memory_usage_bytes': memory_usage,
            'memory_usage_mb': self._convert_bytes_to_mb(memory_usage),
            'node_name': pod_data.get('node_name', 'unknown')
        }
        
        return containers
    
    async def analyze_pod_performance(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Analyze OVN pod performance"""
        try:
            print("Analyzing OVN pod performance...")
            
            # Collect OVN pod metrics
            if duration:
                ovn_pod_data = await self.pods_collector.collect_duration_usage(
                    duration=duration,
                    use_ovn_queries=True
                )
            else:
                ovn_pod_data = await self.pods_collector.collect_instant_usage(
                    use_ovn_queries=True
                )
            
            # Collect Multus pod metrics (networking pods)
            multus_patterns = {
                'pod_pattern': 'multus.*|network-operator.*|whereabouts.*',
                'namespace_pattern': 'openshift-multus|openshift-network-operator'
            }
            
            if duration:
                multus_data = await self.pods_collector.collect_duration_usage(
                    duration=duration,
                    **multus_patterns
                )
            else:
                multus_data = await self.pods_collector.collect_instant_usage(
                    **multus_patterns
                )
            
            # Analyze results
            analysis = {
                'ovn_pods': {
                    'total_analyzed': ovn_pod_data.get('total_pods_analyzed', 0),
                    'top_performers': [],
                    'container_breakdown': {},
                    'risks': []
                },
                'multus_pods': {
                    'total_analyzed': multus_data.get('total_pods_analyzed', 0),
                    'top_performers': [],
                    'risks': []
                }
            }
            
            # Analyze OVN pods
            for pod in ovn_pod_data.get('top_10_pods', []):
                pod_analysis = {
                    'pod_name': pod['pod_name'],
                    'node_name': pod['node_name'],
                    'cpu_metrics': {},
                    'memory_metrics': {},
                    'risk_level': 'normal'
                }
                
                # Extract CPU and memory metrics
                for metric_name, metric_data in pod.get('usage_metrics', {}).items():
                    if 'cpu' in metric_name.lower():
                        pod_analysis['cpu_metrics'] = metric_data
                    elif 'memory' in metric_name.lower():
                        pod_analysis['memory_metrics'] = metric_data
                
                # Assess risk
                cpu_value = pod_analysis['cpu_metrics'].get('max', pod_analysis['cpu_metrics'].get('value', 0))
                memory_mb = self._convert_bytes_to_mb(
                    pod_analysis['memory_metrics'].get('max', pod_analysis['memory_metrics'].get('value', 0))
                )
                
                cpu_risk = self._assess_risk_level(cpu_value, self.risk_thresholds['cpu_high'], self.risk_thresholds['cpu_critical'])
                memory_risk = self._assess_risk_level(memory_mb, self.risk_thresholds['memory_high_mb'], self.risk_thresholds['memory_critical_mb'])
                
                if cpu_risk != 'normal' or memory_risk != 'normal':
                    pod_analysis['risk_level'] = 'critical' if (cpu_risk == 'critical' or memory_risk == 'critical') else 'high'
                    analysis['ovn_pods']['risks'].append({
                        'pod_name': pod['pod_name'],
                        'cpu_risk': cpu_risk,
                        'memory_risk': memory_risk,
                        'cpu_value': cpu_value,
                        'memory_mb': memory_mb
                    })
                
                # Categorize by container type
                containers = self._extract_container_metrics(pod)
                for container_type, container_data in containers.items():
                    if container_type not in analysis['ovn_pods']['container_breakdown']:
                        analysis['ovn_pods']['container_breakdown'][container_type] = []
                    analysis['ovn_pods']['container_breakdown'][container_type].append(container_data)
                
                analysis['ovn_pods']['top_performers'].append(pod_analysis)
            
            # Analyze Multus pods
            for pod in multus_data.get('top_10_pods', []):
                pod_analysis = {
                    'pod_name': pod['pod_name'],
                    'node_name': pod['node_name'],
                    'cpu_metrics': {},
                    'memory_metrics': {},
                    'risk_level': 'normal'
                }
                
                # Extract metrics
                for metric_name, metric_data in pod.get('usage_metrics', {}).items():
                    if 'cpu' in metric_name.lower():
                        pod_analysis['cpu_metrics'] = metric_data
                    elif 'memory' in metric_name.lower():
                        pod_analysis['memory_metrics'] = metric_data
                
                # Assess risk
                cpu_value = pod_analysis['cpu_metrics'].get('max', pod_analysis['cpu_metrics'].get('value', 0))
                memory_mb = self._convert_bytes_to_mb(
                    pod_analysis['memory_metrics'].get('max', pod_analysis['memory_metrics'].get('value', 0))
                )
                
                cpu_risk = self._assess_risk_level(cpu_value, self.risk_thresholds['cpu_high'], self.risk_thresholds['cpu_critical'])
                memory_risk = self._assess_risk_level(memory_mb, self.risk_thresholds['memory_high_mb'], self.risk_thresholds['memory_critical_mb'])
                
                if cpu_risk != 'normal' or memory_risk != 'normal':
                    pod_analysis['risk_level'] = 'critical' if (cpu_risk == 'critical' or memory_risk == 'critical') else 'high'
                    analysis['multus_pods']['risks'].append({
                        'pod_name': pod['pod_name'],
                        'cpu_risk': cpu_risk,
                        'memory_risk': memory_risk,
                        'cpu_value': cpu_value,
                        'memory_mb': memory_mb
                    })
                
                analysis['multus_pods']['top_performers'].append(pod_analysis)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing pod performance: {e}")
            return {'error': f'Pod performance analysis failed: {str(e)}'}
    
    async def analyze_ovs_performance(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Analyze OVS component performance"""
        try:
            print("Analyzing OVS performance...")
            
            # Collect all OVS metrics
            ovs_data = await self.ovs_collector.collect_all_ovs_metrics(duration)
            
            analysis = {
                'cpu_analysis': {
                    'ovs_vswitchd': {'top_nodes': [], 'risks': []},
                    'ovsdb_server': {'top_nodes': [], 'risks': []}
                },
                'memory_analysis': {
                    'ovs_db': {'top_pods': [], 'risks': []},
                    'ovs_vswitchd': {'top_pods': [], 'risks': []}
                },
                'flow_analysis': {
                    'dp_flows': {'top_instances': [], 'risks': []},
                    'bridge_flows': {
                        'br_int': {'top_instances': [], 'risks': []},
                        'br_ex': {'top_instances': [], 'risks': []}
                    }
                },
                'connection_metrics': {}
            }
            
            # Analyze CPU usage
            cpu_data = ovs_data.get('cpu_usage', {})
            if 'error' not in cpu_data:
                # OVS vswitchd CPU
                for node in cpu_data.get('ovs_vswitchd_cpu', []):
                    cpu_value = node.get('max', node.get('avg', 0))
                    risk = self._assess_risk_level(cpu_value, self.risk_thresholds['cpu_high'], self.risk_thresholds['cpu_critical'])
                    
                    node_analysis = {
                        'node_name': node['node_name'],
                        'cpu_percent': cpu_value,
                        'risk_level': risk
                    }
                    
                    analysis['cpu_analysis']['ovs_vswitchd']['top_nodes'].append(node_analysis)
                    
                    if risk != 'normal':
                        analysis['cpu_analysis']['ovs_vswitchd']['risks'].append(node_analysis)
                
                # OVSDB server CPU
                for node in cpu_data.get('ovsdb_server_cpu', []):
                    cpu_value = node.get('max', node.get('avg', 0))
                    risk = self._assess_risk_level(cpu_value, self.risk_thresholds['cpu_high'], self.risk_thresholds['cpu_critical'])
                    
                    node_analysis = {
                        'node_name': node['node_name'],
                        'cpu_percent': cpu_value,
                        'risk_level': risk
                    }
                    
                    analysis['cpu_analysis']['ovsdb_server']['top_nodes'].append(node_analysis)
                    
                    if risk != 'normal':
                        analysis['cpu_analysis']['ovsdb_server']['risks'].append(node_analysis)
            
            # Analyze memory usage
            memory_data = ovs_data.get('memory_usage', {})
            if 'error' not in memory_data:
                # OVS DB memory
                for pod in memory_data.get('ovs_db_memory', []):
                    memory_value = pod.get('max', pod.get('avg', 0))
                    
                    # Convert to MB based on unit
                    if pod.get('unit') == 'GB':
                        memory_mb = memory_value * 1024
                    elif pod.get('unit') == 'MB':
                        memory_mb = memory_value
                    elif pod.get('unit') == 'KB':
                        memory_mb = memory_value / 1024
                    else:  # bytes
                        memory_mb = memory_value / (1024 * 1024)
                    
                    risk = self._assess_risk_level(memory_mb, self.risk_thresholds['memory_high_mb'], self.risk_thresholds['memory_critical_mb'])
                    
                    pod_analysis = {
                        'pod_name': pod['pod_name'],
                        'memory_mb': round(memory_mb, 2),
                        'memory_display': f"{memory_value} {pod.get('unit', 'bytes')}",
                        'risk_level': risk
                    }
                    
                    analysis['memory_analysis']['ovs_db']['top_pods'].append(pod_analysis)
                    
                    if risk != 'normal':
                        analysis['memory_analysis']['ovs_db']['risks'].append(pod_analysis)
                
                # OVS vswitchd memory
                for pod in memory_data.get('ovs_vswitchd_memory', []):
                    memory_value = pod.get('max', pod.get('avg', 0))
                    
                    # Convert to MB
                    if pod.get('unit') == 'GB':
                        memory_mb = memory_value * 1024
                    elif pod.get('unit') == 'MB':
                        memory_mb = memory_value
                    elif pod.get('unit') == 'KB':
                        memory_mb = memory_value / 1024
                    else:
                        memory_mb = memory_value / (1024 * 1024)
                    
                    risk = self._assess_risk_level(memory_mb, self.risk_thresholds['memory_high_mb'], self.risk_thresholds['memory_critical_mb'])
                    
                    pod_analysis = {
                        'pod_name': pod['pod_name'],
                        'memory_mb': round(memory_mb, 2),
                        'memory_display': f"{memory_value} {pod.get('unit', 'bytes')}",
                        'risk_level': risk
                    }
                    
                    analysis['memory_analysis']['ovs_vswitchd']['top_pods'].append(pod_analysis)
                    
                    if risk != 'normal':
                        analysis['memory_analysis']['ovs_vswitchd']['risks'].append(pod_analysis)
            
            # Analyze flow metrics
            dp_flows = ovs_data.get('dp_flows', {})
            if 'error' not in dp_flows:
                for instance in dp_flows.get('data', []):
                    flows_value = instance.get('max', instance.get('avg', 0))
                    risk = self._assess_risk_level(flows_value, self.risk_thresholds['flows_high'], self.risk_thresholds['flows_critical'])
                    
                    instance_analysis = {
                        'instance': instance['instance'],
                        'flow_count': flows_value,
                        'risk_level': risk
                    }
                    
                    analysis['flow_analysis']['dp_flows']['top_instances'].append(instance_analysis)
                    
                    if risk != 'normal':
                        analysis['flow_analysis']['dp_flows']['risks'].append(instance_analysis)
            
            # Analyze bridge flows
            bridge_flows = ovs_data.get('bridge_flows', {})
            if 'error' not in bridge_flows:
                for bridge_type in ['br_int', 'br_ex']:
                    bridge_data = bridge_flows.get(f'{bridge_type}_flows', [])
                    for instance in bridge_data:
                        flows_value = instance.get('max', instance.get('avg', 0))
                        risk = self._assess_risk_level(flows_value, self.risk_thresholds['flows_high'], self.risk_thresholds['flows_critical'])
                        
                        instance_analysis = {
                            'instance': instance['instance'],
                            'bridge': instance['bridge'],
                            'flow_count': flows_value,
                            'risk_level': risk
                        }
                        
                        analysis['flow_analysis']['bridge_flows'][bridge_type]['top_instances'].append(instance_analysis)
                        
                        if risk != 'normal':
                            analysis['flow_analysis']['bridge_flows'][bridge_type]['risks'].append(instance_analysis)
            
            # Add connection metrics
            connection_data = ovs_data.get('connection_metrics', {})
            if 'error' not in connection_data:
                analysis['connection_metrics'] = connection_data.get('connection_metrics', {})
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing OVS performance: {e}")
            return {'error': f'OVS performance analysis failed: {str(e)}'}
    
    async def analyze_sync_performance(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Analyze OVN sync duration performance"""
        try:
            print("Analyzing OVN sync performance...")
            
            if duration:
                sync_data = await self.sync_collector.collect_sync_duration_seconds_metrics(duration)
            else:
                sync_data = await self.sync_collector.collect_instant_metrics()
            
            analysis = {
                'sync_metrics': {},
                'top_slow_syncs': [],
                'risks': [],
                'per_metric_analysis': {}
            }
            
            # Analyze top overall sync durations
            for sync in sync_data.get('top_10_overall', []):
                sync_value = sync.get('max_value', sync.get('value', 0))
                risk = self._assess_risk_level(sync_value, self.risk_thresholds['sync_duration_high'], self.risk_thresholds['sync_duration_critical'])
                
                sync_analysis = {
                    'pod_resource_name': sync.get('pod_resource_name', sync.get('pod_name', 'unknown')),
                    'pod_name': sync.get('pod_name', 'unknown'),
                    'node_name': sync.get('node_name', 'unknown'),
                    'metric_name': sync.get('metric_name', 'unknown'),
                    'sync_duration_seconds': sync_value,
                    'readable_duration': sync.get('readable_duration', {}),
                    'risk_level': risk
                }
                
                analysis['top_slow_syncs'].append(sync_analysis)
                
                if risk != 'normal':
                    analysis['risks'].append(sync_analysis)
            
            # Analyze per-metric performance
            for metric_name, metric_data in sync_data.get('top_10_per_metric', {}).items():
                if 'error' not in metric_data:
                    metric_analysis = {
                        'metric_name': metric_name,
                        'total_series': metric_data.get('series_count', metric_data.get('count', 0)),
                        'top_syncs': [],
                        'risks': []
                    }
                    
                    for sync in metric_data.get('top_10', []):
                        sync_value = sync.get('max_value', sync.get('value', 0))
                        risk = self._assess_risk_level(sync_value, self.risk_thresholds['sync_duration_high'], self.risk_thresholds['sync_duration_critical'])
                        
                        sync_analysis = {
                            'pod_resource_name': sync.get('pod_resource_name', sync.get('pod_name', 'unknown')),
                            'sync_duration_seconds': sync_value,
                            'readable_duration': sync.get('readable_duration', {}),
                            'risk_level': risk
                        }
                        
                        metric_analysis['top_syncs'].append(sync_analysis)
                        
                        if risk != 'normal':
                            metric_analysis['risks'].append(sync_analysis)
                    
                    analysis['per_metric_analysis'][metric_name] = metric_analysis
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing sync performance: {e}")
            return {'error': f'Sync performance analysis failed: {str(e)}'}
    
    async def analyze_database_sizes(self) -> Dict[str, Any]:
        """Analyze OVN database sizes"""
        try:
            print("Analyzing OVN database sizes...")
            
            # Collect database size metrics
            db_metrics = await self.basic_info_collector.collect_max_values()
            
            analysis = {
                'northbound_db': {},
                'southbound_db': {},
                'risks': []
            }
            
            for metric_name, metric_data in db_metrics.items():
                if 'error' not in metric_data:
                    max_value_bytes = metric_data.get('max_value', 0)
                    max_value_mb = self._convert_bytes_to_mb(max_value_bytes) if max_value_bytes else 0
                    
                    risk = self._assess_risk_level(max_value_mb, self.risk_thresholds['db_size_large_mb'], self.risk_thresholds['db_size_critical_mb'])
                    
                    db_analysis = {
                        'size_bytes': max_value_bytes,
                        'size_mb': round(max_value_mb, 2),
                        'labels': metric_data.get('labels', {}),
                        'risk_level': risk
                    }
                    
                    if 'northbound' in metric_name.lower():
                        analysis['northbound_db'] = db_analysis
                    elif 'southbound' in metric_name.lower():
                        analysis['southbound_db'] = db_analysis
                    
                    if risk != 'normal':
                        analysis['risks'].append({
                            'database': metric_name,
                            'size_mb': round(max_value_mb, 2),
                            'risk_level': risk
                        })
                else:
                    print(f"Warning: Error collecting {metric_name}: {metric_data.get('error', 'Unknown error')}")
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing database sizes: {e}")
            return {'error': f'Database size analysis failed: {str(e)}'}
    
    async def generate_comprehensive_analysis(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Generate comprehensive performance analysis"""
        try:
            print(f"Generating comprehensive OVN-Kubernetes analysis{' for duration: ' + duration if duration else ' (instant)'}...")
            
            # Collect all analyses concurrently
            tasks = [
                self.analyze_pod_performance(duration),
                self.analyze_ovs_performance(duration),
                self.analyze_sync_performance(duration),
                self.analyze_database_sizes()
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Assemble comprehensive report
            report = {
                'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
                'analysis_type': 'instant' if not duration else f'duration_{duration}',
                'cluster_info': self.auth_client.get_cluster_summary(),
                'pod_performance': results[0] if not isinstance(results[0], Exception) else {'error': str(results[0])},
                'ovs_performance': results[1] if not isinstance(results[1], Exception) else {'error': str(results[1])},
                'sync_performance': results[2] if not isinstance(results[2], Exception) else {'error': str(results[2])},
                'database_analysis': results[3] if not isinstance(results[3], Exception) else {'error': str(results[3])},
                'risk_summary': {
                    'critical_risks': [],
                    'high_risks': [],
                    'recommendations': []
                }
            }
            
            # Aggregate risks and generate recommendations
            self._aggregate_risks_and_recommendations(report)
            
            print("Comprehensive analysis completed")
            return report
            
        except Exception as e:
            logger.error(f"Error generating comprehensive analysis: {e}")
            return {
                'error': f'Comprehensive analysis failed: {str(e)}',
                'analysis_timestamp': datetime.now(timezone.utc).isoformat()
            }
    
    def _aggregate_risks_and_recommendations(self, report: Dict[str, Any]) -> None:
        """Aggregate risks from all analyses and generate recommendations"""
        critical_risks = []
        high_risks = []
        recommendations = []
        
        # Aggregate pod performance risks
        pod_perf = report.get('pod_performance', {})
        if 'error' not in pod_perf:
            for pod_type in ['ovn_pods', 'multus_pods']:
                for risk in pod_perf.get(pod_type, {}).get('risks', []):
                    risk_info = {
                        'category': f'{pod_type}_performance',
                        'pod_name': risk['pod_name'],
                        'cpu_risk': risk['cpu_risk'],
                        'memory_risk': risk['memory_risk'],
                        'cpu_value': risk['cpu_value'],
                        'memory_mb': risk['memory_mb']
                    }
                    
                    if risk['cpu_risk'] == 'critical' or risk['memory_risk'] == 'critical':
                        critical_risks.append(risk_info)
                    else:
                        high_risks.append(risk_info)
        
        # Aggregate OVS performance risks
        ovs_perf = report.get('ovs_performance', {})
        if 'error' not in ovs_perf:
            # CPU risks
            for component in ['ovs_vswitchd', 'ovsdb_server']:
                for risk in ovs_perf.get('cpu_analysis', {}).get(component, {}).get('risks', []):
                    risk_info = {
                        'category': f'ovs_{component}_cpu',
                        'node_name': risk['node_name'],
                        'cpu_percent': risk['cpu_percent'],
                        'risk_level': risk['risk_level']
                    }
                    
                    if risk['risk_level'] == 'critical':
                        critical_risks.append(risk_info)
                    else:
                        high_risks.append(risk_info)
            
            # Memory risks
            for component in ['ovs_db', 'ovs_vswitchd']:
                for risk in ovs_perf.get('memory_analysis', {}).get(component, {}).get('risks', []):
                    risk_info = {
                        'category': f'ovs_{component}_memory',
                        'pod_name': risk['pod_name'],
                        'memory_mb': risk['memory_mb'],
                        'risk_level': risk['risk_level']
                    }
                    
                    if risk['risk_level'] == 'critical':
                        critical_risks.append(risk_info)
                    else:
                        high_risks.append(risk_info)
            
            # Flow risks
            for risk in ovs_perf.get('flow_analysis', {}).get('dp_flows', {}).get('risks', []):
                risk_info = {
                    'category': 'ovs_dp_flows',
                    'instance': risk['instance'],
                    'flow_count': risk['flow_count'],
                    'risk_level': risk