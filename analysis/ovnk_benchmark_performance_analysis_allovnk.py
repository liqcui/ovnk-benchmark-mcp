"""
OVN-Kubernetes Performance Analysis Module
Comprehensive analysis of OVNK metrics including pods, OVS, sync duration, and basic info
File: analysis/ovnk_benchmark_performance_analysis-allovnk.py
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from pathlib import Path
import statistics

# Import the collector modules
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError
from tools.ovnk_benchmark_prometheus_ovnk_basicinfo import ovnBasicInfoCollector, get_pod_phase_counts
from tools.ovnk_benchmark_prometheus_ovnk_ovs import OVSUsageCollector
from tools.ovnk_benchmark_prometheus_ovnk_sync import OVNSyncDurationCollector
from tools.ovnk_benchmark_prometheus_pods_usage import PodsUsageCollector
from ocauth.ovnk_benchmark_auth import OpenShiftAuth

logger = logging.getLogger(__name__)


class OVNKPerformanceAnalyzer:
    """Comprehensive OVN-Kubernetes performance analyzer"""
    
    def __init__(self, prometheus_url: str, token: Optional[str] = None, auth_client: Optional[OpenShiftAuth] = None):
        """
        Initialize the OVNK Performance Analyzer
        
        Args:
            prometheus_url: Prometheus server URL
            token: Optional authentication token
            auth_client: Optional OpenShift authentication client
        """
        self.prometheus_url = prometheus_url
        self.token = token
        self.auth_client = auth_client
        
        # Performance thresholds for risk assessment
        self.thresholds = {
            'cpu': {
                'warning': 70.0,  # 70% CPU
                'critical': 90.0  # 90% CPU
            },
            'memory': {
                'warning': 1024 * 1024 * 1024,  # 1GB in bytes
                'critical': 2 * 1024 * 1024 * 1024  # 2GB in bytes
            },
            'sync_duration': {
                'warning': 5.0,   # 5 seconds
                'critical': 10.0  # 10 seconds
            },
            'db_size': {
                'warning': 100 * 1024 * 1024,  # 100MB in bytes
                'critical': 500 * 1024 * 1024  # 500MB in bytes
            }
        }
        
        logger.info(f"Initialized OVNKPerformanceAnalyzer with URL={prometheus_url}")
    
    def _assess_risk_level(self, value: float, metric_type: str) -> str:
        """Assess risk level based on metric value and type"""
        if metric_type not in self.thresholds:
            return 'unknown'
        
        thresholds = self.thresholds[metric_type]
        
        if value >= thresholds['critical']:
            return 'critical'
        elif value >= thresholds['warning']:
            return 'warning'
        else:
            return 'normal'
    
    def _convert_bytes_to_readable(self, bytes_value: float) -> Tuple[float, str]:
        """Convert bytes to human-readable format"""
        if bytes_value >= 1024**3:  # GB
            return round(bytes_value / (1024**3), 2), 'GB'
        elif bytes_value >= 1024**2:  # MB
            return round(bytes_value / (1024**2), 2), 'MB'
        elif bytes_value >= 1024:  # KB
            return round(bytes_value / 1024, 2), 'KB'
        else:
            return round(bytes_value, 2), 'B'
    
    def _convert_duration_to_readable(self, seconds: float) -> Tuple[float, str]:
        """Convert duration in seconds to readable format"""
        if seconds < 1:
            return round(seconds * 1000, 2), 'ms'
        elif seconds < 60:
            return round(seconds, 3), 's'
        elif seconds < 3600:
            return round(seconds / 60, 2), 'min'
        else:
            return round(seconds / 3600, 2), 'h'
    
    async def collect_basic_info(self) -> Dict[str, Any]:
        """Collect basic OVN database information and pod status"""
        try:
            logger.info("Collecting basic OVN information...")
            
            # Initialize basic info collector
            basic_collector = ovnBasicInfoCollector(self.prometheus_url, self.token)
            
            # Collect database sizes
            db_sizes = await basic_collector.collect_max_values()
            
            # Collect pod phase counts
            pod_phases = await get_pod_phase_counts(self.prometheus_url, self.token)
            
            return {
                'database_sizes': db_sizes,
                'pod_status': pod_phases,
                'collection_timestamp': datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error collecting basic info: {e}")
            return {'error': str(e)}
    
    async def collect_ovs_metrics(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Collect OVS component metrics"""
        try:
            logger.info(f"Collecting OVS metrics {f'for duration {duration}' if duration else 'instant'}...")
            
            async with PrometheusBaseQuery(self.prometheus_url, self.token) as prometheus_client:
                ovs_collector = OVSUsageCollector(prometheus_client, self.auth_client)
                return await ovs_collector.collect_all_ovs_metrics(duration)
                
        except Exception as e:
            logger.error(f"Error collecting OVS metrics: {e}")
            return {'error': str(e)}
    
    async def collect_sync_metrics(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Collect OVN sync duration metrics"""
        try:
            logger.info(f"Collecting sync metrics {f'for duration {duration}' if duration else 'instant'}...")
            
            async with PrometheusBaseQuery(self.prometheus_url, self.token) as prometheus_client:
                sync_collector = OVNSyncDurationCollector(prometheus_client)
                
                if duration:
                    return await sync_collector.collect_sync_duration_seconds_metrics(duration)
                else:
                    return await sync_collector.collect_instant_metrics()
                    
        except Exception as e:
            logger.error(f"Error collecting sync metrics: {e}")
            return {'error': str(e)}
    
    async def collect_pod_metrics(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """Collect OVNK pod usage metrics"""
        try:
            logger.info(f"Collecting pod metrics {f'for duration {duration}' if duration else 'instant'}...")
            
            async with PrometheusBaseQuery(self.prometheus_url, self.token) as prometheus_client:
                pod_collector = PodsUsageCollector(prometheus_client, self.auth_client)
                
                # Collect OVNK pods specifically
                ovnk_queries = {
                    'ovnkube_node_cpu': 'sum by(pod) (rate(container_cpu_usage_seconds_total{pod=~"ovnkube-node.*", namespace=~"openshift-ovn-kubernetes", container!="", container!="POD"}[1m])) * 100',
                    'ovnkube_node_memory': 'sum by(pod) (container_memory_rss{pod=~"ovnkube-node.*", namespace=~"openshift-ovn-kubernetes", container!="", container!="POD"})',
                    'ovnkube_controller_cpu': 'sum by(pod) (rate(container_cpu_usage_seconds_total{pod=~"ovnkube-master.*", namespace=~"openshift-ovn-kubernetes", container!="", container!="POD"}[1m])) * 100',
                    'ovnkube_controller_memory': 'sum by(pod) (container_memory_rss{pod=~"ovnkube-master.*", namespace=~"openshift-ovn-kubernetes", container!="", container!="POD"})',
                    'multus_cpu': 'sum by(pod) (rate(container_cpu_usage_seconds_total{pod=~"multus.*", container!="", container!="POD"}[1m])) * 100',
                    'multus_memory': 'sum by(pod) (container_memory_rss{pod=~"multus.*", container!="", container!="POD"})'
                }
                
                if duration:
                    return await pod_collector.collect_duration_usage(duration=duration, custom_queries=ovnk_queries)
                else:
                    return await pod_collector.collect_instant_usage(custom_queries=ovnk_queries)
                    
        except Exception as e:
            logger.error(f"Error collecting pod metrics: {e}")
            return {'error': str(e)}
    
    def _analyze_database_risks(self, db_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze database size risks"""
        risks = []
        
        for db_name, db_info in db_data.items():
            if 'error' not in db_info and db_info.get('max_value') is not None:
                max_size = db_info['max_value']
                risk_level = self._assess_risk_level(max_size, 'db_size')
                
                if risk_level != 'normal':
                    readable_size, unit = self._convert_bytes_to_readable(max_size)
                    risks.append({
                        'type': 'database_size',
                        'severity': risk_level,
                        'database': db_name,
                        'size_bytes': max_size,
                        'size_readable': f"{readable_size} {unit}",
                        'threshold_exceeded': self.thresholds['db_size'][risk_level == 'critical' and 'critical' or 'warning'],
                        'description': f"{db_name} database size ({readable_size} {unit}) exceeds {risk_level} threshold"
                    })
        
        return risks
    
    def _analyze_cpu_memory_risks(self, pod_data: List[Dict[str, Any]], component_type: str) -> List[Dict[str, Any]]:
        """Analyze CPU and memory usage risks for pods"""
        risks = []
        
        for pod in pod_data:
            pod_name = pod.get('pod_name', 'unknown')
            metrics = pod.get('usage_metrics', {})
            
            # Analyze CPU metrics
            for metric_name, metric_data in metrics.items():
                if 'cpu' in metric_name.lower() and 'error' not in metric_data:
                    # Use max value for risk assessment
                    cpu_value = metric_data.get('max', metric_data.get('value', 0))
                    risk_level = self._assess_risk_level(cpu_value, 'cpu')
                    
                    if risk_level != 'normal':
                        risks.append({
                            'type': 'cpu_usage',
                            'severity': risk_level,
                            'component_type': component_type,
                            'pod_name': pod_name,
                            'node_name': pod.get('node_name', 'unknown'),
                            'metric_name': metric_name,
                            'cpu_percent': cpu_value,
                            'threshold_exceeded': self.thresholds['cpu'][risk_level == 'critical' and 'critical' or 'warning'],
                            'description': f"{component_type} pod {pod_name} CPU usage ({cpu_value:.2f}%) exceeds {risk_level} threshold"
                        })
                
                # Analyze memory metrics
                elif 'memory' in metric_name.lower() and 'error' not in metric_data:
                    # Convert memory value to bytes if needed
                    memory_value = metric_data.get('max', metric_data.get('value', 0))
                    
                    # Handle different memory units
                    unit = metric_data.get('unit', 'B')
                    if unit == 'GB':
                        memory_bytes = memory_value * 1024**3
                    elif unit == 'MB':
                        memory_bytes = memory_value * 1024**2
                    elif unit == 'KB':
                        memory_bytes = memory_value * 1024
                    else:
                        memory_bytes = memory_value
                    
                    risk_level = self._assess_risk_level(memory_bytes, 'memory')
                    
                    if risk_level != 'normal':
                        readable_memory, readable_unit = self._convert_bytes_to_readable(memory_bytes)
                        risks.append({
                            'type': 'memory_usage',
                            'severity': risk_level,
                            'component_type': component_type,
                            'pod_name': pod_name,
                            'node_name': pod.get('node_name', 'unknown'),
                            'metric_name': metric_name,
                            'memory_bytes': memory_bytes,
                            'memory_readable': f"{readable_memory} {readable_unit}",
                            'threshold_exceeded': self.thresholds['memory'][risk_level == 'critical' and 'critical' or 'warning'],
                            'description': f"{component_type} pod {pod_name} memory usage ({readable_memory} {readable_unit}) exceeds {risk_level} threshold"
                        })
        
        return risks
    
    def _analyze_sync_duration_risks(self, sync_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze sync duration risks"""
        risks = []
        
        # Check top 10 overall sync durations
        top_10_overall = sync_data.get('top_10_overall', [])
        
        for sync_item in top_10_overall:
            duration_value = sync_item.get('max_value', sync_item.get('value', 0))
            risk_level = self._assess_risk_level(duration_value, 'sync_duration')
            
            if risk_level != 'normal':
                readable_duration, unit = self._convert_duration_to_readable(duration_value)
                pod_resource = sync_item.get('pod_resource_name', sync_item.get('pod_name', 'unknown'))
                
                risks.append({
                    'type': 'sync_duration',
                    'severity': risk_level,
                    'component_type': 'ovnkube',
                    'pod_name': sync_item.get('pod_name', 'unknown'),
                    'node_name': sync_item.get('node_name', 'unknown'),
                    'resource_name': pod_resource,
                    'metric_name': sync_item.get('metric_name', 'unknown'),
                    'duration_seconds': duration_value,
                    'duration_readable': f"{readable_duration} {unit}",
                    'threshold_exceeded': self.thresholds['sync_duration'][risk_level == 'critical' and 'critical' or 'warning'],
                    'description': f"Sync duration for {pod_resource} ({readable_duration} {unit}) exceeds {risk_level} threshold"
                })
        
        return risks
    
    def _calculate_component_summary(self, pod_data: List[Dict[str, Any]], component_name: str) -> Dict[str, Any]:
        """Calculate summary statistics for a component"""
        cpu_values = []
        memory_values = []
        pod_count = len(pod_data)
        
        for pod in pod_data:
            metrics = pod.get('usage_metrics', {})
            
            # Extract CPU values
            for metric_name, metric_data in metrics.items():
                if 'cpu' in metric_name.lower() and 'error' not in metric_data:
                    cpu_value = metric_data.get('max', metric_data.get('avg', metric_data.get('value', 0)))
                    if cpu_value > 0:
                        cpu_values.append(cpu_value)
                        break  # Take first CPU metric found
            
            # Extract memory values
            for metric_name, metric_data in metrics.items():
                if 'memory' in metric_name.lower() and 'error' not in metric_data:
                    memory_value = metric_data.get('max', metric_data.get('avg', metric_data.get('value', 0)))
                    
                    # Convert to bytes if needed
                    unit = metric_data.get('unit', 'B')
                    if unit == 'GB':
                        memory_bytes = memory_value * 1024**3
                    elif unit == 'MB':
                        memory_bytes = memory_value * 1024**2
                    elif unit == 'KB':
                        memory_bytes = memory_value * 1024
                    else:
                        memory_bytes = memory_value
                    
                    if memory_bytes > 0:
                        memory_values.append(memory_bytes)
                        break  # Take first memory metric found
        
        # Calculate statistics
        cpu_stats = {}
        memory_stats = {}
        
        if cpu_values:
            cpu_stats = {
                'min': round(min(cpu_values), 2),
                'avg': round(statistics.mean(cpu_values), 2),
                'max': round(max(cpu_values), 2),
                'unit': '%'
            }
        
        if memory_values:
            min_mem, min_unit = self._convert_bytes_to_readable(min(memory_values))
            avg_mem, avg_unit = self._convert_bytes_to_readable(statistics.mean(memory_values))
            max_mem, max_unit = self._convert_bytes_to_readable(max(memory_values))
            
            memory_stats = {
                'min': min_mem,
                'avg': avg_mem,
                'max': max_mem,
                'unit': max_unit,  # Use max unit as representative
                'avg_bytes': statistics.mean(memory_values),
                'max_bytes': max(memory_values)
            }
        
        return {
            'component_name': component_name,
            'pod_count': pod_count,
            'cpu_usage': cpu_stats,
            'memory_usage': memory_stats
        }
    
    async def analyze_comprehensive_performance(self, duration: Optional[str] = None) -> Dict[str, Any]:
        """
        Perform comprehensive performance analysis of all OVNK components
        
        Args:
            duration: Optional duration for range queries (e.g., '5m', '1h')
            
        Returns:
            Comprehensive analysis results in JSON format
        """
        analysis_start_time = datetime.now(timezone.utc)
        
        logger.info(f"Starting comprehensive OVNK performance analysis {f'for duration {duration}' if duration else 'instant'}...")
        
        try:
            # Collect all metrics concurrently
            tasks = [
                self.collect_basic_info(),
                self.collect_ovs_metrics(duration),
                self.collect_sync_metrics(duration),
                self.collect_pod_metrics(duration)
            ]
            
            basic_info, ovs_metrics, sync_metrics, pod_metrics = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Initialize analysis results
            analysis = {
                'analysis_metadata': {
                    'analysis_timestamp': analysis_start_time.isoformat(),
                    'analysis_type': 'instant' if not duration else f'duration_{duration}',
                    'duration': duration,
                    'prometheus_url': self.prometheus_url.replace(self.token or '', '***') if self.token else self.prometheus_url
                },
                'basic_info': basic_info if not isinstance(basic_info, Exception) else {'error': str(basic_info)},
                'ovs_metrics': ovs_metrics if not isinstance(ovs_metrics, Exception) else {'error': str(ovs_metrics)},
                'sync_metrics': sync_metrics if not isinstance(sync_metrics, Exception) else {'error': str(sync_metrics)},
                'pod_metrics': pod_metrics if not isinstance(pod_metrics, Exception) else {'error': str(pod_metrics)},
                'component_summaries': {},
                'risk_assessment': {
                    'total_risks': 0,
                    'critical_risks': 0,
                    'warning_risks': 0,
                    'risks_by_category': {},
                    'risks_by_component': {},
                    'detailed_risks': []
                },
                'performance_summary': {}
            }
            
            # Analyze component summaries
            component_summaries = {}
            all_risks = []
            
            # Analyze basic info risks
            if 'error' not in basic_info and 'database_sizes' in basic_info:
                db_risks = self._analyze_database_risks(basic_info['database_sizes'])
                all_risks.extend(db_risks)
                
                # Database size summary
                db_summary = {}
                for db_name, db_info in basic_info['database_sizes'].items():
                    if 'error' not in db_info and db_info.get('max_value') is not None:
                        size_readable, unit = self._convert_bytes_to_readable(db_info['max_value'])
                        db_summary[db_name] = {
                            'size_bytes': db_info['max_value'],
                            'size_readable': f"{size_readable} {unit}",
                            'risk_level': self._assess_risk_level(db_info['max_value'], 'db_size')
                        }
                
                component_summaries['database_sizes'] = db_summary
            
            # Analyze pod metrics
            if 'error' not in pod_metrics and 'top_10_pods' in pod_metrics:
                pods_by_component = {
                    'ovnkube_node': [],
                    'ovnkube_controller': [],
                    'multus': [],
                    'other': []
                }
                
                # Categorize pods
                for pod in pod_metrics['top_10_pods']:
                    pod_name = pod.get('pod_name', '')
                    if 'ovnkube-node' in pod_name:
                        pods_by_component['ovnkube_node'].append(pod)
                    elif 'ovnkube-master' in pod_name or 'ovnkube-controller' in pod_name:
                        pods_by_component['ovnkube_controller'].append(pod)
                    elif 'multus' in pod_name:
                        pods_by_component['multus'].append(pod)
                    else:
                        pods_by_component['other'].append(pod)
                
                # Generate component summaries and analyze risks
                for component_type, pods in pods_by_component.items():
                    if pods:
                        component_summary = self._calculate_component_summary(pods, component_type)
                        component_summaries[component_type] = component_summary
                        
                        # Analyze risks for this component
                        component_risks = self._analyze_cpu_memory_risks(pods, component_type)
                        all_risks.extend(component_risks)
            
            # Analyze OVS metrics risks
            if 'error' not in ovs_metrics:
                ovs_summary = {}
                
                # Process CPU usage
                if 'cpu_usage' in ovs_metrics and 'error' not in ovs_metrics['cpu_usage']:
                    cpu_data = ovs_metrics['cpu_usage']
                    
                    ovs_vswitchd_cpu = cpu_data.get('ovs_vswitchd_cpu', [])
                    ovsdb_server_cpu = cpu_data.get('ovsdb_server_cpu', [])
                    
                    if ovs_vswitchd_cpu:
                        ovs_summary['ovs_vswitchd'] = self._calculate_component_summary(
                            [{'pod_name': item['node_name'], 'usage_metrics': {'cpu': item}} for item in ovs_vswitchd_cpu],
                            'ovs_vswitchd'
                        )
                    
                    if ovsdb_server_cpu:
                        ovs_summary['ovsdb_server'] = self._calculate_component_summary(
                            [{'pod_name': item['node_name'], 'usage_metrics': {'cpu': item}} for item in ovsdb_server_cpu],
                            'ovsdb_server'
                        )
                
                component_summaries['ovs_components'] = ovs_summary
            
            # Analyze sync duration risks
            if 'error' not in sync_metrics:
                sync_risks = self._analyze_sync_duration_risks(sync_metrics)
                all_risks.extend(sync_risks)
            
            # Compile risk assessment
            analysis['risk_assessment']['detailed_risks'] = all_risks
            analysis['risk_assessment']['total_risks'] = len(all_risks)
            
            # Count risks by severity
            critical_count = len([r for r in all_risks if r['severity'] == 'critical'])
            warning_count = len([r for r in all_risks if r['severity'] == 'warning'])
            
            analysis['risk_assessment']['critical_risks'] = critical_count
            analysis['risk_assessment']['warning_risks'] = warning_count
            
            # Group risks by category and component
            risks_by_category = {}
            risks_by_component = {}
            
            for risk in all_risks:
                # By category
                category = risk['type']
                if category not in risks_by_category:
                    risks_by_category[category] = {'critical': 0, 'warning': 0}
                risks_by_category[category][risk['severity']] += 1
                
                # By component
                component = risk.get('component_type', risk.get('database', 'unknown'))
                if component not in risks_by_component:
                    risks_by_component[component] = {'critical': 0, 'warning': 0}
                risks_by_component[component][risk['severity']] += 1
            
            analysis['risk_assessment']['risks_by_category'] = risks_by_category
            analysis['risk_assessment']['risks_by_component'] = risks_by_component
            
            # Store component summaries
            analysis['component_summaries'] = component_summaries
            
            # Generate performance summary
            analysis['performance_summary'] = {
                'overall_health': 'critical' if critical_count > 0 else ('warning' if warning_count > 0 else 'normal'),
                'total_components_analyzed': len(component_summaries),
                'analysis_duration_seconds': (datetime.now(timezone.utc) - analysis_start_time).total_seconds(),
                'key_findings': self._generate_key_findings(all_risks, component_summaries),
                'recommendations': self._generate_recommendations(all_risks)
            }
            
            logger.info(f"Analysis completed. Found {len(all_risks)} risks ({critical_count} critical, {warning_count} warning)")
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error in comprehensive analysis: {e}")
            return {
                'analysis_metadata': {
                    'analysis_timestamp': analysis_start_time.isoformat(),
                    'error': str(e)
                },
                'error': str(e)
            }
    
    def _generate_key_findings(self, risks: List[Dict[str, Any]], component_summaries: Dict[str, Any]) -> List[str]:
        """Generate key findings from analysis"""
        findings = []
        
        # Database findings
        if 'database_sizes' in component_summaries:
            for db_name, db_info in component_summaries['database_sizes'].items():
                if db_info.get('risk_level') != 'normal':
                    findings.append(f"{db_name} database size ({db_info['size_readable']}) requires attention")
        
        # Component resource findings
        high_cpu_components = []
        high_memory_components = []
        
        for component_name, component_data in component_summaries.items():
            if component_name == 'database_sizes':
                continue
                
            if isinstance(component_data, dict):
                # Handle nested component data (like ovs_components)
                if 'cpu_usage' in component_data:
                    cpu_max = component_data['cpu_usage'].get('max', 0)
                    if cpu_max > self.thresholds['cpu']['warning']:
                        high_cpu_components.append(f"{component_name} ({cpu_max:.1f}%)")
                
                if 'memory_usage' in component_data:
                    memory_max_bytes = component_data['memory_usage'].get('max_bytes', 0)
                    if memory_max_bytes > self.thresholds['memory']['warning']:
                        memory_readable, unit = self._convert_bytes_to_readable(memory_max_bytes)
                        high_memory_components.append(f"{component_name} ({memory_readable} {unit})")
                
                # Handle nested structures
                for sub_component_name, sub_component_data in component_data.items():
                    if isinstance(sub_component_data, dict):
                        if 'cpu_usage' in sub_component_data:
                            cpu_max = sub_component_data['cpu_usage'].get('max', 0)
                            if cpu_max > self.thresholds['cpu']['warning']:
                                high_cpu_components.append(f"{component_name}.{sub_component_name} ({cpu_max:.1f}%)")
                        
                        if 'memory_usage' in sub_component_data:
                            memory_max_bytes = sub_component_data['memory_usage'].get('max_bytes', 0)
                            if memory_max_bytes > self.thresholds['memory']['warning']:
                                memory_readable, unit = self._convert_bytes_to_readable(memory_max_bytes)
                                high_memory_components.append(f"{component_name}.{sub_component_name} ({memory_readable} {unit})")
        
        if high_cpu_components:
            findings.append(f"High CPU usage detected in: {', '.join(high_cpu_components)}")
        
        if high_memory_components:
            findings.append(f"High memory usage detected in: {', '.join(high_memory_components)}")
        
        # Sync duration findings
        critical_sync_risks = [r for r in risks if r['type'] == 'sync_duration' and r['severity'] == 'critical']
        if critical_sync_risks:
            findings.append(f"Critical sync duration delays detected in {len(critical_sync_risks)} resources")
        
        return findings if findings else ["No significant performance issues detected"]
    
    def _generate_recommendations(self, risks: List[Dict[str, Any]]) -> List[str]:
        """Generate recommendations based on identified risks"""
        recommendations = []
        
        # Database size recommendations
        db_risks = [r for r in risks if r['type'] == 'database_size']
        if db_risks:
            recommendations.append("Consider database cleanup or increasing storage capacity for large OVN databases")
            recommendations.append("Monitor database growth trends and implement regular maintenance")
        
        # CPU usage recommendations
        cpu_risks = [r for r in risks if r['type'] == 'cpu_usage']
        if cpu_risks:
            critical_cpu = [r for r in cpu_risks if r['severity'] == 'critical']
            if critical_cpu:
                recommendations.append("Immediate action required: Scale or optimize high CPU usage pods")
            recommendations.append("Consider horizontal pod autoscaling for OVN components")
            recommendations.append("Review resource requests and limits for CPU-intensive pods")
        
        # Memory usage recommendations  
        memory_risks = [r for r in risks if r['type'] == 'memory_usage']
        if memory_risks:
            recommendations.append("Increase memory limits or optimize memory usage for high-consumption pods")
            recommendations.append("Monitor for memory leaks in long-running OVN processes")
        
        # Sync duration recommendations
        sync_risks = [r for r in risks if r['type'] == 'sync_duration']
        if sync_risks:
            recommendations.append("Investigate slow sync operations - may indicate network or API server issues")
            recommendations.append("Consider optimizing OVN database operations and reducing resource contention")
            recommendations.append("Check for large numbers of network policies or complex network configurations")
        
        if not recommendations:
            recommendations.append("Continue monitoring - current performance metrics are within acceptable ranges")
        
        return recommendations
    
    def save_analysis_to_file(self, analysis: Dict[str, Any], filename: Optional[str] = None) -> str:
        """Save analysis results to JSON file"""
        if not filename:
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
            analysis_type = analysis.get('analysis_metadata', {}).get('analysis_type', 'unknown')
            filename = f"ovnk_performance_analysis_{analysis_type}_{timestamp}.json"
        
        try:
            # Ensure directory exists
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
            
            with open(filename, 'w') as f:
                json.dump(analysis, f, indent=2, default=str)
            
            logger.info(f"Analysis saved to: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error saving analysis: {e}")
            return ""
    
    def generate_summary_report(self, analysis: Dict[str, Any]) -> str:
        """Generate a human-readable summary report"""
        try:
            metadata = analysis.get('analysis_metadata', {})
            risk_assessment = analysis.get('risk_assessment', {})
            performance_summary = analysis.get('performance_summary', {})
            
            report_lines = [
                "=" * 80,
                "OVN-KUBERNETES PERFORMANCE ANALYSIS REPORT",
                "=" * 80,
                f"Analysis Timestamp: {metadata.get('analysis_timestamp', 'Unknown')}",
                f"Analysis Type: {metadata.get('analysis_type', 'Unknown')}",
                f"Duration: {metadata.get('duration', 'Instant')}",
                "",
                "OVERALL HEALTH STATUS",
                "-" * 40,
                f"Overall Health: {performance_summary.get('overall_health', 'Unknown').upper()}",
                f"Total Risks Found: {risk_assessment.get('total_risks', 0)}",
                f"  - Critical: {risk_assessment.get('critical_risks', 0)}",
                f"  - Warning: {risk_assessment.get('warning_risks', 0)}",
                "",
            ]
            
            # Key findings
            key_findings = performance_summary.get('key_findings', [])
            if key_findings:
                report_lines.extend([
                    "KEY FINDINGS",
                    "-" * 40,
                ])
                for finding in key_findings:
                    report_lines.append(f"• {finding}")
                report_lines.append("")
            
            # Component summaries
            component_summaries = analysis.get('component_summaries', {})
            if component_summaries:
                report_lines.extend([
                    "COMPONENT PERFORMANCE SUMMARY",
                    "-" * 40,
                ])
                
                for component_name, component_data in component_summaries.items():
                    if component_name == 'database_sizes':
                        report_lines.append("Database Sizes:")
                        for db_name, db_info in component_data.items():
                            risk_indicator = ""
                            if db_info.get('risk_level') == 'critical':
                                risk_indicator = " [CRITICAL]"
                            elif db_info.get('risk_level') == 'warning':
                                risk_indicator = " [WARNING]"
                            report_lines.append(f"  {db_name}: {db_info.get('size_readable', 'Unknown')}{risk_indicator}")
                    
                    elif isinstance(component_data, dict) and 'cpu_usage' in component_data:
                        cpu_info = component_data.get('cpu_usage', {})
                        memory_info = component_data.get('memory_usage', {})
                        pod_count = component_data.get('pod_count', 0)
                        
                        report_lines.append(f"{component_name.replace('_', ' ').title()} ({pod_count} pods):")
                        if cpu_info:
                            report_lines.append(f"  CPU: {cpu_info.get('avg', 0):.1f}% avg, {cpu_info.get('max', 0):.1f}% max")
                        if memory_info:
                            report_lines.append(f"  Memory: {memory_info.get('avg', 0)} {memory_info.get('unit', 'B')} avg, {memory_info.get('max', 0)} {memory_info.get('unit', 'B')} max")
                    
                    elif component_name == 'ovs_components' and isinstance(component_data, dict):
                        report_lines.append("OVS Components:")
                        for ovs_component, ovs_data in component_data.items():
                            if isinstance(ovs_data, dict):
                                cpu_info = ovs_data.get('cpu_usage', {})
                                memory_info = ovs_data.get('memory_usage', {})
                                if cpu_info or memory_info:
                                    report_lines.append(f"  {ovs_component.replace('_', ' ').title()}:")
                                    if cpu_info:
                                        report_lines.append(f"    CPU: {cpu_info.get('avg', 0):.1f}% avg, {cpu_info.get('max', 0):.1f}% max")
                                    if memory_info:
                                        report_lines.append(f"    Memory: {memory_info.get('avg', 0)} {memory_info.get('unit', 'B')} avg, {memory_info.get('max', 0)} {memory_info.get('unit', 'B')} max")
                
                report_lines.append("")
            
            # Detailed risks
            detailed_risks = risk_assessment.get('detailed_risks', [])
            if detailed_risks:
                report_lines.extend([
                    "DETAILED RISK ANALYSIS",
                    "-" * 40,
                ])
                
                # Group by severity
                critical_risks = [r for r in detailed_risks if r['severity'] == 'critical']
                warning_risks = [r for r in detailed_risks if r['severity'] == 'warning']
                
                if critical_risks:
                    report_lines.append("CRITICAL RISKS:")
                    for risk in critical_risks:
                        report_lines.append(f"  • {risk.get('description', 'Unknown risk')}")
                    report_lines.append("")
                
                if warning_risks:
                    report_lines.append("WARNING RISKS:")
                    for risk in warning_risks:
                        report_lines.append(f"  • {risk.get('description', 'Unknown risk')}")
                    report_lines.append("")
            
            # Recommendations
            recommendations = performance_summary.get('recommendations', [])
            if recommendations:
                report_lines.extend([
                    "RECOMMENDATIONS",
                    "-" * 40,
                ])
                for rec in recommendations:
                    report_lines.append(f"• {rec}")
                report_lines.append("")
            
            # Footer
            report_lines.extend([
                "=" * 80,
                f"Report generated at {datetime.now(timezone.utc).isoformat()}",
                "=" * 80
            ])
            
            return "\n".join(report_lines)
            
        except Exception as e:
            logger.error(f"Error generating summary report: {e}")
            return f"Error generating summary report: {e}"


# Convenience functions for easy usage
async def analyze_ovnk_instant_performance(prometheus_url: str, 
                                         token: Optional[str] = None,
                                         auth_client: Optional[OpenShiftAuth] = None) -> Dict[str, Any]:
    """Analyze instant OVNK performance"""
    analyzer = OVNKPerformanceAnalyzer(prometheus_url, token, auth_client)
    return await analyzer.analyze_comprehensive_performance()


async def analyze_ovnk_duration_performance(prometheus_url: str,
                                          duration: str = "5m", 
                                          token: Optional[str] = None,
                                          auth_client: Optional[OpenShiftAuth] = None) -> Dict[str, Any]:
    """Analyze OVNK performance over duration"""
    analyzer = OVNKPerformanceAnalyzer(prometheus_url, token, auth_client)
    return await analyzer.analyze_comprehensive_performance(duration)


# Example usage and testing
async def main():
    """Main function for testing the performance analyzer"""
    try:
        # Initialize authentication
        auth = OpenShiftAuth()
        await auth.initialize()
        
        # Test Prometheus connection
        if not await auth.test_prometheus_connection():
            print("Cannot connect to Prometheus")
            return
        
        print("Starting OVNK Performance Analysis...")
        
        # Initialize analyzer
        analyzer = OVNKPerformanceAnalyzer(
            prometheus_url=auth.prometheus_url,
            token=auth.prometheus_token,
            auth_client=auth
        )
        
        # Perform instant analysis
        print("\n=== INSTANT PERFORMANCE ANALYSIS ===")
        instant_analysis = await analyzer.analyze_comprehensive_performance()
        
        # Save instant analysis
        instant_file = analyzer.save_analysis_to_file(instant_analysis)
        print(f"Instant analysis saved to: {instant_file}")
        
        # Generate and print summary report
        summary_report = analyzer.generate_summary_report(instant_analysis)
        print(summary_report)
        
        # Perform 5-minute duration analysis
        print("\n=== 5-MINUTE DURATION ANALYSIS ===")
        duration_analysis = await analyzer.analyze_comprehensive_performance(duration="5m")
        
        # Save duration analysis
        duration_file = analyzer.save_analysis_to_file(duration_analysis)
        print(f"Duration analysis saved to: {duration_file}")
        
        # Generate and print duration summary
        duration_summary = analyzer.generate_summary_report(duration_analysis)
        print(duration_summary)
        
        # Print key metrics for comparison
        print("\n=== KEY METRICS COMPARISON ===")
        
        # Compare risk counts
        instant_risks = instant_analysis.get('risk_assessment', {}).get('total_risks', 0)
        duration_risks = duration_analysis.get('risk_assessment', {}).get('total_risks', 0)
        
        print(f"Instant Analysis - Total Risks: {instant_risks}")
        print(f"Duration Analysis - Total Risks: {duration_risks}")
        
        # Compare overall health
        instant_health = instant_analysis.get('performance_summary', {}).get('overall_health', 'unknown')
        duration_health = duration_analysis.get('performance_summary', {}).get('overall_health', 'unknown')
        
        print(f"Instant Health Status: {instant_health.upper()}")
        print(f"Duration Health Status: {duration_health.upper()}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        print(f"Error in analysis: {e}")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    asyncio.run(main())