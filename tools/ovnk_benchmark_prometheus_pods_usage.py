"""
Prometheus Pods Usage Collector Module
Collects CPU and memory usage statistics for pods with support for instant and duration queries
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError
from ocauth.ovnk_benchmark_auth import OpenShiftAuth
from kubernetes import client
from kubernetes.client.rest import ApiException


class PodsUsageCollector:
    """Collects and analyzes pod usage metrics from Prometheus"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery, auth: Optional[OpenShiftAuth] = None):
        self.prometheus_client = prometheus_client
        self.auth = auth
        self.node_info_cache: Dict[str, Dict[str, Any]] = {}
        
    def _get_default_queries(self, pod_pattern: str = ".*", container_pattern: str = ".*", namespace_pattern: str = ".*", interval: str = "1m") -> Dict[str, str]:
        """Get default PromQL queries for pod usage"""
        matchers = self._build_label_matchers(pod_pattern, container_pattern, namespace_pattern)
        return {
            'memory_usage': f'sum by(pod) (container_memory_rss{matchers})',
            'memory_working_set': f'sum by(pod) (container_memory_working_set_bytes{matchers})',
            'cpu_usage': f'sum by(pod) (rate(container_cpu_usage_seconds_total{matchers}[{interval}])) * 100',
            'memory_limit': f'sum by(pod) (container_spec_memory_limit_bytes{matchers})',
            'cpu_limit': f'sum by(pod) (container_spec_cpu_quota{matchers}) / sum by(pod) (container_spec_cpu_period{matchers}) * 100'
        }

    def _build_label_matchers(self, pod_pattern: str, container_pattern: str, namespace_pattern: str) -> str:
        """Safely build PromQL label matchers, skipping wildcards and escaping quotes."""
        def _needs_matcher(pattern: str) -> bool:
            return bool(pattern) and pattern != ".*"

        def _escape(value: str) -> str:
            return value.replace('"', '\\"')

        parts: List[str] = []
        if _needs_matcher(pod_pattern):
            parts.append(f'pod=~"{_escape(pod_pattern)}"')
        if _needs_matcher(container_pattern):
            parts.append(f'container=~"{_escape(container_pattern)}"')
        if _needs_matcher(namespace_pattern):
            parts.append(f'namespace=~"{_escape(namespace_pattern)}"')

        # Always exclude empty container and infra container
        parts.append('container!=""')
        parts.append('container!="POD"')

        return '{' + ', '.join(parts) + '}'
    
    def _get_ovn_queries(self, interval: str = "1m") -> Dict[str, str]:
        """Get OVN-specific PromQL queries"""
        return {
            'memory_ovnkube_node_pods': 'sum by(pod) (container_memory_rss{pod=~"ovnkube-node-.*", container=~"kube-rbac-proxy-node|kube-rbac-proxy-ovn-metrics|nbdb|northd|ovn-acl-logging|ovn-controller|ovnkube-controller|sbdb", container!="", container!="POD"})',
            'cpu_ovnkube_node_pods': f'sum by(pod) (rate(container_cpu_usage_seconds_total{{pod=~"ovnkube-node.*", namespace=~"openshift-ovn-kubernetes", container!="", container!="POD"}}[{interval}])) * 100'
        }
    
    async def _get_node_info(self, pod_name: str) -> Optional[Dict[str, Any]]:
        """Get node information for a pod"""
        if not self.auth or not self.auth.kube_client:
            return None
        
        try:
            v1 = client.CoreV1Api(self.auth.kube_client)
            
            # Try to find the pod across all namespaces
            for namespace in ['default', 'openshift-ovn-kubernetes', 'kube-system', 'openshift-monitoring']:
                try:
                    pod = v1.read_namespaced_pod(name=pod_name, namespace=namespace)
                    node_name = pod.spec.node_name
                    
                    if node_name and node_name not in self.node_info_cache:
                        # Get node information
                        try:
                            node = v1.read_node(name=node_name)
                            labels = node.metadata.labels or {}
                            # Derive roles from standard label patterns, e.g.,
                            #  - node-role.kubernetes.io/worker: ""
                            #  - node-role.kubernetes.io/master: ""
                            # Also support legacy kubernetes.io/role: "master|worker"
                            roles_set = set()
                            for label_key, label_value in labels.items():
                                if label_key.startswith('node-role.kubernetes.io/'):
                                    role_suffix = label_key.split('/', 1)[1]
                                    roles_set.add(role_suffix or (label_value or 'unknown'))
                                elif label_key == 'kubernetes.io/role' and label_value:
                                    roles_set.add(label_value)

                            # Prefer modern topology label, fallback to legacy failure-domain label
                            zone = labels.get('topology.kubernetes.io/zone', labels.get('failure-domain.beta.kubernetes.io/zone', 'unknown'))
                            # Prefer modern instance-type label, fallback to legacy beta label
                            instance_type = labels.get('node.kubernetes.io/instance-type', labels.get('beta.kubernetes.io/instance-type', 'unknown'))

                            self.node_info_cache[node_name] = {
                                'name': node_name,
                                'roles': sorted(list(roles_set)),
                                'zone': zone,
                                'instance_type': instance_type,
                                'os': node.status.node_info.os_image,
                                'kernel': node.status.node_info.kernel_version,
                                'kubelet_version': node.status.node_info.kubelet_version,
                                'allocatable_cpu': node.status.allocatable.get('cpu', 'unknown'),
                                'allocatable_memory': node.status.allocatable.get('memory', 'unknown')
                            }
                        except ApiException:
                            self.node_info_cache[node_name] = {'name': node_name, 'error': 'Could not fetch node details'}
                    
                    return {
                        'pod_name': pod_name,
                        'namespace': namespace,
                        'node_name': node_name,
                        'node_info': self.node_info_cache.get(node_name, {}),
                        'creation_timestamp': pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else None,
                        'labels': dict(pod.metadata.labels) if pod.metadata.labels else {},
                        'phase': pod.status.phase,
                        'restart_count': sum([container.restart_count for container in (pod.status.container_statuses or [])])
                    }
                except ApiException:
                    continue
            
            return None
            
        except Exception as e:
            print(f"Warning: Could not get node info for pod {pod_name}: {e}")
            return None
    
    def _extract_pod_name_from_metric(self, metric: Dict[str, Any]) -> str:
        """Extract pod name from Prometheus metric labels"""
        if 'metric' in metric and 'pod' in metric['metric']:
            return metric['metric']['pod']
        return 'unknown'
    
    def _extract_node_name_from_metric(self, metric: Dict[str, Any]) -> str:
        """Extract node name from Prometheus metric labels"""
        if 'metric' in metric and 'node' in metric['metric']:
            return metric['metric']['node']
        return 'unknown'
    
    def _calculate_stats(self, values: List[float]) -> Dict[str, float]:
        """Calculate min, max, avg from a list of values"""
        if not values:
            return {'min': 0.0, 'max': 0.0, 'avg': 0.0}
        
        filtered_values = [v for v in values if v is not None and v >= 0]
        if not filtered_values:
            return {'min': 0.0, 'max': 0.0, 'avg': 0.0}
        
        return {
            'min': min(filtered_values),
            'max': max(filtered_values),
            'avg': sum(filtered_values) / len(filtered_values)
        }
    
    def _format_memory_bytes(self, bytes_value: float) -> Tuple[float, str]:
        """Format memory bytes to appropriate unit"""
        if bytes_value >= 1024**3:  # GB
            return round(bytes_value / (1024**3), 2), 'GB'
        elif bytes_value >= 1024**2:  # MB
            return round(bytes_value / (1024**2), 2), 'MB'
        elif bytes_value >= 1024:  # KB
            return round(bytes_value / 1024, 2), 'KB'
        else:
            return round(bytes_value, 2), 'B'
    
    async def collect_instant_usage(self, 
                                   pod_pattern: str = ".*", 
                                   container_pattern: str = ".*",
                                   namespace_pattern: str = ".*",
                                   custom_queries: Optional[Dict[str, str]] = None,
                                   use_ovn_queries: bool = False,
                                   time: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect instant pod usage metrics
        
        Args:
            pod_pattern: Regular expression pattern for pod names
            container_pattern: Regular expression pattern for container names  
            namespace_pattern: Regular expression pattern for namespace names
            custom_queries: Custom PromQL queries dictionary
            use_ovn_queries: Use predefined OVN queries
            time: Optional specific timestamp
            
        Returns:
            Dictionary containing usage summary and top 10 pods
        """
        try:
            # Determine queries to use
            if custom_queries:
                queries = custom_queries
            elif use_ovn_queries:
                queries = self._get_ovn_queries()
            else:
                queries = self._get_default_queries(pod_pattern, container_pattern, namespace_pattern)
            
            # Execute queries
            results = await self.prometheus_client.query_multiple_instant(queries, time)
            
            # Process results
            pod_usage = {}
            
            for query_name, result in results.items():
                if 'error' in result:
                    print(f"Warning: Query {query_name} failed: {result['error']}")
                    continue
                
                if 'result' not in result:
                    continue
                
                for metric in result['result']:
                    pod_name = self._extract_pod_name_from_metric(metric)
                    node_name = self._extract_node_name_from_metric(metric)
                    
                    if pod_name not in pod_usage:
                        pod_usage[pod_name] = {
                            'pod_name': pod_name,
                            'node_name': node_name,
                            'metrics': {}
                        }
                    
                    # Extract value
                    if 'value' in metric:
                        timestamp, value = metric['value']
                        try:
                            numeric_value = float(value) if value != 'NaN' else 0.0
                        except (ValueError, TypeError):
                            numeric_value = 0.0
                        
                        pod_usage[pod_name]['metrics'][query_name] = {
                            'value': numeric_value,
                            'timestamp': float(timestamp)
                        }
            
            # Get additional pod information
            for pod_name in pod_usage:
                node_info = await self._get_node_info(pod_name)
                if node_info:
                    pod_usage[pod_name]['pod_info'] = node_info
            
            # Create summary
            summary = self._create_usage_summary(pod_usage, is_instant=True)
            
            return summary
            
        except Exception as e:
            raise PrometheusQueryError(f"Failed to collect instant usage: {str(e)}")
    
    async def collect_duration_usage(self,
                                   duration: str,
                                   pod_pattern: str = ".*",
                                   container_pattern: str = ".*", 
                                   namespace_pattern: str = ".*",
                                   custom_queries: Optional[Dict[str, str]] = None,
                                   use_ovn_queries: bool = False,
                                   step: str = "15s",
                                   end_time: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect pod usage metrics over a duration
        
        Args:
            duration: Duration string (e.g., '5m', '1h', '1d')
            pod_pattern: Regular expression pattern for pod names
            container_pattern: Regular expression pattern for container names
            namespace_pattern: Regular expression pattern for namespace names  
            custom_queries: Custom PromQL queries dictionary
            use_ovn_queries: Use predefined OVN queries
            step: Query resolution step
            end_time: Optional end time
            
        Returns:
            Dictionary containing usage summary and top 10 pods
        """
        try:
            # Get time range
            start_time, actual_end_time = self.prometheus_client.get_time_range_from_duration(duration, end_time)
            
            # Determine queries to use with dynamic rate window
            if custom_queries:
                queries = custom_queries
            elif use_ovn_queries:
                rate_window = self._select_rate_window(duration)
                queries = self._get_ovn_queries(interval=rate_window)
            else:
                rate_window = self._select_rate_window(duration)
                queries = self._get_default_queries(pod_pattern, container_pattern, namespace_pattern, interval=rate_window)
            
            # Execute range queries
            results = await self.prometheus_client.query_multiple_range(queries, start_time, actual_end_time, step)
            
            # Process results
            pod_usage = {}
            
            for query_name, result in results.items():
                if 'error' in result:
                    print(f"Warning: Query {query_name} failed: {result['error']}")
                    continue
                
                if 'result' not in result:
                    continue
                
                for metric in result['result']:
                    pod_name = self._extract_pod_name_from_metric(metric)
                    node_name = self._extract_node_name_from_metric(metric)
                    
                    if pod_name not in pod_usage:
                        pod_usage[pod_name] = {
                            'pod_name': pod_name,
                            'node_name': node_name,
                            'metrics': {}
                        }
                    
                    # Extract time series values
                    if 'values' in metric:
                        values = []
                        timestamps = []
                        
                        for timestamp, value in metric['values']:
                            try:
                                numeric_value = float(value) if value != 'NaN' else None
                                if numeric_value is not None and numeric_value >= 0:
                                    values.append(numeric_value)
                                    timestamps.append(float(timestamp))
                            except (ValueError, TypeError):
                                continue
                        
                        if values:
                            stats = self._calculate_stats(values)
                            pod_usage[pod_name]['metrics'][query_name] = {
                                'min': stats['min'],
                                'max': stats['max'],
                                'avg': stats['avg'],
                                'data_points': len(values),
                                'start_time': min(timestamps) if timestamps else None,
                                'end_time': max(timestamps) if timestamps else None
                            }
            
            # Get additional pod information
            for pod_name in pod_usage:
                node_info = await self._get_node_info(pod_name)
                if node_info:
                    pod_usage[pod_name]['pod_info'] = node_info
            
            # Create summary
            summary = self._create_usage_summary(pod_usage, is_instant=False)
            summary['query_info'] = {
                'duration': duration,
                'start_time': start_time,
                'end_time': actual_end_time,
                'step': step
            }
            
            return summary
            
        except Exception as e:
            raise PrometheusQueryError(f"Failed to collect duration usage: {str(e)}")

    def _select_rate_window(self, duration: str) -> str:
        """Select a rate/irate window based on overall query duration to match Grafana-like smoothing."""
        try:
            td = self.prometheus_client.parse_duration(duration)
            total_seconds = td.total_seconds()
            if total_seconds <= 600:  # <= 10m
                return "30s"
            if total_seconds <= 3600:  # <= 1h
                return "1m"
            if total_seconds <= 21600:  # <= 6h
                return "2m"
            if total_seconds <= 43200:  # <= 12h
                return "5m"
            if total_seconds <= 86400:  # <= 24h
                return "10m"
            return "15m"
        except Exception:
            return "1m"
    
    def _create_usage_summary(self, pod_usage: Dict[str, Any], is_instant: bool) -> Dict[str, Any]:
        """Create usage summary from collected pod usage data"""
        
        # Calculate CPU and memory usage for ranking
        pod_rankings = []
        
        for pod_name, pod_data in pod_usage.items():
            metrics = pod_data.get('metrics', {})
            
            # Get CPU usage (try different metric names)
            cpu_usage = 0.0
            for cpu_metric in ['cpu_usage', 'cpu_ovnkube_node_pods', 'cpu-ovnkube-node-pods']:
                if cpu_metric in metrics:
                    if is_instant:
                        cpu_usage = metrics[cpu_metric].get('value', 0.0)
                    else:
                        cpu_usage = metrics[cpu_metric].get('avg', 0.0)
                    break
            
            # Get memory usage (try different metric names) 
            memory_usage = 0.0
            for mem_metric in ['memory_usage', 'memory_working_set', 'memory_ovnkube_node_pods', 'memory-ovnkube-node-pods']:
                if mem_metric in metrics:
                    if is_instant:
                        memory_usage = metrics[mem_metric].get('value', 0.0)
                    else:
                        memory_usage = metrics[mem_metric].get('avg', 0.0)
                    break
            
            # Calculate combined usage score for ranking (CPU% + Memory in MB)
            memory_mb = memory_usage / (1024 * 1024) if memory_usage > 0 else 0
            usage_score = cpu_usage + memory_mb * 0.01  # Weight memory less than CPU
            
            pod_rankings.append({
                'pod_name': pod_name,
                'node_name': pod_data.get('node_name', 'unknown'),
                'cpu_usage_percent': cpu_usage,
                'memory_usage_bytes': memory_usage,
                'usage_score': usage_score,
                'pod_data': pod_data
            })
        
        # Sort by usage score and get top 10
        pod_rankings.sort(key=lambda x: x['usage_score'], reverse=True)
        top_10_pods = pod_rankings[:10]
        
        # Create formatted summary
        summary = {
            'collection_timestamp': datetime.now(timezone.utc).isoformat(),
            'collection_type': 'instant' if is_instant else 'duration',
            'total_pods_analyzed': len(pod_usage),
            'top_10_pods': []
        }
        
        for rank, pod in enumerate(top_10_pods, 1):
            pod_data = pod['pod_data']
            pod_summary = {
                'rank': rank,
                'pod_name': pod['pod_name'],
                'node_name': pod['node_name'],
                'usage_metrics': {}
            }
            
            # Add pod info if available
            if 'pod_info' in pod_data:
                pod_summary['pod_info'] = pod_data['pod_info']
            
            # Format metrics
            for metric_name, metric_data in pod_data.get('metrics', {}).items():
                if 'cpu' in metric_name.lower():
                    if is_instant:
                        pod_summary['usage_metrics'][metric_name] = {
                            'value': round(metric_data.get('value', 0.0), 2),
                            'unit': '%',
                            'timestamp': metric_data.get('timestamp')
                        }
                    else:
                        pod_summary['usage_metrics'][metric_name] = {
                            'min': round(metric_data.get('min', 0.0), 2),
                            'avg': round(metric_data.get('avg', 0.0), 2),
                            'max': round(metric_data.get('max', 0.0), 2),
                            'unit': '%',
                            'data_points': metric_data.get('data_points', 0)
                        }
                        
                elif 'memory' in metric_name.lower():
                    if is_instant:
                        value = metric_data.get('value', 0.0)
                        formatted_value, unit = self._format_memory_bytes(value)
                        pod_summary['usage_metrics'][metric_name] = {
                            'value': formatted_value,
                            'unit': unit,
                            'timestamp': metric_data.get('timestamp')
                        }
                    else:
                        min_val, min_unit = self._format_memory_bytes(metric_data.get('min', 0.0))
                        avg_val, avg_unit = self._format_memory_bytes(metric_data.get('avg', 0.0))
                        max_val, max_unit = self._format_memory_bytes(metric_data.get('max', 0.0))
                        
                        pod_summary['usage_metrics'][metric_name] = {
                            'min': min_val,
                            'avg': avg_val, 
                            'max': max_val,
                            'unit': max_unit,  # Use max unit as representative
                            'data_points': metric_data.get('data_points', 0)
                        }
            
            summary['top_10_pods'].append(pod_summary)
        
        return summary
    
    def export_summary_json(self, summary: Dict[str, Any], output_file: str) -> None:
        """Export summary to JSON file"""
        try:
            with open(output_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            print(f"✅ Summary exported to {output_file}")
        except Exception as e:
            print(f"❌ Failed to export summary: {e}")


# Example usage functions
async def collect_ovn_instant_usage(prometheus_client: PrometheusBaseQuery, auth: Optional[OpenShiftAuth] = None) -> Dict[str, Any]:
    """Collect instant OVN pod usage"""
    collector = PodsUsageCollector(prometheus_client, auth)
    return await collector.collect_instant_usage(use_ovn_queries=True)


async def collect_ovn_duration_usage(prometheus_client: PrometheusBaseQuery, duration: str = "1h", auth: Optional[OpenShiftAuth] = None) -> Dict[str, Any]:
    """Collect OVN pod usage over duration"""
    collector = PodsUsageCollector(prometheus_client, auth)
    return await collector.collect_duration_usage(duration=duration, use_ovn_queries=True)


async def collect_custom_usage(prometheus_client: PrometheusBaseQuery, 
                             queries: Dict[str, str],
                             duration: Optional[str] = None,
                             auth: Optional[OpenShiftAuth] = None) -> Dict[str, Any]:
    """Collect usage with custom queries"""
    collector = PodsUsageCollector(prometheus_client, auth)
    
    if duration:
        return await collector.collect_duration_usage(duration=duration, custom_queries=queries)
    else:
        return await collector.collect_instant_usage(custom_queries=queries)