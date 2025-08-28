"""
OVN-Kubernetes Sync Duration Collector
Collects and analyzes sync duration metrics from OVN-Kubernetes components
"""

import os
import json
import yaml
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError
from ocauth.ovnk_benchmark_auth import auth


class OVNSyncDurationCollector:
    """Collector for OVN-Kubernetes sync duration metrics"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery):
        self.prometheus_client = prometheus_client
        self.auth = auth
        
        # Default metrics configuration
        self.default_metrics = [
            {
                'query': 'topk(10, ovnkube_controller_ready_duration_seconds)',
                'metricName': 'ovnkube_controller_ready_duration_seconds',
                'unit': 'seconds'
            },
            {
                'query': 'topk(10, ovnkube_node_ready_duration_seconds)',
                'metricName': 'ovnkube_node_ready_duration_seconds', 
                'unit': 'seconds'
            },
            {
                'query': 'ovnkube_controller_sync_duration_seconds{namespace="openshift-ovn-kubernetes"}',
                'metricName': 'ovnkube_controller_sync_duration_seconds',
                'unit': 'seconds'
            }
        ]
        
        self.metrics_config = []
        self._load_metrics_config()
    
    def _load_metrics_config(self) -> None:
        """Load metrics configuration from metrics.yaml"""
        metrics_file = Path('metrics.yaml')
        
        if metrics_file.exists():
            try:
                with open(metrics_file, 'r') as f:
                    config = yaml.safe_load(f)
                    self.metrics_config = config.get('metrics', [])
                    print(f"✅ Loaded {len(self.metrics_config)} metrics from metrics.yaml")
            except Exception as e:
                print(f"⚠️  Failed to load metrics.yaml: {e}")
                print("Using default metrics configuration")
                self.metrics_config = self.default_metrics
        else:
            print("📄 metrics.yaml not found, using default metrics configuration")
            self.metrics_config = self.default_metrics
    
    async def _get_node_info(self, pod_name: str) -> Dict[str, str]:
        """Get node information for a pod"""
        try:
            if not self.auth.kube_client:
                await self.auth.initialize()
            
            from kubernetes import client
            v1 = client.CoreV1Api(self.auth.kube_client)
            
            # Search for pod across all namespaces
            pods = v1.list_pod_for_all_namespaces()
            for pod in pods.items:
                if pod.metadata.name == pod_name or pod_name in pod.metadata.name:
                    return {
                        'pod_name': pod.metadata.name,
                        'node_name': pod.spec.node_name or 'unknown',
                        'namespace': pod.metadata.namespace
                    }
            
            return {'pod_name': pod_name, 'node_name': 'unknown', 'namespace': 'unknown'}
            
        except Exception as e:
            print(f"⚠️  Could not get node info for pod {pod_name}: {e}")
            return {'pod_name': pod_name, 'node_name': 'unknown', 'namespace': 'unknown'}
    
    def _extract_resource_info_from_labels(self, labels: Dict[str, str]) -> Dict[str, str]:
        """
        Extract resource information from metric labels
        
        Args:
            labels: Metric labels dictionary
            
        Returns:
            Dictionary with resource information
        """
        resource_info = {
            'resource_name': 'unknown',
            'resource_type': 'unknown',
            'resource_namespace': 'unknown'
        }
        
        # Common label patterns for ovnkube_controller_sync_duration_seconds
        if 'name' in labels:
            resource_info['resource_name'] = labels['name']
        elif 'resource_name' in labels:
            resource_info['resource_name'] = labels['resource_name']
        elif 'object_name' in labels:
            resource_info['resource_name'] = labels['object_name']
        
        if 'kind' in labels:
            resource_info['resource_type'] = labels['kind']
        elif 'resource_type' in labels:
            resource_info['resource_type'] = labels['resource_type']
        elif 'object_type' in labels:
            resource_info['resource_type'] = labels['object_type']
        
        if 'namespace' in labels:
            resource_info['resource_namespace'] = labels['namespace']
        elif 'object_namespace' in labels:
            resource_info['resource_namespace'] = labels['object_namespace']
        
        return resource_info
    
    def _format_pod_resource_name(self, pod_name: str, resource_info: Dict[str, str], metric_name: str) -> str:
        """
        Format pod name with resource information
        
        Args:
            pod_name: Pod name
            resource_info: Resource information from labels
            metric_name: Name of the metric
            
        Returns:
            Formatted string with pod:resource format
        """
        # For ovnkube_controller_sync_duration_seconds, include resource information
        if metric_name == 'ovnkube_controller_sync_duration_seconds':
            resource_name = resource_info.get('resource_name', 'unknown')
            resource_type = resource_info.get('resource_type', 'unknown')
            
            if resource_name != 'unknown' and resource_type != 'unknown':
                return f"{pod_name}:{resource_type}/{resource_name}"
            elif resource_name != 'unknown':
                return f"{pod_name}:{resource_name}"
            else:
                return f"{pod_name}:unknown_resource"
        
        # For other metrics, just return pod name
        return pod_name
    
    def _convert_duration_to_readable(self, seconds: float) -> Dict[str, Any]:
        """Convert duration in seconds to readable format with appropriate unit"""
        if seconds < 1:
            return {'value': round(seconds * 1000, 2), 'unit': 'ms'}
        elif seconds < 60:
            return {'value': round(seconds, 3), 'unit': 's'}
        elif seconds < 3600:
            return {'value': round(seconds / 60, 2), 'unit': 'min'}
        else:
            return {'value': round(seconds / 3600, 2), 'unit': 'h'}
    
    def _format_bytes(self, bytes_value: float) -> Dict[str, Any]:
        """Format bytes to appropriate unit (KB, MB, GB)"""
        if bytes_value < 1024:
            return {'value': round(bytes_value, 2), 'unit': 'B'}
        elif bytes_value < 1024**2:
            return {'value': round(bytes_value / 1024, 2), 'unit': 'KB'}
        elif bytes_value < 1024**3:
            return {'value': round(bytes_value / (1024**2), 2), 'unit': 'MB'}
        else:
            return {'value': round(bytes_value / (1024**3), 2), 'unit': 'GB'}
    
    async def collect_instant_metrics(self, time: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect instant sync duration metrics
        
        Args:
            time: Optional timestamp for instant query
            
        Returns:
            Summary information with top metrics per metric type
        """
        print("🔍 Collecting instant OVN sync duration metrics...")
        
        results = {
            'collection_type': 'instant',
            'timestamp': time or datetime.now(timezone.utc).isoformat(),
            'metrics': {},
            'top_10_per_metric': {},
            'top_10_overall': [],
            'summary': {}
        }
        
        try:
            # Execute all metric queries
            queries = {metric['metricName']: metric['query'] for metric in self.metrics_config}
            query_results = await self.prometheus_client.query_multiple_instant(queries, time)
            
            all_durations = []
            
            for metric_config in self.metrics_config:
                metric_name = metric_config['metricName']
                unit = metric_config.get('unit', 'seconds')
                
                if metric_name in query_results and 'error' not in query_results[metric_name]:
                    formatted_result = self.prometheus_client.format_query_result(query_results[metric_name])
                    
                    # Collect durations for this specific metric
                    metric_durations = []
                    
                    for item in formatted_result:
                        if item.get('value') is not None:
                            labels = item.get('labels', {})
                            
                            # Extract resource information
                            resource_info = self._extract_resource_info_from_labels(labels)
                            
                            duration_info = {
                                'metric_name': metric_name,
                                'value': item['value'],
                                'readable_duration': self._convert_duration_to_readable(item['value']),
                                'labels': labels,
                                'pod_name': 'unknown',
                                'node_name': 'unknown',
                                'resource_info': resource_info
                            }
                            
                            # Extract pod name from labels
                            if 'pod' in labels:
                                duration_info['pod_name'] = labels['pod']
                            elif 'instance' in labels:
                                # Try to extract pod name from instance label
                                instance = labels['instance']
                                if ':' in instance:
                                    duration_info['pod_name'] = instance.split(':')[0]
                                else:
                                    duration_info['pod_name'] = instance
                            
                            # Create formatted pod:resource name
                            duration_info['pod_resource_name'] = self._format_pod_resource_name(
                                duration_info['pod_name'], 
                                resource_info, 
                                metric_name
                            )
                            
                            metric_durations.append(duration_info)
                            all_durations.append(duration_info)
                    
                    # Sort and get top 10 for this metric
                    metric_durations.sort(key=lambda x: x['value'], reverse=True)
                    top_10_metric = metric_durations[:10]
                    
                    # Get node information for top 10 of this metric
                    for duration in top_10_metric:
                        if duration['pod_name'] != 'unknown':
                            node_info = await self._get_node_info(duration['pod_name'])
                            duration.update(node_info)
                    
                    results['metrics'][metric_name] = {
                        'unit': unit,
                        'data': formatted_result,
                        'count': len(formatted_result)
                    }
                    
                    results['top_10_per_metric'][metric_name] = {
                        'unit': unit,
                        'count': len(metric_durations),
                        'top_10': top_10_metric
                    }
                    
                else:
                    error_msg = query_results.get(metric_name, {}).get('error', 'Unknown error')
                    results['metrics'][metric_name] = {'error': error_msg}
                    results['top_10_per_metric'][metric_name] = {'error': error_msg}
                    print(f"⚠️  Error querying {metric_name}: {error_msg}")
            
            # Sort and get top 10 overall durations
            all_durations.sort(key=lambda x: x['value'], reverse=True)
            top_10_overall = all_durations[:10]
            
            # Get node information for top 10 overall (if not already fetched)
            for duration in top_10_overall:
                if duration['pod_name'] != 'unknown' and 'node_name' not in duration:
                    node_info = await self._get_node_info(duration['pod_name'])
                    duration.update(node_info)
            
            results['top_10_overall'] = top_10_overall
            
            # Generate summary
            if all_durations:
                max_duration = max(all_durations, key=lambda x: x['value'])
                avg_duration = sum(d['value'] for d in all_durations) / len(all_durations)
                
                # Generate per-metric summaries
                metric_summaries = {}
                for metric_name, metric_data in results['top_10_per_metric'].items():
                    if 'error' not in metric_data:
                        top_10 = metric_data['top_10']
                        if top_10:
                            max_metric_duration = max(top_10, key=lambda x: x['value'])
                            avg_metric_duration = sum(d['value'] for d in top_10) / len(top_10)
                            
                            metric_summaries[metric_name] = {
                                'total_count': metric_data['count'],
                                'max_duration': {
                                    'value': max_metric_duration['value'],
                                    'readable': self._convert_duration_to_readable(max_metric_duration['value']),
                                    'pod_resource': max_metric_duration['pod_resource_name']
                                },
                                'average_top_10_duration': {
                                    'value': avg_metric_duration,
                                    'readable': self._convert_duration_to_readable(avg_metric_duration)
                                }
                            }
                
                results['summary'] = {
                    'total_metrics': len(all_durations),
                    'overall_max_duration': {
                        'value': max_duration['value'],
                        'readable': self._convert_duration_to_readable(max_duration['value']),
                        'metric': max_duration['metric_name'],
                        'pod': max_duration['pod_name'],
                        'pod_resource': max_duration['pod_resource_name']
                    },
                    'overall_average_duration': {
                        'value': avg_duration,
                        'readable': self._convert_duration_to_readable(avg_duration)
                    },
                    'per_metric_summary': metric_summaries
                }
            
        except Exception as e:
            results['error'] = str(e)
            print(f"❌ Error collecting instant metrics: {e}")
        
        return results
    
    async def collect_sync_duration_seconds_metrics(self, duration: str, end_time: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect sync duration metrics over a time period
        
        Args:
            duration: Duration string (e.g., '5m', '1h', '1d')
            end_time: Optional end time, defaults to now
            
        Returns:
            Summary information with time series data and analysis per metric
        """
        print(f"🔍 Collecting OVN sync duration metrics for period: {duration}")
        
        start_time, end_time_actual = self.prometheus_client.get_time_range_from_duration(duration, end_time)
        
        results = {
            'collection_type': 'duration',
            'duration': duration,
            'start_time': start_time,
            'end_time': end_time_actual,
            'metrics': {},
            'top_10_per_metric': {},
            'top_10_overall': [],
            'time_series_summary': {},
            'summary': {}
        }
        
        try:
            # Execute all metric queries as range queries
            queries = {metric['metricName']: metric['query'] for metric in self.metrics_config}
            query_results = await self.prometheus_client.query_multiple_range(
                queries, start_time, end_time_actual, step='30s'
            )
            
            all_max_durations = []
            time_series_data = {}
            
            for metric_config in self.metrics_config:
                metric_name = metric_config['metricName']
                unit = metric_config.get('unit', 'seconds')
                
                if metric_name in query_results and 'error' not in query_results[metric_name]:
                    formatted_result = self.prometheus_client.format_query_result(query_results[metric_name])
                    
                    # Collect max durations for this specific metric
                    metric_max_durations = []
                    
                    # Process time series data
                    for series in formatted_result:
                        if 'values' in series and series['values']:
                            # Get max value from time series
                            max_value = max(v['value'] for v in series['values'] if v['value'] is not None)
                            
                            labels = series.get('labels', {})
                            resource_info = self._extract_resource_info_from_labels(labels)
                            
                            duration_info = {
                                'metric_name': metric_name,
                                'max_value': max_value,
                                'readable_duration': self._convert_duration_to_readable(max_value),
                                'pod_name': 'unknown',
                                'node_name': 'unknown',
                                'data_points': len(series['values']),
                                'resource_info': resource_info
                            }
                            
                            # Extract pod name from labels
                            if 'pod' in labels:
                                duration_info['pod_name'] = labels['pod']
                            elif 'instance' in labels:
                                instance = labels['instance']
                                if ':' in instance:
                                    duration_info['pod_name'] = instance.split(':')[0]
                                else:
                                    duration_info['pod_name'] = instance
                            
                            # Create formatted pod:resource name
                            duration_info['pod_resource_name'] = self._format_pod_resource_name(
                                duration_info['pod_name'], 
                                resource_info, 
                                metric_name
                            )
                            
                            metric_max_durations.append(duration_info)
                            all_max_durations.append(duration_info)
                            
                            # Store time series summary
                            if metric_name not in time_series_data:
                                time_series_data[metric_name] = []
                            
                            time_series_data[metric_name].append({
                                'pod_resource_name': duration_info['pod_resource_name'],
                                'pod_name': duration_info['pod_name'],
                                'max_value': max_value,
                                'avg_value': sum(v['value'] for v in series['values'] if v['value'] is not None) / len([v for v in series['values'] if v['value'] is not None]),
                                'data_points': len(series['values']),
                                'resource_info': resource_info
                            })
                    
                    # Sort and get top 10 for this metric
                    metric_max_durations.sort(key=lambda x: x['max_value'], reverse=True)
                    top_10_metric = metric_max_durations[:10]
                    
                    # Get node information for top 10 of this metric
                    for duration in top_10_metric:
                        if duration['pod_name'] != 'unknown':
                            node_info = await self._get_node_info(duration['pod_name'])
                            duration.update(node_info)
                    
                    results['metrics'][metric_name] = {
                        'unit': unit,
                        'series_count': len(formatted_result)
                    }
                    
                    results['top_10_per_metric'][metric_name] = {
                        'unit': unit,
                        'series_count': len(metric_max_durations),
                        'top_10': top_10_metric
                    }
                    
                else:
                    error_msg = query_results.get(metric_name, {}).get('error', 'Unknown error')
                    results['metrics'][metric_name] = {'error': error_msg}
                    results['top_10_per_metric'][metric_name] = {'error': error_msg}
                    print(f"⚠️  Error querying {metric_name}: {error_msg}")
            
            # Sort and get top 10 overall max durations
            all_max_durations.sort(key=lambda x: x['max_value'], reverse=True)
            top_10_overall = all_max_durations[:10]
            
            # Get node information for top 10 overall (if not already fetched)
            for duration in top_10_overall:
                if duration['pod_name'] != 'unknown' and 'node_name' not in duration:
                    node_info = await self._get_node_info(duration['pod_name'])
                    duration.update(node_info)
            
            results['top_10_overall'] = top_10_overall
            results['time_series_summary'] = time_series_data
            
            # Generate summary
            if all_max_durations:
                overall_max = max(all_max_durations, key=lambda x: x['max_value'])
                avg_max_duration = sum(d['max_value'] for d in all_max_durations) / len(all_max_durations)
                
                # Generate per-metric summaries
                metric_summaries = {}
                for metric_name, metric_data in results['top_10_per_metric'].items():
                    if 'error' not in metric_data:
                        top_10 = metric_data['top_10']
                        if top_10:
                            max_metric_duration = max(top_10, key=lambda x: x['max_value'])
                            avg_metric_max_duration = sum(d['max_value'] for d in top_10) / len(top_10)
                            
                            metric_summaries[metric_name] = {
                                'total_series': metric_data['series_count'],
                                'max_duration': {
                                    'value': max_metric_duration['max_value'],
                                    'readable': self._convert_duration_to_readable(max_metric_duration['max_value']),
                                    'pod_resource': max_metric_duration['pod_resource_name']
                                },
                                'average_top_10_max_duration': {
                                    'value': avg_metric_max_duration,
                                    'readable': self._convert_duration_to_readable(avg_metric_max_duration)
                                }
                            }
                
                results['summary'] = {
                    'total_series': len(all_max_durations),
                    'overall_max_duration': {
                        'value': overall_max['max_value'],
                        'readable': self._convert_duration_to_readable(overall_max['max_value']),
                        'metric': overall_max['metric_name'],
                        'pod': overall_max['pod_name'],
                        'pod_resource': overall_max['pod_resource_name']
                    },
                    'overall_average_max_duration': {
                        'value': avg_max_duration,
                        'readable': self._convert_duration_to_readable(avg_max_duration)
                    },
                    'per_metric_summary': metric_summaries
                }
            
        except Exception as e:
            results['error'] = str(e)
            print(f"❌ Error collecting duration metrics: {e}")
        
        return results
    
    def save_results(self, results: Dict[str, Any], filename: Optional[str] = None) -> str:
        """
        Save collection results to JSON file
        
        Args:
            results: Collection results
            filename: Optional filename, auto-generated if not provided
            
        Returns:
            Path to saved file
        """
        if not filename:
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
            collection_type = results.get('collection_type', 'unknown')
            filename = f"ovnk_sync_duration_{collection_type}_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            print(f"💾 Results saved to: {filename}")
            return filename
            
        except Exception as e:
            print(f"❌ Error saving results: {e}")
            return ""


async def main():
    """Example usage of OVNSyncDurationCollector"""
    # Initialize authentication
    await auth.initialize()
    
    # Create Prometheus client
    prometheus_client = PrometheusBaseQuery(
        prometheus_url=auth.prometheus_url,
        token=auth.prometheus_token
    )
    
    # Create collector
    collector = OVNSyncDurationCollector(prometheus_client)
    
    try:
        # Test instant collection
        print("=" * 50)
        print("Testing instant metrics collection...")
        instant_results = await collector.collect_instant_metrics()
        instant_file = collector.save_results(instant_results)
        
        print(f"\nTop 3 sync durations by metric:")
        top_10_per_metric = instant_results.get('top_10_per_metric', {})
        for metric_name, metric_data in top_10_per_metric.items():
            if 'error' not in metric_data:
                print(f"\n📊 {metric_name}:")
                for i, duration in enumerate(metric_data['top_10'][:3], 1):
                    readable = duration.get('readable_duration', {})
                    pod_resource = duration.get('pod_resource_name', duration.get('pod_name', 'unknown'))
                    print(f"  {i}. {pod_resource} on {duration['node_name']}: {readable.get('value')} {readable.get('unit')}")
            else:
                print(f"\n❌ {metric_name}: {metric_data['error']}")
        
        print(f"\nTop 3 overall sync durations:")
        for i, duration in enumerate(instant_results.get('top_10_overall', [])[:3], 1):
            readable = duration.get('readable_duration', {})
            pod_resource = duration.get('pod_resource_name', duration.get('pod_name', 'unknown'))
            print(f"{i}. {pod_resource} on {duration['node_name']}: {readable.get('value')} {readable.get('unit')} ({duration['metric_name']})")
        
        # Test duration collection
        print("\n" + "=" * 50)
        print("Testing duration metrics collection (5m)...")
        duration_results = await collector.collect_sync_duration_seconds_metrics('5m')
        duration_file = collector.save_results(duration_results)
        
        print(f"\nTop 3 max sync durations in 5m period by metric:")
        top_10_per_metric = duration_results.get('top_10_per_metric', {})
        for metric_name, metric_data in top_10_per_metric.items():
            if 'error' not in metric_data:
                print(f"\n📊 {metric_name}:")
                for i, duration in enumerate(metric_data['top_10'][:3], 1):
                    readable = duration.get('readable_duration', {})
                    pod_resource = duration.get('pod_resource_name', duration.get('pod_name', 'unknown'))
                    print(f"  {i}. {pod_resource} on {duration['node_name']}: {readable.get('value')} {readable.get('unit')}")
            else:
                print(f"\n❌ {metric_name}: {metric_data['error']}")
        
        print(f"\nTop 3 overall max sync durations in 5m period:")
        for i, duration in enumerate(duration_results.get('top_10_overall', [])[:3], 1):
            readable = duration.get('readable_duration', {})
            pod_resource = duration.get('pod_resource_name', duration.get('pod_name', 'unknown'))
            print(f"{i}. {pod_resource} on {duration['node_name']}: {readable.get('value')} {readable.get('unit')} ({duration['metric_name']})")
            pod_resource = duration.get('pod_resource_name', duration.get('pod_name', 'unknown'))
            print(f"{i}. {pod_resource} on {duration['node_name']}: {readable.get('value')} {readable.get('unit')}")
        
        
        # Test duration collection
        print("\n" + "=" * 50)
        print("Testing duration metrics collection (5m)...")
        duration_results = await collector.collect_sync_duration_seconds_metrics('5m')
        duration_file = collector.save_results(duration_results)
        
        print(f"\nTop 3 max sync durations in 5m period:")
        for i, duration in enumerate(duration_results.get('top_10_max_durations', [])[:3], 1):
            readable = duration.get('readable_duration', {})
            pod_resource = duration.get('pod_resource_name', duration.get('pod_name', 'unknown'))
            print(f"{i}. {pod_resource} on {duration['node_name']}: {readable.get('value')} {readable.get('unit')}")
        
    finally:
        await prometheus_client.close()

if __name__ == "__main__":
    asyncio.run(main())