#!/usr/bin/env python3
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
from tools.ovnk_benchmark_prometheus_utility import mcpToolsUtility
from ocauth.ovnk_benchmark_auth import auth


class OVNSyncDurationCollector:
    """Collector for OVN-Kubernetes sync duration metrics"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery):
        self.prometheus_client = prometheus_client
        self.auth = auth
        self.utility = mcpToolsUtility(auth_client=auth)
        
        # Default metrics configuration
        self.default_metrics = [
            {
                'query': 'topk(10, ovnkube_controller_ready_duration_seconds)',
                'metricName': 'ovnkube_controller_ready_duration_seconds',
                'unit': 'seconds',
                'type': 'ready_duration'
            },
            {
                'query': 'topk(10, ovnkube_node_ready_duration_seconds)',
                'metricName': 'ovnkube_node_ready_duration_seconds', 
                'unit': 'seconds',
                'type': 'ready_duration'
            },
            {
                'query': 'topk(20, ovnkube_controller_sync_duration_seconds{namespace="openshift-ovn-kubernetes"})',
                'metricName': 'ovnkube_controller_sync_duration_seconds',
                'unit': 'seconds',
                'type': 'sync_duration'
            },
            {
                'query': 'topk(10, sum by(pod) (rate(ovnkube_controller_sync_service_total{namespace="openshift-ovn-kubernetes"}[1m])))',
                'metricName': 'ovnkube_controller_sync_service_rate',
                'unit': 'ops/sec',
                'type': 'service_rate'
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
                    print(f"Loaded {len(self.metrics_config)} metrics from metrics.yaml")
            except Exception as e:
                print(f"Failed to load metrics.yaml: {e}")
                print("Using default metrics configuration")
                self.metrics_config = self.default_metrics
        else:
            print("metrics.yaml not found, using default metrics configuration")
            self.metrics_config = self.default_metrics
    
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
    
    def _extract_resource_info_from_labels(self, labels: Dict[str, str]) -> Dict[str, str]:
        """Extract resource information from metric labels"""
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
        """Format pod name with resource information"""
        # For sync duration metrics, include resource information
        if 'sync_duration' in metric_name:
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
    
    async def _get_node_info_for_pod(self, pod_name: str) -> str:
        """Get node name for a pod using utility"""
        try:
            # Get pod-to-node mapping from multiple namespaces
            pod_info = self.utility.get_all_pods_info_across_namespaces([
                'openshift-ovn-kubernetes', 'openshift-multus', 'kube-system', 'default'
            ])
            
            # Direct lookup
            if pod_name in pod_info:
                return pod_info[pod_name].get('node_name', 'unknown')
            
            # Partial match lookup
            for full_pod_name, info in pod_info.items():
                if pod_name in full_pod_name or full_pod_name.startswith(pod_name):
                    return info.get('node_name', 'unknown')
            
            return 'unknown'
            
        except Exception as e:
            print(f"Could not get node info for pod {pod_name}: {e}")
            return 'unknown'
    
    async def collect_comprehensive_metrics(self, time: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect comprehensive OVN sync metrics including ready duration, sync duration, and service rates
        
        Args:
            time: Optional timestamp for instant query
            
        Returns:
            Comprehensive summary with separated metric types
        """
        print("Collecting comprehensive OVN sync metrics...")
        
        results = {
            'collection_timestamp': time or datetime.now(timezone.utc).isoformat(),
            'timezone': 'UTC',
            'controller_ready_duration': {},
            'node_ready_duration': {},
            'controller_sync_duration': {},
            'controller_service_rate': {},
            'overall_summary': {}
        }
        
        try:
            # Execute all metric queries
            queries = {metric['metricName']: metric['query'] for metric in self.metrics_config}
            query_results = await self.prometheus_client.query_multiple_instant(queries, time)
            
            # Process each metric type separately
            for metric_config in self.metrics_config:
                metric_name = metric_config['metricName']
                metric_type = metric_config.get('type', 'unknown')
                unit = metric_config.get('unit', 'seconds')
                
                if metric_name in query_results and 'error' not in query_results[metric_name]:
                    formatted_result = self.prometheus_client.format_query_result(query_results[metric_name])
                    
                    processed_data = []
                    
                    for item in formatted_result:
                        if item.get('value') is not None:
                            labels = item.get('labels', {})
                            
                            # Extract pod name
                            pod_name = 'unknown'
                            if 'pod' in labels:
                                pod_name = labels['pod']
                            elif 'instance' in labels:
                                instance = labels['instance']
                                if ':' in instance:
                                    pod_name = instance.split(':')[0]
                                else:
                                    pod_name = instance
                            
                            # Get node information
                            node_name = await self._get_node_info_for_pod(pod_name)
                            
                            # Extract resource information for sync duration metrics
                            resource_info = self._extract_resource_info_from_labels(labels)
                            pod_resource_name = self._format_pod_resource_name(pod_name, resource_info, metric_name)
                            
                            data_point = {
                                'pod_name': pod_name,
                                'node_name': node_name,
                                'value': item['value'],
                                'readable_value': self._convert_duration_to_readable(item['value']) if unit == 'seconds' else {'value': round(item['value'], 4), 'unit': unit}
                            }
                            
                            # Add resource info for sync duration metrics
                            if metric_type == 'sync_duration':
                                data_point['pod_resource_name'] = pod_resource_name
                                data_point['resource_info'] = resource_info
                            
                            processed_data.append(data_point)
                    
                    # Sort by value (descending)
                    processed_data.sort(key=lambda x: x['value'], reverse=True)
                    
                    # Store results based on metric type
                    if metric_type == 'ready_duration':
                        if 'controller' in metric_name:
                            results['controller_ready_duration'] = {
                                'metric_name': metric_name,
                                'unit': unit,
                                'count': len(processed_data),
                                'top_10': processed_data[:10]
                            }
                        elif 'node' in metric_name:
                            results['node_ready_duration'] = {
                                'metric_name': metric_name,
                                'unit': unit,
                                'count': len(processed_data),
                                'top_10': processed_data[:10]
                            }
                    elif metric_type == 'sync_duration':
                        results['controller_sync_duration'] = {
                            'metric_name': metric_name,
                            'unit': unit,
                            'count': len(processed_data),
                            'top_20': processed_data[:20]
                        }
                    elif metric_type == 'service_rate':
                        results['controller_service_rate'] = {
                            'metric_name': metric_name,
                            'unit': unit,
                            'count': len(processed_data),
                            'top_10': processed_data[:10]
                        }
                
                else:
                    error_msg = query_results.get(metric_name, {}).get('error', 'Unknown error')
                    print(f"Error querying {metric_name}: {error_msg}")
                    
                    # Store error based on metric type
                    if metric_type == 'ready_duration':
                        if 'controller' in metric_name:
                            results['controller_ready_duration'] = {'error': error_msg}
                        elif 'node' in metric_name:
                            results['node_ready_duration'] = {'error': error_msg}
                    elif metric_type == 'sync_duration':
                        results['controller_sync_duration'] = {'error': error_msg}
                    elif metric_type == 'service_rate':
                        results['controller_service_rate'] = {'error': error_msg}
            
            # Generate overall summary
            self._generate_overall_summary(results)
            
        except Exception as e:
            results['error'] = str(e)
            print(f"Error collecting comprehensive metrics: {e}")
        
        return results
    
    def _generate_overall_summary(self, results: Dict[str, Any]) -> None:
        """Generate overall summary from collected metrics"""
        summary = {
            'metrics_collected': 0,
            'total_data_points': 0,
            'max_values': {},
            'avg_values': {}
        }
        
        metric_sections = ['controller_ready_duration', 'node_ready_duration', 'controller_sync_duration', 'controller_service_rate']
        
        all_values = []
        
        for section in metric_sections:
            if section in results and 'error' not in results[section]:
                section_data = results[section]
                summary['metrics_collected'] += 1
                
                # Get data points
                data_key = 'top_20' if section == 'controller_sync_duration' else 'top_10'
                data_points = section_data.get(data_key, [])
                summary['total_data_points'] += len(data_points)
                
                if data_points:
                    # Find max value in this section
                    max_item = max(data_points, key=lambda x: x['value'])
                    avg_value = sum(item['value'] for item in data_points) / len(data_points)
                    
                    summary['max_values'][section] = {
                        'value': max_item['value'],
                        'readable': max_item['readable_value'],
                        'pod_name': max_item['pod_name'],
                        'node_name': max_item['node_name']
                    }
                    
                    # Add resource info for sync duration
                    if section == 'controller_sync_duration':
                        summary['max_values'][section]['pod_resource_name'] = max_item.get('pod_resource_name', max_item['pod_name'])
                    
                    summary['avg_values'][section] = {
                        'value': avg_value,
                        'readable': self._convert_duration_to_readable(avg_value) if section_data['unit'] == 'seconds' else {'value': round(avg_value, 4), 'unit': section_data['unit']}
                    }
                    
                    # Collect values for overall calculations
                    for item in data_points:
                        all_values.append({
                            'value': item['value'],
                            'section': section,
                            'pod_name': item['pod_name'],
                            'node_name': item['node_name'],
                            'readable': item['readable_value']
                        })
        
        # Overall statistics
        if all_values:
            overall_max = max(all_values, key=lambda x: x['value'])
            overall_avg = sum(item['value'] for item in all_values) / len(all_values)
            
            summary['overall_max'] = {
                'value': overall_max['value'],
                'readable': overall_max['readable'],
                'section': overall_max['section'],
                'pod_name': overall_max['pod_name'],
                'node_name': overall_max['node_name']
            }
            
            summary['overall_avg'] = {
                'value': overall_avg,
                'readable': self._convert_duration_to_readable(overall_avg)
            }
        
        results['overall_summary'] = summary
    
    async def collect_duration_metrics(self, duration: str, end_time: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect sync duration metrics over a time period
        
        Args:
            duration: Duration string (e.g., '5m', '1h', '1d')
            end_time: Optional end time, defaults to now
            
        Returns:
            Summary information with time series data and analysis per metric
        """
        print(f"Collecting OVN sync duration metrics for period: {duration}")
        
        start_time, end_time_actual = self.prometheus_client.get_time_range_from_duration(duration, end_time)
        
        results = {
            'collection_type': 'duration',
            'duration': duration,
            'start_time': start_time,
            'end_time': end_time_actual,
            'timezone': 'UTC',
            'controller_ready_duration': {},
            'node_ready_duration': {},
            'controller_sync_duration': {},
            'controller_service_rate': {},
            'overall_summary': {}
        }
        
        try:
            # Execute all metric queries as range queries
            queries = {metric['metricName']: metric['query'] for metric in self.metrics_config}
            query_results = await self.prometheus_client.query_multiple_range(
                queries, start_time, end_time_actual, step='30s'
            )
            
            # Process each metric type separately
            for metric_config in self.metrics_config:
                metric_name = metric_config['metricName']
                metric_type = metric_config.get('type', 'unknown')
                unit = metric_config.get('unit', 'seconds')
                
                if metric_name in query_results and 'error' not in query_results[metric_name]:
                    formatted_result = self.prometheus_client.format_query_result(query_results[metric_name])
                    
                    processed_data = []
                    
                    # Process time series data
                    for series in formatted_result:
                        if 'values' in series and series['values']:
                            # Get max value from time series
                            max_value = max(v['value'] for v in series['values'] if v['value'] is not None)
                            avg_value = sum(v['value'] for v in series['values'] if v['value'] is not None) / len([v for v in series['values'] if v['value'] is not None])
                            
                            labels = series.get('labels', {})
                            
                            # Extract pod name
                            pod_name = 'unknown'
                            if 'pod' in labels:
                                pod_name = labels['pod']
                            elif 'instance' in labels:
                                instance = labels['instance']
                                if ':' in instance:
                                    pod_name = instance.split(':')[0]
                                else:
                                    pod_name = instance
                            
                            # Get node information
                            node_name = await self._get_node_info_for_pod(pod_name)
                            
                            # Extract resource information
                            resource_info = self._extract_resource_info_from_labels(labels)
                            pod_resource_name = self._format_pod_resource_name(pod_name, resource_info, metric_name)
                            
                            data_point = {
                                'pod_name': pod_name,
                                'node_name': node_name,
                                'max_value': max_value,
                                'avg_value': avg_value,
                                'readable_max': self._convert_duration_to_readable(max_value) if unit == 'seconds' else {'value': round(max_value, 4), 'unit': unit},
                                'readable_avg': self._convert_duration_to_readable(avg_value) if unit == 'seconds' else {'value': round(avg_value, 4), 'unit': unit},
                                'data_points': len(series['values'])
                            }
                            
                            # Add resource info for sync duration metrics
                            if metric_type == 'sync_duration':
                                data_point['pod_resource_name'] = pod_resource_name
                                data_point['resource_info'] = resource_info
                            
                            processed_data.append(data_point)
                    
                    # Sort by max value (descending)
                    processed_data.sort(key=lambda x: x['max_value'], reverse=True)
                    
                    # Store results based on metric type
                    if metric_type == 'ready_duration':
                        if 'controller' in metric_name:
                            results['controller_ready_duration'] = {
                                'metric_name': metric_name,
                                'unit': unit,
                                'series_count': len(processed_data),
                                'top_10': processed_data[:10]
                            }
                        elif 'node' in metric_name:
                            results['node_ready_duration'] = {
                                'metric_name': metric_name,
                                'unit': unit,
                                'series_count': len(processed_data),
                                'top_10': processed_data[:10]
                            }
                    elif metric_type == 'sync_duration':
                        results['controller_sync_duration'] = {
                            'metric_name': metric_name,
                            'unit': unit,
                            'series_count': len(processed_data),
                            'top_20': processed_data[:20]
                        }
                    elif metric_type == 'service_rate':
                        results['controller_service_rate'] = {
                            'metric_name': metric_name,
                            'unit': unit,
                            'series_count': len(processed_data),
                            'top_10': processed_data[:10]
                        }
                
                else:
                    error_msg = query_results.get(metric_name, {}).get('error', 'Unknown error')
                    print(f"Error querying {metric_name}: {error_msg}")
                    
                    # Store error based on metric type
                    if metric_type == 'ready_duration':
                        if 'controller' in metric_name:
                            results['controller_ready_duration'] = {'error': error_msg}
                        elif 'node' in metric_name:
                            results['node_ready_duration'] = {'error': error_msg}
                    elif metric_type == 'sync_duration':
                        results['controller_sync_duration'] = {'error': error_msg}
                    elif metric_type == 'service_rate':
                        results['controller_service_rate'] = {'error': error_msg}
            
            # Generate overall summary for duration collection
            self._generate_duration_summary(results)
            
        except Exception as e:
            results['error'] = str(e)
            print(f"Error collecting comprehensive metrics: {e}")
        
        return results
    
    def _generate_duration_summary(self, results: Dict[str, Any]) -> None:
        """Generate overall summary for duration-based collection"""
        summary = {
            'metrics_collected': 0,
            'total_series': 0,
            'max_values': {},
            'avg_max_values': {}
        }
        
        metric_sections = ['controller_ready_duration', 'node_ready_duration', 'controller_sync_duration', 'controller_service_rate']
        
        all_max_values = []
        
        for section in metric_sections:
            if section in results and 'error' not in results[section]:
                section_data = results[section]
                summary['metrics_collected'] += 1
                summary['total_series'] += section_data.get('series_count', 0)
                
                # Get data points
                data_key = 'top_20' if section == 'controller_sync_duration' else 'top_10'
                data_points = section_data.get(data_key, [])
                
                if data_points:
                    # Find max value in this section
                    max_item = max(data_points, key=lambda x: x['max_value'])
                    avg_max_value = sum(item['max_value'] for item in data_points) / len(data_points)
                    
                    summary['max_values'][section] = {
                        'value': max_item['max_value'],
                        'readable': max_item['readable_max'],
                        'pod_name': max_item['pod_name'],
                        'node_name': max_item['node_name']
                    }
                    
                    # Add resource info for sync duration
                    if section == 'controller_sync_duration':
                        summary['max_values'][section]['pod_resource_name'] = max_item.get('pod_resource_name', max_item['pod_name'])
                    
                    summary['avg_max_values'][section] = {
                        'value': avg_max_value,
                        'readable': self._convert_duration_to_readable(avg_max_value) if section_data['unit'] == 'seconds' else {'value': round(avg_max_value, 4), 'unit': section_data['unit']}
                    }
                    
                    # Collect values for overall calculations
                    for item in data_points:
                        all_max_values.append({
                            'value': item['max_value'],
                            'section': section,
                            'pod_name': item['pod_name'],
                            'node_name': item['node_name'],
                            'readable': item['readable_max']
                        })
        
        # Overall statistics
        if all_max_values:
            overall_max = max(all_max_values, key=lambda x: x['value'])
            overall_avg = sum(item['value'] for item in all_max_values) / len(all_max_values)
            
            summary['overall_max'] = {
                'value': overall_max['value'],
                'readable': overall_max['readable'],
                'section': overall_max['section'],
                'pod_name': overall_max['pod_name'],
                'node_name': overall_max['node_name']
            }
            
            summary['overall_avg'] = {
                'value': overall_avg,
                'readable': self._convert_duration_to_readable(overall_avg)
            }
        
        results['overall_summary'] = summary


# Convenience functions
async def collect_ovn_sync_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None) -> Dict[str, Any]:
    """Collect OVN sync metrics and return as dictionary"""
    collector = OVNSyncDurationCollector(prometheus_client)
    return await collector.collect_comprehensive_metrics(time)


async def collect_ovn_sync_metrics_duration(prometheus_client: PrometheusBaseQuery, duration: str, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Collect OVN sync metrics over duration and return as dictionary"""
    collector = OVNSyncDurationCollector(prometheus_client)
    return await collector.collect_duration_metrics(duration, end_time)


async def get_ovn_sync_summary_json(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None) -> str:
    """Collect OVN sync metrics and return as JSON string"""
    results = await collect_ovn_sync_metrics(prometheus_client, time)
    return json.dumps(results, indent=2, default=str)


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
        print("Testing comprehensive metrics collection...")
        instant_results = await collector.collect_comprehensive_metrics()
        
        print("\nController Ready Duration Top 3:")
        if 'error' not in instant_results.get('controller_ready_duration', {}):
            for i, item in enumerate(instant_results['controller_ready_duration'].get('top_10', [])[:3], 1):
                readable = item.get('readable_value', {})
                print(f"  {i}. {item['pod_name']} on {item['node_name']}: {readable.get('value')} {readable.get('unit')}")
        
        print("\nNode Ready Duration Top 3:")
        if 'error' not in instant_results.get('node_ready_duration', {}):
            for i, item in enumerate(instant_results['node_ready_duration'].get('top_10', [])[:3], 1):
                readable = item.get('readable_value', {})
                print(f"  {i}. {item['pod_name']} on {item['node_name']}: {readable.get('value')} {readable.get('unit')}")
        
        print("\nController Sync Duration Top 3:")
        if 'error' not in instant_results.get('controller_sync_duration', {}):
            for i, item in enumerate(instant_results['controller_sync_duration'].get('top_20', [])[:3], 1):
                readable = item.get('readable_value', {})
                pod_resource = item.get('pod_resource_name', item['pod_name'])
                print(f"  {i}. {pod_resource} on {item['node_name']}: {readable.get('value')} {readable.get('unit')}")
        
        print("\nController Service Rate Top 3:")
        if 'error' not in instant_results.get('controller_service_rate', {}):
            for i, item in enumerate(instant_results['controller_service_rate'].get('top_10', [])[:3], 1):
                readable = item.get('readable_value', {})
                print(f"  {i}. {item['pod_name']} on {item['node_name']}: {readable.get('value')} {readable.get('unit')}")
        
        # Test duration collection
        print("\n" + "=" * 50)
        print("Testing duration metrics collection (5m)...")
        duration_results = await collector.collect_duration_metrics('5m')
        
        print("\nDuration Collection Summary:")
        overall_summary = duration_results.get('overall_summary', {})
        if 'overall_max' in overall_summary:
            max_info = overall_summary['overall_max']
            print(f"Overall Max: {max_info['readable']['value']} {max_info['readable']['unit']} on {max_info['pod_name']} ({max_info['section']})")
        
        # Print JSON output for verification
        print("\n" + "=" * 50)
        print("JSON Output Sample:")
        print(json.dumps(instant_results, indent=2, default=str)[:1000] + "...")
        
    finally:
        await prometheus_client.close()

if __name__ == "__main__":
    asyncio.run(main())