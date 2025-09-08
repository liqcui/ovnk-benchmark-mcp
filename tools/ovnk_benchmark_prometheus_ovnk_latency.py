#!/usr/bin/env python3
"""
Enhanced OVN-Kubernetes Latency Collector - Fixed Version with Improved Pod/Node Resolution
Collects and analyzes latency metrics from OVN-Kubernetes components with proper pod/node name resolution
"""

import os
import json
import yaml
import asyncio
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError
from tools.ovnk_benchmark_prometheus_utility import mcpToolsUtility
from ocauth.ovnk_benchmark_auth import auth


class OVNLatencyCollector:
    """Enhanced collector for OVN-Kubernetes latency metrics with improved pod/node name resolution"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery):
        self.prometheus_client = prometheus_client
        self.auth = auth
        self.utility = mcpToolsUtility(auth_client=auth)
        
        # Cache for pod-to-node mappings to avoid repeated lookups
        self._pod_node_cache = {}
        self._cache_timestamp = None
        self._cache_expiry_minutes = 10
        
        # Updated metrics configuration with better queries that preserve pod labels
        self.default_metrics = [
            {
                'query': 'topk(10, ovnkube_controller_ready_duration_seconds)',
                'metricName': 'ovnkube_controller_ready_duration_seconds',
                'unit': 'seconds',
                'type': 'ready_duration',
                'component': 'controller'
            },
            {
                'query': 'topk(10, ovnkube_node_ready_duration_seconds)',
                'metricName': 'ovnkube_node_ready_duration_seconds', 
                'unit': 'seconds',
                'type': 'ready_duration',
                'component': 'node'
            },
            {
                'query': 'topk(20, ovnkube_controller_sync_duration_seconds)', 
                'metricName': 'ovnkube_controller_sync_duration_seconds',
                'unit': 'seconds',
                'type': 'sync_duration',
                'component': 'controller'
            }
        ]
        
        # Fixed extended metrics with better queries that preserve pod information
        self.extended_metrics = [
            # Use sum by (pod, le) to preserve pod labels in percentile calculations
            {
                'query': 'histogram_quantile(0.95, sum by (pod, le) (rate(ovnkube_controller_sync_duration_seconds_bucket[5m])) > 0)',
                'metricName': 'ovnkube_controller_sync_duration_p95',
                'unit': 'seconds',
                'type': 'percentile_latency',
                'component': 'controller',
                'description': '95th percentile of controller sync duration over 5 minutes'
            },
            {
                'query': 'histogram_quantile(0.95, sum by (pod, le) (rate(ovnkube_node_sync_duration_seconds_bucket[5m])) > 0)',
                'metricName': 'ovnkube_node_sync_duration_p95',
                'unit': 'seconds',
                'type': 'percentile_latency',
                'component': 'node',
                'description': '95th percentile of node sync duration over 5 minutes'
            },
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_controller_pod_annotation_latency_seconds_bucket[2m])) > 0)',
                'metricName': 'Pod_Annotation_Latency_p99',
                'unit': 'seconds',
                'type': 'pod_latency',
                'component': 'controller'
            },
            # Fixed CNI queries with proper pod label preservation
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_node_cni_request_duration_seconds_bucket{command="ADD"}[2m])) > 0)',
                'metricName': 'CNI_Request_ADD_Latency_p99',
                'unit': 'seconds',
                'type': 'cni_latency',
                'component': 'node'
            },
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_node_cni_request_duration_seconds_bucket{command="DEL"}[2m])) > 0)',
                'metricName': 'CNI_Request_DEL_Latency_p99',
                'unit': 'seconds',
                'type': 'cni_latency',
                'component': 'node'
            },
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_controller_pod_first_seen_lsp_created_duration_seconds_bucket[2m])) > 0)',
                'metricName': 'Pod_creation_latency_first_seen_lsp_p99',
                'unit': 'seconds',
                'type': 'pod_latency',
                'component': 'controller'
            },
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_controller_pod_lsp_created_port_binding_duration_seconds_bucket[2m])) > 0)',
                'metricName': 'pod_creation_latency_port_binding_p99',
                'unit': 'seconds',
                'type': 'pod_latency',
                'component': 'controller'
            },
            {
                'query': 'sum by (pod) (rate(ovnkube_controller_sync_service_latency_seconds_sum[2m])) / sum by (pod) (rate(ovnkube_controller_sync_service_latency_seconds_count[2m]))',
                'metricName': 'sync_service_latency',
                'unit': 'seconds',
                'type': 'service_latency',
                'component': 'controller'
            },
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_controller_sync_service_latency_seconds_bucket[2m])) > 0)',
                'metricName': 'sync_service_latency_p99',
                'unit': 'seconds',
                'type': 'service_latency',
                'component': 'controller'
            },
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_controller_network_programming_duration_seconds_bucket[2m])) > 0)',
                'metricName': 'apply_network_config_pod_duration_p99',
                'unit': 'seconds',
                'type': 'network_programming',
                'component': 'controller'
            },
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_controller_network_programming_service_duration_seconds_bucket[2m])) > 0)',
                'metricName': 'apply_network_config_service_duration_p99',
                'unit': 'seconds',
                'type': 'network_programming',
                'component': 'controller'
            }
        ]
        
        self.metrics_config = []
        self._load_metrics_config()
    
    def _load_metrics_config(self) -> None:
        """Load metrics configuration from metrics-latency.yml or use defaults"""
        metrics_file = Path('metrics-latency.yml')
        
        # Start with existing default metrics
        self.metrics_config = self.default_metrics.copy()
        
        if metrics_file.exists():
            try:
                with open(metrics_file, 'r') as f:
                    config = yaml.safe_load(f)
                    yaml_metrics = config.get('metrics', [])
                    
                    # Process YAML metrics and replace $interval placeholder
                    processed_yaml_metrics = []
                    for metric in yaml_metrics:
                        processed_metric = metric.copy()
                        if '$interval' in processed_metric.get('query', ''):
                            processed_metric['query'] = processed_metric['query'].replace('$interval', '2m')
                        
                        # Set default type and component if not specified
                        if 'type' not in processed_metric:
                            processed_metric['type'] = 'extended_latency'
                        if 'component' not in processed_metric:
                            processed_metric['component'] = 'unknown'
                        if 'unit' not in processed_metric:
                            processed_metric['unit'] = 'seconds'
                        
                        processed_yaml_metrics.append(processed_metric)
                    
                    # Add YAML metrics to existing config
                    self.metrics_config.extend(processed_yaml_metrics)
                    print(f"Loaded {len(yaml_metrics)} additional metrics from metrics-latency.yml")
            except Exception as e:
                print(f"Failed to load metrics-latency.yml: {e}")
                print("Using hardcoded extended metrics as fallback")
                # Add hardcoded extended metrics as fallback
                self.metrics_config.extend(self.extended_metrics)
        else:
            print("metrics-latency.yml not found, using hardcoded extended metrics")
            # Add hardcoded extended metrics
            self.metrics_config.extend(self.extended_metrics)
    
    async def _refresh_pod_node_cache(self) -> None:
        """Refresh the pod-to-node mapping cache using utility functions"""
        current_time = datetime.now()
        
        # Check if cache is still valid
        if (self._cache_timestamp and 
            self._pod_node_cache and 
            (current_time - self._cache_timestamp).total_seconds() < (self._cache_expiry_minutes * 60)):
            return
        
        print("Refreshing pod-to-node mapping cache using utility functions...")
        
        try:
            # Get comprehensive pod info across multiple namespaces using utility method
            namespaces = [
                'openshift-ovn-kubernetes', 
                'openshift-multus', 
                'kube-system', 
                'default',
                'openshift-monitoring',
                'openshift-network-operator'
            ]
            
            # Use the utility method to get all pod info
            all_pod_info = self.utility.get_all_pods_info_across_namespaces(namespaces)
            
            # Build comprehensive cache with multiple lookup strategies
            self._pod_node_cache = {}
            
            for pod_name, info in all_pod_info.items():
                node_name = info.get('node_name', 'unknown')
                namespace = info.get('namespace', 'unknown')
                
                # Store under multiple keys for flexible lookup
                lookup_keys = [
                    pod_name,  # Full pod name
                ]
                
                # Add short name patterns
                if '-' in pod_name:
                    # For names like ovnkube-node-abc123, create lookup for "ovnkube-node"
                    parts = pod_name.split('-')
                    if len(parts) >= 2:
                        base_name = '-'.join(parts[:2])  # e.g., "ovnkube-node"
                        lookup_keys.append(base_name)
                        
                        # Also add the suffix for exact matching
                        suffix = parts[-1]
                        lookup_keys.append(f"{base_name}-{suffix}")
                
                # Add namespace-qualified keys
                lookup_keys.extend([
                    f"{namespace}/{pod_name}",
                    f"{pod_name}.{namespace}"
                ])
                
                # Store the mapping under all lookup keys
                for key in lookup_keys:
                    if key:  # Ensure key is not empty
                        self._pod_node_cache[key] = {
                            'node_name': node_name,
                            'namespace': namespace,
                            'full_pod_name': pod_name
                        }
            
            self._cache_timestamp = current_time
            print(f"Pod-to-node cache refreshed with {len(all_pod_info)} pods, {len(self._pod_node_cache)} lookup keys")
            
            # Debug: Print some cache entries for OVN pods
            ovn_keys = [k for k in self._pod_node_cache.keys() if 'ovnkube' in k][:5]
            print(f"Sample OVN cache keys: {ovn_keys}")
            
        except Exception as e:
            print(f"Failed to refresh pod-node cache: {e}")
            # Keep existing cache if refresh fails
    
    def _extract_pod_name_from_labels(self, labels: Dict[str, str]) -> str:
        """Extract pod name from metric labels with improved logic"""
        # Priority order for pod name extraction
        pod_name_candidates = [
            labels.get('pod'),
            labels.get('kubernetes_pod_name'), 
            labels.get('pod_name'),
            labels.get('exported_pod'),
        ]
        
        # Check direct pod name labels first
        for candidate in pod_name_candidates:
            if candidate and candidate != 'unknown' and candidate.strip():
                return candidate.strip()
        
        # Extract from instance label (format: pod:port or pod-ip:port)
        instance = labels.get('instance', '')
        if instance and ':' in instance:
            # Handle formats like: 'ovnkube-node-sjk5s:9409' or '10.0.0.1:9409'
            pod_part = instance.split(':')[0]
            # Check if it looks like a pod name (not an IP)
            if not re.match(r'^\d+\.\d+\.\d+\.\d+$', pod_part) and pod_part.strip():
                return pod_part.strip()
        
        # Extract from job label if it contains pod info
        job = labels.get('job', '')
        if job and 'ovnkube' in job:
            return job.strip()
        
        # Look for any label that might contain pod name
        for key, value in labels.items():
            if key.lower() in ['container_name', 'name'] and 'ovnkube' in str(value):
                return str(value).strip()
        
        # If all else fails, try to construct from available labels
        namespace = labels.get('namespace', labels.get('kubernetes_namespace', ''))
        if namespace and 'ovn' in namespace:
            # Look for service or deployment labels
            service = labels.get('service', labels.get('kubernetes_name', ''))
            if service:
                return service.strip()
        
        return 'unknown'
    
    def _find_node_name_for_pod(self, pod_name: str) -> str:
        """Find node name for a given pod using the cache with improved matching"""
        if not pod_name or pod_name == 'unknown':
            return 'unknown'
        
        # Direct lookup first
        if pod_name in self._pod_node_cache:
            return self._pod_node_cache[pod_name]['node_name']
        
        # Fuzzy matching strategies for OVN pods
        search_patterns = [pod_name]
        
        # Add patterns for ovnkube pods
        if 'ovnkube' in pod_name:
            if 'controller' in pod_name:
                search_patterns.extend(['ovnkube-controller'])
            elif 'node' in pod_name:
                search_patterns.extend(['ovnkube-node'])
                # Also try with the full pattern
                if '-' in pod_name:
                    parts = pod_name.split('-')
                    if len(parts) >= 3:
                        # Try exact match for ovnkube-node-suffix
                        search_patterns.append(f"ovnkube-node-{parts[-1]}")
            elif 'master' in pod_name:
                search_patterns.extend(['ovnkube-master'])
        
        # Try each pattern
        for pattern in search_patterns:
            if pattern in self._pod_node_cache:
                return self._pod_node_cache[pattern]['node_name']
        
        # Partial matching as last resort
        for cached_key, cached_info in self._pod_node_cache.items():
            if (pod_name in cached_key or 
                cached_key in pod_name or 
                (len(pod_name) > 5 and pod_name[:10] in cached_key)):
                return cached_info['node_name']
        
        return 'unknown'
    
    async def _collect_metric(self, metric_name: str, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Generic method to collect any configured metric with proper pod/node resolution"""
        metric_config = next((m for m in self.metrics_config if m['metricName'] == metric_name), None)
        if not metric_config:
            return {'error': f'{metric_name} metric not configured'}
        
        # Ensure pod-node cache is fresh
        await self._refresh_pod_node_cache()
        
        try:
            # Execute the query
            query = metric_config['query']
            print(f"Executing query for {metric_name}: {query}")
            
            if duration:
                start_time, end_time_actual = self.prometheus_client.get_time_range_from_duration(duration, end_time)
                result = await self.prometheus_client.query_range(query, start_time, end_time_actual, step='30s')
                formatted_result = self.prometheus_client.format_range_query_result(result)
            else:
                result = await self.prometheus_client.query_instant(query, time)
                formatted_result = self.prometheus_client.format_query_result(result)
            
            # Debug: Print result info
            print(f"Query result for {metric_name}: {len(formatted_result) if formatted_result else 0} data points")
            
            # Check if we got any data
            if not formatted_result:
                print(f"No data returned for metric {metric_name}")
                return {
                    'metric_name': metric_config['metricName'],
                    'component': metric_config.get('component', 'unknown'),
                    'unit': metric_config.get('unit', 'seconds'),
                    'description': metric_config.get('description', ''),
                    'statistics': {'count': 0},
                    'query_type': 'duration' if duration else 'instant'
                }
            
            # Check for valid values
            valid_data = [item for item in formatted_result if item.get('value') is not None and item.get('value') != 0]
            if not valid_data:
                print(f"All values are None or 0 for metric {metric_name}")
                return {
                    'metric_name': metric_config['metricName'],
                    'component': metric_config.get('component', 'unknown'),
                    'unit': metric_config.get('unit', 'seconds'),
                    'description': metric_config.get('description', ''),
                    'statistics': {'count': 0},
                    'query_type': 'duration' if duration else 'instant'
                }
            
            processed_data = self._process_metric_data_points_with_node_resolution(formatted_result, metric_config)
            stats = self._calculate_statistics(processed_data, metric_config)
            
            return {
                'metric_name': metric_config['metricName'],
                'component': metric_config.get('component', 'unknown'),
                'unit': metric_config.get('unit', 'seconds'),
                'description': metric_config.get('description', ''),
                'statistics': stats,
                'query_type': 'duration' if duration else 'instant'
            }
        except Exception as e:
            print(f"Error collecting metric {metric_name}: {str(e)}")
            return {'error': str(e)}
    
    def _convert_duration_to_readable(self, seconds: float) -> Dict[str, Any]:
        """Convert duration in seconds to readable format with appropriate unit"""
        if seconds == 0:
            return {'value': 0, 'unit': 'ms'}
        elif seconds < 1:
            return {'value': round(seconds * 1000, 2), 'unit': 'ms'}
        elif seconds < 60:
            return {'value': round(seconds, 3), 'unit': 's'}
        elif seconds < 3600:
            return {'value': round(seconds / 60, 2), 'unit': 'min'}
        else:
            return {'value': round(seconds / 3600, 2), 'unit': 'h'}
    
    def _extract_resource_info_from_labels(self, labels: Dict[str, str]) -> Dict[str, str]:
        """Extract resource information from metric labels (optimized)"""
        resource_info = {
            'resource_name': 'unknown',
            'resource_namespace': 'unknown'
        }
        
        # Common label patterns for resource name
        if 'name' in labels:
            resource_info['resource_name'] = labels['name']
        elif 'resource_name' in labels:
            resource_info['resource_name'] = labels['resource_name']
        elif 'object_name' in labels:
            resource_info['resource_name'] = labels['object_name']
        elif 'service' in labels:
            resource_info['resource_name'] = labels['service']
        elif 'resource' in labels:
            resource_info['resource_name'] = labels['resource']
        
        # For controller sync metrics, try to extract resource type
        if 'resource' in labels or 'kind' in labels:
            resource_type = labels.get('resource', labels.get('kind', 'unknown'))
            if resource_type != 'unknown':
                resource_info['resource_name'] = resource_type
        
        # Special case for sync duration metrics - use "all watchers" if no specific resource
        if resource_info['resource_name'] == 'unknown' and any('sync' in k.lower() for k in labels.keys()):
            resource_info['resource_name'] = 'all watchers'
        
        # Namespace extraction
        if 'namespace' in labels:
            resource_info['resource_namespace'] = labels['namespace']
        elif 'object_namespace' in labels:
            resource_info['resource_namespace'] = labels['object_namespace']
        
        return resource_info
    
    def _process_metric_data_points_with_node_resolution(self, formatted_result: List[Dict], metric_config: Dict) -> List[Dict[str, Any]]:
        """Process metric data points with proper pod and node name resolution"""
        processed_data = []
        metric_name = metric_config['metricName']
        unit = metric_config.get('unit', 'seconds')
        
        # Check if this is a sync duration metric that needs resource_name
        include_resource_name = 'sync_duration' in metric_name or metric_name == 'ovnkube_controller_sync_duration_seconds'
        
        for item in formatted_result:
            value = item.get('value')
            
            # Skip invalid or zero values for latency metrics
            if value is None or (isinstance(value, (int, float)) and value <= 0):
                continue
            
            labels = item.get('labels', {})
            
            # Extract pod name with improved logic
            pod_name = self._extract_pod_name_from_labels(labels)
            
            # Resolve node name using cache
            node_name = self._find_node_name_for_pod(pod_name)
            
            # Debug output for troubleshooting
            if pod_name != 'unknown' and node_name != 'unknown':
                print(f"Successfully resolved pod '{pod_name}' -> node '{node_name}'")
            else:
                print(f"Failed to resolve: pod='{pod_name}', node='{node_name}', available_labels={list(labels.keys())}")
            
            # Base data point structure
            data_point = {
                'pod_name': pod_name,
                'node_name': node_name,
                'value': value,
                'readable_value': self._convert_duration_to_readable(value) if unit == 'seconds' else {'value': round(value, 4), 'unit': unit}
            }
            
            # For service metrics, use service name as identifier
            if 'service' in labels and metric_config.get('type') == 'network_programming':
                service_name = labels['service']
                data_point.update({
                    'service_name': service_name,
                    'pod_name': 'N/A',
                    'node_name': 'N/A'
                })
            
            # Add resource_name only for sync duration metrics
            if include_resource_name:
                resource_info = self._extract_resource_info_from_labels(labels)
                data_point['resource_name'] = resource_info.get('resource_name', 'all watchers')
            
            processed_data.append(data_point)
        
        print(f"Processed {len(processed_data)} valid data points for {metric_name}")
        return processed_data
    
    def _calculate_statistics(self, data_points: List[Dict], metric_config: Dict) -> Dict[str, Any]:
        """Calculate statistics for data points with better validation"""
        if not data_points:
            return {'count': 0}
        
        values = [dp['value'] for dp in data_points if dp.get('value') is not None and dp.get('value') > 0]
        if not values:
            return {'count': 0}
        
        # Sort data points by value (descending)
        sorted_data = sorted(data_points, key=lambda x: x.get('value', 0), reverse=True)
        
        max_value = max(values)
        avg_value = sum(values) / len(values)
        
       # Get top entries based on metric type - CHANGED: top 20 for sync duration metrics
        if metric_config.get('type') == 'sync_duration' and metric_config.get('metricName') == 'ovnkube_controller_sync_duration_seconds':
            top_entries = sorted_data[:20]  # CHANGED: from [:5] to [:20]
            top_key = 'top_20'  # CHANGED: from 'top_5' to 'top_20'
        else:
            top_entries = sorted_data[:5]
            top_key = 'top_5'
        
        return {
            'count': len(data_points),
            'max_value': max_value,
            'avg_value': avg_value,
            'readable_max': self._convert_duration_to_readable(max_value),
            'readable_avg': self._convert_duration_to_readable(avg_value),
            top_key: top_entries  # CHANGED: dynamic key based on metric type
        }

    # Individual metric collection functions
    async def collect_controller_ready_duration(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect controller ready duration metrics"""
        return await self._collect_metric('ovnkube_controller_ready_duration_seconds', time, duration, end_time)
    
    async def collect_node_ready_duration(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect node ready duration metrics"""
        return await self._collect_metric('ovnkube_node_ready_duration_seconds', time, duration, end_time)
    
    async def collect_controller_sync_duration(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect controller sync duration metrics"""
        return await self._collect_metric('ovnkube_controller_sync_duration_seconds', time, duration, end_time)
    
    async def collect_controller_sync_duration_p95(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect controller sync duration 95th percentile metrics"""
        return await self._collect_metric('ovnkube_controller_sync_duration_p95', time, duration, end_time)
    
    async def collect_node_sync_duration_p95(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect node sync duration 95th percentile metrics"""
        return await self._collect_metric('ovnkube_node_sync_duration_p95', time, duration, end_time)
    
    async def collect_pod_annotation_latency_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect pod annotation latency 99th percentile metrics"""
        return await self._collect_metric('Pod_Annotation_Latency_p99', time, duration, end_time)
    
    async def collect_cni_request_add_latency_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect CNI ADD request latency 99th percentile metrics"""
        return await self._collect_metric('CNI_Request_ADD_Latency_p99', time, duration, end_time)
    
    async def collect_cni_request_del_latency_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect CNI DEL request latency 99th percentile metrics"""
        return await self._collect_metric('CNI_Request_DEL_Latency_p99', time, duration, end_time)
    
    async def collect_pod_creation_first_seen_lsp_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect pod creation first seen LSP latency 99th percentile metrics"""
        return await self._collect_metric('Pod_creation_latency_first_seen_lsp_p99', time, duration, end_time)
    
    async def collect_pod_creation_port_binding_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect pod creation port binding latency 99th percentile metrics"""
        return await self._collect_metric('pod_creation_latency_port_binding_p99', time, duration, end_time)
    
    async def collect_sync_service_latency(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect sync service latency metrics"""
        return await self._collect_metric('sync_service_latency', time, duration, end_time)
    
    async def collect_sync_service_latency_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect sync service latency 99th percentile metrics"""
        return await self._collect_metric('sync_service_latency_p99', time, duration, end_time)
    
    async def collect_network_programming_pod_duration_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect network programming pod duration 99th percentile metrics"""
        return await self._collect_metric('apply_network_config_pod_duration_p99', time, duration, end_time)
    
    async def collect_network_programming_service_duration_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect network programming service duration 99th percentile metrics"""
        return await self._collect_metric('apply_network_config_service_duration_p99', time, duration, end_time)
    
    async def collect_comprehensive_enhanced_metrics(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None, categories: Optional[List[str]] = None, include_controller_metrics: bool = True, include_node_metrics: bool = True, include_pod_latency: bool = True, include_service_latency: bool = True, top_n_results: int = 5, include_statistics: bool = True) -> Dict[str, Any]:
        """
        Collect all enhanced OVN latency metrics with better filtering and validation
        """
        print(f"Collecting comprehensive enhanced OVN latency metrics... (Query type: {'duration' if duration else 'instant'})")
        
        results = {
            'collection_timestamp': time or datetime.now(timezone.utc).isoformat(),
            'timezone': 'UTC',
            'collection_type': 'enhanced_comprehensive',
            'query_type': 'duration' if duration else 'instant',
            'ready_duration_metrics': {},
            'sync_duration_metrics': {},
            'percentile_latency_metrics': {},
            'pod_latency_metrics': {},
            'cni_latency_metrics': {},
            'service_latency_metrics': {},
            'network_programming_metrics': {},
            'legacy_metrics': {},
            'overall_summary': {}
        }
        
        if duration:
            start_time, end_time_actual = self.prometheus_client.get_time_range_from_duration(duration, end_time)
            results['query_parameters'] = {'duration': duration, 'start_time': start_time, 'end_time': end_time_actual}
        
        try:
            # Build metric tasks based on parameters
            all_metric_tasks = []
            
            if include_controller_metrics:
                all_metric_tasks.extend([
                    ('controller_ready_duration', 'ready_duration', self.collect_controller_ready_duration(time, duration, end_time)),
                    ('controller_sync_duration', 'sync_duration', self.collect_controller_sync_duration(time, duration, end_time)),
                    ('controller_sync_duration_p95', 'percentile_latency', self.collect_controller_sync_duration_p95(time, duration, end_time)),
                ])
            
            if include_node_metrics:
                all_metric_tasks.extend([
                    ('node_ready_duration', 'ready_duration', self.collect_node_ready_duration(time, duration, end_time)),
                    ('node_sync_duration_p95', 'percentile_latency', self.collect_node_sync_duration_p95(time, duration, end_time)),
                    ('cni_add_latency_p99', 'cni_latency', self.collect_cni_request_add_latency_p99(time, duration, end_time)),
                    ('cni_del_latency_p99', 'cni_latency', self.collect_cni_request_del_latency_p99(time, duration, end_time)),
                ])
            
            if include_pod_latency:
                all_metric_tasks.extend([
                    ('pod_annotation_latency_p99', 'pod_latency', self.collect_pod_annotation_latency_p99(time, duration, end_time)),
                    ('pod_creation_first_seen_lsp_p99', 'pod_latency', self.collect_pod_creation_first_seen_lsp_p99(time, duration, end_time)),
                    ('pod_creation_port_binding_p99', 'pod_latency', self.collect_pod_creation_port_binding_p99(time, duration, end_time)),
                ])
            
            if include_service_latency:
                all_metric_tasks.extend([
                    ('sync_service_latency', 'service_latency', self.collect_sync_service_latency(time, duration, end_time)),
                    ('sync_service_latency_p99', 'service_latency', self.collect_sync_service_latency_p99(time, duration, end_time)),
                ])
            
            # Always include network programming metrics for now
            all_metric_tasks.extend([
                ('network_programming_pod_p99', 'network_programming', self.collect_network_programming_pod_duration_p99(time, duration, end_time)),
                ('network_programming_service_p99', 'network_programming', self.collect_network_programming_service_duration_p99(time, duration, end_time)),
            ])

            # Filter by requested categories if provided
            if categories:
                category_set = set(categories)
                metric_tasks = [(k, c, coro) for (k, c, coro) in all_metric_tasks if c in category_set]
            else:
                metric_tasks = all_metric_tasks

            # Timeout configuration
            def _parse_duration_seconds(d: Optional[str]) -> int:
                if not d:
                    return 0
                m = re.match(r"^(\d+)([smhd])$", d.strip())
                if not m:
                    return 0
                val = int(m.group(1)); unit = m.group(2)
                return val if unit == 's' else val*60 if unit == 'm' else val*3600 if unit == 'h' else val*86400

            total_seconds = _parse_duration_seconds(duration)
            
            if total_seconds == 0:
                per_metric_timeout = 600.0  # 10 minutes for instant queries
                concurrency = 3
            elif total_seconds > 3600:
                per_metric_timeout = 1200.0  # 20 minutes for long duration queries
                concurrency = 2
            elif total_seconds > 300:
                per_metric_timeout = 900.0   # 15 minutes for medium duration queries
                concurrency = 3
            else:
                per_metric_timeout = 600.0   # 10 minutes for short duration queries
                concurrency = 4

            # Ensure minimum timeout
            per_metric_timeout = max(per_metric_timeout, 600.0)

            sem = asyncio.Semaphore(concurrency)

            async def run_task(coro):
                async with sem:
                    try:
                        return await asyncio.wait_for(coro, timeout=per_metric_timeout)
                    except asyncio.TimeoutError:
                        return {'error': f'metric timeout after {per_metric_timeout}s'}

            # Execute tasks with bounded concurrency
            task_results = await asyncio.gather(*[run_task(coro) for _, _, coro in metric_tasks], return_exceptions=True)
            
            # Organize results by category
            for (metric_key, metric_category, _), result in zip(metric_tasks, task_results):
                if isinstance(result, Exception):
                    result = {'error': str(result)}
                
                # Categorize metrics
                if metric_category == 'ready_duration':
                    results['ready_duration_metrics'][metric_key] = result
                elif metric_category == 'sync_duration':
                    results['sync_duration_metrics'][metric_key] = result
                elif metric_category == 'percentile_latency':
                    results['percentile_latency_metrics'][metric_key] = result
                elif metric_category == 'pod_latency':
                    results['pod_latency_metrics'][metric_key] = result
                elif metric_category == 'cni_latency':
                    results['cni_latency_metrics'][metric_key] = result
                elif metric_category == 'service_latency':
                    results['service_latency_metrics'][metric_key] = result
                elif metric_category == 'network_programming':
                    results['network_programming_metrics'][metric_key] = result
                else:
                    results['legacy_metrics'][metric_key] = result
            
            # Generate overall summary
            self._generate_enhanced_summary(results)
            
        except Exception as e:
            results['error'] = str(e)
            print(f"Error collecting enhanced metrics: {e}")
        
        return results
    
    def _generate_enhanced_summary(self, results: Dict[str, Any]) -> None:
        """Generate overall summary for enhanced metrics collection"""
        summary = {
            'total_metrics_collected': 0,
            'successful_metrics': 0,
            'failed_metrics': 0,
            'category_summary': {},
            'top_latencies': [],
            'component_breakdown': {'controller': 0, 'node': 0, 'unknown': 0}
        }
        
        all_metric_values = []
        
        # Process each category
        categories = ['ready_duration_metrics', 'sync_duration_metrics', 'percentile_latency_metrics', 'pod_latency_metrics', 
                     'cni_latency_metrics', 'service_latency_metrics', 'network_programming_metrics', 'legacy_metrics']
        
        for category in categories:
            if category in results:
                category_data = results[category]
                category_summary = {
                    'metrics_count': len(category_data),
                    'successful_count': 0,
                    'failed_count': 0,
                    'max_latency': None,
                    'avg_latency': None
                }
                
                category_values = []
                
                for metric_name, metric_result in category_data.items():
                    summary['total_metrics_collected'] += 1
                    
                    if 'error' in metric_result:
                        summary['failed_metrics'] += 1
                        category_summary['failed_count'] += 1
                    else:
                        summary['successful_metrics'] += 1
                        category_summary['successful_count'] += 1
                        
                        # Track component
                        component = metric_result.get('component', 'unknown')
                        if component in summary['component_breakdown']:
                            summary['component_breakdown'][component] += 1
                        
                        # Extract statistics
                        stats = metric_result.get('statistics', {})
                        if stats.get('max_value') is not None and stats.get('count', 0) > 0:
                            max_val = stats['max_value']
                            avg_val = stats.get('avg_value', max_val)
                            
                            # Only include non-zero values
                            if max_val > 0:
                                category_values.extend([max_val, avg_val])
                                all_metric_values.append({
                                    'metric_name': metric_result.get('metric_name', metric_name),
                                    'category': category.replace('_metrics', ''),
                                    'component': component,
                                    'max_value': max_val,
                                    'avg_value': avg_val,
                                    'readable_max': stats.get('readable_max', {}),
                                    'readable_avg': stats.get('readable_avg', {}),
                                    'data_points': stats.get('count', 0)
                                })
                
                # Category statistics
                if category_values:
                    category_summary['max_latency'] = {
                        'value': max(category_values),
                        'readable': self._convert_duration_to_readable(max(category_values))
                    }
                    category_summary['avg_latency'] = {
                        'value': sum(category_values) / len(category_values),
                        'readable': self._convert_duration_to_readable(sum(category_values) / len(category_values))
                    }
                
                summary['category_summary'][category] = category_summary
        
        # Overall top latencies (top 10)
        if all_metric_values:
            # Sort by max value
            sorted_metrics = sorted(all_metric_values, key=lambda x: x['max_value'], reverse=True)
            summary['top_latencies'] = sorted_metrics[:10]
            
            # Overall statistics
            all_max_values = [m['max_value'] for m in all_metric_values if m['max_value'] > 0]
            if all_max_values:
                summary['overall_max_latency'] = {
                    'value': max(all_max_values),
                    'readable': self._convert_duration_to_readable(max(all_max_values)),
                    'metric': next(m['metric_name'] for m in sorted_metrics if m['max_value'] == max(all_max_values))
                }
                
                summary['overall_avg_latency'] = {
                    'value': sum(all_max_values) / len(all_max_values),
                    'readable': self._convert_duration_to_readable(sum(all_max_values) / len(all_max_values))
                }
        
        results['overall_summary'] = summary


# Enhanced convenience functions with better parameter support
async def collect_enhanced_ovn_latency_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None, include_controller_metrics: bool = True, include_node_metrics: bool = True, include_pod_latency: bool = True, include_service_latency: bool = True, metric_categories: Optional[List[str]] = None, top_n_results: int = 5, include_statistics: bool = True) -> Dict[str, Any]:
    """Collect enhanced OVN latency metrics with better filtering options"""
    collector = OVNLatencyCollector(prometheus_client)
    return await collector.collect_comprehensive_enhanced_metrics(
        time=time, 
        duration=duration, 
        end_time=end_time, 
        categories=metric_categories,
        include_controller_metrics=include_controller_metrics,
        include_node_metrics=include_node_metrics,
        include_pod_latency=include_pod_latency,
        include_service_latency=include_service_latency,
        top_n_results=top_n_results,
        include_statistics=include_statistics
    )


async def get_enhanced_ovn_latency_summary_json(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> str:
    """Collect enhanced OVN latency metrics and return as JSON string"""
    results = await collect_enhanced_ovn_latency_metrics(prometheus_client, time, duration, end_time)
    return json.dumps(results, indent=2, default=str)


# Individual metric convenience functions
async def get_controller_ready_duration_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Get controller ready duration metrics"""
    collector = OVNLatencyCollector(prometheus_client)
    return await collector.collect_controller_ready_duration(time, duration, end_time)


async def get_controller_sync_duration_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Get controller sync duration metrics"""
    collector = OVNLatencyCollector(prometheus_client)
    return await collector.collect_controller_sync_duration(time, duration, end_time)


async def get_pod_latency_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Get all pod-related latency metrics"""
    collector = OVNLatencyCollector(prometheus_client)
    
    base_timestamp = time or datetime.now(timezone.utc).isoformat()
    results = {
        'collection_timestamp': base_timestamp,
        'query_type': 'duration' if duration else 'instant',
        'pod_annotation_latency_p99': await collector.collect_pod_annotation_latency_p99(time, duration, end_time),
        'pod_creation_first_seen_lsp_p99': await collector.collect_pod_creation_first_seen_lsp_p99(time, duration, end_time),
        'pod_creation_port_binding_p99': await collector.collect_pod_creation_port_binding_p99(time, duration, end_time)
    }
    
    if duration:
        start_time, end_time_actual = collector.prometheus_client.get_time_range_from_duration(duration, end_time)
        results['query_parameters'] = {'duration': duration, 'start_time': start_time, 'end_time': end_time_actual}
    
    return results


async def get_cni_latency_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Get all CNI-related latency metrics"""
    collector = OVNLatencyCollector(prometheus_client)
    
    base_timestamp = time or datetime.now(timezone.utc).isoformat()
    results = {
        'collection_timestamp': base_timestamp,
        'query_type': 'duration' if duration else 'instant',
        'cni_add_latency_p99': await collector.collect_cni_request_add_latency_p99(time, duration, end_time),
        'cni_del_latency_p99': await collector.collect_cni_request_del_latency_p99(time, duration, end_time)
    }
    
    if duration:
        start_time, end_time_actual = collector.prometheus_client.get_time_range_from_duration(duration, end_time)
        results['query_parameters'] = {'duration': duration, 'start_time': start_time, 'end_time': end_time_actual}
    
    return results


async def get_service_latency_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Get all service-related latency metrics"""
    collector = OVNLatencyCollector(prometheus_client)
    
    base_timestamp = time or datetime.now(timezone.utc).isoformat()
    results = {
        'collection_timestamp': base_timestamp,
        'query_type': 'duration' if duration else 'instant',
        'sync_service_latency': await collector.collect_sync_service_latency(time, duration, end_time),
        'sync_service_latency_p99': await collector.collect_sync_service_latency_p99(time, duration, end_time)
    }
    
    if duration:
        start_time, end_time_actual = collector.prometheus_client.get_time_range_from_duration(duration, end_time)
        results['query_parameters'] = {'duration': duration, 'start_time': start_time, 'end_time': end_time_actual}
    
    return results


# Legacy compatibility functions (maintain existing API)
async def collect_ovn_sync_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None) -> Dict[str, Any]:
    """Legacy function - collect OVN sync metrics and return as dictionary (instant query only)"""
    collector = OVNLatencyCollector(prometheus_client)
    return await collector.collect_comprehensive_enhanced_metrics(time)


async def collect_ovn_sync_metrics_duration(prometheus_client: PrometheusBaseQuery, duration: str, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Legacy function - collect OVN sync metrics over duration and return as dictionary"""
    collector = OVNLatencyCollector(prometheus_client)
    return await collector.collect_comprehensive_enhanced_metrics(None, duration, end_time)


async def get_ovn_sync_summary_json(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None) -> str:
    """Legacy function - collect OVN sync metrics and return as JSON string (instant query only)"""
    results = await collect_ovn_sync_metrics(prometheus_client, time)
    return json.dumps(results, indent=2, default=str)


async def main():
    """Example usage of OVNLatencyCollector with improved pod/node resolution"""
    # Initialize authentication
    await auth.initialize()
    
    # Create Prometheus client
    prometheus_client = PrometheusBaseQuery(
        prometheus_url=auth.prometheus_url,
        token=auth.prometheus_token
    )
    
    # Create collector
    collector = OVNLatencyCollector(prometheus_client)
    
    try:
        # Test instant query with proper pod/node resolution
        print("=" * 60)
        print("Testing Fixed Pod/Node Resolution...")
        
        # First test cache refresh
        await collector._refresh_pod_node_cache()
        print(f"Cache populated with {len(collector._pod_node_cache)} entries")
        
        # Test individual metric collection
        print("\nTesting controller ready duration...")
        controller_results = await collector.collect_controller_ready_duration()
        if 'statistics' in controller_results and controller_results['statistics'].get('count', 0) > 0:
            top_entries = controller_results['statistics'].get('top_5', [])
            for entry in top_entries[:3]:
                print(f"  Pod: {entry.get('pod_name')} -> Node: {entry.get('node_name')}")
        
        print("\nTesting comprehensive collection...")
        instant_results = await collector.collect_comprehensive_enhanced_metrics()
        
        # Check pod/node resolution across all metrics
        pod_node_resolved = 0
        total_entries = 0
        
        for category_name, category_data in instant_results.items():
            if category_name.endswith('_metrics'):
                for metric_name, metric_result in category_data.items():
                    if isinstance(metric_result, dict) and 'statistics' in metric_result:
                        top_entries = metric_result['statistics'].get('top_5', [])
                        for entry in top_entries:
                            total_entries += 1
                            if (entry.get('pod_name') != 'unknown' and 
                                entry.get('node_name') != 'unknown'):
                                pod_node_resolved += 1
        
        resolution_rate = (pod_node_resolved / total_entries * 100) if total_entries > 0 else 0
        print(f"\nPod/Node Resolution Summary:")
        print(f"  Resolved: {pod_node_resolved}/{total_entries} ({resolution_rate:.1f}%)")
        
        # Show sample resolved entries
        print(f"\nSample resolved entries:")
        for category_name, category_data in instant_results.items():
            if category_name.endswith('_metrics'):
                for metric_name, metric_result in category_data.items():
                    if isinstance(metric_result, dict) and 'statistics' in metric_result:
                        top_entries = metric_result['statistics'].get('top_5', [])
                        for entry in top_entries[:1]:  # Just first entry per metric
                            if (entry.get('pod_name') != 'unknown' and 
                                entry.get('node_name') != 'unknown'):
                                print(f"  {metric_name}: {entry.get('pod_name')} -> {entry.get('node_name')}")
                        break  # Only show one per category
                break
        
    except Exception as e:
        print(f"Error during testing: {e}")
    finally:
        await prometheus_client.close()


if __name__ == "__main__":
    asyncio.run(main())