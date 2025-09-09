"""
Kubernetes API Server Metrics Module
Queries and processes Kubernetes API server performance metrics
"""

import asyncio
from datetime import datetime, timezone
import math
from typing import Dict, List, Any, Optional
from .ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery


class kubeAPICollector:
    """Handles Kubernetes API server metrics queries"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery):
        self.prometheus_client = prometheus_client
    
    async def get_metrics(self, duration: str = "5m", start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """
        Get Kubernetes API server metrics
        
        Args:
            duration: Query duration (e.g., '5m', '1h')
            start_time: Optional start time in ISO format
            end_time: Optional end time in ISO format
            
        Returns:
            Dictionary containing API server metrics
        """
        result = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'duration': duration,
            'metrics': {},
            'summary': {},
            'errors': []
        }
        
        try:
            # Get all metrics concurrently
            tasks = [
                self.get_readonly_latency_metrics(duration, start_time, end_time),
                self.get_mutating_latency_metrics(duration, start_time, end_time),
                self.get_basic_api_metrics(duration, start_time, end_time),
                self.get_watch_events_metrics(duration, start_time, end_time),
                self.get_cache_list_metrics(duration, start_time, end_time),
                self.get_watch_cache_received_metrics(duration, start_time, end_time),
                self.get_watch_cache_dispatched_metrics(duration, start_time, end_time),
                self.get_rest_client_metrics(duration, start_time, end_time),
                self.get_ovnkube_controller_metrics(duration, start_time, end_time),
                self.get_etcd_requests_metrics(duration, start_time, end_time)
            ]
            
            metrics_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            metric_names = [
                'readonly_latency', 'mutating_latency', 'basic_api', 'watch_events',
                'cache_list', 'watch_cache_received', 'watch_cache_dispatched',
                'rest_client', 'ovnkube_controller', 'etcd_requests'
            ]
            
            for i, metric_result in enumerate(metrics_results):
                metric_name = metric_names[i]
                if isinstance(metric_result, Exception):
                    result['errors'].append(f"{metric_name}: {str(metric_result)}")
                else:
                    result['metrics'][metric_name] = metric_result
            
            # Generate comprehensive summary
            result['summary'] = self._generate_comprehensive_summary(result['metrics'])
            
        except Exception as e:
            result['errors'].append(f"Failed to get API server metrics: {str(e)}")
        
        return result
    
    async def get_readonly_latency_metrics(self, duration: str = "5m", start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get read-only API calls latency metrics with top 5 avg and max values"""
        elapsed_placeholder = f"[{duration}:]"
        
        queries = {
            'avg_ro_apicalls_latency': f'''avg_over_time(histogram_quantile(0.99, sum(irate(apiserver_request_duration_seconds_bucket{{apiserver="kube-apiserver", verb=~"LIST|GET", subresource!~"log|exec|portforward|attach|proxy"}}[2m])) by (le, resource, verb, scope)){elapsed_placeholder}) > 0''',
            'max_ro_apicalls_latency': f'''max_over_time(histogram_quantile(0.99, sum(irate(apiserver_request_duration_seconds_bucket{{apiserver="kube-apiserver", verb=~"LIST|GET", subresource!~"log|exec|portforward|attach|proxy"}}[2m])) by (le, resource, verb, scope)){elapsed_placeholder}) > 0'''
        }
        
        return await self._execute_and_process_queries(queries, 'readonly_latency', start_time, end_time)
            
    async def get_mutating_latency_metrics(self, duration: str = "5m", start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get mutating API calls latency metrics with top 5 avg and max values"""
        elapsed_placeholder = f"[{duration}:]"
            
        queries = {
            'avg_mutating_apicalls_latency': f'''avg_over_time(histogram_quantile(0.99, sum(irate(apiserver_request_duration_seconds_bucket{{apiserver="kube-apiserver", verb=~"POST|PUT|DELETE|PATCH", subresource!~"log|exec|portforward|attach|proxy"}}[2m])) by (le, resource, verb, scope)){elapsed_placeholder}) > 0''',
            'max_mutating_apicalls_latency': f'''max_over_time(histogram_quantile(0.99, sum(irate(apiserver_request_duration_seconds_bucket{{apiserver="kube-apiserver", verb=~"POST|PUT|DELETE|PATCH", subresource!~"log|exec|portforward|attach|proxy"}}[2m])) by (le, resource, verb, scope)){elapsed_placeholder}) > 0'''
        }
            
        return await self._execute_and_process_queries(queries, 'mutating_latency', start_time, end_time)
            
    async def get_basic_api_metrics(self, duration: str = "5m", start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get basic API server metrics"""
        queries = {
            'api_request_rate': 'sum(rate(apiserver_request_total{apiserver="kube-apiserver"}[5m])) by (verb, resource)',
            'api_request_errors': 'sum(rate(apiserver_request_total{apiserver="kube-apiserver", code!~"2.."}[5m])) by (verb, resource, code)',
            'api_server_current_inflight_requests': 'sum(apiserver_current_inflight_requests{apiserver="kube-apiserver"}) by (request_kind)',
            'etcd_request_duration': 'histogram_quantile(0.99, sum(rate(etcd_request_duration_seconds_bucket{job=~".*etcd.*"}[5m])) by (le, operation, type))'
        }
        
        return await self._execute_and_process_queries(queries, 'basic_api', start_time, end_time)
    
    async def get_watch_events_metrics(self, duration: str = "5m", start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get top 5 watch events metrics with instance:group:kind format"""
        query = 'rate(apiserver_watch_events_total[5m])'
        
        result = await self._execute_single_query(query, start_time, end_time)
        
        return {
            'metric_type': 'watch_events',
            'query': query,
            'top5_avg': self._get_top5_values(result, 'avg', lambda m: f"{m.get('instance', 'unknown')}:{m.get('group', 'unknown')}:{m.get('kind', 'unknown')}"),
            'top5_max': self._get_top5_values(result, 'max', lambda m: f"{m.get('instance', 'unknown')}:{m.get('group', 'unknown')}:{m.get('kind', 'unknown')}"),
            'unit': 'events/second'
        }
    
    async def get_cache_list_metrics(self, duration: str = "5m", start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get top 5 cache list metrics with instance:resource_prefix format"""
        query = 'topk(5, rate(apiserver_cache_list_total{job="apiserver"}[5m]))'
        
        result = await self._execute_single_query(query, start_time, end_time)
        
        return {
            'metric_type': 'cache_list',
            'query': query,
            'top5_avg': self._get_top5_values(result, 'avg', lambda m: f"{m.get('instance', 'unknown')}:{m.get('resource_prefix', 'unknown')}"),
            'top5_max': self._get_top5_values(result, 'max', lambda m: f"{m.get('instance', 'unknown')}:{m.get('resource_prefix', 'unknown')}"),
            'unit': 'operations/second'
        }
    
    async def get_watch_cache_received_metrics(self, duration: str = "5m", start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get top 5 watch cache received metrics with instance:resource format"""
        query = 'topk(5, rate(apiserver_watch_cache_events_received_total[5m]))'
        
        result = await self._execute_single_query(query, start_time, end_time)
        
        return {
            'metric_type': 'watch_cache_received',
            'query': query,
            'top5_avg': self._get_top5_values(result, 'avg', lambda m: f"{m.get('instance', 'unknown')}:{m.get('resource', 'unknown')}"),
            'top5_max': self._get_top5_values(result, 'max', lambda m: f"{m.get('instance', 'unknown')}:{m.get('resource', 'unknown')}"),
            'unit': 'events/second'
        }
    
    async def get_watch_cache_dispatched_metrics(self, duration: str = "5m", start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get top 5 watch cache dispatched metrics with instance:resource format"""
        query = 'rate(apiserver_watch_cache_events_dispatched_total[5m])'
        
        result = await self._execute_single_query(query, start_time, end_time)
        
        return {
            'metric_type': 'watch_cache_dispatched',
            'query': query,
            'top5_avg': self._get_top5_values(result, 'avg', lambda m: f"{m.get('instance', 'unknown')}:{m.get('resource', 'unknown')}"),
            'top5_max': self._get_top5_values(result, 'max', lambda m: f"{m.get('instance', 'unknown')}:{m.get('resource', 'unknown')}"),
            'unit': 'events/second'
        }
    
    async def get_rest_client_metrics(self, duration: str = "5m", start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get top 5 REST client request duration metrics with service:verb format"""
        query = 'histogram_quantile(0.99, sum by(le, service, verb) (rate(rest_client_request_duration_seconds_bucket{job=~"kube-controller-manager|scheduler|check-endpoints|kubelet"}[5m])))'
        
        result = await self._execute_single_query(query, start_time, end_time)
        
        return {
            'metric_type': 'rest_client_duration',
            'query': query,
            'top5_avg': self._get_top5_values(result, 'avg', lambda m: f"{m.get('service', 'unknown')}:{m.get('verb', 'unknown')}"),
            'top5_max': self._get_top5_values(result, 'max', lambda m: f"{m.get('service', 'unknown')}:{m.get('verb', 'unknown')}"),
            'unit': 'seconds'
        }
    
    async def get_ovnkube_controller_metrics(self, duration: str = "5m", start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get top 5 OVNKube controller resource update metrics with instance:event:name format"""
        query = 'topk(10, rate(ovnkube_controller_resource_update_total[5m]))'
        
        result = await self._execute_single_query(query, start_time, end_time)
        
        return {
            'metric_type': 'ovnkube_controller_updates',
            'query': query,
            'top5_avg': self._get_top5_values(result, 'avg', lambda m: f"{m.get('instance', 'unknown')}:{m.get('event', 'unknown')}:{m.get('name', 'unknown')}"),
            'top5_max': self._get_top5_values(result, 'max', lambda m: f"{m.get('instance', 'unknown')}:{m.get('event', 'unknown')}:{m.get('name', 'unknown')}"),
            'unit': 'updates/second'
        }
    
    async def get_etcd_requests_metrics(self, duration: str = "5m", start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get top 5 etcd requests metrics with operation:type format"""
        query = 'rate(etcd_requests_total[5m])'
        
        result = await self._execute_single_query(query, start_time, end_time)
        
        return {
            'metric_type': 'etcd_requests',
            'query': query,
            'top5_avg': self._get_top5_values(result, 'avg', lambda m: f"{m.get('operation', 'unknown')}:{m.get('type', 'unknown')}"),
            'top5_max': self._get_top5_values(result, 'max', lambda m: f"{m.get('operation', 'unknown')}:{m.get('type', 'unknown')}"),
            'unit': 'requests/second'
        }
    
    async def _execute_single_query(self, query: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Execute a single query with proper time handling"""
        try:
            if start_time and end_time:
                return await self.prometheus_client.query_range(query, start_time, end_time, step='15s')
            else:
                return await self.prometheus_client.query_instant(query)
        except Exception as e:
            return {'error': str(e)}
    
    async def _execute_and_process_queries(self, queries: Dict[str, str], metric_type: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Execute multiple queries and process results with top 5 values"""
        try:
            if start_time and end_time:
                metrics_data = await self.prometheus_client.query_multiple_range(
                    queries, start_time, end_time, step='15s'
                )
            else:
                metrics_data = await self.prometheus_client.query_multiple_instant(queries)
            
            result = {
                'metric_type': metric_type,
                'queries': queries,
                'top5_avg': {},
                'top5_max': {},
                'unit': self._get_metric_unit_by_type(metric_type)
            }
            
            for query_name, data in metrics_data.items():
                if 'error' in data:
                    continue
                
                # Get top 5 values for this query
                if 'avg' in query_name:
                    result['top5_avg'][query_name] = self._get_top5_values(data, 'avg')
                elif 'max' in query_name:
                    result['top5_max'][query_name] = self._get_top5_values(data, 'max')
            
            return result
            
        except Exception as e:
            return {'error': str(e), 'metric_type': metric_type}
    
    def _get_top5_values(self, data: Dict[str, Any], stat_type: str = 'avg', label_formatter: Optional[callable] = None) -> List[Dict[str, Any]]:
        """Get top 5 values from query result"""
        if 'result' not in data:
            return []
        
        values_list = []
        
        for result_item in data['result']:
            labels = result_item.get('metric', {})
            
            # Calculate value based on data type
            if 'value' in result_item:
                # Instant query
                _, value = result_item['value']
                numeric_value = self._to_number(value)
                if numeric_value is None:
                    continue
            elif 'values' in result_item:
                # Range query - calculate avg or max
                values = []
                for _, val in result_item['values']:
                    num = self._to_number(val)
                    if num is not None:
                        values.append(num)
                
                if not values:
                    continue
                
                numeric_value = sum(values) / len(values) if stat_type == 'avg' else max(values)
            else:
                continue
            
            # Format label if formatter provided
            if label_formatter:
                label_key = label_formatter(labels)
            else:
                # Default label formatting
                label_parts = []
                for key in ['resource', 'verb', 'scope', 'instance', 'group', 'kind', 'operation', 'type', 'service', 'event', 'name']:
                    if key in labels:
                        label_parts.append(labels[key])
                label_key = ':'.join(label_parts) if label_parts else 'unknown'
            
            values_list.append({
                'label': label_key,
                'value': round(numeric_value, 6),
                'raw_labels': labels
            })
        
        # Sort by value descending and take top 5
        values_list.sort(key=lambda x: x['value'], reverse=True)
        return values_list[:5]
    
    def _get_metric_unit_by_type(self, metric_type: str) -> str:
        """Get unit for metric type"""
        unit_mapping = {
            'readonly_latency': 'seconds',
            'mutating_latency': 'seconds',
            'basic_api': 'mixed',
            'watch_events': 'events/second',
            'cache_list': 'operations/second',
            'watch_cache_received': 'events/second',
            'watch_cache_dispatched': 'events/second',
            'rest_client': 'seconds',
            'ovnkube_controller': 'updates/second',
            'etcd_requests': 'requests/second'
        }
        return unit_mapping.get(metric_type, 'count')
    
    def _process_metric_data(self, metric_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process individual metric data"""
        processed = {
            'metric_name': metric_name,
            'values': [],
            'statistics': {},
            'unit': self._get_metric_unit(metric_name)
        }
        
        if 'result' not in data:
            processed['error'] = 'No result data'
            return processed
        
        all_values = []
        
        for result_item in data['result']:
            item_data = {
                'labels': {k: v for k, v in result_item.get('metric', {}).items() if k != '__name__'},
                'values': []
            }
            
            # Handle instant query results
            if 'value' in result_item:
                timestamp, value = result_item['value']
                numeric_value = self._to_number(value)
                if numeric_value is None:
                    continue
                item_data['values'].append({
                    'timestamp': float(timestamp),
                    'value': numeric_value
                })
                all_values.append(numeric_value)
            
            # Handle range query results
            elif 'values' in result_item:
                for timestamp, value in result_item['values']:
                    numeric_value = self._to_number(value)
                    if numeric_value is None:
                        continue
                    item_data['values'].append({
                        'timestamp': float(timestamp),
                        'value': numeric_value
                    })
                    all_values.append(numeric_value)
            
            if item_data['values']:
                processed['values'].append(item_data)
        
        # Calculate statistics
        if all_values:
            finite_values = [v for v in all_values if self._is_finite(v)]
            if finite_values:
                processed['statistics'] = {
                    'count': len(finite_values),
                    'min': min(finite_values),
                    'max': max(finite_values),
                    'avg': sum(finite_values) / len(finite_values),
                    'p50': self._percentile(finite_values, 50),
                    'p90': self._percentile(finite_values, 90),
                    'p99': self._percentile(finite_values, 99)
                }
        
        return processed
    
    def _get_metric_unit(self, metric_name: str) -> str:
        """Get the unit for a metric"""
        if 'latency' in metric_name or 'duration' in metric_name:
            return 'seconds'
        elif 'rate' in metric_name or 'request' in metric_name:
            return 'requests/second'
        elif 'inflight' in metric_name:
            return 'requests'
        else:
            return 'count'
    
    def _percentile(self, values: List[float], percentile: int) -> float:
        """Calculate percentile of values"""
        if not values:
            return 0.0
        
        # Filter out non-finite values to avoid NaN in results
        sorted_values = sorted([v for v in values if self._is_finite(v)])
        if not sorted_values:
            return 0.0
        k = (len(sorted_values) - 1) * percentile / 100
        f = int(k)
        c = k - f
        
        if f == len(sorted_values) - 1:
            return sorted_values[f]
        else:
            return sorted_values[f] * (1 - c) + sorted_values[f + 1] * c
    
    def _to_number(self, value: Any) -> Optional[float]:
        """Convert input to a finite float; return None for NaN/Inf or invalid."""
        try:
            num = float(value)
            if self._is_finite(num):
                return num
            return None
        except (ValueError, TypeError):
            return None

    def _is_finite(self, num: float) -> bool:
        return not math.isnan(num) and not math.isinf(num)
    
    def _generate_comprehensive_summary(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive summary of all API server metrics"""
        summary = {
            'total_metric_types': len(metrics),
            'performance_overview': {},
            'top_issues': [],
            'health_scores': {},
            'overall_health': 100
        }
        
        # Analyze readonly latency
        if 'readonly_latency' in metrics:
            readonly_data = metrics['readonly_latency']
            avg_top = readonly_data.get('top5_avg', {}).get('avg_ro_apicalls_latency', [])
            max_top = readonly_data.get('top5_max', {}).get('max_ro_apicalls_latency', [])
            
            if avg_top or max_top:
                highest_avg = avg_top[0]['value'] if avg_top else 0
                highest_max = max_top[0]['value'] if max_top else 0
                
                summary['performance_overview']['readonly_latency'] = {
                    'highest_avg_p99': round(highest_avg, 4),
                    'highest_max_p99': round(highest_max, 4),
                    'status': self._evaluate_latency_status(max(highest_avg, highest_max))
                }
                
                if highest_max > 1.0:
                    summary['overall_health'] -= 20
                    summary['top_issues'].append(f"High readonly latency: {highest_max:.3f}s")
        
        # Analyze mutating latency
        if 'mutating_latency' in metrics:
            mutating_data = metrics['mutating_latency']
            avg_top = mutating_data.get('top5_avg', {}).get('avg_mutating_apicalls_latency', [])
            max_top = mutating_data.get('top5_max', {}).get('max_mutating_apicalls_latency', [])
            
            if avg_top or max_top:
                highest_avg = avg_top[0]['value'] if avg_top else 0
                highest_max = max_top[0]['value'] if max_top else 0
                
                summary['performance_overview']['mutating_latency'] = {
                    'highest_avg_p99': round(highest_avg, 4),
                    'highest_max_p99': round(highest_max, 4),
                    'status': self._evaluate_latency_status(max(highest_avg, highest_max))
                }
                
                if highest_max > 2.0:
                    summary['overall_health'] -= 25
                    summary['top_issues'].append(f"High mutating latency: {highest_max:.3f}s")
        
        # Analyze watch events
        if 'watch_events' in metrics:
            watch_data = metrics['watch_events']
            top_avg = watch_data.get('top5_avg', [])
            top_max = watch_data.get('top5_max', [])
            
            if top_avg or top_max:
                highest_rate = max(
                    top_avg[0]['value'] if top_avg else 0,
                    top_max[0]['value'] if top_max else 0
                )
                summary['performance_overview']['watch_events'] = {
                    'highest_rate': round(highest_rate, 2),
                    'top_consumer': top_max[0]['label'] if top_max else 'unknown'
                }
        
        # Analyze etcd requests
        if 'etcd_requests' in metrics:
            etcd_data = metrics['etcd_requests']
            top_avg = etcd_data.get('top5_avg', [])
            top_max = etcd_data.get('top5_max', [])
            
            if top_avg or top_max:
                highest_rate = max(
                    top_avg[0]['value'] if top_avg else 0,
                    top_max[0]['value'] if top_max else 0
                )
                summary['performance_overview']['etcd_requests'] = {
                    'highest_rate': round(highest_rate, 2),
                    'top_operation': top_max[0]['label'] if top_max else 'unknown'
                }
        
        # Calculate individual health scores
        for metric_type in ['readonly_latency', 'mutating_latency', 'watch_events', 'etcd_requests']:
            if metric_type in summary['performance_overview']:
                summary['health_scores'][metric_type] = self._calculate_metric_health_score(
                    metric_type, summary['performance_overview'][metric_type]
                )
        
        # Overall health assessment
        if summary['overall_health'] >= 90:
            summary['overall_status'] = 'excellent'
        elif summary['overall_health'] >= 80:
            summary['overall_status'] = 'good'
        elif summary['overall_health'] >= 70:
            summary['overall_status'] = 'warning'
        else:
            summary['overall_status'] = 'critical'
        
        return summary
    
    def _calculate_metric_health_score(self, metric_type: str, metric_data: Dict[str, Any]) -> int:
        """Calculate health score for individual metric type"""
        if metric_type in ['readonly_latency', 'mutating_latency']:
            max_latency = metric_data.get('highest_max_p99', 0)
            if max_latency <= 0.1:
                return 100
            elif max_latency <= 0.5:
                return 85
            elif max_latency <= 1.0:
                return 70
            elif max_latency <= 2.0:
                return 50
            else:
                return 25
        else:
            # For rate-based metrics, assume healthy if data exists
            return 85 if metric_data.get('highest_rate', 0) > 0 else 50
    
    def _evaluate_latency_status(self, latency: float) -> str:
        """Evaluate latency status"""
        if latency <= 0.1:  # <= 100ms
            return 'excellent'
        elif latency <= 0.5:  # <= 500ms
            return 'good'
        elif latency <= 1.0:  # <= 1s
            return 'warning'
        else:
            return 'critical'
    
    async def get_comprehensive_metrics_summary(self, duration: str = "5m", start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """
        Get comprehensive metrics summary with all metric types
        
        Returns:
            Complete JSON summary with all metrics organized by type
        """
        try:
            # Get all metrics
            all_metrics = await self.get_metrics(duration, start_time, end_time)
            
            # Assemble comprehensive summary
            comprehensive_summary = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'duration': duration,
                'query_period': {
                    'start_time': start_time,
                    'end_time': end_time
                },
                'api_latency': {
                    'readonly': all_metrics['metrics'].get('readonly_latency', {}),
                    'mutating': all_metrics['metrics'].get('mutating_latency', {})
                },
                'api_activity': {
                    'watch_events': all_metrics['metrics'].get('watch_events', {}),
                    'cache_operations': all_metrics['metrics'].get('cache_list', {}),
                    'watch_cache': {
                        'received': all_metrics['metrics'].get('watch_cache_received', {}),
                        'dispatched': all_metrics['metrics'].get('watch_cache_dispatched', {})
                    }
                },
                'component_metrics': {
                    'rest_client': all_metrics['metrics'].get('rest_client', {}),
                    'ovnkube_controller': all_metrics['metrics'].get('ovnkube_controller', {}),
                    'etcd': all_metrics['metrics'].get('etcd_requests', {})
                },
                'health_summary': all_metrics.get('summary', {}),
                'errors': all_metrics.get('errors', [])
            }
            
            return comprehensive_summary
            
        except Exception as e:
            return {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'error': f"Failed to get comprehensive metrics: {str(e)}",
                'duration': duration,
                'query_period': {
                    'start_time': start_time,
                    'end_time': end_time
                }
            }