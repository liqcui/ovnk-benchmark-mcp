"""
Kubernetes API Server Metrics Module
Queries and processes Kubernetes API server performance metrics
"""

import asyncio
from datetime import datetime, timezone
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
            # Define API server queries based on metrics.yml
            queries = self._get_api_server_queries(duration)
            
            if start_time and end_time:
                # Use range query for historical data
                metrics_data = await self.prometheus_client.query_multiple_range(
                    queries, start_time, end_time, step='15s'
                )
            else:
                # Use instant query for current data
                metrics_data = await self.prometheus_client.query_multiple_instant(queries)
            
            # Process each metric
            for metric_name, data in metrics_data.items():
                if 'error' in data:
                    result['errors'].append(f"{metric_name}: {data['error']}")
                    continue
                
                processed_metric = self._process_metric_data(metric_name, data)
                result['metrics'][metric_name] = processed_metric
            
            # Generate summary
            result['summary'] = self._generate_summary(result['metrics'])
            
        except Exception as e:
            result['errors'].append(f"Failed to get API server metrics: {str(e)}")
        
        return result
    
    def _get_api_server_queries(self, duration: str) -> Dict[str, str]:
        """Get API server PromQL queries"""
        elapsed_placeholder = f"[{duration}:]"
        
        queries = {
            'avg_ro_apicalls_latency': f'''avg_over_time(histogram_quantile(0.99, sum(irate(apiserver_request_duration_seconds_bucket{{apiserver="kube-apiserver", verb=~"LIST|GET", subresource!~"log|exec|portforward|attach|proxy"}}[2m])) by (le, resource, verb, scope)){elapsed_placeholder}) > 0''',
            
            'max_ro_apicalls_latency': f'''max_over_time(histogram_quantile(0.99, sum(irate(apiserver_request_duration_seconds_bucket{{apiserver="kube-apiserver", verb=~"LIST|GET", subresource!~"log|exec|portforward|attach|proxy"}}[2m])) by (le, resource, verb, scope)){elapsed_placeholder}) > 0''',
            
            'avg_mutating_apicalls_latency': f'''avg_over_time(histogram_quantile(0.99, sum(irate(apiserver_request_duration_seconds_bucket{{apiserver="kube-apiserver", verb=~"POST|PUT|DELETE|PATCH", subresource!~"log|exec|portforward|attach|proxy"}}[2m])) by (le, resource, verb, scope)){elapsed_placeholder}) > 0''',
            
            'max_mutating_apicalls_latency': f'''max_over_time(histogram_quantile(0.99, sum(irate(apiserver_request_duration_seconds_bucket{{apiserver="kube-apiserver", verb=~"POST|PUT|DELETE|PATCH", subresource!~"log|exec|portforward|attach|proxy"}}[2m])) by (le, resource, verb, scope)){elapsed_placeholder}) > 0''',
            
            # Additional useful API server metrics
            'api_request_rate': 'sum(rate(apiserver_request_total{apiserver="kube-apiserver"}[5m])) by (verb, resource)',
            
            'api_request_errors': 'sum(rate(apiserver_request_total{apiserver="kube-apiserver", code!~"2.."}[5m])) by (verb, resource, code)',
            
            'api_server_current_inflight_requests': 'sum(apiserver_current_inflight_requests{apiserver="kube-apiserver"}) by (request_kind)',
            
            'etcd_request_duration': 'histogram_quantile(0.99, sum(rate(etcd_request_duration_seconds_bucket{job=~".*etcd.*"}[5m])) by (le, operation, type))'
        }
        
        return queries
    
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
                try:
                    numeric_value = float(value)
                    item_data['values'].append({
                        'timestamp': float(timestamp),
                        'value': numeric_value
                    })
                    all_values.append(numeric_value)
                except (ValueError, TypeError):
                    continue
            
            # Handle range query results
            elif 'values' in result_item:
                for timestamp, value in result_item['values']:
                    try:
                        numeric_value = float(value)
                        item_data['values'].append({
                            'timestamp': float(timestamp),
                            'value': numeric_value
                        })
                        all_values.append(numeric_value)
                    except (ValueError, TypeError):
                        continue
            
            if item_data['values']:
                processed['values'].append(item_data)
        
        # Calculate statistics
        if all_values:
            processed['statistics'] = {
                'count': len(all_values),
                'min': min(all_values),
                'max': max(all_values),
                'avg': sum(all_values) / len(all_values),
                'p50': self._percentile(all_values, 50),
                'p90': self._percentile(all_values, 90),
                'p99': self._percentile(all_values, 99)
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
        
        sorted_values = sorted(values)
        k = (len(sorted_values) - 1) * percentile / 100
        f = int(k)
        c = k - f
        
        if f == len(sorted_values) - 1:
            return sorted_values[f]
        else:
            return sorted_values[f] * (1 - c) + sorted_values[f + 1] * c
    
    def _generate_summary(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary of API server metrics"""
        summary = {
            'total_metrics': len(metrics),
            'api_performance': {},
            'alerts': [],
            'health_score': 100  # Start with perfect score
        }
        
        # Analyze read-only API calls latency
        if 'avg_ro_apicalls_latency' in metrics and metrics['avg_ro_apicalls_latency'].get('statistics'):
            ro_avg = metrics['avg_ro_apicalls_latency']['statistics'].get('avg', 0)
            ro_p99 = metrics['avg_ro_apicalls_latency']['statistics'].get('p99', 0)
            
            summary['api_performance']['readonly_latency'] = {
                'avg_seconds': round(ro_avg, 4),
                'p99_seconds': round(ro_p99, 4),
                'status': self._evaluate_latency_status(ro_p99)
            }
            
            # Health score impact
            if ro_p99 > 1.0:  # > 1 second is concerning
                summary['health_score'] -= 20
                summary['alerts'].append(f"High read-only API latency: {ro_p99:.3f}s")
            elif ro_p99 > 0.5:  # > 500ms is warning
                summary['health_score'] -= 10
                summary['alerts'].append(f"Elevated read-only API latency: {ro_p99:.3f}s")
        
        # Analyze mutating API calls latency
        if 'avg_mutating_apicalls_latency' in metrics and metrics['avg_mutating_apicalls_latency'].get('statistics'):
            mut_avg = metrics['avg_mutating_apicalls_latency']['statistics'].get('avg', 0)
            mut_p99 = metrics['avg_mutating_apicalls_latency']['statistics'].get('p99', 0)
            
            summary['api_performance']['mutating_latency'] = {
                'avg_seconds': round(mut_avg, 4),
                'p99_seconds': round(mut_p99, 4),
                'status': self._evaluate_latency_status(mut_p99)
            }
            
            # Health score impact
            if mut_p99 > 2.0:  # > 2 seconds is concerning for mutations
                summary['health_score'] -= 25
                summary['alerts'].append(f"High mutating API latency: {mut_p99:.3f}s")
            elif mut_p99 > 1.0:  # > 1 second is warning
                summary['health_score'] -= 15
                summary['alerts'].append(f"Elevated mutating API latency: {mut_p99:.3f}s")
        
        # Analyze request rates and errors
        if 'api_request_rate' in metrics and metrics['api_request_rate'].get('values'):
            total_rate = sum(
                item['values'][-1]['value'] if item['values'] else 0
                for item in metrics['api_request_rate']['values']
            )
            summary['api_performance']['request_rate'] = {
                'total_rps': round(total_rate, 2),
                'status': 'normal' if total_rate > 0 else 'low'
            }
        
        # Check for high inflight requests
        if 'api_server_current_inflight_requests' in metrics and metrics['api_server_current_inflight_requests'].get('statistics'):
            inflight = metrics['api_server_current_inflight_requests']['statistics'].get('max', 0)
            summary['api_performance']['inflight_requests'] = {
                'max_concurrent': int(inflight),
                'status': 'high' if inflight > 100 else 'normal'
            }
            
            if inflight > 100:
                summary['health_score'] -= 10
                summary['alerts'].append(f"High inflight requests: {int(inflight)}")
        
        # Overall health assessment
        if summary['health_score'] >= 90:
            summary['overall_status'] = 'excellent'
        elif summary['health_score'] >= 80:
            summary['overall_status'] = 'good'
        elif summary['health_score'] >= 70:
            summary['overall_status'] = 'warning'
        else:
            summary['overall_status'] = 'critical'
        
        return summary
    
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