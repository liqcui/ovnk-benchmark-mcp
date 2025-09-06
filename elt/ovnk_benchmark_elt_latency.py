"""
OVN-Kubernetes Latency Metrics Collection and Analysis
Prometheus-based latency analysis for OVN-Kubernetes components
Reuses existing ELT infrastructure for consistent table formatting and display
"""

import logging
from typing import Dict, Any, List, Optional, Union, Tuple
import pandas as pd
import json
from datetime import datetime, timedelta
import asyncio
import re

from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class ovnLatencyELT(EltUtility):
    """
    OVN-Kubernetes latency metrics analyzer using Prometheus queries
    """
    
    def __init__(self, prometheus_client=None):
        """
        Initialize the OVN latency analyzer/ELT helper.
        The prometheus_client is optional for pure ELT transformations.
        """
        super().__init__()
        self.prometheus_client = prometheus_client
        self.default_duration = '10m'
        
        # Define OVN latency metrics to collect
        self.latency_metrics = {
            # Controller Ready Duration Metrics
            'controller_ready_duration_metrics': [
                'ovnkube_controller_pod_creation_latency_seconds',
                'ovnkube_controller_network_programming_duration_seconds', 
                'ovnkube_controller_sync_duration_seconds'
            ],
            
            # Node Ready Duration Metrics
            'node_ready_duration_metrics': [
                'ovnkube_node_pod_creation_latency_seconds',
                'ovnkube_node_network_programming_duration_seconds',
                'ovnkube_node_sync_duration_seconds'
            ],
            
            # Sync Duration Metrics
            'sync_duration_metrics': [
                'ovnkube_controller_sync_duration_seconds',
                'ovnkube_node_sync_duration_seconds',
                'ovnkube_controller_service_sync_duration_seconds'
            ],
            
            # Percentile Latency Metrics  
            'percentile_latency_metrics': [
                'ovnkube_controller_pod_creation_latency_seconds_p99',
                'ovnkube_controller_pod_creation_latency_seconds_p95', 
                'ovnkube_node_pod_creation_latency_seconds_p99',
                'ovnkube_node_pod_creation_latency_seconds_p95'
            ],
            
            # Pod-specific Latency Metrics
            'pod_latency_metrics': [
                'ovnkube_controller_pod_creation_latency_seconds',
                'ovnkube_node_pod_creation_latency_seconds',
                'ovnkube_controller_pod_deletion_latency_seconds',
                'ovnkube_node_pod_deletion_latency_seconds'
            ],
            
            # CNI Latency Metrics
            'cni_latency_metrics': [
                'ovnkube_node_cni_request_duration_seconds',
                'ovnkube_node_cni_request_duration_seconds_p99',
                'ovnkube_node_cni_request_duration_seconds_p95'
            ],
            
            # Service Latency Metrics
            'service_latency_metrics': [
                'ovnkube_controller_service_sync_duration_seconds',
                'ovnkube_controller_service_creation_latency_seconds',
                'ovnkube_controller_service_deletion_latency_seconds'
            ],
            
            # Network Programming Metrics
            'network_programming_metrics': [
                'ovnkube_controller_network_programming_duration_seconds',
                'ovnkube_node_network_programming_duration_seconds',
                'ovnkube_controller_network_programming_duration_seconds_p99',
                'ovnkube_node_network_programming_duration_seconds_p99'
            ]
        }

    async def collect_ovn_latency_metrics(self, duration: str = None, 
                                        query_type: str = 'comprehensive') -> Dict[str, Any]:
        """
        Collect comprehensive OVN latency metrics from Prometheus
        
        Args:
            duration: Time duration for metrics collection (default: 10m)
            query_type: Type of query ('comprehensive', 'basic', 'detailed')
            
        Returns:
            Dictionary containing structured latency metrics
        """
        try:
            duration = duration or self.default_duration
            collection_timestamp = datetime.now().isoformat()
            
            logger.info(f"Starting OVN latency metrics collection for duration: {duration}")
            
            # Initialize results structure
            results = {
                'collection_timestamp': collection_timestamp,
                'collection_type': 'enhanced_comprehensive',
                'query_type': query_type,
                'timezone': 'UTC',
                'query_parameters': {
                    'duration': duration
                }
            }
            
            # Collect metrics by category
            collected_metrics = {}
            failed_metrics = {}
            total_metrics = 0
            successful_metrics = 0
            
            for category, metric_names in self.latency_metrics.items():
                logger.info(f"Collecting {category} metrics...")
                category_results = {}
                
                for metric_name in metric_names:
                    total_metrics += 1
                    try:
                        metric_data = await self._query_latency_metric(metric_name, duration)
                        if metric_data and not metric_data.get('error'):
                            category_results[metric_name] = metric_data
                            successful_metrics += 1
                        else:
                            failed_metrics[metric_name] = metric_data.get('error', 'No data returned') if metric_data else 'No data returned'
                    
                    except Exception as e:
                        logger.error(f"Failed to collect metric {metric_name}: {e}")
                        failed_metrics[metric_name] = str(e)
                
                if category_results:
                    collected_metrics[category] = category_results
            
            # Add collected metrics to results
            results.update(collected_metrics)
            
            # Generate overall summary
            results['overall_summary'] = await self._generate_overall_summary(
                collected_metrics, total_metrics, successful_metrics, len(failed_metrics)
            )
            
            # Add failed metrics info if any
            if failed_metrics:
                results['failed_metrics_info'] = failed_metrics
            
            logger.info(f"OVN latency collection completed: {successful_metrics}/{total_metrics} metrics successful")
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to collect OVN latency metrics: {e}")
            return {
                'collection_timestamp': datetime.now().isoformat(),
                'error': str(e),
                'query_type': query_type,
                'collection_type': 'failed'
            }

    async def _query_latency_metric(self, metric_name: str, duration: str) -> Dict[str, Any]:
        """
        Query a specific latency metric from Prometheus
        
        Args:
            metric_name: Name of the metric to query
            duration: Time duration for the query
            
        Returns:
            Dictionary containing metric data and statistics
        """
        try:
            # Build Prometheus query
            query = f'max_over_time({metric_name}[{duration}])'
            
            # Execute query
            result = await self.prometheus_client.custom_query(query)
            
            if not result or 'data' not in result or not result['data'].get('result'):
                return {'error': f'No data found for metric {metric_name}'}
            
            # Parse results
            values = []
            for item in result['data']['result']:
                try:
                    value = float(item['value'][1])
                    values.append(value)
                except (ValueError, IndexError):
                    continue
            
            if not values:
                return {'error': f'No valid values found for metric {metric_name}'}
            
            # Calculate statistics
            statistics = self._calculate_metric_statistics(values)
            
            # Determine component from metric name
            component = self._extract_component_from_metric_name(metric_name)
            
            return {
                'metric_name': metric_name,
                'component': component,
                'unit': 'seconds',
                'description': self._get_metric_description(metric_name),
                'statistics': statistics,
                'query': query,
                'collection_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to query metric {metric_name}: {e}")
            return {'error': str(e)}

    def _calculate_metric_statistics(self, values: List[float]) -> Dict[str, Any]:
        """Calculate statistical measures for metric values"""
        try:
            if not values:
                return {}
            
            sorted_values = sorted(values)
            count = len(values)
            
            statistics = {
                'count': count,
                'min_value': min(values),
                'max_value': max(values),
                'avg_value': sum(values) / count,
                'median_value': sorted_values[count // 2] if count > 0 else 0
            }
            
            # Calculate percentiles if we have enough data points
            if count >= 4:
                p95_idx = int(0.95 * (count - 1))
                p99_idx = int(0.99 * (count - 1))
                statistics['p95_value'] = sorted_values[p95_idx]
                statistics['p99_value'] = sorted_values[p99_idx]
            
            return statistics
            
        except Exception as e:
            logger.error(f"Failed to calculate statistics: {e}")
            return {}

    def _extract_component_from_metric_name(self, metric_name: str) -> str:
        """Extract component type from metric name"""
        if 'ovnkube_controller' in metric_name:
            return 'controller'
        elif 'ovnkube_node' in metric_name:
            return 'node'
        elif 'cni' in metric_name.lower():
            return 'cni'
        else:
            return 'unknown'

    def _get_metric_description(self, metric_name: str) -> str:
        """Get human-readable description for a metric"""
        descriptions = {
            'pod_creation_latency_seconds': 'Pod creation latency',
            'pod_deletion_latency_seconds': 'Pod deletion latency', 
            'network_programming_duration_seconds': 'Network programming duration',
            'sync_duration_seconds': 'Sync operation duration',
            'service_sync_duration_seconds': 'Service sync duration',
            'service_creation_latency_seconds': 'Service creation latency',
            'service_deletion_latency_seconds': 'Service deletion latency',
            'cni_request_duration_seconds': 'CNI request duration'
        }
        
        for key, desc in descriptions.items():
            if key in metric_name:
                component = self._extract_component_from_metric_name(metric_name)
                percentile = ''
                if '_p99' in metric_name:
                    percentile = ' (99th percentile)'
                elif '_p95' in metric_name:
                    percentile = ' (95th percentile)'
                
                return f"{component.title()} {desc}{percentile}"
        
        return metric_name.replace('_', ' ').title()

    async def _generate_overall_summary(self, collected_metrics: Dict[str, Any], 
                                      total_metrics: int, successful_metrics: int, 
                                      failed_metrics: int) -> Dict[str, Any]:
        """Generate overall summary of collected latency metrics"""
        try:
            summary = {
                'total_metrics_collected': total_metrics,
                'successful_metrics': successful_metrics,
                'failed_metrics': failed_metrics,
                'component_breakdown': {},
                'top_latencies': []
            }
            
            # Count metrics by component
            all_metrics = []
            component_counts = {}
            
            for category, metrics in collected_metrics.items():
                for metric_name, metric_data in metrics.items():
                    if 'error' not in metric_data:
                        component = metric_data.get('component', 'unknown')
                        component_counts[component] = component_counts.get(component, 0) + 1
                        
                        # Add to all metrics for top latencies calculation
                        statistics = metric_data.get('statistics', {})
                        max_value = statistics.get('max_value', 0)
                        
                        if max_value > 0:
                            all_metrics.append({
                                'metric_name': metric_name,
                                'component': component,
                                'max_value': max_value,
                                'avg_value': statistics.get('avg_value', 0),
                                'unit': metric_data.get('unit', 'seconds'),
                                'readable_max': self._format_latency_for_summary(max_value)
                            })
            
            summary['component_breakdown'] = component_counts
            
            # Sort and get top 5 latencies
            sorted_metrics = sorted(all_metrics, key=lambda x: x['max_value'], reverse=True)
            summary['top_latencies'] = sorted_metrics[:5]
            
            # Calculate overall statistics
            if all_metrics:
                max_latencies = [m['max_value'] for m in all_metrics]
                avg_latencies = [m['avg_value'] for m in all_metrics]
                
                summary['overall_max_latency'] = {
                    'value': max(max_latencies),
                    'readable': self._format_latency_for_summary(max(max_latencies))
                }
                
                summary['overall_avg_latency'] = {
                    'value': sum(avg_latencies) / len(avg_latencies),
                    'readable': self._format_latency_for_summary(sum(avg_latencies) / len(avg_latencies))
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to generate overall summary: {e}")
            return {
                'total_metrics_collected': total_metrics,
                'successful_metrics': successful_metrics,
                'failed_metrics': failed_metrics,
                'error': str(e)
            }

    def _format_latency_for_summary(self, value_seconds: float) -> Dict[str, Union[str, float]]:
        """Format latency value for summary display"""
        try:
            if value_seconds < 1:
                return {'value': round(value_seconds * 1000, 2), 'unit': 'ms'}
            elif value_seconds < 60:
                return {'value': round(value_seconds, 3), 'unit': 's'}
            elif value_seconds < 3600:
                return {'value': round(value_seconds / 60, 2), 'unit': 'min'}
            else:
                return {'value': round(value_seconds / 3600, 2), 'unit': 'h'}
        except:
            return {'value': value_seconds, 'unit': 'seconds'}

    # ================= ELT-style extraction and presentation methods =================
    def extract_ovn_latency_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVN latency data from JSON results (ELT pipeline entry)."""
        try:
            extracted: Dict[str, Any] = {
                'metadata': {},
                'ready_duration': [],
                'sync_duration': [],
                'percentile_latency': [],
                'pod_latency': [],
                'cni_latency': [],
                'service_latency': [],
                'network_programming': [],
                'top_latencies': [],
                'summary': {}
            }

            extracted['metadata'] = {
                'collection_timestamp': data.get('collection_timestamp', 'unknown'),
                'collection_type': data.get('collection_type', 'unknown'),
                'query_type': data.get('query_type', 'instant'),
                'timezone': data.get('timezone', 'UTC')
            }

            self._extract_metric_category(data.get('ready_duration_metrics', {}), extracted['ready_duration'])
            self._extract_metric_category(data.get('sync_duration_metrics', {}), extracted['sync_duration'])
            self._extract_metric_category(data.get('percentile_latency_metrics', {}), extracted['percentile_latency'])
            self._extract_metric_category(data.get('pod_latency_metrics', {}), extracted['pod_latency'])
            self._extract_metric_category(data.get('cni_latency_metrics', {}), extracted['cni_latency'])
            self._extract_metric_category(data.get('service_latency_metrics', {}), extracted['service_latency'])
            self._extract_metric_category(data.get('network_programming_metrics', {}), extracted['network_programming'])

            overall_summary = data.get('overall_summary', {})
            top_latencies = overall_summary.get('top_latencies', [])
            for idx, metric in enumerate(top_latencies[:10], 1):
                readable_max = metric.get('readable_max', {})
                extracted['top_latencies'].append({
                    'rank': idx,
                    'metric_name': self.truncate_metric_name(metric.get('metric_name', 'unknown'), 35),
                    'component': metric.get('component', 'unknown'),
                    'category': metric.get('category', 'unknown').replace('_', ' ').title() if isinstance(metric.get('category'), str) else 'Unknown',
                    'max_latency': f"{readable_max.get('value', 0)} {readable_max.get('unit', 'ms')}",
                    'data_points': metric.get('data_points', 0)
                })

            extracted['summary'] = self._extract_summary_data(overall_summary)
            return extracted
        except Exception as e:
            logger.error(f"Failed to extract OVN latency data: {e}")
            return {'error': str(e)}

    def _extract_metric_category(self, category_data: Dict[str, Any], output_list: List[Dict[str, Any]]) -> None:
        """Extract metrics from a specific category into a flat list for tables."""
        for metric_key, metric_data in category_data.items():
            if not isinstance(metric_data, dict) or 'error' in metric_data:
                continue
            statistics = metric_data.get('statistics', {}) if isinstance(metric_data.get('statistics'), dict) else {}
            if statistics.get('count', 0) == 0:
                continue
            metric_info = {
                'metric_name': self.truncate_metric_name(metric_data.get('metric_name', metric_key), 30),
                'component': metric_data.get('component', 'unknown'),
                'unit': metric_data.get('unit', 'seconds'),
                'data_points': statistics.get('count', 0)
            }
            readable_max = statistics.get('readable_max', {}) if isinstance(statistics.get('readable_max'), dict) else {}
            readable_avg = statistics.get('readable_avg', {}) if isinstance(statistics.get('readable_avg'), dict) else {}
            metric_info.update({
                'max_latency': f"{readable_max.get('value', 0)} {readable_max.get('unit', 'ms')}",
                'avg_latency': f"{readable_avg.get('value', 0)} {readable_avg.get('unit', 'ms')}"
            })
            top_5_data = statistics.get('top_5', []) if isinstance(statistics.get('top_5'), list) else []
            if top_5_data:
                for idx, entry in enumerate(top_5_data[:5], 1):
                    if not isinstance(entry, dict):
                        continue
                    detailed_entry = metric_info.copy()
                    readable_value = entry.get('readable_value', {}) if isinstance(entry.get('readable_value'), dict) else {}
                    # Resolve pod and node names from multiple possible keys to avoid 'unknown'
                    pod_name_resolved, node_name_resolved = self._resolve_pod_and_node_names(entry)
                    detailed_entry.update({
                        'rank': idx,
                        'pod_name': self.truncate_text(pod_name_resolved, 25),
                        'node_name': self.truncate_text(node_name_resolved, 20),
                        'value': f"{readable_value.get('value', 0)} {readable_value.get('unit', 'ms')}"
                    })
                    if 'resource_name' in entry:
                        # Keep the column name as 'resource_name' to surface in HTML tables
                        detailed_entry['resource_name'] = self.truncate_text(entry.get('resource_name', 'unknown'), 20)
                    if 'service_name' in entry:
                        detailed_entry['service'] = self.truncate_text(entry.get('service_name', 'N/A'), 20)
                        detailed_entry['pod_name'] = 'N/A'
                        detailed_entry['node_name'] = 'N/A'
                    output_list.append(detailed_entry)
            else:
                output_list.append(metric_info)

    def _resolve_pod_and_node_names(self, entry: Dict[str, Any]) -> Tuple[str, str]:
        """Resolve pod_name and node_name robustly from an entry with multiple possible key variants."""
        def first_non_empty(d: Dict[str, Any], keys: List[str], default: str) -> str:
            for k in keys:
                v = d.get(k)
                if isinstance(v, str) and v.strip():
                    return v
            # Also look into nested 'labels' or 'metadata' dicts if present
            labels = d.get('labels') if isinstance(d.get('labels'), dict) else {}
            for k in keys:
                v = labels.get(k)
                if isinstance(v, str) and v.strip():
                    return v
            meta = d.get('metadata') if isinstance(d.get('metadata'), dict) else {}
            for k in keys:
                v = meta.get(k)
                if isinstance(v, str) and v.strip():
                    return v
            return default

        pod_name = first_non_empty(entry, ['pod_name', 'pod', 'podname', 'podName'], 'N/A')
        node_name = first_non_empty(entry, ['node_name', 'node', 'instance', 'nodename', 'nodeName', 'host'], 'unknown')
        return pod_name, node_name

    def _extract_summary_data(self, overall_summary: Dict[str, Any]) -> List[Dict[str, str]]:
        summary_data: List[Dict[str, str]] = []
        if not isinstance(overall_summary, dict):
            return summary_data
        summary_data.append({'property': 'Total Metrics Collected', 'value': str(overall_summary.get('total_metrics_collected', 0))})
        summary_data.append({'property': 'Successful Metrics', 'value': str(overall_summary.get('successful_metrics', 0))})
        summary_data.append({'property': 'Failed Metrics', 'value': str(overall_summary.get('failed_metrics', 0))})
        component_breakdown = overall_summary.get('component_breakdown', {}) if isinstance(overall_summary.get('component_breakdown'), dict) else {}
        for component, count in component_breakdown.items():
            try:
                if int(count) > 0:
                    summary_data.append({'property': f"{str(component).title()} Metrics", 'value': str(count)})
            except Exception:
                continue
        overall_max = overall_summary.get('overall_max_latency', {}) if isinstance(overall_summary.get('overall_max_latency'), dict) else {}
        if 'readable' in overall_max and isinstance(overall_max['readable'], dict):
            readable = overall_max['readable']
            summary_data.append({'property': 'Highest Latency', 'value': f"{readable.get('value', 0)} {readable.get('unit', 'ms')} ({overall_max.get('metric', 'unknown')})"})
        overall_avg = overall_summary.get('overall_avg_latency', {}) if isinstance(overall_summary.get('overall_avg_latency'), dict) else {}
        if 'readable' in overall_avg and isinstance(overall_avg['readable'], dict):
            readable = overall_avg['readable']
            summary_data.append({'property': 'Average Max Latency', 'value': f"{readable.get('value', 0)} {readable.get('unit', 'ms')}"})
        return summary_data

    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        dataframes: Dict[str, pd.DataFrame] = {}
        try:
            if structured_data.get('metadata'):
                metadata_list: List[Dict[str, Any]] = []
                for key, value in structured_data['metadata'].items():
                    metadata_list.append({'property': key.replace('_', ' ').title(), 'value': str(value)})
                if metadata_list:
                    dataframes['latency_metadata'] = pd.DataFrame(metadata_list)
            if structured_data.get('summary'):
                dataframes['latency_summary'] = pd.DataFrame(structured_data['summary'])
            if structured_data.get('top_latencies'):
                dataframes['top_latencies'] = pd.DataFrame(structured_data['top_latencies'])
            categories = {
                'ready_duration': 'Ready Duration Metrics',
                'sync_duration': 'Sync Duration Metrics',
                'percentile_latency': 'Percentile Latency Metrics',
                'pod_latency': 'Pod Latency Metrics',
                'cni_latency': 'CNI Latency Metrics',
                'service_latency': 'Service Latency Metrics',
                'network_programming': 'Network Programming Metrics'
            }
            for category_key in categories.keys():
                category_data = structured_data.get(category_key, [])
                if category_data:
                    df = pd.DataFrame(category_data)
                    if not df.empty:
                        dataframes[category_key] = df
            return dataframes
        except Exception as e:
            logger.error(f"Failed to transform OVN latency data to DataFrames: {e}")
            return {}

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        html_tables: Dict[str, str] = {}
        try:
            for table_name, df in dataframes.items():
                if df.empty:
                    continue
                if table_name in ['latency_metadata', 'latency_summary']:
                    limited_df = self.limit_dataframe_columns(df, 2, table_name)
                else:
                    # Work on a copy and drop the 'component' column if present
                    limited_df = df.copy()
                if 'component' in limited_df.columns:
                    limited_df = limited_df.drop(columns=['component'])
                html_table = self.create_html_table(limited_df, table_name)
                html_tables[table_name] = html_table
            return html_tables
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for OVN latency data: {e}")
            return {}

    def summarize_ovn_latency_data(self, structured_data: Dict[str, Any]) -> str:
        try:
            summary_parts: List[str] = ["OVN Latency Analysis:"]
            metadata = structured_data.get('metadata', {}) if isinstance(structured_data.get('metadata'), dict) else {}
            collection_type = metadata.get('collection_type', 'unknown')
            query_type = metadata.get('query_type', 'instant')
            summary_parts.append(f"• Collection type: {collection_type} ({query_type} query)")
            total_metrics = 0
            categories_with_data: List[str] = []
            categories = ['ready_duration', 'sync_duration', 'percentile_latency', 'pod_latency', 'cni_latency', 'service_latency', 'network_programming']
            for category in categories:
                count = len(structured_data.get(category, [])) if isinstance(structured_data.get(category), list) else 0
                if count > 0:
                    total_metrics += count
                    categories_with_data.append(f"{category.replace('_', ' ')}: {count}")
            if total_metrics > 0:
                summary_parts.append(f"• Total metrics with data: {total_metrics}")
                summary_parts.append(f"• Categories: {', '.join(categories_with_data[:3])}")
            top_latencies = structured_data.get('top_latencies', []) if isinstance(structured_data.get('top_latencies'), list) else []
            if top_latencies:
                top_metric = top_latencies[0]
                summary_parts.append(f"• Highest latency: {top_metric.get('max_latency', 'unknown')} ({top_metric.get('metric_name', 'unknown')})")
            summary_data = structured_data.get('summary', []) if isinstance(structured_data.get('summary'), list) else []
            successful_metrics = next((item.get('value') for item in summary_data if isinstance(item, dict) and 'successful' in str(item.get('property', '')).lower()), '0')
            failed_metrics = next((item.get('value') for item in summary_data if isinstance(item, dict) and 'failed' in str(item.get('property', '')).lower()), '0')
            summary_parts.append(f"• Success rate: {successful_metrics} successful, {failed_metrics} failed")
            return " ".join(summary_parts)
        except Exception as e:
            logger.error(f"Failed to summarize OVN latency data: {e}")
            return f"OVN Latency Analysis summary generation failed: {str(e)}"

# Main extraction functions using ELT infrastructure
def extract_ovn_latency_to_readable_tables(prometheus_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract OVN latency metrics and convert to readable HTML tables using existing ELT infrastructure
    
    Args:
        prometheus_results: Dictionary containing OVN latency metrics from Prometheus
        
    Returns:
        Dictionary with structured data, HTML tables, and summary
    """
    from .ovnk_benchmark_elt_json2table import convert_json_to_tables
    
    try:
        # Use the existing ELT infrastructure to convert data to tables
        result = convert_json_to_tables(
            json_data=prometheus_results,
            table_format="html",
            compact=True
        )
        
        if 'error' in result:
            logger.error(f"Failed to extract OVN latency data: {result['error']}")
            return {
                'success': False,
                'error': result['error'],
                'raw_data': prometheus_results
            }
        
        # Extract the converted data
        html_tables = result.get('html', {})
        summary = result.get('summary', 'OVN latency metrics processed')
        metadata = result.get('metadata', {})
        
        # Create organized response
        response = {
            'success': True,
            'data_type': metadata.get('data_type', 'ovn_latency_metrics'),
            'summary': summary,
            'html_tables': html_tables,
            'metadata': {
                'collection_timestamp': prometheus_results.get('collection_timestamp', datetime.now().isoformat()),
                'query_type': prometheus_results.get('query_type', 'ovn_latency'),
                'tables_generated': len(html_tables),
                'table_names': list(html_tables.keys()) if html_tables else []
            },
            'raw_prometheus_data': prometheus_results
        }
        
        # Add specific OVN latency insights if available
        if 'overall_summary' in prometheus_results:
            overall = prometheus_results['overall_summary']
            response['insights'] = {
                'total_metrics': overall.get('total_metrics_collected', 0),
                'successful_metrics': overall.get('successful_metrics', 0),
                'failed_metrics': overall.get('failed_metrics', 0),
                'top_latency_components': [
                    {
                        'component': lat.get('component', 'Unknown'),
                        'metric': lat.get('metric_name', 'Unknown'),
                        'max_latency': lat.get('readable_max', {})
                    }
                    for lat in overall.get('top_latencies', [])[:3]
                ]
            }
        
        return response
        
    except Exception as e:
        logger.error(f"Failed to extract OVN latency metrics: {e}")
        return {
            'success': False,
            'error': str(e),
            'raw_data': prometheus_results
        }

def format_ovn_latency_response_for_display(extracted_data: Dict[str, Any]) -> str:
    """
    Format the extracted OVN latency data for human-readable display
    
    Args:
        extracted_data: Output from extract_ovn_latency_to_readable_tables
        
    Returns:
        Formatted HTML string for display
    """
    try:
        if not extracted_data.get('success', False):
            error_msg = extracted_data.get('error', 'Unknown error occurred')
            return f"""
            <div class="alert alert-danger">
                <h4>OVN Latency Analysis Failed</h4>
                <p>Error: {error_msg}</p>
            </div>
            """
        
        # Build the display output
        output_parts = []
        
        # Add header with data type and timestamp
        metadata = extracted_data.get('metadata', {})
        data_type = metadata.get('data_type', 'OVN Latency').replace('_', ' ').title()
        timestamp = metadata.get('collection_timestamp', 'Unknown')
        
        output_parts.append(f"""
        <div class="mb-3">
            <span class="badge badge-primary">{data_type}</span>
            <small class="text-muted ml-2">Collected: {timestamp[:19] if timestamp != 'Unknown' else timestamp}</small>
        </div>
        """)
        
        # Add summary
        summary = extracted_data.get('summary', '')
        if summary:
            output_parts.append(f"""
            <div class="alert alert-info">
                <strong>Summary:</strong> {summary}
            </div>
            """)
        
        # Add insights if available
        insights = extracted_data.get('insights', {})
        if insights:
            total_metrics = insights.get('total_metrics', 0)
            successful = insights.get('successful_metrics', 0)
            failed = insights.get('failed_metrics', 0)
            
            output_parts.append(f"""
            <div class="alert alert-light">
                <strong>Metrics Overview:</strong> {total_metrics} total ({successful} successful, {failed} failed)
            </div>
            """)
            
            # Add top latency components
            top_components = insights.get('top_latency_components', [])
            if top_components:
                components_html = []
                for i, comp in enumerate(top_components, 1):
                    readable = comp.get('max_latency', {})
                    value = readable.get('value', 'N/A')
                    unit = readable.get('unit', 'ms')
                    components_html.append(f"{i}. {comp.get('component', 'Unknown')}: {value} {unit}")
                
                output_parts.append(f"""
                <div class="alert alert-warning">
                    <strong>Top Latencies:</strong><br>
                    {'<br>'.join(components_html)}
                </div>
                """)
        
        # Add HTML tables
        html_tables = extracted_data.get('html_tables', {})
        if html_tables:
            # Define preferred table order for OVN latency metrics
            preferred_order = [
                'latency_metadata', 'latency_summary', 'top_latencies',
                'ready_duration', 'sync_duration', 'percentile_latency',
                'pod_latency', 'cni_latency', 'service_latency', 
                'network_programming'
            ]
            
            # Add tables in preferred order
            added_tables = set()
            for table_name in preferred_order:
                if table_name in html_tables:
                    table_title = table_name.replace('_', ' ').title()
                    if table_name == 'latency_metadata':
                        table_title = 'Collection Metadata'
                    elif table_name == 'latency_summary':
                        table_title = 'Overall Summary'
                    elif table_name == 'top_latencies':
                        table_title = 'Top 5 Latencies (Overall)'
                    elif 'duration' in table_name or 'latency' in table_name:
                        table_title = f"Top 5 {table_title} Metrics"
                    
                    output_parts.append(f"""
                    <div class="mt-4">
                        <h5 class="text-primary">{table_title}</h5>
                        {html_tables[table_name]}
                    </div>
                    """)
                    added_tables.add(table_name)
            
            # Add any remaining tables
            for table_name, table_html in html_tables.items():
                if table_name not in added_tables:
                    table_title = table_name.replace('_', ' ').title()
                    output_parts.append(f"""
                    <div class="mt-4">
                        <h5 class="text-primary">{table_title}</h5>
                        {table_html}
                    </div>
                    """)
        else:
            output_parts.append("""
            <div class="alert alert-warning">
                No tables were generated from the latency data.
            </div>
            """)
        
        # Add footer with table count
        table_count = len(html_tables)
        output_parts.append(f"""
        <div class="mt-3 text-muted">
            <small>Generated {table_count} table{'s' if table_count != 1 else ''} from OVN latency metrics</small>
        </div>
        """)
        
        return ''.join(output_parts)
        
    except Exception as e:
        logger.error(f"Failed to format OVN latency response: {e}")
        return f"""
        <div class="alert alert-danger">
            <h4>Display Formatting Failed</h4>
            <p>Error: {str(e)}</p>
        </div>
        """

def get_ovn_latency_brief_summary(prometheus_results: Dict[str, Any]) -> str:
    """
    Get a brief text summary of OVN latency metrics using existing ELT infrastructure
    
    Args:
        prometheus_results: Dictionary containing OVN latency metrics from Prometheus
        
    Returns:
        Brief text summary string
    """
    try:
        # Use the specialized OVN latency ELT module for summary generation
        ovn_elt = ovnLatencyELT()
        
        # Extract structured data first
        structured_data = ovn_elt.extract_ovn_latency_data(prometheus_results)
        
        if 'error' in structured_data:
            return f"OVN Latency Analysis: Failed to process data ({structured_data['error']})"
        
        # Generate summary using the specialized module
        summary = ovn_elt.summarize_ovn_latency_data(structured_data)
        
        return summary
        
    except Exception as e:
        logger.error(f"Failed to generate OVN latency brief summary: {e}")
        return f"OVN Latency Analysis: Summary generation failed ({str(e)})"

# Main entry points for external use
async def collect_and_analyze_ovn_latency(prometheus_client, duration: str = '10m', 
                                        query_type: str = 'comprehensive') -> Dict[str, Any]:
    """
    Main entry point for collecting and analyzing OVN latency metrics
    
    Args:
        prometheus_client: Prometheus client instance
        duration: Duration for metrics collection
        query_type: Type of analysis to perform
        
    Returns:
        Complete analysis with metrics, tables, and summaries
    """
    try:
        # Initialize analyzer
        analyzer = ovnLatencyELT(prometheus_client)
        
        # Collect raw metrics
        raw_metrics = await analyzer.collect_ovn_latency_metrics(duration, query_type)
        
        if 'error' in raw_metrics:
            return {
                'success': False,
                'error': raw_metrics['error'],
                'collection_timestamp': raw_metrics.get('collection_timestamp')
            }
        
        # Extract and format using ELT infrastructure
        extracted_data = extract_ovn_latency_to_readable_tables(raw_metrics)
        
        if not extracted_data.get('success', False):
            return {
                'success': False,
                'error': extracted_data.get('error'),
                'raw_metrics': raw_metrics
            }
        
        # Format for display
        formatted_display = format_ovn_latency_response_for_display(extracted_data)
        
        # Get brief summary
        brief_summary = get_ovn_latency_brief_summary(raw_metrics)
        
        return {
            'success': True,
            'raw_metrics': raw_metrics,
            'extracted_data': extracted_data,
            'formatted_display': formatted_display,
            'brief_summary': brief_summary,
            'analysis_metadata': {
                'analyzer_version': '2.0',
                'collection_duration': duration,
                'query_type': query_type,
                'analysis_timestamp': datetime.now().isoformat(),
                'uses_elt_infrastructure': True
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to collect and analyze OVN latency: {e}")
        return {
            'success': False,
            'error': str(e),
            'analysis_timestamp': datetime.now().isoformat()
        }

# Export main functions
__all__ = [
    'ovnLatencyELT',
    'extract_ovn_latency_to_readable_tables',
    'format_ovn_latency_response_for_display', 
    'get_ovn_latency_brief_summary',
    'collect_and_analyze_ovn_latency'
]