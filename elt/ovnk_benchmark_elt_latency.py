"""
ELT module for OVN-Kubernetes Latency Metrics
Handles extraction, transformation and HTML generation for latency data
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Optional, Union
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class latencyELT(EltUtility):
    """ELT module for OVN-Kubernetes latency metrics"""
    
    def __init__(self):
        super().__init__()
        self.max_columns = 6  # Allow more columns for latency data
    
    def extract_ovn_latency_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVN latency data from collected metrics"""
        try:
            structured = {
                'collection_info': self._extract_collection_info(data),
                'ready_duration_metrics': self._extract_ready_duration_metrics(data),
                'sync_duration_metrics': self._extract_sync_duration_metrics(data), 
                'cni_latency_metrics': self._extract_cni_latency_metrics(data),
                'pod_annotation_metrics': self._extract_pod_annotation_metrics(data),
                'pod_creation_metrics': self._extract_pod_creation_metrics(data),
                'service_latency_metrics': self._extract_service_latency_metrics(data),
                'network_config_metrics': self._extract_network_config_metrics(data),
                'latency_summary': self._extract_latency_summary(data)
            }
            
            return structured
            
        except Exception as e:
            logger.error(f"Failed to extract latency data: {e}")
            return {'error': str(e)}
    
    def _extract_collection_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract collection metadata"""
        return {
            'timestamp': data.get('collection_timestamp', 'Unknown'),
            'query_type': data.get('query_type', 'Unknown'),
            'duration': data.get('query_parameters', {}).get('duration', 'N/A'),
            'total_metrics': data.get('summary', {}).get('total_metrics', 0),
            'successful_metrics': data.get('summary', {}).get('successful_metrics', 0),
            'failed_metrics': data.get('summary', {}).get('failed_metrics', 0)
        }
    
    def _extract_ready_duration_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract ready duration metrics"""
        metrics = []
        ready_data = data.get('ready_duration_metrics', {})
        
        for metric_key, metric_info in ready_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)),
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0))
                }
                
                # Add top pod info
                top_entries = stats.get('top_5', [])
                if top_entries:
                    top_pod = top_entries[0]
                    metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                    metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_sync_duration_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract sync duration metrics"""
        metrics = []
        sync_data = data.get('sync_duration_metrics', {})
        
        for metric_key, metric_info in sync_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)),
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0))
                }
                
                # For sync duration, show top 20 entries
                top_entries = stats.get('top_20', stats.get('top_5', []))
                if top_entries:
                    top_pod = top_entries[0]
                    metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                    metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                    metric_entry['top_resource'] = top_pod.get('resource_name', 'N/A')
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_cni_latency_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract CNI latency metrics"""
        metrics = []
        cni_data = data.get('cni_latency_metrics', {})
        
        for metric_key, metric_info in cni_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)),
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0))
                }
                
                top_entries = stats.get('top_5', [])
                if top_entries:
                    top_pod = top_entries[0]
                    metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                    metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_pod_annotation_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract pod annotation metrics"""
        metrics = []
        pod_data = data.get('pod_annotation_metrics', {})
        
        for metric_key, metric_info in pod_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)),
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0))
                }
                
                top_entries = stats.get('top_5', [])
                if top_entries:
                    top_pod = top_entries[0]
                    metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                    metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_pod_creation_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract pod creation metrics"""
        metrics = []
        creation_data = data.get('pod_creation_metrics', {})
        
        for metric_key, metric_info in creation_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)) if stats.get('max_value') else 'N/A',
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)) if stats.get('avg_value') else 'N/A',
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0)) if stats.get('max_value') else 'unknown'
                }
                
                top_entries = stats.get('top_5', [])
                if top_entries:
                    # Find first non-null entry
                    top_pod = None
                    for entry in top_entries:
                        if entry.get('value') is not None:
                            top_pod = entry
                            break
                    
                    if top_pod:
                        metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                        metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                    else:
                        metric_entry['top_pod'] = 'N/A'
                        metric_entry['top_node'] = 'N/A'
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_service_latency_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract service latency metrics"""
        metrics = []
        service_data = data.get('service_latency_metrics', {})
        
        for metric_key, metric_info in service_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)),
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0))
                }
                
                top_entries = stats.get('top_5', [])
                if top_entries:
                    top_pod = top_entries[0]
                    metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                    metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_network_config_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract network configuration metrics"""
        metrics = []
        network_data = data.get('network_config_metrics', {})
        
        for metric_key, metric_info in network_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)),
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0))
                }
                
                top_entries = stats.get('top_5', [])
                if top_entries:
                    top_pod = top_entries[0]
                    metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                    metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                    # Handle service name for network config metrics
                    if top_pod.get('service_name'):
                        metric_entry['top_service'] = top_pod.get('service_name', 'N/A')
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_latency_summary(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract overall latency summary"""
        summary = data.get('summary', {})
        
        summary_data = {
            'total_metrics': summary.get('total_metrics', 0),
            'successful_metrics': summary.get('successful_metrics', 0),
            'failed_metrics': summary.get('failed_metrics', 0),
            'controller_metrics': summary.get('component_breakdown', {}).get('controller', 0),
            'node_metrics': summary.get('component_breakdown', {}).get('node', 0),
            'overall_max_latency': 'N/A',
            'overall_avg_latency': 'N/A',
            'critical_metric': 'N/A'
        }
        
        # Extract overall latency info
        if 'overall_max_latency' in summary:
            max_info = summary['overall_max_latency']
            readable = max_info.get('readable', {})
            summary_data['overall_max_latency'] = f"{readable.get('value', 0)} {readable.get('unit', '')}"
            summary_data['critical_metric'] = max_info.get('metric', 'N/A')
        
        if 'overall_avg_latency' in summary:
            avg_info = summary['overall_avg_latency']
            readable = avg_info.get('readable', {})
            summary_data['overall_avg_latency'] = f"{readable.get('value', 0)} {readable.get('unit', '')}"
        
        return summary_data
    
    def _extract_top_latencies_detail(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract detailed top latencies for comprehensive view"""
        top_latencies = []
        summary = data.get('summary', {})
        
        for metric in summary.get('top_latencies', [])[:10]:  # Top 10
            latency_entry = {
                'rank': len(top_latencies) + 1,
                'metric_name': self.truncate_metric_name(metric.get('metric_name', 'Unknown')),
                'component': metric.get('component', 'Unknown'),
                'max_latency': f"{metric.get('readable_max', {}).get('value', 0)} {metric.get('readable_max', {}).get('unit', '')}",
                'avg_latency': f"{metric.get('readable_avg', {}).get('value', 0)} {metric.get('readable_avg', {}).get('unit', '')}",
                'data_points': metric.get('data_points', 0),
                'severity': self.categorize_latency_severity(metric.get('max_value', 0))
            }
            top_latencies.append(latency_entry)
        
        return top_latencies
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured latency data to DataFrames"""
        dataframes = {}
        
        try:
            # Collection info
            if 'collection_info' in structured_data:
                collection_data = [
                    {'Property': 'Collection Time', 'Value': structured_data['collection_info']['timestamp'][:19]},
                    {'Property': 'Query Type', 'Value': structured_data['collection_info']['query_type'].title()},
                    {'Property': 'Duration', 'Value': structured_data['collection_info']['duration']},
                    {'Property': 'Total Metrics', 'Value': str(structured_data['collection_info']['total_metrics'])},
                    {'Property': 'Success Rate', 'Value': f"{structured_data['collection_info']['successful_metrics']}/{structured_data['collection_info']['total_metrics']}"}
                ]
                dataframes['latencyelt_collection_info'] = pd.DataFrame(collection_data)
            
            # Ready duration metrics
            if structured_data.get('ready_duration_metrics'):
                df = pd.DataFrame(structured_data['ready_duration_metrics'])
                if not df.empty:
                    dataframes['latencyelt_ready_duration'] = df
            
            # Sync duration metrics
            if structured_data.get('sync_duration_metrics'):
                df = pd.DataFrame(structured_data['sync_duration_metrics'])
                if not df.empty:
                    dataframes['latencyelt_sync_duration'] = df
            
            # CNI latency metrics
            if structured_data.get('cni_latency_metrics'):
                df = pd.DataFrame(structured_data['cni_latency_metrics'])
                if not df.empty:
                    dataframes['latencyelt_cni_latency'] = df
            
            # Pod annotation metrics
            if structured_data.get('pod_annotation_metrics'):
                df = pd.DataFrame(structured_data['pod_annotation_metrics'])
                if not df.empty:
                    dataframes['latencyelt_pod_annotation'] = df
            
            # Pod creation metrics
            if structured_data.get('pod_creation_metrics'):
                df = pd.DataFrame(structured_data['pod_creation_metrics'])
                if not df.empty:
                    dataframes['latencyelt_pod_creation'] = df
            
            # Service latency metrics
            if structured_data.get('service_latency_metrics'):
                df = pd.DataFrame(structured_data['service_latency_metrics'])
                if not df.empty:
                    dataframes['latencyelt_service_latency'] = df
            
            # Network config metrics
            if structured_data.get('network_config_metrics'):
                df = pd.DataFrame(structured_data['network_config_metrics'])
                if not df.empty:
                    dataframes['latencyelt_network_config'] = df
            
            # Latency summary
            if 'latency_summary' in structured_data:
                summary_data = [
                    {'Property': 'Total Metrics', 'Value': str(structured_data['latency_summary']['total_metrics'])},
                    {'Property': 'Controller Metrics', 'Value': str(structured_data['latency_summary']['controller_metrics'])},
                    {'Property': 'Node Metrics', 'Value': str(structured_data['latency_summary']['node_metrics'])},
                    {'Property': 'Overall Max Latency', 'Value': structured_data['latency_summary']['overall_max_latency']},
                    {'Property': 'Overall Avg Latency', 'Value': structured_data['latency_summary']['overall_avg_latency']},
                    {'Property': 'Critical Metric', 'Value': self.truncate_text(structured_data['latency_summary']['critical_metric'], 40)}
                ]
                dataframes['latencyelt_summary'] = pd.DataFrame(summary_data)
            
            # Top latencies detail (from summary)
            top_latencies = self._extract_top_latencies_detail({'summary': structured_data.get('latency_summary', {})})
            if top_latencies:
                dataframes['latencyelt_top_latencies'] = pd.DataFrame(top_latencies)
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to create latency DataFrames: {e}")
            return {}
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables with latency-specific styling"""
        html_tables = {}
        
        try:
            for table_name, df in dataframes.items():
                if df.empty:
                    continue
                
                # Apply column limiting based on table type
                df_limited = self.limit_dataframe_columns(df, table_name=table_name)
                
                # Apply latency-specific formatting
                if 'severity' in df_limited.columns:
                    df_formatted = df_limited.copy()
                    
                    # Create severity badges for max_value column
                    if 'max_value' in df_formatted.columns:
                        df_formatted['max_value'] = df_formatted.apply(
                            lambda row: self._create_latency_badge_html(row['max_value'], row.get('severity', 'unknown')), 
                            axis=1
                        )
                    
                    # Create severity badges for component column
                    if 'component' in df_formatted.columns:
                        df_formatted['component'] = df_formatted['component'].apply(
                            lambda x: f'<span class="badge badge-secondary">{x}</span>'
                        )
                    
                    html_tables[table_name] = self._create_enhanced_html_table(df_formatted, table_name)
                
                elif table_name == 'latencyelt_top_latencies':
                    df_formatted = df_limited.copy()
                    
                    # Highlight rank 1 (critical)
                    if 'rank' in df_formatted.columns:
                        df_formatted['rank'] = df_formatted['rank'].apply(
                            lambda x: f'<span class="badge badge-danger">#{x}</span>' if x == 1 
                            else f'<span class="badge badge-warning">#{x}</span>' if x <= 3
                            else f'<span class="badge badge-info">#{x}</span>'
                        )
                    
                    if 'max_latency' in df_formatted.columns:
                        df_formatted['max_latency'] = df_formatted.apply(
                            lambda row: f'<span class="text-danger font-weight-bold">{row["max_latency"]}</span>' if row.get('rank', 999) == 1 
                            else f'<span class="text-warning">{row["max_latency"]}</span>' if row.get('rank', 999) <= 3
                            else row['max_latency'], 
                            axis=1
                        )
                    
                    html_tables[table_name] = self._create_enhanced_html_table(df_formatted, table_name)
                
                else:
                    html_tables[table_name] = self.create_html_table(df_limited, table_name)
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate latency HTML tables: {e}")
            return {}
    
    def _create_latency_badge_html(self, value: str, severity: str) -> str:
        """Create HTML badge for latency value with severity color"""
        badge_colors = {
            'critical': 'danger',
            'high': 'warning', 
            'medium': 'info',
            'low': 'success',
            'unknown': 'secondary'
        }
        
        color = badge_colors.get(severity, 'secondary')
        return f'<span class="badge badge-{color}">{value}</span>'
    
    def _create_enhanced_html_table(self, df: pd.DataFrame, table_name: str) -> str:
        """Create enhanced HTML table with custom styling for latency data"""
        try:
            # Add custom CSS class based on table type
            css_class = 'table table-striped table-bordered table-sm'
            if 'top_latencies' in table_name:
                css_class += ' table-hover'
            elif 'summary' in table_name:
                css_class += ' table-info'
            
            html = df.to_html(
                index=False,
                classes=css_class,
                escape=False,
                table_id=f"table-{table_name.replace('_', '-')}",
                border=1
            )
            
            # Clean up HTML
            html = self.clean_html(html)
            
            # Add responsive wrapper
            html = f'<div class="table-responsive">{html}</div>'
            
            return html
            
        except Exception as e:
            logger.error(f"Failed to create enhanced HTML table for {table_name}: {e}")
            return f'<div class="alert alert-danger">Error generating table: {str(e)}</div>'
    
    def summarize_ovn_latency_data(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary text for OVN latency data"""
        try:
            summary_parts = []
            
            # Collection info
            if 'collection_info' in structured_data:
                info = structured_data['collection_info']
                summary_parts.append(f"Collected {info['successful_metrics']}/{info['total_metrics']} latency metrics")
                if info['duration'] != 'N/A':
                    summary_parts.append(f"over {info['duration']}")
            
            # Overall latency summary
            if 'latency_summary' in structured_data:
                summary = structured_data['latency_summary']
                if summary['overall_max_latency'] != 'N/A':
                    summary_parts.append(f"• Maximum latency: {summary['overall_max_latency']}")
                if summary['critical_metric'] != 'N/A':
                    metric_name = self.truncate_metric_name(summary['critical_metric'], 25)
                    summary_parts.append(f"• Critical metric: {metric_name}")
                
                summary_parts.append(f"• Controller metrics: {summary['controller_metrics']}")
                summary_parts.append(f"• Node metrics: {summary['node_metrics']}")
            
            # Component breakdown
            component_counts = {}
            for category in ['ready_duration_metrics', 'sync_duration_metrics', 'cni_latency_metrics',
                           'pod_annotation_metrics', 'pod_creation_metrics', 'service_latency_metrics', 
                           'network_config_metrics']:
                if category in structured_data and structured_data[category]:
                    count = len(structured_data[category])
                    category_name = category.replace('_metrics', '').replace('_', ' ').title()
                    component_counts[category_name] = count
            
            if component_counts:
                summary_parts.append("• Metrics by category: " + 
                                   ", ".join([f"{k}: {v}" for k, v in component_counts.items()]))
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate latency summary: {e}")
            return f"Latency summary generation failed: {str(e)}"

# Query functions for latency data collection
async def latencyelt_get_controller_sync_top20(prometheus_client, time: str = None, duration: str = None, end_time: str = None):
    """Get top 20 controller sync duration metrics with detailed pod/node/resource info"""
    from tools.ovnk_benchmark_prometheus_ovnk_latency import OVNLatencyCollector
    
    collector = OVNLatencyCollector(prometheus_client)
    result = await collector.collect_controller_sync_duration(time, duration, end_time)
    
    # Extract top 20 detailed entries
    if 'statistics' in result and 'top_20' in result['statistics']:
        top_20_entries = []
        for entry in result['statistics']['top_20']:
            detailed_entry = {
                'pod_name': entry.get('pod_name', 'N/A'),
                'node_name': entry.get('node_name', 'N/A'),
                'resource_name': entry.get('resource_name', 'N/A'),
                'value_seconds': entry.get('value', 0),
                'readable_value': entry.get('readable_value', {}),
                'formatted_value': f"{entry.get('readable_value', {}).get('value', 0)} {entry.get('readable_value', {}).get('unit', '')}"
            }
            top_20_entries.append(detailed_entry)
        
        return {
            'metric_name': 'ovnkube_controller_sync_duration_seconds',
            'total_entries': len(top_20_entries),
            'max_value': result['statistics'].get('max_value', 0),
            'avg_value': result['statistics'].get('avg_value', 0),
            'top_20_detailed': top_20_entries
        }
    
    return {'error': 'No sync duration data available'}

async def latencyelt_get_comprehensive_metrics(prometheus_client, time: str = None, duration: str = None, end_time: str = None):
    """Get comprehensive latency metrics for all components"""
    from tools.ovnk_benchmark_prometheus_ovnk_latency import collect_enhanced_ovn_latency_metrics
    
    return await collect_enhanced_ovn_latency_metrics(
        prometheus_client, time, duration, end_time,
        include_controller_metrics=True,
        include_node_metrics=True, 
        include_extended_metrics=True
    )

async def latencyelt_get_metric_top5_detail(prometheus_client, metric_name: str, time: str = None, duration: str = None, end_time: str = None):
    """Get detailed top 5 entries for any specific metric"""
    from tools.ovnk_benchmark_prometheus_ovnk_latency import OVNLatencyCollector
    
    collector = OVNLatencyCollector(prometheus_client)
    result = await collector._collect_metric(metric_name, time, duration, end_time)
    
    if 'statistics' in result and result['statistics'].get('count', 0) > 0:
        stats = result['statistics']
        top_entries = stats.get('top_5', [])
        
        detailed_entries = []
        for i, entry in enumerate(top_entries[:5]):
            detailed_entry = {
                'rank': i + 1,
                'pod_name': entry.get('pod_name', 'N/A'),
                'node_name': entry.get('node_name', 'N/A'),
                'value_seconds': entry.get('value', 0),
                'readable_value': entry.get('readable_value', {}),
                'formatted_value': f"{entry.get('readable_value', {}).get('value', 0)} {entry.get('readable_value', {}).get('unit', '')}"
            }
            
            # Add resource name for sync metrics
            if 'resource_name' in entry:
                detailed_entry['resource_name'] = entry.get('resource_name', 'N/A')
            
            # Add service name for network config metrics
            if 'service_name' in entry:
                detailed_entry['service_name'] = entry.get('service_name', 'N/A')
            
            detailed_entries.append(detailed_entry)
        
        return {
            'metric_name': metric_name,
            'component': result.get('component', 'Unknown'),
            'unit': result.get('unit', 'seconds'),
            'total_entries': stats.get('count', 0),
            'max_value': stats.get('max_value', 0),
            'avg_value': stats.get('avg_value', 0),
            'top_5_detailed': detailed_entries
        }
    
    return {'error': f'No data available for metric: {metric_name}'}

async def latencyelt_get_all_metrics_avg_max(prometheus_client, time: str = None, duration: str = None, end_time: str = None):
    """Get avg/max values for all latency metrics with top 5 details"""
    comprehensive_data = await latencyelt_get_comprehensive_metrics(prometheus_client, time, duration, end_time)
    
    if 'error' in comprehensive_data:
        return comprehensive_data
    
    metrics_summary = {}
    
    # Process all metric categories
    categories = [
        'ready_duration_metrics', 'sync_duration_metrics', 'cni_latency_metrics',
        'pod_annotation_metrics', 'pod_creation_metrics', 'service_latency_metrics', 
        'network_config_metrics'
    ]
    
    for category in categories:
        if category in comprehensive_data:
            category_data = comprehensive_data[category]
            
            for metric_key, metric_info in category_data.items():
                if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                    stats = metric_info['statistics']
                    
                    # Extract top 5 pod details
                    top_5_details = []
                    top_entries = stats.get('top_5', stats.get('top_20', [])[:5])
                    
                    for i, entry in enumerate(top_entries[:5]):
                        if entry.get('value') is not None:
                            detail = {
                                'rank': i + 1,
                                'pod_name': entry.get('pod_name', 'N/A'),
                                'node_name': entry.get('node_name', 'N/A'),
                                'value': entry.get('value', 0),
                                'unit': metric_info.get('unit', 'seconds'),
                                'formatted_value': f"{entry.get('readable_value', {}).get('value', 0)} {entry.get('readable_value', {}).get('unit', '')}"
                            }
                            
                            # Add additional fields if present
                            if 'resource_name' in entry:
                                detail['resource_name'] = entry.get('resource_name', 'N/A')
                            if 'service_name' in entry:
                                detail['service_name'] = entry.get('service_name', 'N/A')
                            
                            top_5_details.append(detail)
                    
                    metrics_summary[metric_key] = {
                        'metric_name': metric_info.get('metric_name', metric_key),
                        'component': metric_info.get('component', 'Unknown'),
                        'unit': metric_info.get('unit', 'seconds'),
                        'max_value': stats.get('max_value', 0),
                        'avg_value': stats.get('avg_value', 0),
                        'readable_max': stats.get('readable_max', {}),
                        'readable_avg': stats.get('readable_avg', {}),
                        'data_points': stats.get('count', 0),
                        'top_5_pods': top_5_details
                    }
    
    return {
        'collection_timestamp': comprehensive_data.get('collection_timestamp', ''),
        'query_type': comprehensive_data.get('query_type', 'instant'),
        'total_metrics': len(metrics_summary),
        'metrics_details': metrics_summary,
        'overall_summary': comprehensive_data.get('summary', {})
    }