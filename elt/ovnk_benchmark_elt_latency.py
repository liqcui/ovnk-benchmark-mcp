"""
Extract, Load, Transform module for OVN Latency metrics
Processes OVN-Kubernetes latency performance data from Prometheus queries
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class ovnLatencyELT(EltUtility):
    """Extract, Load, Transform class for OVN latency metrics"""
    
    def __init__(self):
        super().__init__()
    
    def extract_ovn_latency_data(self, mcp_results: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVN latency data from MCP results"""
        try:
            structured_data = {
                'metadata': {},
                'ready_duration_metrics': {},
                'sync_duration_metrics': {},
                'percentile_latency_metrics': {},
                'pod_latency_metrics': {},
                'cni_latency_metrics': {},
                'service_latency_metrics': {},
                'network_programming_metrics': {},
                'overall_summary': {}
            }
            
            # Extract metadata
            structured_data['metadata'] = {
                'collection_timestamp': mcp_results.get('collection_timestamp', 'Unknown'),
                'collection_type': mcp_results.get('collection_type', 'Unknown'),
                'query_type': mcp_results.get('query_type', 'Unknown'),
                'timezone': mcp_results.get('timezone', 'UTC')
            }
            
            if 'query_parameters' in mcp_results:
                structured_data['metadata']['query_parameters'] = mcp_results['query_parameters']
            
            # Extract each metric category
            metric_categories = [
                'ready_duration_metrics',
                'sync_duration_metrics', 
                'percentile_latency_metrics',
                'pod_latency_metrics',
                'cni_latency_metrics',
                'service_latency_metrics',
                'network_programming_metrics'
            ]
            
            for category in metric_categories:
                if category in mcp_results:
                    structured_data[category] = self._process_metric_category(mcp_results[category], category)
            
            # Extract overall summary
            if 'overall_summary' in mcp_results:
                structured_data['overall_summary'] = self._process_overall_summary(mcp_results['overall_summary'])
            
            return structured_data
            
        except Exception as e:
            logger.error(f"Failed to extract OVN latency data: {e}")
            return {'error': str(e), 'raw_data': mcp_results}
    
    def _process_metric_category(self, category_data: Dict[str, Any], category_name: str) -> Dict[str, Any]:
        """Process individual metric category data"""
        processed_category = {}
        
        for metric_name, metric_data in category_data.items():
            if isinstance(metric_data, dict) and 'error' not in metric_data:
                processed_metric = {
                    'metric_name': metric_data.get('metric_name', metric_name),
                    'component': metric_data.get('component', 'unknown'),
                    'unit': metric_data.get('unit', 'seconds'),
                    'description': metric_data.get('description', ''),
                    'statistics': metric_data.get('statistics', {})
                }
                processed_category[metric_name] = processed_metric
            else:
                # Handle error cases
                processed_category[metric_name] = {
                    'metric_name': metric_name,
                    'error': metric_data.get('error', 'Unknown error') if isinstance(metric_data, dict) else str(metric_data)
                }
        
        return processed_category
    
    def _process_overall_summary(self, summary_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process overall summary data"""
        processed_summary = {
            'total_metrics_collected': summary_data.get('total_metrics_collected', 0),
            'successful_metrics': summary_data.get('successful_metrics', 0),
            'failed_metrics': summary_data.get('failed_metrics', 0),
            'component_breakdown': summary_data.get('component_breakdown', {}),
            'top_latencies': summary_data.get('top_latencies', [])[:5]  # Limit to top 5
        }
        
        # Add overall statistics if available
        if 'overall_max_latency' in summary_data:
            processed_summary['overall_max_latency'] = summary_data['overall_max_latency']
        if 'overall_avg_latency' in summary_data:
            processed_summary['overall_avg_latency'] = summary_data['overall_avg_latency']
        
        return processed_summary
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform OVN latency data into pandas DataFrames"""
        dataframes = {}
        
        try:
            # Create metadata summary table
            if 'metadata' in structured_data:
                metadata_rows = []
                metadata = structured_data['metadata']
                metadata_rows.append({'Property': 'Collection Time', 'Value': metadata.get('collection_timestamp', 'Unknown')})
                metadata_rows.append({'Property': 'Collection Type', 'Value': metadata.get('collection_type', 'Unknown')})
                metadata_rows.append({'Property': 'Query Type', 'Value': metadata.get('query_type', 'Unknown')})
                metadata_rows.append({'Property': 'Timezone', 'Value': metadata.get('timezone', 'UTC')})
                
                if 'query_parameters' in metadata:
                    params = metadata['query_parameters']
                    if 'duration' in params:
                        metadata_rows.append({'Property': 'Duration', 'Value': params['duration']})
                
                dataframes['latency_metadata'] = pd.DataFrame(metadata_rows)
            
            # Create overall summary table
            if 'overall_summary' in structured_data:
                summary = structured_data['overall_summary']
                summary_rows = []
                
                summary_rows.append({'Property': 'Total Metrics', 'Value': str(summary.get('total_metrics_collected', 0))})
                summary_rows.append({'Property': 'Successful Metrics', 'Value': str(summary.get('successful_metrics', 0))})
                summary_rows.append({'Property': 'Failed Metrics', 'Value': str(summary.get('failed_metrics', 0))})
                
                # Add component breakdown
                component_breakdown = summary.get('component_breakdown', {})
                for component, count in component_breakdown.items():
                    summary_rows.append({'Property': f'{component.title()} Components', 'Value': str(count)})
                
                # Add overall latency statistics
                if 'overall_max_latency' in summary:
                    max_latency = summary['overall_max_latency']
                    readable = max_latency.get('readable', {})
                    summary_rows.append({'Property': 'Max Latency', 'Value': f"{readable.get('value', 0)} {readable.get('unit', 'ms')}"})
                
                if 'overall_avg_latency' in summary:
                    avg_latency = summary['overall_avg_latency']
                    readable = avg_latency.get('readable', {})
                    summary_rows.append({'Property': 'Avg Latency', 'Value': f"{readable.get('value', 0)} {readable.get('unit', 'ms')}"})
                
                dataframes['latency_summary'] = pd.DataFrame(summary_rows)
            
            # Create top latencies table
            if 'overall_summary' in structured_data and 'top_latencies' in structured_data['overall_summary']:
                top_latencies = structured_data['overall_summary']['top_latencies']
                if top_latencies:
                    top_latency_rows = []
                    for i, latency in enumerate(top_latencies[:5], 1):
                        readable_max = latency.get('readable_max', {})
                        top_latency_rows.append({
                            'Rank': i,
                            'Metric': self.truncate_text(latency.get('metric_name', 'Unknown'), 25),
                            'Component': latency.get('component', 'Unknown'),
                            'Category': latency.get('category', 'Unknown').replace('_', ' ').title(),
                            'Max Latency': f"{readable_max.get('value', 0)} {readable_max.get('unit', 'ms')}",
                            'Data Points': str(latency.get('data_points', 0))
                        })
                    
                    df = pd.DataFrame(top_latency_rows)
                    dataframes['top_latencies'] = self.limit_dataframe_columns(df, 4, 'top_latencies')
            
            # Process metric categories
            metric_categories = [
                ('ready_duration_metrics', 'Ready Duration'),
                ('sync_duration_metrics', 'Sync Duration'),
                ('percentile_latency_metrics', 'Percentile Latency'),
                ('pod_latency_metrics', 'Pod Latency'),
                ('cni_latency_metrics', 'CNI Latency'),
                ('service_latency_metrics', 'Service Latency'),
                ('network_programming_metrics', 'Network Programming')
            ]
            
            for category_key, category_display in metric_categories:
                if category_key in structured_data:
                    category_df = self._create_category_dataframe(structured_data[category_key], category_display)
                    if not category_df.empty:
                        table_name = category_key.replace('_metrics', '')
                        dataframes[table_name] = self.limit_dataframe_columns(category_df, 4, table_name)
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform OVN latency data to DataFrames: {e}")
            return {}
    
    def _create_category_dataframe(self, category_data: Dict[str, Any], category_name: str) -> pd.DataFrame:
        """Create DataFrame for a metric category"""
        rows = []
        
        for metric_name, metric_info in category_data.items():
            if 'error' in metric_info:
                rows.append({
                    'Metric': self.truncate_text(metric_name.replace('_', ' ').title(), 25),
                    'Component': 'Error',
                    'Status': 'Failed',
                    'Details': self.truncate_text(str(metric_info['error']), 40)
                })
            else:
                statistics = metric_info.get('statistics', {})
                readable_max = statistics.get('readable_max', {})
                readable_avg = statistics.get('readable_avg', {})
                
                row = {
                    'Metric': self.truncate_text(metric_info.get('metric_name', metric_name).replace('_', ' '), 25),
                    'Component': metric_info.get('component', 'Unknown').title(),
                    'Count': str(statistics.get('count', 0)),
                    'Max': f"{readable_max.get('value', 0)} {readable_max.get('unit', 'ms')}" if readable_max else 'N/A',
                    'Avg': f"{readable_avg.get('value', 0)} {readable_avg.get('unit', 'ms')}" if readable_avg else 'N/A',
                    'Unit': metric_info.get('unit', 'seconds')
                }
                rows.append(row)
        
        return pd.DataFrame(rows)
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables for OVN latency metrics"""
        html_tables = {}
        
        # Define table order and titles
        table_order = [
            ('latency_metadata', 'Collection Metadata'),
            ('latency_summary', 'Overall Summary'),
            ('top_latencies', 'Top Latencies'),
            ('ready_duration', 'Ready Duration Metrics'),
            ('sync_duration', 'Sync Duration Metrics'),
            ('percentile_latency', 'Percentile Latency Metrics'),
            ('pod_latency', 'Pod Latency Metrics'),
            ('cni_latency', 'CNI Latency Metrics'),
            ('service_latency', 'Service Latency Metrics'),
            ('network_programming', 'Network Programming Metrics')
        ]
        
        for table_key, table_title in table_order:
            if table_key in dataframes and not dataframes[table_key].empty:
                df = dataframes[table_key]
                
                # Apply column limiting based on table type
                if table_key in ['latency_metadata', 'latency_summary']:
                    df = self.limit_dataframe_columns(df, 2, table_key)
                else:
                    df = self.limit_dataframe_columns(df, 4, table_key)
                
                html_tables[table_key] = self.create_html_table(df, table_key)
        
        return html_tables
    
    def summarize_ovn_latency_data(self, structured_data: Dict[str, Any]) -> str:
        """Generate a brief summary of OVN latency data"""
        try:
            summary_parts = ["OVN Latency Analysis:"]
            
            # Overall statistics
            if 'overall_summary' in structured_data:
                summary = structured_data['overall_summary']
                total = summary.get('total_metrics_collected', 0)
                successful = summary.get('successful_metrics', 0)
                failed = summary.get('failed_metrics', 0)
                
                summary_parts.append(f"• Collected {total} metrics ({successful} successful, {failed} failed)")
                
                # Component breakdown
                components = summary.get('component_breakdown', {})
                if components:
                    comp_info = []
                    for comp, count in components.items():
                        if count > 0:
                            comp_info.append(f"{count} {comp}")
                    if comp_info:
                        summary_parts.append(f"• Components: {', '.join(comp_info)}")
                
                # Top latency
                if 'overall_max_latency' in summary:
                    max_latency = summary['overall_max_latency']
                    readable = max_latency.get('readable', {})
                    metric_name = max_latency.get('metric', 'Unknown')
                    summary_parts.append(f"• Highest latency: {readable.get('value', 0)} {readable.get('unit', 'ms')} ({metric_name})")
            
            # Query type info
            if 'metadata' in structured_data:
                metadata = structured_data['metadata']
                query_type = metadata.get('query_type', 'Unknown')
                collection_type = metadata.get('collection_type', 'Unknown')
                summary_parts.append(f"• Query type: {query_type} ({collection_type})")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate OVN latency summary: {e}")
            return f"OVN Latency Analysis: Summary generation failed ({str(e)})"