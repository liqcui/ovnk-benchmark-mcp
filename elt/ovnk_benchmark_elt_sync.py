"""
Extract, Load, Transform module for OVN Sync Duration metrics
Handles sync duration metrics from OVN-Kubernetes components
"""

import logging
from typing import Dict, Any, List, Optional
import pandas as pd
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)


class syncDurationELT(EltUtility):
    """ELT module for OVN sync duration metrics"""
    
    def __init__(self):
        super().__init__()
    
    def extract_sync_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVN sync duration data from raw JSON"""
        try:
            # Direct extraction from the sync metrics structure
            extracted = {
                'collection_timestamp': raw_data.get('collection_timestamp'),
                'timezone': raw_data.get('timezone', 'UTC'),
                'controller_ready_duration': raw_data.get('controller_ready_duration', {}),
                'node_ready_duration': raw_data.get('node_ready_duration', {}),
                'controller_sync_duration': raw_data.get('controller_sync_duration', {}),
                'controller_service_rate': raw_data.get('controller_service_rate', {}),
                'overall_summary': raw_data.get('overall_summary', {})
            }
            
            return extracted
            
        except Exception as e:
            logger.error(f"Failed to extract sync data: {e}")
            return {'error': str(e)}
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform sync data into pandas DataFrames"""
        try:
            dataframes = {}
            
            # Transform controller ready duration
            if 'controller_ready_duration' in structured_data and 'error' not in structured_data['controller_ready_duration']:
                controller_ready = structured_data['controller_ready_duration']
                if 'top_10' in controller_ready:
                    controller_data = []
                    for idx, item in enumerate(controller_ready['top_10'][:10], 1):
                        readable = item.get('readable_value', {})
                        controller_data.append({
                            'Rank': idx,
                            'Pod': self.truncate_text(item['pod_name'], 25),
                            'Node': self.truncate_node_name(item['node_name'], 20),
                            'Duration': f"{readable.get('value', 'N/A')} {readable.get('unit', '')}"
                        })
                    
                    if controller_data:
                        df = pd.DataFrame(controller_data)
                        dataframes['controller_ready_duration'] = self.limit_dataframe_columns(df, 4, 'controller_ready_duration')
            
            # Transform node ready duration
            if 'node_ready_duration' in structured_data and 'error' not in structured_data['node_ready_duration']:
                node_ready = structured_data['node_ready_duration']
                if 'top_10' in node_ready:
                    node_data = []
                    for idx, item in enumerate(node_ready['top_10'][:10], 1):
                        readable = item.get('readable_value', {})
                        node_data.append({
                            'Rank': idx,
                            'Pod': self.truncate_text(item['pod_name'], 25),
                            'Node': self.truncate_node_name(item['node_name'], 20),
                            'Duration': f"{readable.get('value', 'N/A')} {readable.get('unit', '')}"
                        })
                    
                    if node_data:
                        df = pd.DataFrame(node_data)
                        dataframes['node_ready_duration'] = self.limit_dataframe_columns(df, 4, 'node_ready_duration')
            
            # Transform controller sync duration
            if 'controller_sync_duration' in structured_data and 'error' not in structured_data['controller_sync_duration']:
                sync_duration = structured_data['controller_sync_duration']
                if 'top_20' in sync_duration:
                    sync_data = []
                    for idx, item in enumerate(sync_duration['top_20'][:15], 1):
                        readable = item.get('readable_value', {})
                        # Extract resource type from pod_resource_name
                        resource_name = item.get('pod_resource_name', item['pod_name'])
                        if ':' in resource_name:
                            pod_name, resource = resource_name.split(':', 1)
                        else:
                            pod_name = resource_name
                            resource = 'unknown'
                        
                        sync_data.append({
                            'Rank': idx,
                            'Pod': self.truncate_text(pod_name, 20),
                            'Resource': self.truncate_text(resource, 15),
                            'Node': self.truncate_node_name(item['node_name'], 20),
                            'Duration': f"{readable.get('value', 'N/A')} {readable.get('unit', '')}"
                        })
                    
                    if sync_data:
                        df = pd.DataFrame(sync_data)
                        dataframes['controller_sync_duration'] = self.limit_dataframe_columns(df, 5, 'controller_sync_duration')
            
            # Transform controller service rate
            if 'controller_service_rate' in structured_data and 'error' not in structured_data['controller_service_rate']:
                service_rate = structured_data['controller_service_rate']
                if 'top_10' in service_rate:
                    service_data = []
                    for idx, item in enumerate(service_rate['top_10'][:10], 1):
                        readable = item.get('readable_value', {})
                        service_data.append({
                            'Rank': idx,
                            'Pod': self.truncate_text(item['pod_name'], 25),
                            'Node': self.truncate_node_name(item['node_name'], 20),
                            'Rate': f"{readable.get('value', 'N/A')} {readable.get('unit', '')}"
                        })
                    
                    if service_data:
                        df = pd.DataFrame(service_data)
                        dataframes['controller_service_rate'] = self.limit_dataframe_columns(df, 4, 'controller_service_rate')
            
            # Transform overall summary
            if 'overall_summary' in structured_data:
                summary = structured_data['overall_summary']
                summary_data = []
                
                # Basic metrics
                summary_data.append({
                    'Property': 'Metrics Collected',
                    'Value': str(summary.get('metrics_collected', 'N/A'))
                })
                summary_data.append({
                    'Property': 'Total Data Points',
                    'Value': str(summary.get('total_data_points', 'N/A'))
                })
                
                # Overall max
                if 'overall_max' in summary:
                    overall_max = summary['overall_max']
                    readable = overall_max.get('readable', {})
                    summary_data.append({
                        'Property': 'Overall Max Duration',
                        'Value': f"{readable.get('value', 'N/A')} {readable.get('unit', '')}"
                    })
                    summary_data.append({
                        'Property': 'Max Duration Pod',
                        'Value': self.truncate_text(overall_max.get('pod_name', 'N/A'), 30)
                    })
                    summary_data.append({
                        'Property': 'Max Duration Section',
                        'Value': overall_max.get('section', 'N/A').replace('_', ' ').title()
                    })
                
                # Overall average
                if 'overall_avg' in summary:
                    overall_avg = summary['overall_avg']
                    readable = overall_avg.get('readable', {})
                    summary_data.append({
                        'Property': 'Overall Avg Duration',
                        'Value': f"{readable.get('value', 'N/A')} {readable.get('unit', '')}"
                    })
                
                if summary_data:
                    df = pd.DataFrame(summary_data)
                    dataframes['sync_summary'] = self.limit_dataframe_columns(df, 2, 'sync_summary')
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform sync data to DataFrames: {e}")
            return {}
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables for sync duration data"""
        try:
            html_tables = {}
            
            # Define table order and titles
            table_info = {
                'sync_summary': 'Sync Duration Summary',
                'controller_ready_duration': 'Controller Ready Duration (Top 10)',
                'node_ready_duration': 'Node Ready Duration (Top 10)',
                'controller_sync_duration': 'Controller Sync Duration (Top 15)',
                'controller_service_rate': 'Controller Service Rate (Top 10)'
            }
            
            for table_name, title in table_info.items():
                if table_name in dataframes and not dataframes[table_name].empty:
                    html_tables[table_name] = self.create_html_table(dataframes[table_name], table_name)
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for sync data: {e}")
            return {}
    
    def summarize_sync_data(self, structured_data: Dict[str, Any]) -> str:
        """Generate a brief summary of sync duration metrics"""
        try:
            summary_parts = ["OVN Sync Duration Metrics Summary:"]
            
            # Overall summary info
            if 'overall_summary' in structured_data:
                overall = structured_data['overall_summary']
                metrics_count = overall.get('metrics_collected', 0)
                total_points = overall.get('total_data_points', 0)
                
                summary_parts.append(f"• {metrics_count} metric types collected with {total_points} total data points")
                
                # Overall max duration
                if 'overall_max' in overall:
                    max_info = overall['overall_max']
                    readable = max_info.get('readable', {})
                    pod_name = max_info.get('pod_name', 'unknown')
                    section = max_info.get('section', 'unknown').replace('_', ' ')
                    
                    summary_parts.append(f"• Maximum duration: {readable.get('value', 'N/A')} {readable.get('unit', '')} from {pod_name} ({section})")
                
                # Overall average
                if 'overall_avg' in overall:
                    avg_info = overall['overall_avg']
                    readable = avg_info.get('readable', {})
                    summary_parts.append(f"• Average duration across all metrics: {readable.get('value', 'N/A')} {readable.get('unit', '')}")
            
            # Controller ready duration
            if 'controller_ready_duration' in structured_data and 'error' not in structured_data['controller_ready_duration']:
                controller_ready = structured_data['controller_ready_duration']
                count = controller_ready.get('count', 0)
                summary_parts.append(f"• Controller ready duration: {count} pods analyzed")
            
            # Node ready duration
            if 'node_ready_duration' in structured_data and 'error' not in structured_data['node_ready_duration']:
                node_ready = structured_data['node_ready_duration']
                count = node_ready.get('count', 0)
                summary_parts.append(f"• Node ready duration: {count} pods analyzed")
            
            # Controller sync duration
            if 'controller_sync_duration' in structured_data and 'error' not in structured_data['controller_sync_duration']:
                sync_duration = structured_data['controller_sync_duration']
                count = sync_duration.get('count', 0)
                summary_parts.append(f"• Controller sync duration: {count} operations analyzed")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate sync data summary: {e}")
            return f"Sync duration summary generation failed: {str(e)}"