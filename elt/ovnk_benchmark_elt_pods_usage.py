"""
Extract, Load, Transform module for OpenShift Pods Usage
Handles pod usage data from ovnk_benchmark_prometheus_pods_usage.py
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class PodsUsageELT(EltUtility):
    """Extract, Load, Transform class for pod usage data"""
    
    def __init__(self):
        super().__init__()
        
    def extract_pod_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract pod usage metrics from ovnk_benchmark_prometheus_pods_usage.py output"""
        structured = {
            'usage_summary': [],
            'top_cpu_pods': [],
            'top_memory_pods': [],
            'cpu_detailed': [],
            'memory_detailed': []
        }
        
        # Usage collection summary
        query_info = data.get('query_info', {})
        structured['usage_summary'] = [
            {'Property': 'Collection Type', 'Value': data.get('collection_type', 'instant')},
            {'Property': 'Collection Time', 'Value': self.format_timestamp(data.get('collection_timestamp', 'Unknown'))},
            {'Property': 'Total Analyzed', 'Value': data.get('total_analyzed', 0)},
            {'Property': 'Include Containers', 'Value': 'Yes' if data.get('include_containers', False) else 'No'},
            {'Property': 'Duration', 'Value': query_info.get('duration', 'N/A')},
            {'Property': 'Query Step', 'Value': query_info.get('step', 'N/A')}
        ]
        
        # Top 5 CPU usage - simplified view
        cpu_usage = data.get('top_5_cpu_usage', [])
        for item in cpu_usage:
            rank = item.get('rank', 0)
            pod_name = item.get('pod_name', 'unknown')
            node_name = self.truncate_node_name(item.get('node_name', 'unknown'))
            container_name = item.get('container_name', '')
            namespace = item.get('namespace', 'unknown')
            
            # Format pod display name
            if container_name and container_name != 'unknown':
                pod_display = f"{pod_name}:{container_name}"
            else:
                pod_display = pod_name
            
            # Truncate long names
            if len(pod_display) > 35:
                pod_display = pod_display[:32] + '...'
            
            # Get CPU metric value - prioritize the main cpu_usage metric
            cpu_value = 'N/A'
            metrics = item.get('metrics', {})
            
            # Look for cpu_usage metric first
            cpu_metric = metrics.get('cpu_usage', {})
            if cpu_metric:
                if data.get('collection_type') == 'instant':
                    cpu_value = f"{cpu_metric.get('value', 0):.2f}%"
                else:
                    # For duration queries, show avg/max
                    avg_val = cpu_metric.get('avg', 0)
                    max_val = cpu_metric.get('max', 0)
                    cpu_value = f"{avg_val:.1f}% (max: {max_val:.1f}%)"
            
            structured['top_cpu_pods'].append({
                'Rank': rank,
                'Pod': self.truncate_text(pod_display, 30),
                'Node': node_name,
                'CPU Usage': cpu_value
            })
            
            # Detailed CPU metrics for separate table
            if cpu_metric and data.get('collection_type') == 'duration':
                structured['cpu_detailed'].append({
                    'Pod': self.truncate_text(pod_display, 25),
                    'Min %': f"{cpu_metric.get('min', 0):.2f}",
                    'Avg %': f"{cpu_metric.get('avg', 0):.2f}",
                    'Max %': f"{cpu_metric.get('max', 0):.2f}",
                    'Node': self.truncate_node_name(node_name, 20),
                    'Namespace': namespace
                })
        
        # Top 5 Memory usage - simplified view  
        memory_usage = data.get('top_5_memory_usage', [])
        for item in memory_usage:
            rank = item.get('rank', 0)
            pod_name = item.get('pod_name', 'unknown')
            node_name = self.truncate_node_name(item.get('node_name', 'unknown'))
            container_name = item.get('container_name', '')
            namespace = item.get('namespace', 'unknown')
            
            # Format pod display name
            if container_name and container_name != 'unknown':
                pod_display = f"{pod_name}:{container_name}"
            else:
                pod_display = pod_name
            
            # Truncate long names
            if len(pod_display) > 35:
                pod_display = pod_display[:32] + '...'
            
            # Get memory metric value - prioritize memory_usage (working set)
            memory_value = 'N/A'
            metrics = item.get('metrics', {})
            
            # Look for memory_usage metric first, then memory_rss
            memory_metric = metrics.get('memory_usage') or metrics.get('memory_working_set') or metrics.get('memory_rss', {})
            if memory_metric:
                unit = memory_metric.get('unit', 'B')
                if data.get('collection_type') == 'instant':
                    memory_value = f"{memory_metric.get('value', 0)} {unit}"
                else:
                    # For duration queries, show avg/max
                    avg_val = memory_metric.get('avg', 0)
                    max_val = memory_metric.get('max', 0)
                    memory_value = f"{avg_val:.0f} {unit} (max: {max_val:.0f})"
            
            structured['top_memory_pods'].append({
                'Rank': rank,
                'Pod': self.truncate_text(pod_display, 30),
                'Node': node_name,
                'Memory Usage': memory_value
            })
            
            # Detailed Memory metrics for separate table
            if memory_metric and data.get('collection_type') == 'duration':
                unit = memory_metric.get('unit', 'MB')
                structured['memory_detailed'].append({
                    'Pod': self.truncate_text(pod_display, 25),
                    f'Min {unit}': f"{memory_metric.get('min', 0):.0f}",
                    f'Avg {unit}': f"{memory_metric.get('avg', 0):.0f}",
                    f'Max {unit}': f"{memory_metric.get('max', 0):.0f}",
                    'Node': self.truncate_node_name(node_name, 20),
                    'Namespace': namespace
                })
        
        return structured

    def summarize_pod_usage(self, data: Dict[str, Any]) -> str:
        """Generate pod usage summary"""
        summary = ["Pod Usage Analysis:"]
        
        if 'usage_summary' in data:
            collection_type = next((item['Value'] for item in data['usage_summary'] if item['Property'] == 'Collection Type'), 'unknown')
            total_analyzed = next((item['Value'] for item in data['usage_summary'] if item['Property'] == 'Total Analyzed'), 0)
            duration = next((item['Value'] for item in data['usage_summary'] if item['Property'] == 'Duration'), 'N/A')
            
            if duration != 'N/A':
                summary.append(f"• {collection_type} collection over {duration} ({total_analyzed} pods analyzed)")
            else:
                summary.append(f"• {collection_type} collection ({total_analyzed} pods analyzed)")
        
        # Top CPU consumer
        if 'top_cpu_pods' in data and data['top_cpu_pods']:
            top_cpu = data['top_cpu_pods'][0]
            pod_name = top_cpu.get('Pod', 'unknown')
            cpu_usage = top_cpu.get('CPU Usage', 'N/A')
            summary.append(f"• Top CPU: {pod_name} ({cpu_usage})")
        
        # Top Memory consumer
        if 'top_memory_pods' in data and data['top_memory_pods']:
            top_memory = data['top_memory_pods'][0]
            pod_name = top_memory.get('Pod', 'unknown')
            memory_usage = top_memory.get('Memory Usage', 'N/A')
            summary.append(f"• Top Memory: {pod_name} ({memory_usage})")
        
        # Add collection info
        if 'cpu_detailed' in data and data['cpu_detailed']:
            summary.append(f"• Detailed metrics available for {len(data['cpu_detailed'])} pods")
        
        return " ".join(summary)

    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames with proper column limiting"""
        dataframes = {}
        
        try:
            for key, value in structured_data.items():
                if isinstance(value, list) and value:
                    df = pd.DataFrame(value)
                    if not df.empty:
                        # Apply specific column limiting based on table type
                        if key in ['usage_summary']:
                            # Summary tables - limit to 2 columns for better readability
                            df = self.limit_dataframe_columns(df, max_cols=2, table_name=key)
                        elif key in ['top_cpu_pods', 'top_memory_pods']:
                            # Top usage tables - limit to 4 columns for readability
                            df = self.limit_dataframe_columns(df, max_cols=4, table_name=key)
                        elif key in ['cpu_detailed', 'memory_detailed']:
                            # Detailed tables - allow up to 6 columns but prioritize key metrics
                            df = self.limit_dataframe_columns(df, max_cols=6, table_name=key)
                        else:
                            # Default limiting
                            df = self.limit_dataframe_columns(df, table_name=key)
                        
                        dataframes[key] = df
                        
        except Exception as e:
            logger.error(f"Failed to transform pod usage to DataFrames: {e}")
        
        return dataframes

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            for name, df in dataframes.items():
                if not df.empty:
                    html_tables[name] = self.create_html_table(df, name)
        except Exception as e:
            logger.error(f"Failed to generate pod usage HTML tables: {e}")
        
        return html_tables

