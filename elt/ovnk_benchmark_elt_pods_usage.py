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
            'top_memory_pods': []
        }
        
        # Usage collection summary
        structured['usage_summary'] = [
            {'Property': 'Collection Type', 'Value': data.get('collection_type', 'instant')},
            {'Property': 'Collection Time', 'Value': self.format_timestamp(data.get('collection_timestamp', 'Unknown'))},
            {'Property': 'Total Analyzed', 'Value': data.get('total_analyzed', 0)},
            {'Property': 'Include Containers', 'Value': 'Yes' if data.get('include_containers', False) else 'No'},
            {'Property': 'Duration', 'Value': data.get('query_info', {}).get('duration', 'N/A')}
        ]
        
        # Top 5 CPU usage
        cpu_usage = data.get('top_5_cpu_usage', [])
        for item in cpu_usage:
            rank = item.get('rank', 0)
            pod_name = item.get('pod_name', 'unknown')
            node_name = item.get('node_name', 'unknown')
            container_name = item.get('container_name', '')
            
            # Format pod display name
            if container_name and container_name != 'unknown':
                pod_display = f"{pod_name}:{container_name}"
            else:
                pod_display = pod_name
            
            # Truncate long names
            if len(pod_display) > 45:
                pod_display = pod_display[:42] + '...'
            
            # Get CPU metric value
            cpu_value = 'N/A'
            metrics = item.get('metrics', {})
            for metric_name, metric_data in metrics.items():
                if 'cpu' in metric_name.lower():
                    if data.get('collection_type') == 'instant':
                        cpu_value = f"{metric_data.get('value', 0):.2f}%"
                    else:
                        cpu_value = f"{metric_data.get('max', 0):.2f}%"
                    break
            
            structured['top_cpu_pods'].append({
                'Rank': rank,
                'Pod[:Container]': pod_display,
                'Node': node_name,
                'CPU Usage': cpu_value,
                'Namespace': item.get('namespace', 'unknown')
            })
        
        # Top 5 Memory usage  
        memory_usage = data.get('top_5_memory_usage', [])
        for item in memory_usage:
            rank = item.get('rank', 0)
            pod_name = item.get('pod_name', 'unknown')
            node_name = item.get('node_name', 'unknown')
            container_name = item.get('container_name', '')
            
            # Format pod display name
            if container_name and container_name != 'unknown':
                pod_display = f"{pod_name}:{container_name}"
            else:
                pod_display = pod_name
            
            # Truncate long names
            if len(pod_display) > 45:
                pod_display = pod_display[:42] + '...'
            
            # Get memory metric value
            memory_value = 'N/A'
            metrics = item.get('metrics', {})
            for metric_name, metric_data in metrics.items():
                if 'memory' in metric_name.lower():
                    if data.get('collection_type') == 'instant':
                        memory_value = f"{metric_data.get('value', 0)} {metric_data.get('unit', 'B')}"
                    else:
                        memory_value = f"{metric_data.get('max', 0)} {metric_data.get('unit', 'B')}"
                    break
            
            structured['top_memory_pods'].append({
                'Rank': rank,
                'Pod[:Container]': pod_display,
                'Node': node_name,
                'Memory Usage': memory_value,
                'Namespace': item.get('namespace', 'unknown')
            })
        
        return structured

    def summarize_pod_usage(self, data: Dict[str, Any]) -> str:
        """Generate pod usage summary"""
        summary = ["Pod Usage Analysis:"]
        
        if 'usage_summary' in data:
            collection_type = next((item['Value'] for item in data['usage_summary'] if item['Property'] == 'Collection Type'), 'unknown')
            total_analyzed = next((item['Value'] for item in data['usage_summary'] if item['Property'] == 'Total Analyzed'), 0)
            summary.append(f"• Collection: {collection_type} ({total_analyzed} pods analyzed)")
        
        # Top CPU consumer
        if 'top_cpu_pods' in data and data['top_cpu_pods']:
            top_cpu = data['top_cpu_pods'][0]
            pod_name = top_cpu.get('Pod[:Container]', 'unknown')
            cpu_usage = top_cpu.get('CPU Usage', 'N/A')
            summary.append(f"• Top CPU: {pod_name} ({cpu_usage})")
        
        # Top Memory consumer
        if 'top_memory_pods' in data and data['top_memory_pods']:
            top_memory = data['top_memory_pods'][0]
            pod_name = top_memory.get('Pod[:Container]', 'unknown')
            memory_usage = top_memory.get('Memory Usage', 'N/A')
            summary.append(f"• Top Memory: {pod_name} ({memory_usage})")
        
        return " ".join(summary)
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        dataframes = {}
        
        try:
            for key, value in structured_data.items():
                if isinstance(value, list) and value:
                    df = pd.DataFrame(value)
                    if not df.empty:
                        df = self.limit_dataframe_columns(df)
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