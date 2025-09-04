"""
Extract, Load, Transform module for OpenShift Node Usage
Handles node usage data from ovnk_benchmark_prometheus_nodes_usage.py
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class NodesUsageELT(EltUtility):
    """Extract, Load, Transform class for node usage data"""
    
    def __init__(self):
        super().__init__()
        
    def extract_node_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract node usage metrics from ovnk_benchmark_prometheus_nodes_usage.py output"""
        structured = {
            'usage_overview': [],
            'controlplane_summary': [],
            'infra_summary': [],
            'worker_summary': [],
            'controlplane_nodes_detail': [],
            'infra_nodes_detail': [],
            'top_cpu_workers': [],
            'top_memory_workers': []
        }
        
        # Usage overview from metadata
        metadata = data.get('metadata', {})
        structured['usage_overview'] = [
            {'Property': 'Query Duration', 'Value': metadata.get('duration', 'Unknown')},
            {'Property': 'Collection Time', 'Value': self.format_timestamp(metadata.get('query_time', 'Unknown'))},
            {'Property': 'Start Time', 'Value': self.format_timestamp(metadata.get('start_time', 'Unknown'))},
            {'Property': 'End Time', 'Value': self.format_timestamp(metadata.get('end_time', 'Unknown'))},
            {'Property': 'Timezone', 'Value': metadata.get('timezone', 'UTC')}
        ]
        
        # Process groups
        groups = data.get('groups', {})
        
        # Control Plane Summary
        controlplane = groups.get('controlplane', {})
        if controlplane.get('count', 0) > 0:
            summary = controlplane.get('summary', {})
            cpu_usage = summary.get('cpu_usage', {})
            memory_usage = summary.get('memory_usage', {})
            network_rx = summary.get('network_rx', {})
            network_tx = summary.get('network_tx', {})
            
            structured['controlplane_summary'] = [
                {'Metric': 'Node Count', 'Value': controlplane.get('count', 0)},
                {'Metric': 'CPU Avg (%)', 'Value': f"{cpu_usage.get('avg', 0):.1f}"},
                {'Metric': 'CPU Max (%)', 'Value': f"{cpu_usage.get('max', 0):.1f}"},
                {'Metric': 'Memory Avg (GB)', 'Value': f"{memory_usage.get('avg', 0)/1024:.1f}"},
                {'Metric': 'Memory Max (GB)', 'Value': f"{memory_usage.get('max', 0)/1024:.1f}"},
                {'Metric': 'Network RX Avg (MB/s)', 'Value': f"{network_rx.get('avg', 0)/1024/1024:.2f}"},
                {'Metric': 'Network TX Avg (MB/s)', 'Value': f"{network_tx.get('avg', 0)/1024/1024:.2f}"}
            ]
            
            # Control Plane Nodes Detail - Extract from nodes array with proper metrics parsing
            cp_nodes = controlplane.get('nodes', [])
            for node in cp_nodes:
                node_name = node.get('name', 'unknown')
                metrics = node.get('metrics', {})
                
                cpu_metrics = metrics.get('cpu_usage', {})
                memory_metrics = metrics.get('memory_usage', {})
                network_rx_metrics = metrics.get('network_rx', {})
                network_tx_metrics = metrics.get('network_tx', {})
                
                structured['controlplane_nodes_detail'].append({
                    'Node Name': self.truncate_node_name(node_name, 35),
                    'CPU Max (%)': f"{cpu_metrics.get('max', 0):.2f}",
                    'CPU Avg (%)': f"{cpu_metrics.get('avg', 0):.2f}",
                    'Memory Max (GB)': f"{memory_metrics.get('max', 0)/1024:.2f}",
                    'Memory Avg (GB)': f"{memory_metrics.get('avg', 0)/1024:.2f}",
                    'Network RX Max (MB/s)': f"{network_rx_metrics.get('max', 0)/1024/1024:.2f}",
                    'Network RX Avg (MB/s)': f"{network_rx_metrics.get('avg', 0)/1024/1024:.2f}",
                    'Network TX Max (MB/s)': f"{network_tx_metrics.get('max', 0)/1024/1024:.2f}",
                    'Network TX Avg (MB/s)': f"{network_tx_metrics.get('avg', 0)/1024/1024:.2f}",
                    'Role': 'Control Plane'
                })
        else:
            structured['controlplane_summary'] = [{'Status': 'No Control Plane nodes found', 'Count': 0}]
        
        # Infra Summary
        infra = groups.get('infra', {})
        if infra.get('count', 0) > 0:
            summary = infra.get('summary', {})
            cpu_usage = summary.get('cpu_usage', {})
            memory_usage = summary.get('memory_usage', {})
            network_rx = summary.get('network_rx', {})
            network_tx = summary.get('network_tx', {})
            
            structured['infra_summary'] = [
                {'Metric': 'Node Count', 'Value': infra.get('count', 0)},
                {'Metric': 'CPU Avg (%)', 'Value': f"{cpu_usage.get('avg', 0):.1f}"},
                {'Metric': 'CPU Max (%)', 'Value': f"{cpu_usage.get('max', 0):.1f}"},
                {'Metric': 'Memory Avg (GB)', 'Value': f"{memory_usage.get('avg', 0)/1024:.1f}"},
                {'Metric': 'Memory Max (GB)', 'Value': f"{memory_usage.get('max', 0)/1024:.1f}"},
                {'Metric': 'Network RX Avg (MB/s)', 'Value': f"{network_rx.get('avg', 0)/1024/1024:.2f}"},
                {'Metric': 'Network TX Avg (MB/s)', 'Value': f"{network_tx.get('avg', 0)/1024/1024:.2f}"}
            ]
            
            # Infra Nodes Detail - Extract from nodes array with proper metrics parsing
            infra_nodes = infra.get('nodes', [])
            for node in infra_nodes:
                node_name = node.get('name', 'unknown')
                metrics = node.get('metrics', {})
                
                cpu_metrics = metrics.get('cpu_usage', {})
                memory_metrics = metrics.get('memory_usage', {})
                network_rx_metrics = metrics.get('network_rx', {})
                network_tx_metrics = metrics.get('network_tx', {})
                
                structured['infra_nodes_detail'].append({
                    'Node Name': self.truncate_node_name(node_name, 35),
                    'CPU Max (%)': f"{cpu_metrics.get('max', 0):.2f}",
                    'CPU Avg (%)': f"{cpu_metrics.get('avg', 0):.2f}",
                    'Memory Max (GB)': f"{memory_metrics.get('max', 0)/1024:.2f}",
                    'Memory Avg (GB)': f"{memory_metrics.get('avg', 0)/1024:.2f}",
                    'Network RX Max (MB/s)': f"{network_rx_metrics.get('max', 0)/1024/1024:.2f}",
                    'Network RX Avg (MB/s)': f"{network_rx_metrics.get('avg', 0)/1024/1024:.2f}",
                    'Network TX Max (MB/s)': f"{network_tx_metrics.get('max', 0)/1024/1024:.2f}",
                    'Network TX Avg (MB/s)': f"{network_tx_metrics.get('avg', 0)/1024/1024:.2f}",
                    'Role': 'Infrastructure'
                })
        else:
            structured['infra_summary'] = [{'Status': 'No Infrastructure nodes found', 'Count': 0}]
        
        # Worker Summary
        worker = groups.get('worker', {})
        if worker.get('count', 0) > 0:
            summary = worker.get('summary', {})
            cpu_usage = summary.get('cpu_usage', {})
            memory_usage = summary.get('memory_usage', {})
            network_rx = summary.get('network_rx', {})
            network_tx = summary.get('network_tx', {})
            
            structured['worker_summary'] = [
                {'Metric': 'Node Count', 'Value': worker.get('count', 0)},
                {'Metric': 'CPU Avg (%)', 'Value': f"{cpu_usage.get('avg', 0):.1f}"},
                {'Metric': 'CPU Max (%)', 'Value': f"{cpu_usage.get('max', 0):.1f}"},
                {'Metric': 'Memory Avg (GB)', 'Value': f"{memory_usage.get('avg', 0)/1024:.1f}"},
                {'Metric': 'Memory Max (GB)', 'Value': f"{memory_usage.get('max', 0)/1024:.1f}"},
                {'Metric': 'Network RX Avg (MB/s)', 'Value': f"{network_rx.get('avg', 0)/1024/1024:.2f}"},
                {'Metric': 'Network TX Avg (MB/s)', 'Value': f"{network_tx.get('avg', 0)/1024/1024:.2f}"}
            ]
        else:
            structured['worker_summary'] = [{'Status': 'No Worker nodes found', 'Count': 0}]
        
        # Top 5 CPU usage workers - Extract from top_usage with proper key names (removed instance column)
        top_cpu = data.get('top_usage', {}).get('cpu', [])
        for i, node in enumerate(top_cpu[:5], 1):
            node_name = node.get('name', 'unknown')
            cpu_max = node.get('cpu_max', 0)
            cpu_avg = node.get('cpu_avg', 0)
            
            structured['top_cpu_workers'].append({
                'Rank': i,
                'Node Name': self.truncate_node_name(node_name),
                'CPU Max (%)': f"{cpu_max:.2f}",
                'CPU Avg (%)': f"{cpu_avg:.2f}",
                'Role': 'Worker'
            })
        
        # Top 5 memory usage workers - Extract from top_usage with proper key names (removed instance column)
        top_memory = data.get('top_usage', {}).get('memory', [])
        for i, node in enumerate(top_memory[:5], 1):
            node_name = node.get('name', 'unknown')
            memory_max = node.get('memory_max', 0)
            memory_avg = node.get('memory_avg', 0)
            
            structured['top_memory_workers'].append({
                'Rank': i,
                'Node Name': self.truncate_node_name(node_name),
                'Memory Max (GB)': f"{memory_max/1024:.2f}",
                'Memory Avg (GB)': f"{memory_avg/1024:.2f}",
                'Role': 'Worker'
            })
        
        # If no top usage data, add placeholder
        if not structured['top_cpu_workers']:
            structured['top_cpu_workers'] = [{'Status': 'No CPU usage data available', 'Nodes': 0}]
        
        if not structured['top_memory_workers']:
            structured['top_memory_workers'] = [{'Status': 'No memory usage data available', 'Nodes': 0}]
        
        return structured

    def summarize_node_usage(self, data: Dict[str, Any]) -> str:
        """Generate node usage summary with control plane and infra node details"""
        summary = ["Node Usage Analysis:"]
        
        # Collection info
        if 'usage_overview' in data:
            duration = next((item['Value'] for item in data['usage_overview'] if item['Property'] == 'Query Duration'), 'Unknown')
            summary.append(f"• Collection Duration: {duration}")
        
        # Node group summaries
        group_summaries = [
            ('controlplane_summary', 'Control Plane'),
            ('infra_summary', 'Infrastructure'),
            ('worker_summary', 'Worker')
        ]
        
        for table_name, group_name in group_summaries:
            if table_name in data and data[table_name]:
                node_count = next((item['Value'] for item in data[table_name] if item['Metric'] == 'Node Count'), 0)
                if node_count > 0:
                    cpu_avg = next((item['Value'] for item in data[table_name] if item['Metric'] == 'CPU Avg (%)'), 'N/A')
                    memory_avg = next((item['Value'] for item in data[table_name] if item['Metric'] == 'Memory Avg (GB)'), 'N/A')
                    summary.append(f"• {group_name}: {node_count} nodes (CPU: {cpu_avg}%, Memory: {memory_avg}GB)")
        
        # Control Plane detailed table in HTML format (no column limit)
        if 'controlplane_nodes_detail' in data and data['controlplane_nodes_detail']:
            cp_count = len(data['controlplane_nodes_detail'])
            summary.append(f"• Control Plane Details: {cp_count} nodes with comprehensive metrics")
            
            # Create HTML table for control plane nodes
            cp_table_html = '<div class="table-responsive mt-2"><table class="table table-striped table-bordered table-sm">'
            cp_table_html += '<thead class="thead-dark"><tr>'
            
            # Table headers (no instance column)
            headers = ['Node Name', 'CPU Max (%)', 'CPU Avg (%)', 'Memory Max (GB)', 'Memory Avg (GB)', 
                    'Network RX Max (MB/s)', 'Network RX Avg (MB/s)', 'Network TX Max (MB/s)', 'Network TX Avg (MB/s)']
            for header in headers:
                cp_table_html += f'<th>{header}</th>'
            cp_table_html += '</tr></thead><tbody>'
            
            # Table rows
            for node in data['controlplane_nodes_detail']:
                cp_table_html += '<tr>'
                for header in headers:
                    value = node.get(header, 'N/A')
                    cp_table_html += f'<td>{value}</td>'
                cp_table_html += '</tr>'
            
            cp_table_html += '</tbody></table></div>'
            summary.append(cp_table_html)
        
        # Infrastructure detailed table in HTML format (no column limit)
        if 'infra_nodes_detail' in data and data['infra_nodes_detail']:
            infra_count = len(data['infra_nodes_detail'])
            summary.append(f"• Infrastructure Details: {infra_count} nodes with comprehensive metrics")
            
            # Create HTML table for infra nodes
            infra_table_html = '<div class="table-responsive mt-2"><table class="table table-striped table-bordered table-sm">'
            infra_table_html += '<thead class="thead-dark"><tr>'
            
            # Table headers (no instance column)
            headers = ['Node Name', 'CPU Max (%)', 'CPU Avg (%)', 'Memory Max (GB)', 'Memory Avg (GB)', 
                    'Network RX Max (MB/s)', 'Network RX Avg (MB/s)', 'Network TX Max (MB/s)', 'Network TX Avg (MB/s)']
            for header in headers:
                infra_table_html += f'<th>{header}</th>'
            infra_table_html += '</tr></thead><tbody>'
            
            # Table rows
            for node in data['infra_nodes_detail']:
                infra_table_html += '<tr>'
                for header in headers:
                    value = node.get(header, 'N/A')
                    infra_table_html += f'<td>{value}</td>'
                infra_table_html += '</tr>'
            
            infra_table_html += '</tbody></table></div>'
            summary.append(infra_table_html)
        
        # Top resource consumers
        if 'top_cpu_workers' in data and data['top_cpu_workers'] and 'Status' not in data['top_cpu_workers'][0]:
            top_cpu_node = data['top_cpu_workers'][0]
            summary.append(f"• Top CPU Worker: {top_cpu_node.get('Node Name', 'unknown')} ({top_cpu_node.get('CPU Max (%)', 'N/A')}%)")
        
        if 'top_memory_workers' in data and data['top_memory_workers'] and 'Status' not in data['top_memory_workers'][0]:
            top_memory_node = data['top_memory_workers'][0]
            memory_value = top_memory_node.get('Memory Max (GB)', 'N/A')
            summary.append(f"• Top Memory Worker: {top_memory_node.get('Node Name', 'unknown')} ({memory_value}GB)")
        
        return " ".join(summary)

    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames - no column limits for detail tables"""
        dataframes = {}
        
        try:
            for key, value in structured_data.items():
                if isinstance(value, list) and value:
                    df = pd.DataFrame(value)
                    if not df.empty:
                        # Don't limit columns for control plane and infra detail tables
                        if key not in ['controlplane_nodes_detail', 'infra_nodes_detail']:
                            df = self.limit_dataframe_columns(df)
                        dataframes[key] = df
                        
        except Exception as e:
            logger.error(f"Failed to transform node usage to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            for name, df in dataframes.items():
                if not df.empty:
                    html_tables[name] = self.create_html_table(df, name)
        except Exception as e:
            logger.error(f"Failed to generate node usage HTML tables: {e}")
        
        return html_tables
