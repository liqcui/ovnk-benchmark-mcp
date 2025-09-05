"""
Utility functions for OpenShift Benchmark ELT modules
Common functions used across multiple ELT modules
"""

import logging
import re
from typing import Dict, Any, List, Union
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

class EltUtility:
    """Common utility functions for ELT modules"""
    
    def __init__(self):
        self.max_columns = 6
    
    def truncate_text(self, text: str, max_length: int = 30, suffix: str = '...') -> str:
        """Truncate text for display"""
        if len(text) <= max_length:
            return text
        return text[:max_length-len(suffix)] + suffix
    
    def truncate_url(self, url: str, max_length: int = 50) -> str:
        """Truncate URL for display"""
        return self.truncate_text(url, max_length)
    
    def truncate_node_name(self, name: str, max_length: int = 25) -> str:
        """Truncate node name for display"""
        return self.truncate_text(name, max_length)
    
    def truncate_kernel_version(self, kernel_ver: str, max_length: int = 30) -> str:
        """Truncate kernel version for display"""
        return self.truncate_text(kernel_ver, max_length)
    
    def truncate_runtime(self, runtime: str, max_length: int = 25) -> str:
        """Truncate container runtime for display"""
        if len(runtime) <= max_length:
            return runtime
        # Try to keep the version part
        if '://' in runtime:
            protocol, version = runtime.split('://', 1)
            return f"{protocol}://{version[:max_length-len(protocol)-6]}..."
        return runtime[:max_length-3] + '...'
    
    def truncate_os_image(self, os_image: str, max_length: int = 35) -> str:
        """Truncate OS image for display"""
        return self.truncate_text(os_image, max_length)
    
    def parse_cpu_capacity(self, cpu_str: str) -> int:
        """Parse CPU capacity string to integer"""
        try:
            # Handle formats like "32", "32000m"
            if cpu_str.endswith('m'):
                return int(cpu_str[:-1]) // 1000
            return int(cpu_str)
        except (ValueError, TypeError):
            return 0
    
    def parse_memory_capacity(self, memory_str: str) -> float:
        """Parse memory capacity string to GB"""
        try:
            if memory_str.endswith('Ki'):
                # Convert KiB to GB
                kib = int(memory_str[:-2])
                return kib / (1024 * 1024)  # KiB to GB
            elif memory_str.endswith('Mi'):
                # Convert MiB to GB  
                mib = int(memory_str[:-2])
                return mib / 1024  # MiB to GB
            elif memory_str.endswith('Gi'):
                # Already in GiB, close enough to GB
                return float(memory_str[:-2])
            else:
                # Assume it's already in bytes, convert to GB
                return int(memory_str) / (1024**3)
        except (ValueError, TypeError):
            return 0.0
    
    def format_memory_display(self, memory_str: str) -> str:
        """Format memory for display"""
        try:
            gb_value = self.parse_memory_capacity(memory_str)
            if gb_value >= 1:
                return f"{gb_value:.0f} GB"
            else:
                # Show in MB for small values
                return f"{gb_value * 1024:.0f} MB"
        except:
            return memory_str
    
    def categorize_resource_type(self, resource_name: str) -> str:
        """Categorize resource type for better organization"""
        resource_lower = resource_name.lower()
        
        if any(keyword in resource_lower for keyword in ['network', 'policy', 'egress', 'udn']):
            return 'Network & Security'
        elif any(keyword in resource_lower for keyword in ['config', 'secret']):
            return 'Configuration'
        elif any(keyword in resource_lower for keyword in ['pod', 'service']):
            return 'Workloads'
        elif any(keyword in resource_lower for keyword in ['namespace']):
            return 'Organization'
        else:
            return 'Other'
    
    def format_timestamp(self, timestamp_str: str, length: int = 19) -> str:
        """Format timestamp for display"""
        if not timestamp_str:
            return 'Unknown'
        return timestamp_str[:length]
    
    def clean_html(self, html: str) -> str:
        """Clean up HTML (remove newlines and extra whitespace)"""
        html = re.sub(r'\s+', ' ', html.replace('\n', ' ').replace('\r', ''))
        return html.strip()
    
    def create_html_table(self, df: pd.DataFrame, table_name: str) -> str:
        """Generate HTML table from DataFrame with improved styling"""
        try:
            if df.empty:
                return ""
            
            # Create styled HTML table
            html = df.to_html(
                index=False,
                classes='table table-striped table-bordered table-sm',
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
            logger.error(f"Failed to generate HTML table for {table_name}: {e}")
            return f'<div class="alert alert-danger">Error generating table: {str(e)}</div>'
    
    def limit_dataframe_columns(self, df: pd.DataFrame, max_cols: int = None, table_name: str = None) -> pd.DataFrame:
        """Limit DataFrame columns to maximum number"""
        if max_cols is None:
            max_cols = self.max_columns
        
        # Special handling for specific tables that should show all columns or have different limits
        if table_name in ['controlplane_nodes_detail', 'infra_nodes_detail']:
            return df  # Don't limit detail tables
        elif table_name == 'node_distribution':
            return df  # Don't limit node distribution table
        elif table_name == 'node_groups':  # NEW: Allow more columns for node groups
            max_cols = 6  # Node groups can show more columns
        elif table_name == 'network_analysis':  # NEW: Allow more columns for network analysis
            max_cols = 6  # Network analysis can show more columns
        # NEW: OVS-specific table handling
        elif table_name in ['cpu_usage_summary', 'memory_usage_summary', 'dp_flows_top', 'bridge_flows_summary']:
            max_cols = 4  # OVS usage tables get 4 columns for readability
        elif table_name in ['connection_metrics']:
            max_cols = 2  # Connection metrics are simple key-value                        
        elif 'top_' in (table_name or ''):
            max_cols = 4  # Limit top usage tables to 4 columns for readability
        elif 'summary' in (table_name or '') or 'metadata' in (table_name or ''):
            max_cols = 2  # Limit summary/metadata tables to 2 columns for better readability
        elif table_name == 'alerts' and len(df.columns) > 4:
            max_cols = 4  # NEW: Limit alerts to 4 columns
        elif table_name in ['cluster_health', 'resource_utilization', 'cluster_operators', 'mcp_status']:  # NEW: 2-column for status tables
            max_cols = 2
            
        if len(df.columns) <= max_cols:
            return df
        
        # Keep most important columns based on table type
        if table_name in ['summary', 'metadata', 'cluster_health', 'resource_utilization', 'cluster_operators', 'mcp_status']:
            # For summary/status tables, keep metric and value columns
            priority_cols = ['metric', 'value', 'property']
        elif table_name == 'node_groups':
            # For node groups, prioritize key status columns
            priority_cols = ['node_type', 'node type', 'total', 'ready', 'health_score', 'health score', 'cpu_cores', 'cpu cores']
        elif table_name == 'alerts':
            # For alerts, prioritize severity and message
            priority_cols = ['alert', 'severity', 'component', 'message']
        elif table_name in ['cpu_usage_summary', 'memory_usage_summary']:
            # NEW: For OVS usage summaries, prioritize component, node/pod, and key metrics
            priority_cols = ['component', 'node', 'pod', 'max', 'avg', 'cpu', 'memory']
        elif table_name in ['dp_flows_top', 'bridge_flows_summary']:
            # NEW: For flow tables, prioritize instance/bridge and flow counts
            priority_cols = ['instance', 'bridge', 'max', 'avg', 'flows']            
        elif 'top_' in (table_name or ''):
            # For top usage tables, keep rank, name, and main metric columns
            priority_cols = ['rank', 'name', 'node', 'cpu', 'memory', 'max', 'avg']
        else:
            # Default priority columns
            priority_cols = ['name', 'status', 'value', 'count', 'property', 'rank', 'node', 'type', 'ready', 'cpu', 'memory', 'metric']
        
        # Find priority columns that exist
        keep_cols = []
        for col in df.columns:
            col_lower = col.lower()
            if any(priority in col_lower for priority in priority_cols):
                keep_cols.append(col)
        
        # Add remaining columns up to limit
        remaining_cols = [col for col in df.columns if col not in keep_cols]
        while len(keep_cols) < max_cols and remaining_cols:
            keep_cols.append(remaining_cols.pop(0))
        
        return df[keep_cols[:max_cols]] 

    def create_property_value_table(self, data: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """Create a property-value table format"""
        return [{'Property': item.get('Property', ''), 'Value': str(item.get('Value', ''))} for item in data]
    
    def calculate_totals_from_nodes(self, nodes: List[Dict[str, Any]]) -> Dict[str, Union[int, float]]:
        """Calculate total CPU and memory from node list"""
        total_cpu = sum(self.parse_cpu_capacity(node.get('cpu_capacity', '0')) for node in nodes)
        total_memory_gb = sum(self.parse_memory_capacity(node.get('memory_capacity', '0Ki')) for node in nodes)
        ready_count = sum(1 for node in nodes if 'Ready' in node.get('ready_status', ''))
        schedulable_count = sum(1 for node in nodes if node.get('schedulable', False))
        
        return {
            'total_cpu': total_cpu,
            'total_memory_gb': total_memory_gb,
            'ready_count': ready_count,
            'schedulable_count': schedulable_count,
            'count': len(nodes)
        }

