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
        elif table_name == 'node_groups':  # Allow more columns for node groups
            max_cols = 6  # Node groups can show more columns
        elif table_name == 'network_analysis':  # Allow more columns for network analysis
            max_cols = 6  # Network analysis can show more columns
        # OVS-specific table handling
        elif table_name in ['cpu_usage_summary', 'memory_usage_summary', 'dp_flows_top', 'bridge_flows_summary']:
            max_cols = 4  # OVS usage tables get 4 columns for readability
        elif table_name in ['connection_metrics']:
            max_cols = 2  # Connection metrics are simple key-value
        # Pods usage specific handling
        elif table_name in ['top_cpu_pods', 'top_memory_pods']:
            max_cols = 4  # Top pods usage tables - 4 columns for readability
        elif table_name in ['cpu_detailed', 'memory_detailed']:
            max_cols = 6  # Detailed pods metrics - allow more columns
        elif table_name == 'usage_summary':
            max_cols = 2  # Usage summary - simple property-value format                      
        elif 'top_' in (table_name or ''):
            max_cols = 4  # Limit top usage tables to 4 columns for readability
        elif 'summary' in (table_name or '') or 'metadata' in (table_name or ''):
            max_cols = 2  # Limit summary/metadata tables to 2 columns for better readability
        elif table_name == 'alerts' and len(df.columns) > 4:
            max_cols = 4  # Limit alerts to 4 columns
        elif table_name in ['cluster_health', 'resource_utilization', 'cluster_operators', 'mcp_status']:  # 2-column for status tables
            max_cols = 2
        # Sync duration specific handling
        elif table_name in ['controller_ready_duration', 'node_ready_duration', 'controller_service_rate']:
            max_cols = 4  # Sync duration tables get 4 columns for readability
        elif table_name == 'controller_sync_duration':
            max_cols = 5  # Controller sync duration needs 5 columns (rank, pod, resource, node, duration)
        elif table_name == 'sync_summary':
            max_cols = 2  # Sync summary - simple property-value format
        # Kube API metrics specific handling
        elif table_name in ['health_summary', 'api_metadata']:
            max_cols = 2  # Health and metadata summaries use 2 columns
        elif table_name in ['readonly_latency', 'mutating_latency']:
            max_cols = 4  # Latency tables get 4 columns (Rank, Resource, Value, Type)
        elif table_name in ['top_issues']:
            max_cols = 2  # Issues table uses 2 columns (Rank, Issue)
        elif 'top' in (table_name or '') and 'kube' in str(table_name).lower():
            max_cols = 4  # Kube API top metrics get 4 columns
        elif 'metrics' in (table_name or '') and any(x in str(table_name) for x in ['watch_events', 'etcd_requests', 'rest_client', 'ovnkube_controller']):
            max_cols = 4  # Component metrics get 4 columns
        # UPDATED: OVN Latency specific handling - allow all columns for full metric names
        elif table_name in ['latency_metadata', 'latency_summary']:
            max_cols = 2  # Latency metadata and summary use 2 columns
        elif table_name == 'top_latencies':
            # Don't limit columns for top latencies to show full metric names
            return df
        elif table_name in ['ready_duration', 'sync_duration', 'percentile_latency', 'pod_latency', 'cni_latency', 'service_latency', 'network_programming']:
            # Don't limit columns for OVN latency category tables to show full metric names
            return df
        elif 'latency' in (table_name or '').lower() and 'ovn' in str(table_name).lower():
            # Don't limit columns for any OVN latency related tables
            return df

        if len(df.columns) <= max_cols:
            return df
        
        # Keep most important columns based on table type
        if table_name in ['summary', 'metadata', 'cluster_health', 'resource_utilization', 'cluster_operators', 'mcp_status', 'usage_summary', 'health_summary', 'api_metadata', 'latency_metadata', 'latency_summary']:
            # For summary/status tables, keep metric and value columns
            priority_cols = ['metric', 'value', 'property']
        elif table_name == 'node_groups':
            # For node groups, prioritize key status columns
            priority_cols = ['node_type', 'node type', 'total', 'ready', 'health_score', 'health score', 'cpu_cores', 'cpu cores']
        elif table_name == 'alerts':
            # For alerts, prioritize severity and message
            priority_cols = ['alert', 'severity', 'component', 'message']
        elif table_name in ['cpu_usage_summary', 'memory_usage_summary']:
            # For OVS usage summaries, prioritize component, node/pod, and key metrics
            priority_cols = ['component', 'node', 'pod', 'max', 'avg', 'cpu', 'memory']
        elif table_name in ['dp_flows_top', 'bridge_flows_summary']:
            # For flow tables, prioritize instance/bridge and flow counts
            priority_cols = ['instance', 'bridge', 'max', 'avg', 'flows']
        elif table_name in ['top_cpu_pods', 'top_memory_pods']:
            # For top pods usage tables, prioritize rank, pod, and usage metrics
            priority_cols = ['rank', 'pod', 'cpu', 'memory', 'usage', 'node']
        elif table_name in ['cpu_detailed', 'memory_detailed']:
            # For detailed pods metrics, prioritize pod name and key metrics
            priority_cols = ['pod', 'min', 'avg', 'max', 'node', 'namespace']            
        elif 'top_' in (table_name or ''):
            # For top usage tables, keep rank, name, and main metric columns
            priority_cols = ['rank', 'name', 'node', 'cpu', 'memory', 'max', 'avg']
        elif table_name in ['controller_ready_duration', 'node_ready_duration', 'controller_service_rate']:
            # For sync duration tables, prioritize rank, pod, node, and duration/rate
            priority_cols = ['rank', 'pod', 'node', 'duration', 'rate']
        elif table_name == 'controller_sync_duration':
            # For controller sync duration, prioritize rank, pod, resource, node, duration
            priority_cols = ['rank', 'pod', 'resource', 'node', 'duration']
        elif table_name == 'sync_summary':
            # For sync summary, prioritize property and value columns
            priority_cols = ['property', 'value']
        # Kube API metrics priority columns            
        elif table_name in ['readonly_latency', 'mutating_latency']:
            # For latency tables, prioritize rank, resource, and values
            priority_cols = ['rank', 'resource', 'avg', 'max', 'p99', 'type', 'value']
        elif table_name == 'top_issues':
            # For issues table, prioritize rank and issue description
            priority_cols = ['rank', 'issue']
        elif 'metrics' in (table_name or '') or '_top' in (table_name or ''):
            # For component metrics tables, prioritize rank, resource, and value
            priority_cols = ['rank', 'resource', 'value', 'unit', 'metric']
        # OVN Latency tables are handled above and return early - these won't be reached
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

    def format_latency_value(self, value: Union[float, int], unit: str = 'seconds') -> str:
        """Format latency value with appropriate unit for display"""
        try:
            if unit == 'seconds' and isinstance(value, (int, float)):
                if value == 0:
                    return '0 ms'
                elif value < 1:
                    return f"{round(value * 1000, 2)} ms"
                elif value < 60:
                    return f"{round(value, 3)} s"
                elif value < 3600:
                    return f"{round(value / 60, 2)} min"
                else:
                    return f"{round(value / 3600, 2)} h"
            else:
                return f"{round(float(value), 4)} {unit}"
        except (ValueError, TypeError):
            return str(value)

    def categorize_latency_severity(self, value: float, unit: str = 'seconds') -> str:
        """Categorize latency severity for color coding"""
        try:
            if unit == 'seconds':
                if value < 0.1:  # Less than 100ms
                    return 'low'
                elif value < 1.0:  # Less than 1 second
                    return 'medium'
                elif value < 5.0:  # Less than 5 seconds
                    return 'high'
                else:  # 5+ seconds
                    return 'critical'
            else:
                # For other units, assume already in appropriate scale
                return 'medium'
        except (ValueError, TypeError):
            return 'unknown'

    def truncate_metric_name(self, metric_name: str, max_length: int = 30) -> str:
        """Truncate metric name for display while preserving key information"""
        if len(metric_name) <= max_length:
            return metric_name
        
        # Try to preserve important parts like percentile info
        if 'p99' in metric_name:
            base_name = metric_name.replace('_p99', '').replace('p99', '')
            truncated = self.truncate_text(base_name, max_length - 4)
            return f"{truncated} p99"
        elif 'p95' in metric_name:
            base_name = metric_name.replace('_p95', '').replace('p95', '')
            truncated = self.truncate_text(base_name, max_length - 4)
            return f"{truncated} p95"
        else:
            return self.truncate_text(metric_name, max_length)

    def extract_component_from_metric(self, metric_name: str) -> str:
        """Extract component type from metric name"""
        metric_lower = metric_name.lower()
        
        if any(keyword in metric_lower for keyword in ['controller', 'ovnkube_controller']):
            return 'Controller'
        elif any(keyword in metric_lower for keyword in ['node', 'ovnkube_node']):
            return 'Node'
        elif any(keyword in metric_lower for keyword in ['cni']):
            return 'CNI'
        elif any(keyword in metric_lower for keyword in ['pod']):
            return 'Pod'
        elif any(keyword in metric_lower for keyword in ['service']):
            return 'Service'
        elif any(keyword in metric_lower for keyword in ['network']):
            return 'Network'
        else:
            return 'Unknown'

    def create_latency_severity_badge(self, value: float, unit: str = 'seconds') -> str:
        """Create HTML badge for latency severity"""
        severity = self.categorize_latency_severity(value, unit)
        formatted_value = self.format_latency_value(value, unit)
        
        badge_colors = {
            'low': 'success',
            'medium': 'warning', 
            'high': 'danger',
            'critical': 'dark',
            'unknown': 'secondary'
        }
        
        color = badge_colors.get(severity, 'secondary')
        return f'<span class="badge badge-{color}">{formatted_value}</span>'

    def sort_latency_metrics_by_priority(self, metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sort latency metrics by priority/importance"""
        def get_priority_score(metric: Dict[str, Any]) -> int:
            """Calculate priority score for a metric (higher = more important)"""
            score = 0
            metric_name = metric.get('metric_name', '').lower()
            
            # Component priority
            component = metric.get('component', '').lower()
            if component == 'controller':
                score += 100
            elif component == 'node':
                score += 80
            elif component == 'cni':
                score += 60
            
            # Metric type priority
            if 'ready' in metric_name:
                score += 50
            elif 'sync' in metric_name:
                score += 40
            elif 'pod' in metric_name:
                score += 30
            elif 'service' in metric_name:
                score += 20
            
            # Percentile priority (p99 > p95 > avg)
            if 'p99' in metric_name:
                score += 15
            elif 'p95' in metric_name:
                score += 10
            elif 'avg' in metric_name:
                score += 5
            
            # Value-based priority (higher latency = higher priority for attention)
            try:
                max_value = metric.get('statistics', {}).get('max_value', 0)
                if max_value > 5:  # > 5 seconds
                    score += 30
                elif max_value > 1:  # > 1 second
                    score += 20
                elif max_value > 0.5:  # > 500ms
                    score += 10
            except (ValueError, TypeError):
                pass
            
            return score
        
        return sorted(metrics, key=get_priority_score, reverse=True)
