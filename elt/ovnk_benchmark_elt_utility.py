"""
Utility functions for OpenShift Benchmark ELT modules
Common functions used across multiple ELT modules
"""

import logging
import re
from typing import Dict, Any, List, Union, Tuple
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
        """Limit DataFrame columns to maximum number - Updated with Deep Drive and Latency support"""
        if max_cols is None:
            max_cols = self.max_columns

      # Special handling for latencyELT tables
        if table_name and table_name.startswith('latencyelt_'):
            if table_name in ['latencyelt_collection_info', 'latencyelt_essential_summary']:
                max_cols = 2  # Property-value format
            elif table_name == 'latencyelt_top_latencies_ranking':
                max_cols = 6  # Show rank, metric, component, latency, severity, data points
            elif table_name == 'latencyelt_controller_sync_top20':
                max_cols = 5  # Show rank, pod, node, resource, latency
            elif table_name == 'latencyelt_all_metrics_top5':
                max_cols = 7  # Show metric, component, category, rank, pod, node, latency
            elif table_name == 'latencyelt_critical_findings':
                max_cols = 3  # Show type, description, details
            elif table_name in ['latencyelt_ready_duration', 'latencyelt_sync_duration', 
                            'latencyelt_cni_latency', 'latencyelt_pod_annotation',
                            'latencyelt_pod_creation', 'latencyelt_service_latency', 
                            'latencyelt_network_config']:
                max_cols = 6  # Show more columns for detailed latency metrics
        # Continue with existing logic...
        if len(df.columns) <= max_cols:
            return df
        
        # Enhanced priority columns for latencyELT tables
        if table_name and table_name.startswith('latencyelt_'):
            if 'ranking' in table_name or 'top20' in table_name:
                priority_cols = ['rank', 'metric name', 'metric_name', 'pod name', 'pod_name', 'node name', 'node_name', 'latency', 'max latency', 'component', 'severity']
            elif 'top5' in table_name or 'all_metrics' in table_name:
                priority_cols = ['metric name', 'metric_name', 'component', 'category', 'rank', 'pod name', 'pod_name', 'node name', 'node_name', 'latency', 'value']
            else:
                priority_cols = ['property', 'value', 'metric', 'component', 'max_value', 'avg_value', 'severity', 'rank']
        else:
            # Default priority columns for other tables
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

        # Special handling for specific tables that should show all columns or have different limits
        if table_name in ['controlplane_nodes_detail', 'infra_nodes_detail', 'nodes_usage_detailed', 'nodes_network_usage']:
            return df  # Don't limit detail tables - UPDATED to include both new node tables
        elif table_name == 'node_distribution':
            return df  # Don't limit node distribution table
        elif table_name == 'node_groups':  # Allow more columns for node groups
            max_cols = 6  # Node groups can show more columns
        elif table_name == 'network_analysis':  # Allow more columns for network analysis
            max_cols = 6  # Network analysis can show more columns
        
        # NEW: Latency summary tables - show all columns for comprehensive view
        elif table_name == 'latency_overview':
            return df  # Don't limit the new latency overview table to show all metric details
        elif table_name == 'latency_overall_stats':
            max_cols = 3  # Overall stats table uses 3 columns for property-value-metric format
        
        # Existing latency tables - show all columns for detailed latency analysis
        elif table_name in ['controller_ready_duration', 'node_ready_duration', 'sync_duration', 
                        'pod_latency', 'cni_latency', 'service_latency', 'network_programming']:
            return df  # Don't limit latency tables to show all metric details
        elif table_name in ['latency_summary', 'performance_summary', 'findings_and_recommendations']:
            max_cols = 2  # Summary tables use 2 columns for property-value format
        
        # OVS-specific table handling
        elif table_name in ['cpu_usage_summary', 'memory_usage_summary', 'dp_flows_top', 'bridge_flows_summary']:
            max_cols = 4  # OVS usage tables get 4 columns for readability
        # Deep Drive OVS-specific table handling
        elif table_name in ['ovs_vswitchd_cpu', 'ovsdb_server_cpu', 'ovs_db_memory', 'ovs_vswitchd_memory']:
            max_cols = 4  # OVS CPU and memory tables get 4 columns for readability
        elif table_name in ['ovs_dp_flows', 'ovs_br_int_flows', 'ovs_br_ex_flows']:
            max_cols = 4  # OVS flow tables get 4 columns for readability
        elif table_name == 'ovs_connection_metrics':
            max_cols = 4  # OVS connection metrics get 4 columns to show status            
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
        
        # Deep Drive specific handling
        elif table_name in ['analysis_metadata', 'cluster_overview', 'insights']:
            max_cols = 2  # Deep drive metadata and summary tables use 2 columns
        elif table_name in ['top_worker_nodes', 'controlplane_nodes', 'container_usage']:
            max_cols = 4  # Deep drive resource usage tables get 4 columns for readability
        elif table_name == 'deep_drive_summary':
            max_cols = 2  # Deep drive summary - simple property-value format            
        elif table_name in ['database_sizes', 'latency_categories']:
            max_cols = 3  # Allow 3 columns for these specific tables

        if len(df.columns) <= max_cols:
            return df
        
        # Keep most important columns based on table type
        if table_name in ['summary', 'metadata', 'cluster_health', 'resource_utilization', 'cluster_operators', 'mcp_status', 'usage_summary', 'analysis_metadata', 'performance_summary', 'cluster_overview', 'insights', 'latency_summary', 'findings_and_recommendations']:
            # For summary/status tables, keep metric and value columns
            priority_cols = ['metric', 'value', 'property', 'type', 'description']
        elif table_name == 'node_groups':
            # For node groups, prioritize key status columns
            priority_cols = ['node_type', 'node type', 'total', 'ready', 'health_score', 'health score', 'cpu_cores', 'cpu cores']
        elif table_name == 'alerts':
            # For alerts, prioritize severity and message
            priority_cols = ['alert', 'severity', 'component', 'message', 'rank']
        elif table_name in ['cpu_usage_summary', 'memory_usage_summary', 'ovs_cpu_usage']:
            # For OVS usage summaries, prioritize component, node/pod, and key metrics
            priority_cols = ['component', 'node', 'pod', 'max', 'avg', 'cpu', 'memory', 'rank']
        elif table_name in ['dp_flows_top', 'bridge_flows_summary', 'ovs_flows']:
            # For flow tables, prioritize instance/bridge and flow counts
            priority_cols = ['instance', 'bridge', 'max', 'avg', 'flows', 'type']
        elif table_name in ['top_cpu_pods', 'top_memory_pods']:
            # For top pods usage tables, prioritize rank, pod, and usage metrics
            priority_cols = ['rank', 'pod', 'cpu', 'memory', 'usage', 'node']
        elif table_name in ['cpu_detailed', 'memory_detailed']:
            # For detailed pods metrics, prioritize pod name and key metrics
            priority_cols = ['pod', 'min', 'avg', 'max', 'node', 'namespace']
        elif table_name in ['container_usage']:
            # For container usage, prioritize container name and metrics
            priority_cols = ['container', 'cpu', 'memory', 'status']
        elif table_name in ['node_summary', 'top_worker_nodes']:
            # For node analysis, prioritize node info and usage
            priority_cols = ['node', 'type', 'rank', 'cpu', 'memory', 'count', 'status']
        elif table_name in ['database_sizes']:
            # For database info, prioritize database and size
            priority_cols = ['database', 'size', 'status']
        elif table_name in ['latency_categories']:
            # For latency categories, prioritize category and latency
            priority_cols = ['category', 'latency', 'severity']
        elif 'top_' in (table_name or ''):
            # For top usage tables, keep rank, name, and main metric columns
            priority_cols = ['rank', 'name', 'node', 'cpu', 'memory', 'max', 'avg', 'metric', 'component']
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

    def format_latency_for_summary(self, value_seconds: float) -> Dict[str, Union[str, float]]:
        """Format latency for summary blocks as a dict with value and unit."""
        try:
            if value_seconds < 1:
                return {'value': round(value_seconds * 1000, 2), 'unit': 'ms'}
            elif value_seconds < 60:
                return {'value': round(value_seconds, 3), 'unit': 's'}
            elif value_seconds < 3600:
                return {'value': round(value_seconds / 60, 2), 'unit': 'min'}
            else:
                return {'value': round(value_seconds / 3600, 2), 'unit': 'h'}
        except Exception:
            return {'value': value_seconds, 'unit': 'seconds'}

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

    def resolve_pod_and_node_names(self, entry: Dict[str, Any]) -> Tuple[str, str]:
        """Resolve pod_name and node_name from multiple possible keys (including nested dicts)."""
        def first_non_empty(d: Dict[str, Any], keys: List[str], default: str) -> str:
            for k in keys:
                v = d.get(k)
                if isinstance(v, str) and v.strip():
                    return v
            # Also check nested 'labels' and 'metadata' if present
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

    def create_status_badge(self, status: str, value: str = None) -> str:
        """Create HTML badge for status with optional value"""
        badge_colors = {
            'success': 'success',
            'warning': 'warning',
            'danger': 'danger',
            'info': 'info',
            'critical': 'danger',
            'high': 'warning',
            'medium': 'info',
            'low': 'success',
            'normal': 'success'
        }
        
        color = badge_colors.get(status.lower(), 'secondary')
        display_text = value if value else status.title()
        return f'<span class="badge badge-{color}">{display_text}</span>'

    def highlight_critical_values(self, value: float, thresholds: Dict[str, float], unit: str = "") -> str:
        """Highlight critical values with color coding"""
        critical = thresholds.get('critical', 90)
        warning = thresholds.get('warning', 70)
        
        if value >= critical:
            return f'<span class="text-danger font-weight-bold">{value}{unit}</span>'
        elif value >= warning:
            return f'<span class="text-warning font-weight-bold">{value}{unit}</span>'
        else:
            return f'{value}{unit}'

    def create_latencyelt_severity_badge(self, value: str, severity: str) -> str:
        """Create HTML badge for latencyELT severity with enhanced styling"""
        badge_colors = {
            'critical': 'danger',
            'high': 'warning',
            'medium': 'info', 
            'low': 'success',
            'unknown': 'secondary'
        }
        
        color = badge_colors.get(severity, 'secondary')
        
        # Add pulsing animation for critical values
        if severity == 'critical':
            return f'<span class="badge badge-{color} badge-pulse">{value}</span>'
        elif severity == 'high':
            return f'<span class="badge badge-{color} font-weight-bold">{value}</span>'
        else:
            return f'<span class="badge badge-{color}">{value}</span>'

    def highlight_latencyelt_critical_metric(self, metric_name: str, rank: int = None) -> str:
        """Highlight critical metrics in latencyELT tables"""
        if rank == 1:
            return f'<span class="text-danger font-weight-bold">{metric_name}</span> <span class="badge badge-danger">CRITICAL</span>'
        elif rank and rank <= 3:
            return f'<span class="text-warning font-weight-bold">{metric_name}</span>'
        else:
            return metric_name

    def create_latencyelt_component_badge(self, component: str) -> str:
        """Create component badge for latencyELT with appropriate styling"""
        component_colors = {
            'controller': 'primary',
            'node': 'info',
            'cni': 'success',
            'unknown': 'secondary'
        }
        
        color = component_colors.get(component.lower(), 'secondary')
        return f'<span class="badge badge-{color}">{component.title()}</span>'

    def format_latencyelt_duration(self, value: float, unit: str = 'seconds') -> str:
        """Format duration values for latencyELT with enhanced readability"""
        try:
            if unit == 'seconds' and isinstance(value, (int, float)):
                if value == 0:
                    return '0 ms'
                elif value < 0.001:  # Less than 1ms
                    return f"{round(value * 1000000, 1)} μs"
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

    def categorize_latencyelt_performance(self, max_value: float, avg_value: float, unit: str = 'seconds') -> Dict[str, str]:
        """Categorize latencyELT performance with detailed analysis"""
        try:
            if unit == 'seconds':
                # Performance thresholds
                if max_value > 300:  # > 5 minutes
                    max_severity = 'critical'
                elif max_value > 60:  # > 1 minute
                    max_severity = 'high'
                elif max_value > 5:  # > 5 seconds
                    max_severity = 'medium'
                elif max_value > 1:  # > 1 second
                    max_severity = 'low'
                else:
                    max_severity = 'excellent'
                
                if avg_value > 60:  # > 1 minute
                    avg_severity = 'critical'
                elif avg_value > 10:  # > 10 seconds
                    avg_severity = 'high'
                elif avg_value > 2:  # > 2 seconds
                    avg_severity = 'medium'
                elif avg_value > 0.5:  # > 500ms
                    avg_severity = 'low'
                else:
                    avg_severity = 'excellent'
                    
                return {
                    'max_severity': max_severity,
                    'avg_severity': avg_severity,
                    'overall': max_severity if max_severity in ['critical', 'high'] else avg_severity
                }
            else:
                return {'max_severity': 'unknown', 'avg_severity': 'unknown', 'overall': 'unknown'}
        except (ValueError, TypeError):
            return {'max_severity': 'unknown', 'avg_severity': 'unknown', 'overall': 'unknown'}

    def create_latencyelt_rank_badge(self, rank: int) -> str:
        """Create rank badge for latencyELT top metrics with special styling"""
        if rank == 1:
            return f'<span class="badge badge-danger badge-lg"><i class="fas fa-exclamation-triangle"></i> #{rank}</span>'
        elif rank <= 3:
            return f'<span class="badge badge-warning font-weight-bold">#{rank}</span>'
        elif rank <= 5:
            return f'<span class="badge badge-info">#{rank}</span>'
        else:
            return f'<span class="badge badge-secondary">#{rank}</span>'

    def truncate_latencyelt_pod_name(self, pod_name: str, max_length: int = 25) -> str:
        """Truncate pod name for latencyELT tables with intelligent shortening"""
        if len(pod_name) <= max_length:
            return pod_name
        
        # For OVN pods, keep the important parts
        if 'ovnkube' in pod_name:
            if 'controller' in pod_name:
                return pod_name.replace('ovnkube-controller-', 'ovn-ctrl-')[:max_length]
            elif 'node' in pod_name:
                parts = pod_name.split('-')
                if len(parts) >= 3:
                    return f"ovn-node-{parts[-1]}"
            elif 'master' in pod_name:
                return pod_name.replace('ovnkube-master-', 'ovn-mstr-')[:max_length]
        
        return self.truncate_text(pod_name, max_length)

    def sort_latencyelt_metrics_by_criticality(self, metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sort latencyELT metrics by criticality and performance impact"""
        def get_criticality_score(metric: Dict[str, Any]) -> int:
            """Calculate criticality score for a metric (higher = more critical)"""
            score = 0
            
            # Severity-based scoring
            severity = metric.get('severity', 'unknown')
            severity_scores = {'critical': 1000, 'high': 500, 'medium': 100, 'low': 50, 'unknown': 0}
            score += severity_scores.get(severity, 0)
            
            # Component priority (controller issues are more critical)
            component = metric.get('component', '').lower()
            if component == 'controller':
                score += 200
            elif component == 'node':
                score += 100
            
            # Metric type priority
            metric_name = metric.get('metric_name', '').lower()
            if 'ready' in metric_name:
                score += 150  # Ready duration issues are critical
            elif 'network_config' in metric_name or 'programming' in metric_name:
                score += 140  # Network programming delays are very important
            elif 'pod_annotation' in metric_name or 'pod_creation' in metric_name:
                score += 120  # Pod creation issues are important
            elif 'sync' in metric_name:
                score += 100  # Sync issues affect overall performance
            elif 'cni' in metric_name:
                score += 80   # CNI issues affect pod networking
            elif 'service' in metric_name:
                score += 60   # Service latency issues
            
            # Data points (more data points = more reliable, slightly higher score)
            data_points = metric.get('data_points', 0)
            if data_points > 10:
                score += 20
            elif data_points > 5:
                score += 10
            
            return score
        
        return sorted(metrics, key=get_criticality_score, reverse=True)