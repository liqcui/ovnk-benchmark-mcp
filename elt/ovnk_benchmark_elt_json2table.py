"""
Extract, Load, Transform module for OpenShift Benchmark Performance Data
Converts JSON outputs to table format and generates brief results
Updated with improved table conversion functionality for cluster info, prometheus metrics, and kube API data
"""

import logging
from typing import Dict, Any, List, Optional, Union, Tuple
import json
import pandas as pd
from datetime import datetime
import re
from tabulate import tabulate

logger = logging.getLogger(__name__)

class PerformanceDataELT:
    """Extract, Load, Transform class for performance data"""
    
    def __init__(self):
        self.processed_data = {}
        self.max_columns = 5  # Maximum columns per table
        
    def extract_json_data(self, mcp_results: Union[Dict[str, Any], str]) -> Dict[str, Any]:
        """Extract relevant data from MCP tool results"""
        try:
            # Normalize input to a dictionary
            if isinstance(mcp_results, str):
                try:
                    mcp_results = json.loads(mcp_results)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON string in extract_json_data: {e}")
                    return {'error': f"Invalid JSON string: {str(e)}", 'raw_data': mcp_results}
            if not isinstance(mcp_results, dict):
                return {'error': 'Input must be a dictionary or valid JSON string', 'raw_data': mcp_results}

            extracted = {
                'timestamp': mcp_results.get('timestamp', datetime.now().isoformat()),
                'data_type': self._identify_data_type(mcp_results),
                'raw_data': mcp_results,
                'structured_data': {}
            }
            
            # Extract structured data based on type
            if extracted['data_type'] == 'cluster_info':
                extracted['structured_data'] = self._extract_cluster_info(mcp_results)
            elif extracted['data_type'] == 'prometheus_basic_info':
                extracted['structured_data'] = self._extract_prometheus_basic_info(mcp_results)
            elif extracted['data_type'] == 'kube_api_metrics':
                extracted['structured_data'] = self._extract_kube_api_metrics(mcp_results)
            elif extracted['data_type'] == 'node_usage':
                extracted['structured_data'] = self._extract_node_usage(mcp_results)
            elif extracted['data_type'] == 'pod_status':
                extracted['structured_data'] = self._extract_pod_status(mcp_results)
            elif extracted['data_type'] == 'prometheus_query':
                extracted['structured_data'] = self._extract_prometheus_data(mcp_results)
            elif extracted['data_type'] == 'cluster_status':
                extracted['structured_data'] = self._extract_cluster_status(mcp_results)
            elif extracted['data_type'] == 'cluster_status_analysis':
                extracted['structured_data'] = self._extract_cluster_status_analysis(mcp_results)                                
            elif extracted['data_type'] == 'ovn_sync_duration':
                extracted['structured_data'] = self._extract_ovn_sync_duration(mcp_results)
            elif extracted['data_type'] == 'pod_usage':
                extracted['structured_data'] = self._extract_pod_usage(mcp_results)                
            elif extracted['data_type'] == 'ovs_usage':
                extracted['structured_data'] = self._extract_ovs_usage(mcp_results)
            elif extracted['data_type'] == 'ovs_comprehensive':
                extracted['structured_data'] = self._extract_ovs_comprehensive(mcp_results)

            else:
                extracted['structured_data'] = self._extract_generic_data(mcp_results)
            
            return extracted
            
        except Exception as e:
            logger.error(f"Failed to extract JSON data: {e}")
            return {'error': str(e), 'raw_data': mcp_results}
    
    def _identify_data_type(self, data: Dict[str, Any]) -> str:
        # Check for comprehensive OVS metrics (from collect_all_ovs_metrics)
        if ('cpu_usage' in data and 'memory_usage' in data and 'dp_flows' in data and 
            'bridge_flows' in data and 'connection_metrics' in data):
            return 'ovs_comprehensive'

        # Check for individual OVS usage metrics 
        if (('ovs_vswitchd_cpu' in data or 'ovsdb_server_cpu' in data) and 
            'collection_type' in data):
            return 'ovs_usage'

        """Identify the type of data from MCP results"""
        # Check for OVN sync duration metrics (from ovnk_benchmark_prometheus_ovnk_sync.py)
        if ('controller_ready_duration' in data and 'node_ready_duration' in data and 
            'controller_sync_duration' in data and 'overall_summary' in data):
            return 'ovn_sync_duration'
        
        # Check for pod usage metrics (from ovnk_benchmark_prometheus_pods_usage.py)
        if ('top_5_cpu_usage' in data and 'top_5_memory_usage' in data and 
            'collection_type' in data and 'total_analyzed' in data):
            return 'pod_usage'

        # Check for cluster info (from ovnk_benchmark_openshift_cluster_info.py)
        if 'cluster_name' in data and 'cluster_version' in data and 'master_nodes' in data:
            return 'cluster_info'
        
        # Check for prometheus basic info (from ovnk_benchmark_prometheus_basicinfo.py)
        if 'ovn_northbound_db_size' in data or 'ovn_southbound_db_size' in data:
            return 'prometheus_basic_info'
        
        # Check for pod status metrics
        if 'metric_name' in data and data.get('metric_name') == 'pod-status':
            return 'pod_status'
        
        # Check for kube API metrics (from ovnk_benchmark_prometheus_kubeapi.py)
        if 'metrics' in data and any(key in data['metrics'] for key in 
                                   ['readonly_latency', 'mutating_latency', 'watch_events', 'rest_client']):
            return 'kube_api_metrics'
        
        # Check for node usage data
        if 'groups' in data and 'metadata' in data and 'top_usage' in data:
            return 'node_usage'

        # Check for cluster status analysis (from ovnk_benchmark_analysis_cluster_stat.py)
        if ('metadata' in data and 'cluster_health' in data and 'node_groups' in data and 
            'alerts' in data and 'recommendations' in data):
            # Further verify it's the analysis output by checking metadata structure
            metadata = data.get('metadata', {})
            if ('analysis_timestamp' in metadata and 'collection_type' in metadata and 
                metadata.get('collection_type') == 'cluster_status'):
                return 'cluster_status_analysis'

        # Legacy checks
        if 'version' in data and 'identity' in data:
            return 'cluster_info'
        elif 'nodes_by_role' in data or 'total_nodes' in data:
            return 'node_info'
        elif 'api_server_latency' in data or 'latency_metrics' in data:
            return 'api_metrics'
        elif 'result' in data and 'query' in data:
            return 'prometheus_query'
        elif 'analysis_type' in data and data.get('analysis_type') == 'cluster_status_analysis':
            return 'cluster_status'
        elif 'node_status' in data and 'operator_status' in data:
            return 'cluster_status'
        else:
            return 'generic'

    def _extract_cluster_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cluster information from ovnk_benchmark_openshift_cluster_info.py output"""
        structured = {
            'cluster_overview': [],
            'resource_summary': [],
            'all_resource_counts': [],
            'collection_metadata': [],
            'node_distribution': [],
            'master_nodes_detail': [],
            'worker_nodes_detail': [],
            'infra_nodes_detail': [],
            'cluster_health_status': [],
            'mcp_status_detail': [],
            'unavailable_operators_detail': []
        }
        
        # Cluster overview (2-column format for basic info)
        structured['cluster_overview'] = [
            {'Property': 'Cluster Name', 'Value': data.get('cluster_name', 'Unknown')},
            {'Property': 'Version', 'Value': data.get('cluster_version', 'Unknown')},
            {'Property': 'Platform', 'Value': data.get('platform', 'Unknown')},
            {'Property': 'Total Nodes', 'Value': data.get('total_nodes', 0)},
            {'Property': 'API Server', 'Value': self._truncate_url(data.get('api_server_url', 'Unknown'))},
            {'Property': 'Collection Time', 'Value': data.get('collection_timestamp', 'Unknown')[:19] if data.get('collection_timestamp') else 'Unknown'}
        ]
        
        # Enhanced resource counts table - includes all available resource types from the JSON
        resource_items = [
            ('namespaces_count', 'Namespaces'),
            ('pods_count', 'Pods'), 
            ('services_count', 'Services'),
            ('secrets_count', 'Secrets'),
            ('configmaps_count', 'Config Maps'),
            ('networkpolicies_count', 'Network Policies'),
            ('adminnetworkpolicies_count', 'Admin Network Policies'),
            ('baselineadminnetworkpolicies_count', 'Baseline Admin Network Policies'),
            ('egressfirewalls_count', 'Egress Firewalls'),
            ('egressips_count', 'Egress IPs'),
            ('clusteruserdefinednetworks_count', 'Cluster User Defined Networks'),
            ('userdefinednetworks_count', 'User Defined Networks')
        ]
        
        # Create comprehensive resource table (flexible column format)
        for field, label in resource_items:
            count = data.get(field, 0)
            category = self._categorize_resource_type(label)
            structured['all_resource_counts'].append({
                'Resource Type': label,
                'Count': count,
                'Category': category,
                'Status': 'Available' if count >= 0 else 'Error'
            })
        
        # Collection metadata table (if available)
        metadata = data.get('collection_metadata', {})
        if metadata:
            structured['collection_metadata'] = [
                {'Metadata Field': 'Tool Name', 'Value': metadata.get('tool_name', 'Unknown')},
                {'Metadata Field': 'Collection Duration (s)', 'Value': metadata.get('collection_duration_seconds', 'N/A')},
                {'Metadata Field': 'Data Freshness', 'Value': metadata.get('data_freshness', 'Unknown')[:19] if metadata.get('data_freshness') else 'Unknown'},
                {'Metadata Field': 'Total Fields Collected', 'Value': metadata.get('total_fields_collected', 0)},
                {'Metadata Field': 'Node Details Included', 'Value': 'Yes' if metadata.get('parameters_applied', {}).get('include_node_details', False) else 'No'},
                {'Metadata Field': 'Resource Counts Included', 'Value': 'Yes' if metadata.get('parameters_applied', {}).get('include_resource_counts', False) else 'No'},
                {'Metadata Field': 'Network Policies Included', 'Value': 'Yes' if metadata.get('parameters_applied', {}).get('include_network_policies', False) else 'No'},
                {'Metadata Field': 'Operator Status Included', 'Value': 'Yes' if metadata.get('parameters_applied', {}).get('include_operator_status', False) else 'No'},
                {'Metadata Field': 'MCP Status Included', 'Value': 'Yes' if metadata.get('parameters_applied', {}).get('include_mcp_status', False) else 'No'}
            ]
        
        # Enhanced resource summary with totals and categories 
        total_resources = sum(data.get(field, 0) for field, _ in resource_items)
        network_resources = sum(data.get(field, 0) for field, _ in resource_items if 'network' in field.lower() or 'egress' in field.lower() or 'udn' in field.lower())
        policy_resources = sum(data.get(field, 0) for field, _ in resource_items if 'policy' in field.lower() or 'policies' in field.lower())
        
        structured['resource_summary'] = [
            {'Metric': 'Total Resources', 'Value': total_resources},
            {'Metric': 'Network-Related Resources', 'Value': network_resources},
            {'Metric': 'Policy Resources', 'Value': policy_resources},
            {'Metric': 'Core Resources (Pods+Services)', 'Value': data.get('pods_count', 0) + data.get('services_count', 0)},
            {'Metric': 'Config Resources (Secrets+ConfigMaps)', 'Value': data.get('secrets_count', 0) + data.get('configmaps_count', 0)}
        ]
        
        # ENHANCED NODE DISTRIBUTION SUMMARY - Complete table with all requested information
        node_types = [
            ('master_nodes', 'Master'),
            ('worker_nodes', 'Worker'),
            ('infra_nodes', 'Infra')
        ]
        
        for field, role in node_types:
            nodes = data.get(field, [])
            if nodes:
                # Calculate resource totals for this node type
                total_cpu = sum(self._parse_cpu_capacity(node.get('cpu_capacity', '0')) for node in nodes)
                total_memory_gb = sum(self._parse_memory_capacity(node.get('memory_capacity', '0Ki')) for node in nodes)
                ready_count = sum(1 for node in nodes if 'Ready' in node.get('ready_status', ''))
                schedulable_count = sum(1 for node in nodes if node.get('schedulable', False))
                
                # Create the comprehensive node distribution entry with ALL fields
                structured['node_distribution'].append({
                    'Node Type': role,
                    'Count': len(nodes),
                    'Ready': ready_count,
                    'Schedulable': schedulable_count,
                    'Total CPU (cores)': total_cpu,
                    'Total Memory (GB)': f"{total_memory_gb:.0f}",
                    'Health Ratio': f"{ready_count}/{len(nodes)}",
                    'Avg CPU per Node': f"{total_cpu/len(nodes):.0f}" if len(nodes) > 0 else "0"
                })
            else:
                # Handle case where no nodes exist for this type
                structured['node_distribution'].append({
                    'Node Type': role,
                    'Count': 0,
                    'Ready': 0,
                    'Schedulable': 0,
                    'Total CPU (cores)': 0,
                    'Total Memory (GB)': '0',
                    'Health Ratio': '0/0',
                    'Avg CPU per Node': '0'
                })
        
        # Master nodes detail (comprehensive node info with more columns)
        master_nodes = data.get('master_nodes', [])
        for node in master_nodes:
            structured['master_nodes_detail'].append({
                'Name': self._truncate_node_name(node.get('name', 'unknown')),
                'CPU Cores': node.get('cpu_capacity', 'Unknown'),
                'Memory': self._format_memory_display(node.get('memory_capacity', '0Ki')),
                'Architecture': node.get('architecture', 'Unknown'),
                'Kernel Version': self._truncate_kernel_version(node.get('kernel_version', 'Unknown')),
                'Kubelet Version': node.get('kubelet_version', 'Unknown').replace('v', ''),
                'Container Runtime': self._truncate_runtime(node.get('container_runtime', 'Unknown')),
                'OS Image': self._truncate_os_image(node.get('os_image', 'Unknown')),
                'Status': node.get('ready_status', 'Unknown'),
                'Schedulable': 'Yes' if node.get('schedulable', False) else 'No',
                'Creation Time': node.get('creation_timestamp', 'Unknown')[:10] if node.get('creation_timestamp') else 'Unknown'
            })
        
        # Worker nodes detail (comprehensive node info with more columns)
        worker_nodes = data.get('worker_nodes', [])
        for node in worker_nodes:
            structured['worker_nodes_detail'].append({
                'Name': self._truncate_node_name(node.get('name', 'unknown')),
                'CPU Cores': node.get('cpu_capacity', 'Unknown'),
                'Memory': self._format_memory_display(node.get('memory_capacity', '0Ki')),
                'Architecture': node.get('architecture', 'Unknown'),
                'Kernel Version': self._truncate_kernel_version(node.get('kernel_version', 'Unknown')),
                'Kubelet Version': node.get('kubelet_version', 'Unknown').replace('v', ''),
                'Container Runtime': self._truncate_runtime(node.get('container_runtime', 'Unknown')),
                'OS Image': self._truncate_os_image(node.get('os_image', 'Unknown')),
                'Status': node.get('ready_status', 'Unknown'),
                'Schedulable': 'Yes' if node.get('schedulable', False) else 'No',
                'Creation Time': node.get('creation_timestamp', 'Unknown')[:10] if node.get('creation_timestamp') else 'Unknown'
            })
        
        # Infra nodes detail (if any exist)
        infra_nodes = data.get('infra_nodes', [])
        if infra_nodes:
            for node in infra_nodes:
                structured['infra_nodes_detail'].append({
                    'Name': self._truncate_node_name(node.get('name', 'unknown')),
                    'CPU Cores': node.get('cpu_capacity', 'Unknown'),
                    'Memory': self._format_memory_display(node.get('memory_capacity', '0Ki')),
                    'Architecture': node.get('architecture', 'Unknown'),
                    'Kernel Version': self._truncate_kernel_version(node.get('kernel_version', 'Unknown')),
                    'Kubelet Version': node.get('kubelet_version', 'Unknown').replace('v', ''),
                    'Container Runtime': self._truncate_runtime(node.get('container_runtime', 'Unknown')),
                    'OS Image': self._truncate_os_image(node.get('os_image', 'Unknown')),
                    'Status': node.get('ready_status', 'Unknown'),
                    'Schedulable': 'Yes' if node.get('schedulable', False) else 'No',
                    'Creation Time': node.get('creation_timestamp', 'Unknown')[:10] if node.get('creation_timestamp') else 'Unknown'
                })
        
        # Cluster health status (enhanced with more metrics)
        unavailable_ops = data.get('unavailable_cluster_operators', [])
        mcp_status = data.get('mcp_status', {})
        
        health_items = [
            ('Unavailable Operators', len(unavailable_ops)),
            ('Total MCP Pools', len(mcp_status)),
            ('MCP Updated Pools', sum(1 for status in mcp_status.values() if status == 'Updated')),
            ('MCP Degraded Pools', sum(1 for status in mcp_status.values() if status == 'Degraded')),
            ('MCP Updating Pools', sum(1 for status in mcp_status.values() if status == 'Updating')),
            ('Overall Cluster Health', 'Healthy' if len(unavailable_ops) == 0 and all(status in ['Updated'] for status in mcp_status.values()) else 'Issues Detected'),
            ('Node Health Score', f"{sum(1 for field, _ in node_types for node in data.get(field, []) if 'Ready' in node.get('ready_status', ''))}/{data.get('total_nodes', 0)}")
        ]
        
        for metric, value in health_items:
            structured['cluster_health_status'].append({
                'Health Metric': metric,
                'Value': value
            })
        
        # MCP Status Detail (2-column format)
        for pool_name, status in mcp_status.items():
            structured['mcp_status_detail'].append({
                'Machine Config Pool': pool_name.title(),
                'Status': status
            })
        
        # Unavailable Operators Detail (2-column format) 
        if unavailable_ops:
            for i, op in enumerate(unavailable_ops, 1):
                structured['unavailable_operators_detail'].append({
                    'Operator #': i,
                    'Operator Name': op
                })
        else:
            structured['unavailable_operators_detail'].append({
                'Status': 'All operators are available',
                'Message': 'No unavailable operators detected'
            })
        
        return structured

    def _categorize_resource_type(self, resource_name: str) -> str:
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

    def _truncate_kernel_version(self, kernel_ver: str, max_length: int = 30) -> str:
        """Truncate kernel version for display"""
        if len(kernel_ver) <= max_length:
            return kernel_ver
        return kernel_ver[:max_length-3] + '...'

    def _truncate_runtime(self, runtime: str, max_length: int = 25) -> str:
        """Truncate container runtime for display"""
        if len(runtime) <= max_length:
            return runtime
        # Try to keep the version part
        if '://' in runtime:
            protocol, version = runtime.split('://', 1)
            return f"{protocol}://{version[:max_length-len(protocol)-6]}..."
        return runtime[:max_length-3] + '...'

    def _truncate_os_image(self, os_image: str, max_length: int = 35) -> str:
        """Truncate OS image for display"""
        if len(os_image) <= max_length:
            return os_image
        # Try to keep the important part (usually the beginning)
        return os_image[:max_length-3] + '...'

    def _truncate_url(self, url: str, max_length: int = 50) -> str:
        """Truncate URL for display"""
        if len(url) <= max_length:
            return url
        return url[:max_length-3] + '...'

    def _truncate_node_name(self, name: str, max_length: int = 25) -> str:
        """Truncate node name for display"""
        if len(name) <= max_length:
            return name
        # Try to keep the meaningful part (usually the beginning)
        return name[:max_length-3] + '...'

    def _parse_cpu_capacity(self, cpu_str: str) -> int:
        """Parse CPU capacity string to integer"""
        try:
            # Handle formats like "32", "32000m"
            if cpu_str.endswith('m'):
                return int(cpu_str[:-1]) // 1000
            return int(cpu_str)
        except (ValueError, TypeError):
            return 0

    def _parse_memory_capacity(self, memory_str: str) -> float:
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

    def _format_memory_display(self, memory_str: str) -> str:
        """Format memory for display"""
        try:
            gb_value = self._parse_memory_capacity(memory_str)
            if gb_value >= 1:
                return f"{gb_value:.0f} GB"
            else:
                # Show in MB for small values
                return f"{gb_value * 1024:.0f} MB"
        except:
            return memory_str

    def _extract_prometheus_basic_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Prometheus basic info from ovnk_benchmark_prometheus_basicinfo.py output"""
        structured = {
            'ovn_database_metrics': [],
            'metric_details': []
        }
        
        # OVN database size metrics
        for metric_name, metric_data in data.items():
            if isinstance(metric_data, dict) and 'max_value' in metric_data:
                # Convert bytes to MB for readability
                max_value = metric_data.get('max_value')
                if max_value is not None and metric_data.get('unit') == 'bytes':
                    max_value_mb = round(max_value / (1024 * 1024), 2)
                    display_value = f"{max_value_mb} MB"
                else:
                    display_value = str(max_value) if max_value is not None else 'N/A'
                
                structured['ovn_database_metrics'].append({
                    'Database': metric_name.replace('ovn_', '').replace('_db_size', '').replace('_', ' ').title(),
                    'Max Size': display_value,
                    'Unit': metric_data.get('unit', 'unknown'),
                    'Status': 'Available' if max_value is not None else 'Error'
                })
                
                # Add detailed info if labels exist
                labels = metric_data.get('labels', {})
                if labels:
                    for key, value in list(labels.items())[:3]:  # Limit to 3 label pairs
                        structured['metric_details'].append({
                            'Metric': metric_name,
                            'Label Key': key,
                            'Label Value': str(value)[:50] + '...' if len(str(value)) > 50 else str(value)
                        })
        
        return structured

    def _extract_pod_status(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract pod status metrics"""
        structured = {
            'pod_overview': [],
            'phase_distribution': []
        }
        
        # Pod overview
        structured['pod_overview'] = [
            {'Metric': 'Total Pods', 'Value': data.get('total_pods', 0)},
            {'Metric': 'Query Type', 'Value': data.get('query_type', 'unknown')},
            {'Metric': 'Timestamp', 'Value': str(data.get('timestamp', 'N/A'))[:19] if data.get('timestamp') else 'N/A'}
        ]
        
        # Phase distribution
        phases = data.get('phases', {})
        total_pods = data.get('total_pods', 0)
        
        for phase, count in phases.items():
            percentage = (count / total_pods * 100) if total_pods > 0 else 0
            structured['phase_distribution'].append({
                'Phase': phase.title(),
                'Count': count,
                'Percentage': f"{percentage:.1f}%"
            })
        
        return structured

    def _extract_kube_api_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Kubernetes API metrics from ovnk_benchmark_prometheus_kubeapi.py output"""
        structured = {
            'api_summary': [],
            'latency_metrics': [],
            'top_latency_readonly': [],
            'top_latency_mutating': [],
            'api_activity': []
        }
        
        # API summary
        metadata = data.get('metadata', {})
        summary_data = data.get('summary', {})
        
        structured['api_summary'] = [
            {'Property': 'Query Duration', 'Value': data.get('duration', 'Unknown')},
            {'Property': 'Collection Time', 'Value': data.get('timestamp', 'Unknown')[:19]},
            {'Property': 'Overall Health', 'Value': f"{summary_data.get('overall_health', 0)}/100"},
            {'Property': 'Health Status', 'Value': summary_data.get('overall_status', 'unknown').upper()},
            {'Property': 'Metric Types', 'Value': summary_data.get('total_metric_types', 0)}
        ]
        
        # Extract performance overview from summary
        perf_overview = summary_data.get('performance_overview', {})
        
        # Latency metrics summary
        if 'readonly_latency' in perf_overview:
            ro_latency = perf_overview['readonly_latency']
            structured['latency_metrics'].append({
                'Operation Type': 'Read-Only',
                'Avg P99 (s)': f"{ro_latency.get('highest_avg_p99', 0):.4f}",
                'Max P99 (s)': f"{ro_latency.get('highest_max_p99', 0):.4f}",
                'Status': ro_latency.get('status', 'unknown').upper()
            })
        
        if 'mutating_latency' in perf_overview:
            mut_latency = perf_overview['mutating_latency']
            structured['latency_metrics'].append({
                'Operation Type': 'Mutating',
                'Avg P99 (s)': f"{mut_latency.get('highest_avg_p99', 0):.4f}",
                'Max P99 (s)': f"{mut_latency.get('highest_max_p99', 0):.4f}",
                'Status': mut_latency.get('status', 'unknown').upper()
            })
        
        # Extract detailed metrics for top latency operations
        metrics = data.get('metrics', {})
        
        # Top read-only latency operations
        if 'readonly_latency' in metrics:
            ro_metrics = metrics['readonly_latency']
            if 'top5_avg' in ro_metrics and 'avg_ro_apicalls_latency' in ro_metrics['top5_avg']:
                for i, item in enumerate(ro_metrics['top5_avg']['avg_ro_apicalls_latency'][:5], 1):
                    structured['top_latency_readonly'].append({
                        'Rank': i,
                        'Resource:Verb:Scope': item.get('label', 'unknown'),
                        'Avg Latency (s)': f"{item.get('value', 0):.4f}"
                    })
        
        # Top mutating latency operations
        if 'mutating_latency' in metrics:
            mut_metrics = metrics['mutating_latency']
            if 'top5_avg' in mut_metrics and 'avg_mutating_apicalls_latency' in mut_metrics['top5_avg']:
                for i, item in enumerate(mut_metrics['top5_avg']['avg_mutating_apicalls_latency'][:5], 1):
                    structured['top_latency_mutating'].append({
                        'Rank': i,
                        'Resource:Verb:Scope': item.get('label', 'unknown'),
                        'Avg Latency (s)': f"{item.get('value', 0):.4f}"
                    })
        
        # API activity metrics
        activity_metrics = [
            ('watch_events', 'Watch Events'),
            ('cache_list', 'Cache Operations'),
            ('etcd_requests', 'ETCD Requests')
        ]
        
        for metric_key, metric_label in activity_metrics:
            if metric_key in metrics:
                metric_data = metrics[metric_key]
                if 'top5_avg' in metric_data and metric_data['top5_avg']:
                    top_item = metric_data['top5_avg'][0] if metric_data['top5_avg'] else {}
                    structured['api_activity'].append({
                        'Metric Type': metric_label,
                        'Top Consumer': top_item.get('label', 'unknown')[:40] + '...' if len(top_item.get('label', '')) > 40 else top_item.get('label', 'unknown'),
                        'Rate': f"{top_item.get('value', 0):.3f}",
                        'Unit': metric_data.get('unit', 'unknown')
                    })
        
        return structured
    
    def _extract_node_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract node usage metrics from ovnk_benchmark_prometheus_nodes_usage.py output"""
        structured = {
            'usage_overview': [],
            'controlplane_summary': [],
            'infra_summary': [],
            'worker_summary': [],
            'top_cpu_workers': [],
            'top_memory_workers': []
        }
        
        # Usage overview from metadata
        metadata = data.get('metadata', {})
        structured['usage_overview'] = [
            {'Property': 'Query Duration', 'Value': metadata.get('duration', 'Unknown')},
            {'Property': 'Collection Time', 'Value': metadata.get('query_time', 'Unknown')[:19] if metadata.get('query_time') else 'Unknown'},
            {'Property': 'Start Time', 'Value': metadata.get('start_time', 'Unknown')[:19] if metadata.get('start_time') else 'Unknown'},
            {'Property': 'End Time', 'Value': metadata.get('end_time', 'Unknown')[:19] if metadata.get('end_time') else 'Unknown'},
            {'Property': 'Timezone', 'Value': metadata.get('timezone', 'UTC')}
        ]
        
        # Process groups
        groups = data.get('groups', {})
        
        # Control Plane Summary (2-column format)
        controlplane = groups.get('controlplane', {})
        if controlplane.get('count', 0) > 0:
            summary = controlplane.get('summary', {})
            structured['controlplane_summary'] = [
                {'Metric': 'Node Count', 'Value': controlplane.get('count', 0)},
                {'Metric': 'CPU Avg (%)', 'Value': f"{summary.get('cpu_usage', {}).get('avg', 0):.1f}"},
                {'Metric': 'CPU Max (%)', 'Value': f"{summary.get('cpu_usage', {}).get('max', 0):.1f}"},
                {'Metric': 'Memory Avg (GB)', 'Value': f"{summary.get('memory_usage', {}).get('avg', 0)/1024:.1f}"},
                {'Metric': 'Memory Max (GB)', 'Value': f"{summary.get('memory_usage', {}).get('max', 0)/1024:.1f}"},
                {'Metric': 'Network RX Avg (MB/s)', 'Value': f"{summary.get('network_rx', {}).get('avg', 0)/1024/1024:.2f}"},
                {'Metric': 'Network TX Avg (MB/s)', 'Value': f"{summary.get('network_tx', {}).get('avg', 0)/1024/1024:.2f}"}
            ]
        else:
            structured['controlplane_summary'] = [{'Status': 'No Control Plane nodes found', 'Count': 0}]
        
        # Infra Summary (2-column format)
        infra = groups.get('infra', {})
        if infra.get('count', 0) > 0:
            summary = infra.get('summary', {})
            structured['infra_summary'] = [
                {'Metric': 'Node Count', 'Value': infra.get('count', 0)},
                {'Metric': 'CPU Avg (%)', 'Value': f"{summary.get('cpu_usage', {}).get('avg', 0):.1f}"},
                {'Metric': 'CPU Max (%)', 'Value': f"{summary.get('cpu_usage', {}).get('max', 0):.1f}"},
                {'Metric': 'Memory Avg (GB)', 'Value': f"{summary.get('memory_usage', {}).get('avg', 0)/1024:.1f}"},
                {'Metric': 'Memory Max (GB)', 'Value': f"{summary.get('memory_usage', {}).get('max', 0)/1024:.1f}"},
                {'Metric': 'Network RX Avg (MB/s)', 'Value': f"{summary.get('network_rx', {}).get('avg', 0)/1024/1024:.2f}"},
                {'Metric': 'Network TX Avg (MB/s)', 'Value': f"{summary.get('network_tx', {}).get('avg', 0)/1024/1024:.2f}"}
            ]
        else:
            structured['infra_summary'] = [{'Status': 'No Infrastructure nodes found', 'Count': 0}]
        
        # Worker Summary (2-column format)
        worker = groups.get('worker', {})
        if worker.get('count', 0) > 0:
            summary = worker.get('summary', {})
            structured['worker_summary'] = [
                {'Metric': 'Node Count', 'Value': worker.get('count', 0)},
                {'Metric': 'CPU Avg (%)', 'Value': f"{summary.get('cpu_usage', {}).get('avg', 0):.1f}"},
                {'Metric': 'CPU Max (%)', 'Value': f"{summary.get('cpu_usage', {}).get('max', 0):.1f}"},
                {'Metric': 'Memory Avg (GB)', 'Value': f"{summary.get('memory_usage', {}).get('avg', 0)/1024:.1f}"},
                {'Metric': 'Memory Max (GB)', 'Value': f"{summary.get('memory_usage', {}).get('max', 0)/1024:.1f}"},
                {'Metric': 'Network RX Avg (MB/s)', 'Value': f"{summary.get('network_rx', {}).get('avg', 0)/1024/1024:.2f}"},
                {'Metric': 'Network TX Avg (MB/s)', 'Value': f"{summary.get('network_tx', {}).get('avg', 0)/1024/1024:.2f}"}
            ]
        else:
            structured['worker_summary'] = [{'Status': 'No Worker nodes found', 'Count': 0}]
        
        # Top 5 CPU usage workers (6-column format)
        top_cpu = data.get('top_usage', {}).get('cpu', [])
        for i, node in enumerate(top_cpu[:5], 1):
            structured['top_cpu_workers'].append({
                'Rank': i,
                'Node Name': self._truncate_node_name(node.get('name', 'unknown')),
                'Instance': self._truncate_node_name(node.get('instance', 'unknown')),
                'CPU Max (%)': f"{node.get('cpu_max', 0):.2f}",
                'CPU Avg (%)': f"{node.get('cpu_avg', 0):.2f}",
                'Role': 'Worker'
            })
        
        # Top 5 memory usage workers (6-column format)
        top_memory = data.get('top_usage', {}).get('memory', [])
        for i, node in enumerate(top_memory[:5], 1):
            structured['top_memory_workers'].append({
                'Rank': i,
                'Node Name': self._truncate_node_name(node.get('name', 'unknown')),
                'Instance': self._truncate_node_name(node.get('instance', 'unknown')),
                'Memory Max (MB)': f"{node.get('memory_max', 0):.0f}",
                'Memory Avg (MB)': f"{node.get('memory_avg', 0):.0f}",
                'Role': 'Worker'
            })
        
        # If no top usage data, add placeholder
        if not structured['top_cpu_workers']:
            structured['top_cpu_workers'] = [{'Status': 'No CPU usage data available', 'Nodes': 0}]
        
        if not structured['top_memory_workers']:
            structured['top_memory_workers'] = [{'Status': 'No memory usage data available', 'Nodes': 0}]
        
        return structured
    
    def _extract_prometheus_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract generic Prometheus query results"""
        structured = {
            'query_results': []
        }
        
        if 'result' in data:
            results = data['result']
            if isinstance(results, list):
                for i, result in enumerate(results[:10]):  # Limit to 10 results
                    metric = result.get('metric', {})
                    value = result.get('value', [])
                    
                    # Get most relevant labels (limit to 2)
                    important_labels = []
                    label_priority = ['__name__', 'instance', 'job', 'node', 'namespace', 'pod']
                    
                    for label_key in label_priority:
                        if label_key in metric:
                            important_labels.append(f"{label_key}={metric[label_key]}")
                        if len(important_labels) >= 2:
                            break
                    
                    # Add any remaining labels if we have space
                    for key, val in metric.items():
                        if key not in [l.split('=')[0] for l in important_labels] and len(important_labels) < 2:
                            important_labels.append(f"{key}={val}")
                    
                    if len(value) >= 2:
                        structured['query_results'].append({
                            'Index': i + 1,
                            'Labels': ', '.join(important_labels) if important_labels else 'none',
                            'Timestamp': value[0] if isinstance(value[0], (int, float)) else str(value[0])[:19],
                            'Value': f"{float(value[1]):.6f}" if isinstance(value[1], (int, float, str)) and str(value[1]).replace('.','').replace('-','').isdigit() else str(value[1])
                        })
        
        return structured
    
    def _extract_cluster_status(self, data: Union[Dict[str, Any], str]) -> Dict[str, Any]:
        """Extract cluster status analysis into structured format"""
        structured = {
            'executive_summary': [],
            'component_health': [],
            'critical_issues': [],
            'top_recommendations': []
        }
        
        try:
            # Handle string input
            if isinstance(data, str):
                data = json.loads(data)
            
            # Executive Summary (limit to 5 key metrics)
            summary = data.get('summary', {})
            if summary:
                structured['executive_summary'] = [
                    {'Property': 'Cluster Status', 'Value': summary.get('cluster_status', 'Unknown').upper()},
                    {'Property': 'Health Score', 'Value': f"{summary.get('health_score', 0)}/100"},
                    {'Property': 'Critical Issues', 'Value': summary.get('critical_issues_count', 0)},
                    {'Property': 'Warnings', 'Value': summary.get('warnings_count', 0)},
                    {'Property': 'Action Required', 'Value': 'Yes' if summary.get('immediate_action_required', False) else 'No'}
                ]
            
            # Component Health (limit to 5 columns)
            overall_health = data.get('overall_health', {})
            component_scores = overall_health.get('component_scores', {})
            for component, score_info in list(component_scores.items())[:10]:  # Limit rows
                structured['component_health'].append({
                    'Component': component.replace('_', ' ').title(),
                    'Score': f"{score_info.get('score', 0):.1f}",
                    'Status': score_info.get('status', 'unknown').upper()
                })
            
            # Critical Issues (top 5)
            critical_issues = data.get('critical_issues', [])
            for i, issue in enumerate(critical_issues[:5], 1):
                structured['critical_issues'].append({
                    'Priority': i,
                    'Component': issue.get('component', 'Unknown').upper(),
                    'Severity': issue.get('severity', 'Unknown').upper(),
                    'Issue': issue.get('message', 'No message')[:80] + '...' if len(issue.get('message', '')) > 80 else issue.get('message', 'No message')
                })
            
            # Top Recommendations (limit to 5)
            recommendations = data.get('recommendations', [])
            for i, rec in enumerate(recommendations[:5], 1):
                structured['top_recommendations'].append({
                    'Priority': i,
                    'Recommendation': rec[:100] + '...' if len(rec) > 100 else rec
                })
        
        except Exception as e:
            logger.error(f"Error extracting cluster status: {e}")
            structured['error'] = str(e)
        
        return structured
    
    def _extract_generic_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract generic data with smart column limiting"""
        structured = {'key_value_pairs': []}
        
        def extract_important_fields(d: Dict[str, Any], max_fields: int = 20) -> List[Tuple[str, Any]]:
            """Extract most important fields from a dictionary"""
            fields = []
            
            # Priority fields (always include if present)
            priority_keys = [
                'name', 'status', 'version', 'timestamp', 'count', 'total',
                'health', 'error', 'message', 'value', 'metric', 'result'
            ]
            
            # Add priority fields first
            for key in priority_keys:
                if key in d:
                    fields.append((key, d[key]))
            
            # Add remaining fields up to limit
            remaining_keys = [k for k in d.keys() if k not in priority_keys]
            for key in remaining_keys:
                if len(fields) < max_fields:
                    fields.append((key, d[key]))
                else:
                    break
            
            return fields
        
        important_fields = extract_important_fields(data)
        
        for key, value in important_fields:
            # Format value for display
            if isinstance(value, (dict, list)):
                if isinstance(value, dict):
                    display_value = f"Dict({len(value)} keys)"
                else:
                    display_value = f"List({len(value)} items)"
            else:
                value_str = str(value)
                display_value = value_str[:100] + '...' if len(value_str) > 100 else value_str
            
            structured['key_value_pairs'].append({
                'Property': key.replace('_', ' ').title(),
                'Value': display_value
            })
        
        return structured

    def _extract_cluster_status_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cluster status analysis results from ovnk_benchmark_analysis_cluster_stat.py output"""
        structured = {
            'analysis_overview': [],
            'cluster_health_summary': [],
            'node_groups_health': [],
            'resource_utilization': [],
            'critical_alerts': [],
            'top_recommendations': []
        }
        
        # Analysis Overview (metadata)
        metadata = data.get('metadata', {})
        cluster_health = data.get('cluster_health', {})
        
        structured['analysis_overview'] = [
            {'Property': 'Analysis Type', 'Value': metadata.get('collection_type', 'cluster_status')},
            {'Property': 'Analysis Time', 'Value': metadata.get('analysis_timestamp', 'Unknown')[:19]},
            {'Property': 'Duration', 'Value': metadata.get('duration', 'N/A')},
            {'Property': 'Items Analyzed', 'Value': metadata.get('total_items_analyzed', 0)},
            {'Property': 'Overall Score', 'Value': f"{cluster_health.get('overall_score', 0)}/100"}
        ]
        
        # Cluster Health Summary
        structured['cluster_health_summary'] = [
            {'Health Metric': 'Overall Score', 'Value': f"{cluster_health.get('overall_score', 0)}/100"},
            {'Health Metric': 'Health Level', 'Value': cluster_health.get('health_level', 'unknown').upper()},
            {'Health Metric': 'Critical Issues', 'Value': cluster_health.get('critical_issues_count', 0)},
            {'Health Metric': 'Warning Issues', 'Value': cluster_health.get('warning_issues_count', 0)},
            {'Health Metric': 'Healthy Items', 'Value': cluster_health.get('healthy_items_count', 0)}
        ]
        
        # Node Groups Health (limit to 5 columns for readability)
        node_groups = data.get('node_groups', {})
        for group_type, group_data in node_groups.items():
            resource_summary = group_data.get('resource_summary', {})
            
            structured['node_groups_health'].append({
                'Node Type': group_type.title(),
                'Total': group_data.get('total_nodes', 0),
                'Ready': group_data.get('ready_nodes', 0),
                'Health Score': f"{group_data.get('health_score', 0):.1f}%",
                'CPU Cores': resource_summary.get('total_cpu_cores', 0)
            })
        
        # Resource Utilization Summary
        resource_util = data.get('resource_utilization', {})
        network_analysis = data.get('network_policy_analysis', {})
        
        structured['resource_utilization'] = [
            {'Resource Metric': 'Pod Density', 'Value': resource_util.get('pod_density', 0)},
            {'Resource Metric': 'Namespaces', 'Value': resource_util.get('namespace_distribution', 0)},
            {'Resource Metric': 'Service/Pod Ratio', 'Value': resource_util.get('service_to_pod_ratio', 0)},
            {'Resource Metric': 'Network Policies', 'Value': network_analysis.get('total_network_resources', 0)},
            {'Resource Metric': 'Network Complexity', 'Value': f"{network_analysis.get('network_complexity_score', 0)}/100"}
        ]
        
        # Critical Alerts (top 5 most severe)
        alerts = data.get('alerts', [])
        critical_alerts = [alert for alert in alerts if alert.get('severity', '').upper() in ['CRITICAL', 'HIGH']]
        
        for i, alert in enumerate(critical_alerts[:5], 1):
            # Truncate long messages for readability
            message = alert.get('message', 'No message')
            if len(message) > 60:
                message = message[:57] + '...'
            
            structured['critical_alerts'].append({
                'Priority': i,
                'Severity': alert.get('severity', 'unknown').upper(),
                'Component': alert.get('component_name', 'unknown'),
                'Issue': message,
                'Current': f"{alert.get('current_value', 0)} {alert.get('unit', '')}"
            })
        
        # Top Recommendations (limit to 5 for readability)
        recommendations = data.get('recommendations', [])
        for i, recommendation in enumerate(recommendations[:5], 1):
            # Truncate long recommendations
            rec_text = recommendation
            if len(rec_text) > 80:
                rec_text = rec_text[:77] + '...'
            
            structured['top_recommendations'].append({
                'Priority': i,
                'Recommendation': rec_text
            })
        
        # Additional tables for comprehensive view (2-column format)
        
        # Cluster Operators Summary
        operators_summary = data.get('cluster_operators_summary', {})
        if operators_summary:
            structured['operators_status'] = [
                {'Operator Metric': 'Total Operators', 'Value': operators_summary.get('total_operators_estimated', 0)},
                {'Operator Metric': 'Available', 'Value': operators_summary.get('available_operators', 0)},
                {'Operator Metric': 'Unavailable', 'Value': operators_summary.get('unavailable_operators', 0)},
                {'Operator Metric': 'Availability %', 'Value': f"{operators_summary.get('availability_percentage', 0)}%"},
                {'Operator Metric': 'Health Status', 'Value': operators_summary.get('health_status', 'unknown').upper()}
            ]
        
        # MCP Summary
        mcp_summary = data.get('mcp_summary', {})
        if mcp_summary:
            status_dist = mcp_summary.get('status_distribution', {})
            structured['mcp_status'] = [
                {'MCP Metric': 'Total Pools', 'Value': mcp_summary.get('total_pools', 0)},
                {'MCP Metric': 'Health Score', 'Value': f"{mcp_summary.get('health_score', 0):.1f}/100"},
                {'MCP Metric': 'Updated Pools', 'Value': status_dist.get('Updated', 0)},
                {'MCP Metric': 'Degraded Pools', 'Value': status_dist.get('Degraded', 0)},
                {'MCP Metric': 'Health Status', 'Value': mcp_summary.get('health_status', 'unknown').upper()}
            ]
        
        return structured

    def _extract_ovn_sync_duration(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVN sync duration metrics from ovnk_benchmark_prometheus_ovnk_sync.py output"""
        structured = {
            'sync_summary': [],
            'controller_ready_duration_top5': [],
            'node_ready_duration_top5': [],
            'controller_sync_duration_top5': [],
            'controller_sync_service_total_top5': []
        }
        
        # Collection summary
        structured['sync_summary'] = [
            {'Property': 'Collection Type', 'Value': data.get('collection_type', 'instant')},
            {'Property': 'Collection Time', 'Value': data.get('collection_timestamp', 'Unknown')[:19]},
            {'Property': 'Duration', 'Value': data.get('duration', 'N/A')},
            {'Property': 'Timezone', 'Value': data.get('timezone', 'UTC')},
            {'Property': 'Total Metrics', 'Value': data.get('overall_summary', {}).get('metrics_collected', 0)}
        ]
        
        # Controller ready duration (top 5)
        controller_ready = data.get('controller_ready_duration', {})
        if 'error' not in controller_ready and 'top_10' in controller_ready:
            for i, item in enumerate(controller_ready['top_10'][:5], 1):
                readable = item.get('readable_value', {}) if data.get('collection_type') == 'instant' else item.get('readable_max', {})
                structured['controller_ready_duration_top5'].append({
                    'Rank': i,
                    'Pod Name': item.get('pod_name', 'unknown'),
                    'Node': item.get('node_name', 'unknown'),
                    'Duration': f"{readable.get('value', 0)} {readable.get('unit', 's')}",
                    'Raw Value': f"{item.get('value', item.get('max_value', 0)):.4f}"
                })
        
        # Node ready duration (top 5)
        node_ready = data.get('node_ready_duration', {})
        if 'error' not in node_ready and 'top_10' in node_ready:
            for i, item in enumerate(node_ready['top_10'][:5], 1):
                readable = item.get('readable_value', {}) if data.get('collection_type') == 'instant' else item.get('readable_max', {})
                structured['node_ready_duration_top5'].append({
                    'Rank': i,
                    'Pod Name': item.get('pod_name', 'unknown'),
                    'Node': item.get('node_name', 'unknown'),
                    'Duration': f"{readable.get('value', 0)} {readable.get('unit', 's')}",
                    'Raw Value': f"{item.get('value', item.get('max_value', 0)):.4f}"
                })
        
        # Sync duration (top 5 from top 20)
        sync_duration = data.get('controller_sync_duration', {})
        if 'error' not in sync_duration and 'top_20' in sync_duration:
            for i, item in enumerate(sync_duration['top_20'][:5], 1):
                readable = item.get('readable_value', {}) if data.get('collection_type') == 'instant' else item.get('readable_max', {})
                pod_resource = item.get('pod_resource_name', item.get('pod_name', 'unknown'))
                # Truncate long resource names
                if len(pod_resource) > 50:
                    pod_resource = pod_resource[:47] + '...'
                
                structured['controller_sync_duration_top5'].append({
                    'Rank': i,
                    'Pod:Resource': pod_resource,
                    'Node': item.get('node_name', 'unknown'),
                    'Duration': f"{readable.get('value', 0)} {readable.get('unit', 's')}",
                    'Raw Value': f"{item.get('value', item.get('max_value', 0)):.4f}"
                })
        
        # Service rate (top 5)
        service_rate = data.get('controller_service_rate', {})
        if 'error' not in service_rate and 'top_10' in service_rate:
            for i, item in enumerate(service_rate['top_10'][:5], 1):
                readable = item.get('readable_value', {}) if data.get('collection_type') == 'instant' else item.get('readable_max', {})
                structured['controller_sync_service_total_top5'].append({
                    'Rank': i,
                    'Pod Name': item.get('pod_name', 'unknown'),
                    'Node': item.get('node_name', 'unknown'),
                    'Rate': f"{readable.get('value', 0)} {readable.get('unit', 'ops/sec')}",
                    'Raw Value': f"{item.get('value', item.get('max_value', 0)):.4f}"
                })
        
        return structured

    def _extract_pod_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract pod usage metrics from ovnk_benchmark_prometheus_pods_usage.py output"""
        structured = {
            'usage_summary': [],
            'top_cpu_pods': [],
            'top_memory_pods': []
        }
        
        # Usage collection summary
        structured['usage_summary'] = [
            {'Property': 'Collection Type', 'Value': data.get('collection_type', 'instant')},
            {'Property': 'Collection Time', 'Value': data.get('collection_timestamp', 'Unknown')[:19]},
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

    def _extract_ovs_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVS usage metrics from ovnk_benchmark_prometheus_ovnk_ovs.py output"""
        structured = {
            'collection_summary': [],
            'cpu_usage_summary': [],
            'memory_usage_summary': [],
            'ovs_vswitchd_top5': [],
            'ovsdb_server_top5': [],
            'ovs_memory_top5': [],
            'dp_flows_summary': [],
            'bridge_flows_summary': [],
            'connection_metrics_summary': []
        }
        
        # Collection summary
        structured['collection_summary'] = [
            {'Property': 'Collection Type', 'Value': data.get('collection_type', 'unknown')},
            {'Property': 'Collection Time', 'Value': data.get('timestamp', 'Unknown')[:19]},
            {'Property': 'Query Type', 'Value': 'Range' if 'range' in data.get('collection_type', '') else 'Instant'}
        ]
        
        # CPU Usage Summary
        cpu_data = data.get('cpu_usage', {})
        if 'error' not in cpu_data:
            vswitchd_count = len(cpu_data.get('ovs_vswitchd_cpu', []))
            ovsdb_count = len(cpu_data.get('ovsdb_server_cpu', []))
            
            structured['cpu_usage_summary'] = [
                {'Component': 'OVS vSwitchd', 'Node Count': vswitchd_count, 'Status': 'Available'},
                {'Component': 'OVSDB Server', 'Node Count': ovsdb_count, 'Status': 'Available'}
            ]
            
            # Top 5 OVS vSwitchd CPU usage
            vswitchd_top = cpu_data.get('summary', {}).get('ovs_vswitchd_top10', [])
            for i, item in enumerate(vswitchd_top[:5], 1):
                structured['ovs_vswitchd_top5'].append({
                    'Rank': i,
                    'Node': item.get('node_name', 'unknown'),
                    'Max CPU (%)': f"{item.get('max', 0):.2f}",
                    'Avg CPU (%)': f"{item.get('avg', 0):.2f}"
                })
            
            # Top 5 OVSDB Server CPU usage
            ovsdb_top = cpu_data.get('summary', {}).get('ovsdb_server_top10', [])
            for i, item in enumerate(ovsdb_top[:5], 1):
                structured['ovsdb_server_top5'].append({
                    'Rank': i,
                    'Node': item.get('node_name', 'unknown'),
                    'Max CPU (%)': f"{item.get('max', 0):.2f}",
                    'Avg CPU (%)': f"{item.get('avg', 0):.2f}"
                })
        else:
            structured['cpu_usage_summary'] = [
                {'Component': 'CPU Usage', 'Status': 'Error', 'Message': cpu_data.get('error', 'Unknown error')}
            ]
        
        # Memory Usage Summary
        memory_data = data.get('memory_usage', {})
        if 'error' not in memory_data:
            db_count = len(memory_data.get('ovs_db_memory', []))
            vswitchd_mem_count = len(memory_data.get('ovs_vswitchd_memory', []))
            
            structured['memory_usage_summary'] = [
                {'Component': 'OVS DB', 'Pod Count': db_count, 'Status': 'Available'},
                {'Component': 'OVS vSwitchd', 'Pod Count': vswitchd_mem_count, 'Status': 'Available'}
            ]
            
            # Top 5 Memory consumers (combine both types)
            all_memory = []
            
            # Add OVS DB memory
            for item in memory_data.get('summary', {}).get('ovs_db_top10', []):
                all_memory.append({
                    'Type': 'OVS DB',
                    'Pod': item.get('pod_name', 'unknown'),
                    'Max Memory': f"{item.get('max', 0)} {item.get('unit', 'MB')}",
                    'Avg Memory': f"{item.get('avg', 0)} {item.get('unit', 'MB')}"
                })
            
            # Add OVS vSwitchd memory
            for item in memory_data.get('summary', {}).get('ovs_vswitchd_top10', []):
                all_memory.append({
                    'Type': 'vSwitchd',
                    'Pod': item.get('pod_name', 'unknown'),
                    'Max Memory': f"{item.get('max', 0)} {item.get('unit', 'MB')}",
                    'Avg Memory': f"{item.get('avg', 0)} {item.get('unit', 'MB')}"
                })
            
            # Sort by max memory and take top 5
            try:
                all_memory.sort(key=lambda x: float(x['Max Memory'].split()[0]), reverse=True)
            except:
                pass  # Keep original order if parsing fails
            
            for i, item in enumerate(all_memory[:5], 1):
                structured['ovs_memory_top5'].append({
                    'Rank': i,
                    'Type': item['Type'],
                    'Pod': item['Pod'],
                    'Max Memory': item['Max Memory'],
                    'Avg Memory': item['Avg Memory']
                })
        else:
            structured['memory_usage_summary'] = [
                {'Component': 'Memory Usage', 'Status': 'Error', 'Message': memory_data.get('error', 'Unknown error')}
            ]
        
        # DP Flows Summary
        dp_flows = data.get('dp_flows', {})
        if 'error' not in dp_flows:
            flow_count = len(dp_flows.get('data', []))
            top_flows = dp_flows.get('top_10', [])
            
            structured['dp_flows_summary'] = [
                {'Metric': 'Total Instances', 'Value': flow_count},
                {'Metric': 'Top Flow Count', 'Value': f"{top_flows[0].get('max', 0):.0f} flows" if top_flows else 'N/A'},
                {'Metric': 'Metric Name', 'Value': dp_flows.get('metric', 'ovs_vswitchd_dp_flows_total')}
            ]
        else:
            structured['dp_flows_summary'] = [
                {'Metric': 'DP Flows', 'Status': 'Error', 'Message': dp_flows.get('error', 'Unknown error')}
            ]
        
        # Bridge Flows Summary
        bridge_flows = data.get('bridge_flows', {})
        if 'error' not in bridge_flows:
            br_int_count = len(bridge_flows.get('br_int_flows', []))
            br_ex_count = len(bridge_flows.get('br_ex_flows', []))
            
            # Get top flows for each bridge
            br_int_top = bridge_flows.get('top_10', {}).get('br_int', [])
            br_ex_top = bridge_flows.get('top_10', {}).get('br_ex', [])
            
            structured['bridge_flows_summary'] = [
                {'Bridge': 'br-int', 'Instance Count': br_int_count, 'Top Flows': f"{br_int_top[0].get('max', 0):.0f}" if br_int_top else 'N/A'},
                {'Bridge': 'br-ex', 'Instance Count': br_ex_count, 'Top Flows': f"{br_ex_top[0].get('max', 0):.0f}" if br_ex_top else 'N/A'}
            ]
        else:
            structured['bridge_flows_summary'] = [
                {'Bridge': 'Bridge Flows', 'Status': 'Error', 'Message': bridge_flows.get('error', 'Unknown error')}
            ]
        
        # Connection Metrics Summary
        conn_metrics = data.get('connection_metrics', {})
        if 'error' not in conn_metrics:
            metrics_data = conn_metrics.get('connection_metrics', {})
            
            for metric_name, metric_info in metrics_data.items():
                if 'error' not in metric_info:
                    structured['connection_metrics_summary'].append({
                        'Metric': metric_name.replace('_', ' ').title(),
                        'Max': f"{metric_info.get('max', 0):.0f}",
                        'Avg': f"{metric_info.get('avg', 0):.0f}",
                        'Unit': metric_info.get('unit', 'count')
                    })
                else:
                    structured['connection_metrics_summary'].append({
                        'Metric': metric_name.replace('_', ' ').title(),
                        'Status': 'Error',
                        'Message': metric_info.get('error', 'Unknown error')[:50]
                    })
        
        return structured

    def _extract_node_usage_enhanced(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced extraction for node usage metrics from ovnk_benchmark_prometheus_nodes_usage.py output"""
        structured = {
            'collection_summary': [],
            'node_group_overview': [],
            'top_cpu_nodes': [],
            'top_memory_nodes': [],
            'network_activity_summary': []
        }
        
        # Collection summary
        metadata = data.get('metadata', {})
        structured['collection_summary'] = [
            {'Property': 'Query Duration', 'Value': metadata.get('duration', 'Unknown')},
            {'Property': 'Collection Time', 'Value': metadata.get('query_time', 'Unknown')[:19]},
            {'Property': 'Start Time', 'Value': metadata.get('start_time', 'Unknown')[:19]},
            {'Property': 'End Time', 'Value': metadata.get('end_time', 'Unknown')[:19]},
            {'Property': 'Timezone', 'Value': metadata.get('timezone', 'UTC')}
        ]
        
        # Node group overview (enhanced with network data)
        groups = data.get('groups', {})
        for role, group_data in groups.items():
            if group_data.get('nodes'):
                summary = group_data.get('summary', {})
                cpu_summary = summary.get('cpu_usage', {})
                memory_summary = summary.get('memory_usage', {})
                network_rx_summary = summary.get('network_rx', {})
                network_tx_summary = summary.get('network_tx', {})
                
                # Format network values (convert bytes/s to MB/s if needed)
                net_rx_max = network_rx_summary.get('max', 0)
                net_tx_max = network_tx_summary.get('max', 0)
                
                # Convert to MB/s for readability
                rx_mbps = f"{net_rx_max / (1024*1024):.1f}" if net_rx_max and net_rx_max > 0 else "0.0"
                tx_mbps = f"{net_tx_max / (1024*1024):.1f}" if net_tx_max and net_tx_max > 0 else "0.0"
                
                structured['node_group_overview'].append({
                    'Role': role.title(),
                    'Node Count': group_data.get('count', 0),
                    'CPU Avg (%)': f"{cpu_summary.get('avg', 0):.1f}" if cpu_summary.get('avg') is not None else 'N/A',
                    'CPU Max (%)': f"{cpu_summary.get('max', 0):.1f}" if cpu_summary.get('max') is not None else 'N/A',
                    'Memory Max (MB)': f"{memory_summary.get('max', 0):.0f}" if memory_summary.get('max') is not None else 'N/A'
                })
        
        # Top CPU usage nodes (enhanced with more details)
        top_cpu = data.get('top_usage', {}).get('cpu', [])
        for i, node in enumerate(top_cpu[:5], 1):
            structured['top_cpu_nodes'].append({
                'Rank': i,
                'Node Name': node.get('name', 'unknown'),
                'CPU Max (%)': f"{node.get('cpu_max', 0):.1f}",
                'CPU Avg (%)': f"{node.get('cpu_avg', 0):.1f}",
                'Instance': node.get('instance', 'unknown').split(':')[0]  # Show just hostname part
            })
        
        # Top memory usage nodes (enhanced with more details)
        top_memory = data.get('top_usage', {}).get('memory', [])
        for i, node in enumerate(top_memory[:5], 1):
            structured['top_memory_nodes'].append({
                'Rank': i,
                'Node Name': node.get('name', 'unknown'),
                'Memory Max (MB)': f"{node.get('memory_max', 0):.0f}",
                'Memory Avg (MB)': f"{node.get('memory_avg', 0):.0f}",
                'Instance': node.get('instance', 'unknown').split(':')[0]
            })
        
        # Network activity summary (new section for network metrics)
        worker_group = groups.get('worker', {})
        if worker_group.get('summary'):
            worker_summary = worker_group['summary']
            network_rx = worker_summary.get('network_rx', {})
            network_tx = worker_summary.get('network_tx', {})
            
            structured['network_activity_summary'] = [
                {'Metric': 'Worker RX Max (MB/s)', 'Value': f"{(network_rx.get('max', 0) or 0) / (1024*1024):.2f}"},
                {'Metric': 'Worker RX Avg (MB/s)', 'Value': f"{(network_rx.get('avg', 0) or 0) / (1024*1024):.2f}"},
                {'Metric': 'Worker TX Max (MB/s)', 'Value': f"{(network_tx.get('max', 0) or 0) / (1024*1024):.2f}"},
                {'Metric': 'Worker TX Avg (MB/s)', 'Value': f"{(network_tx.get('avg', 0) or 0) / (1024*1024):.2f}"}
            ]
        
        return structured

    def _extract_ovs_comprehensive(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract comprehensive OVS metrics from collect_all_ovs_metrics output"""
        structured = {
            'ovs_overview': [],
            'cpu_performance': [],
            'memory_performance': [],
            'flow_metrics': [],
            'connection_health': []
        }
        
        # OVS Overview
        structured['ovs_overview'] = [
            {'Property': 'Collection Type', 'Value': data.get('collection_type', 'unknown')},
            {'Property': 'Collection Time', 'Value': data.get('timestamp', 'Unknown')[:19]},
            {'Property': 'CPU Metrics', 'Value': 'Available' if data.get('cpu_usage', {}).get('error') is None else 'Error'},
            {'Property': 'Memory Metrics', 'Value': 'Available' if data.get('memory_usage', {}).get('error') is None else 'Error'},
            {'Property': 'Flow Metrics', 'Value': 'Available' if data.get('dp_flows', {}).get('error') is None else 'Error'}
        ]
        
        # CPU Performance (top performers from both components)
        cpu_data = data.get('cpu_usage', {})
        if 'error' not in cpu_data:
            # Combine top performers from both components
            all_cpu_performers = []
            
            # Add vSwitchd top performers
            for item in cpu_data.get('summary', {}).get('ovs_vswitchd_top10', [])[:3]:
                all_cpu_performers.append({
                    'Component': 'vSwitchd',
                    'Node': item.get('node_name', 'unknown'),
                    'Max CPU (%)': f"{item.get('max', 0):.2f}",
                    'Performance': 'High' if item.get('max', 0) > 50 else 'Normal'
                })
            
            # Add OVSDB top performers
            for item in cpu_data.get('summary', {}).get('ovsdb_server_top10', [])[:2]:
                all_cpu_performers.append({
                    'Component': 'OVSDB',
                    'Node': item.get('node_name', 'unknown'),
                    'Max CPU (%)': f"{item.get('max', 0):.2f}",
                    'Performance': 'High' if item.get('max', 0) > 30 else 'Normal'
                })
            
            structured['cpu_performance'] = all_cpu_performers[:5]
        
        # Memory Performance (top 5 across all components)
        memory_data = data.get('memory_usage', {})
        if 'error' not in memory_data:
            all_memory_performers = []
            
            # Add DB memory consumers
            for item in memory_data.get('summary', {}).get('ovs_db_top10', [])[:3]:
                all_memory_performers.append({
                    'Component': 'OVS DB',
                    'Pod': item.get('pod_name', 'unknown'),
                    'Max Memory': f"{item.get('max', 0)} {item.get('unit', 'MB')}",
                    'Avg Memory': f"{item.get('avg', 0)} {item.get('unit', 'MB')}"
                })
            
            # Add vSwitchd memory consumers
            for item in memory_data.get('summary', {}).get('ovs_vswitchd_top10', [])[:2]:
                all_memory_performers.append({
                    'Component': 'vSwitchd',
                    'Pod': item.get('pod_name', 'unknown'),
                    'Max Memory': f"{item.get('max', 0)} {item.get('unit', 'MB')}",
                    'Avg Memory': f"{item.get('avg', 0)} {item.get('unit', 'MB')}"
                })
            
            structured['memory_performance'] = all_memory_performers[:5]
        
        # Flow Metrics Summary
        dp_flows = data.get('dp_flows', {})
        bridge_flows = data.get('bridge_flows', {})
        
        flow_summary = []
        
        # DP Flows
        if 'error' not in dp_flows:
            top_dp = dp_flows.get('top_10', [])
            if top_dp:
                flow_summary.append({
                    'Flow Type': 'DP Flows',
                    'Top Instance': top_dp[0].get('instance', 'unknown'),
                    'Max Flows': f"{top_dp[0].get('max', 0):.0f}"
                })
        
        # Bridge Flows
        if 'error' not in bridge_flows:
            br_int_top = bridge_flows.get('top_10', {}).get('br_int', [])
            br_ex_top = bridge_flows.get('top_10', {}).get('br_ex', [])
            
            if br_int_top:
                flow_summary.append({
                    'Flow Type': 'br-int',
                    'Top Instance': br_int_top[0].get('instance', 'unknown'),
                    'Max Flows': f"{br_int_top[0].get('max', 0):.0f}"
                })
            
            if br_ex_top:
                flow_summary.append({
                    'Flow Type': 'br-ex',
                    'Top Instance': br_ex_top[0].get('instance', 'unknown'),
                    'Max Flows': f"{br_ex_top[0].get('max', 0):.0f}"
                })
        
        structured['flow_metrics'] = flow_summary[:5]
        
        # Connection Health
        conn_metrics = data.get('connection_metrics', {})
        if 'error' not in conn_metrics:
            metrics_data = conn_metrics.get('connection_metrics', {})
            
            for metric_name, metric_info in list(metrics_data.items())[:5]:
                if 'error' not in metric_info:
                    structured['connection_health'].append({
                        'Connection Metric': metric_name.replace('_', ' ').title(),
                        'Max Count': f"{metric_info.get('max', 0):.0f}",
                        'Avg Count': f"{metric_info.get('avg', 0):.0f}",
                        'Status': 'Healthy' if metric_info.get('max', 0) == 0 or 'overflow' not in metric_name else 'Check'
                    })
        
        return structured

    def _summarize_cluster_info(self, data: Dict[str, Any]) -> str:
        """Generate cluster info summary"""
        summary = ["Cluster Information Summary:"]
        
        # Basic cluster info
        cluster_name = data.get('cluster_overview', [{}])[0].get('Value', 'Unknown')
        if cluster_name != 'Unknown':
            summary.append(f"• Cluster: {cluster_name}")
        
        version_info = next((item for item in data.get('cluster_overview', []) if item.get('Property') == 'Version'), {})
        if version_info.get('Value', 'Unknown') != 'Unknown':
            summary.append(f"• Version: {version_info['Value']}")
        
        platform_info = next((item for item in data.get('cluster_overview', []) if item.get('Property') == 'Platform'), {})
        if platform_info.get('Value', 'Unknown') != 'Unknown':
            summary.append(f"• Platform: {platform_info['Value']}")
        
        # Node summary
        if 'node_distribution' in data:
            total_nodes = sum(item.get('Count', 0) for item in data['node_distribution'])
            total_ready = sum(item.get('Ready', 0) for item in data['node_distribution'])
            summary.append(f"• Nodes: {total_ready}/{total_nodes} ready")
            
            # Details by type
            for item in data['node_distribution']:
                if item.get('Count', 0) > 0:
                    node_type = item['Node Type']
                    count = item['Count'] 
                    ready = item['Ready']
                    summary.append(f"• {node_type}: {ready}/{count} ready")
        
        # Resource highlights
        if 'resource_summary' in data:
            # Find pods and namespaces from the resource summary
            pods_count = 0
            namespaces_count = 0
            
            for row in data['resource_summary']:
                if row.get('Resource Type 1') == 'Pods':
                    pods_count = row.get('Count 1', 0)
                elif row.get('Resource Type 2') == 'Pods':
                    pods_count = row.get('Count 2', 0)
                elif row.get('Resource Type 1') == 'Namespaces':
                    namespaces_count = row.get('Count 1', 0)
                elif row.get('Resource Type 2') == 'Namespaces':
                    namespaces_count = row.get('Count 2', 0)
            
            if pods_count > 0:
                summary.append(f"• Pods: {pods_count}")
            if namespaces_count > 0:
                summary.append(f"• Namespaces: {namespaces_count}")
        
        # Health status
        if 'cluster_health_status' in data:
            unavailable_ops = next((item for item in data['cluster_health_status'] 
                                if item.get('Health Metric') == 'Unavailable Operators'), {})
            if unavailable_ops.get('Value', 0) > 0:
                summary.append(f"⚠ {unavailable_ops['Value']} operators unavailable")
            
            degraded_mcp = next((item for item in data['cluster_health_status'] 
                            if item.get('Health Metric') == 'MCP Degraded Pools'), {})
            if degraded_mcp.get('Value', 0) > 0:
                summary.append(f"⚠ {degraded_mcp['Value']} MCP pools degraded")
        
        return " ".join(summary)

    def _summarize_prometheus_basic_info(self, data: Dict[str, Any]) -> str:
        """Generate Prometheus basic info summary"""
        summary = ["OVN Database Summary:"]
        
        if 'database_sizes' in data:
            for db in data['database_sizes']:
                status_indicator = "✓" if db['Status'] == 'Available' else "✗"
                summary.append(f"• {db['Database']}: {db['Max Size']} {status_indicator}")
        
        return " ".join(summary)
    
    def _summarize_kube_api_metrics(self, data: Dict[str, Any]) -> str:
        """Generate Kubernetes API metrics summary"""
        summary = ["API Server Performance Summary:"]
        
        if 'api_summary' in data:
            for item in data['api_summary']:
                if item['Property'] in ['Overall Health', 'Health Status']:
                    summary.append(f"• {item['Property']}: {item['Value']}")
        
        if 'latency_metrics' in data:
            summary.append(" Latency Performance:")
            for item in data['latency_metrics']:
                summary.append(f"• {item['Operation Type']}: {item['Max P99 (s)']}s max ({item['Status']})")
        
        return " ".join(summary)
    
    def _summarize_node_usage(self, data: Dict[str, Any]) -> str:
        """Generate node usage summary"""
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
        
        # Detailed node information
        if 'controlplane_nodes_detail' in data and data['controlplane_nodes_detail']:
            cp_count = len(data['controlplane_nodes_detail'])
            summary.append(f"• Control Plane Details: {cp_count} nodes with individual metrics")
        
        if 'infra_nodes_detail' in data and data['infra_nodes_detail']:
            infra_count = len(data['infra_nodes_detail'])
            summary.append(f"• Infrastructure Details: {infra_count} nodes with individual metrics")
        
        # Top resource consumers
        if 'top_cpu_workers' in data and data['top_cpu_workers'] and 'Status' not in data['top_cpu_workers'][0]:
            top_cpu_node = data['top_cpu_workers'][0]
            summary.append(f"• Top CPU Worker: {top_cpu_node.get('Node Name', 'unknown')} ({top_cpu_node.get('CPU Max (%)', 'N/A')}%)")
        
        if 'top_memory_workers' in data and data['top_memory_workers'] and 'Status' not in data['top_memory_workers'][0]:
            top_memory_node = data['top_memory_workers'][0]
            summary.append(f"• Top Memory Worker: {top_memory_node.get('Node Name', 'unknown')} ({top_memory_node.get('Memory Max (MB)', 'N/A')}MB)")
        
        return " ".join(summary)

    def _summarize_pod_status(self, data: Dict[str, Any]) -> str:
        """Generate pod status summary"""
        summary = ["Pod Status Summary:"]
        
        if 'pod_overview' in data:
            total_pods = next((item['Value'] for item in data['pod_overview'] if item['Metric'] == 'Total Pods'), 0)
            summary.append(f"• Total Pods: {total_pods}")
        
        if 'phase_distribution' in data:
            running_pods = next((item for item in data['phase_distribution'] if item['Phase'] == 'Running'), None)
            if running_pods:
                summary.append(f"• Running: {running_pods['Count']} ({running_pods['Percentage']})")
            
            # Check for problematic phases
            problem_phases = ['Failed', 'Pending', 'Unknown']
            for item in data['phase_distribution']:
                if item['Phase'] in problem_phases and int(item['Count']) > 0:
                    summary.append(f"• {item['Phase']}: {item['Count']} pods")
        
        return " ".join(summary)
    
    def _summarize_cluster_status(self, data: Dict[str, Any]) -> str:
        """Generate cluster status summary"""
        summary = ["Cluster Status Analysis:"]
        
        if 'executive_summary' in data:
            for item in data['executive_summary']:
                if item['Property'] in ['Cluster Status', 'Health Score', 'Critical Issues']:
                    summary.append(f"• {item['Property']}: {item['Value']}")
        
        if 'critical_issues' in data:
            critical_count = len(data['critical_issues'])
            if critical_count > 0:
                summary.append(f" {critical_count} critical issues found")
                top_issue = data['critical_issues'][0]
                summary.append(f"• Top Issue: [{top_issue['Component']}] {top_issue['Issue'][:50]}...")
        
        return " ".join(summary)
    
    def _summarize_generic(self, data: Dict[str, Any]) -> str:
        """Generate generic summary"""
        summary = ["Data Summary:"]
        
        if 'key_value_pairs' in data:
            summary.append(f"• Total properties: {len(data['key_value_pairs'])}")
            
            # Show first few important properties
            for item in data['key_value_pairs'][:3]:
                value_preview = str(item['Value'])[:30] + "..." if len(str(item['Value'])) > 30 else str(item['Value'])
                summary.append(f"• {item['Property']}: {value_preview}")
        
        return " ".join(summary)

    def _summarize_cluster_status_analysis(self, data: Dict[str, Any]) -> str:
        """Generate cluster status analysis summary"""
        summary = ["Cluster Status Analysis Summary:"]
        
        # Overall health
        if 'cluster_health_summary' in data:
            health_items = data['cluster_health_summary']
            overall_score = next((item['Value'] for item in health_items if item['Health Metric'] == 'Overall Score'), '0/100')
            health_level = next((item['Value'] for item in health_items if item['Health Metric'] == 'Health Level'), 'UNKNOWN')
            critical_count = next((item['Value'] for item in health_items if item['Health Metric'] == 'Critical Issues'), 0)
            
            summary.append(f"• Overall Health: {overall_score} ({health_level})")
            if critical_count > 0:
                summary.append(f"• Critical Issues: {critical_count}")
        
        # Node groups health
        if 'node_groups_health' in data:
            total_nodes = sum(item['Total'] for item in data['node_groups_health'])
            ready_nodes = sum(item['Ready'] for item in data['node_groups_health'])
            summary.append(f"• Nodes: {ready_nodes}/{total_nodes} ready")
            
            # Report any unhealthy node groups
            for group in data['node_groups_health']:
                if group['Ready'] < group['Total']:
                    not_ready = group['Total'] - group['Ready']
                    summary.append(f"• {group['Node Type']}: {not_ready} nodes not ready")
        
        # Critical alerts
        if 'critical_alerts' in data and data['critical_alerts']:
            alert_count = len(data['critical_alerts'])
            summary.append(f"• Critical Alerts: {alert_count}")
            
            # Mention top critical issue
            top_alert = data['critical_alerts'][0]
            summary.append(f"• Top Issue: [{top_alert['Component']}] {top_alert['Issue']}")
        
        # Operators status
        if 'operators_status' in data:
            unavailable = next((item['Value'] for item in data['operators_status'] if item['Operator Metric'] == 'Unavailable'), 0)
            if unavailable > 0:
                summary.append(f"• Unavailable Operators: {unavailable}")
        
        return " ".join(summary) 

    def _summarize_ovn_sync_duration(self, data: Dict[str, Any]) -> str:
        """Generate OVN sync duration summary"""
        summary = ["OVN Sync Duration Analysis:"]
        
        if 'sync_summary' in data:
            collection_type = next((item['Value'] for item in data['sync_summary'] if item['Property'] == 'Collection Type'), 'unknown')
            total_metrics = next((item['Value'] for item in data['sync_summary'] if item['Property'] == 'Total Metrics'), 0)
            summary.append(f"• Collection: {collection_type} ({total_metrics} metrics)")
        
        # Report top performers from each category
        categories = [
            ('controller_ready_duration_top5', 'Controller Ready'),
            ('node_ready_duration_top5', 'Node Ready'),
            ('controller_sync_duration_top5', 'Sync Duration'),
            ('controller_sync_service_total_top5', 'Service Rate')
        ]
        
        for table_name, category_name in categories:
            if table_name in data and data[table_name]:
                top_item = data[table_name][0]
                if table_name == 'controller_sync_duration_top5':
                    identifier = top_item.get('Pod:Resource', 'unknown')
                else:
                    identifier = top_item.get('Pod Name', 'unknown')
                
                if table_name == 'controller_sync_service_total_top5':
                    value = top_item.get('Rate', 'N/A')
                else:
                    value = top_item.get('Duration', 'N/A')
                
                summary.append(f"• Top {category_name}: {identifier} ({value})")
        
        return " ".join(summary)

    def _summarize_pod_usage(self, data: Dict[str, Any]) -> str:
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

    def _summarize_ovs_usage(self, data: Dict[str, Any]) -> str:
        """Generate OVS usage summary"""
        summary = ["OVS Usage Analysis:"]
        
        collection_type = data.get('collection_type', 'unknown')
        summary.append(f"• Collection: {collection_type}")
        
        # CPU summary
        if 'cpu_usage_summary' in data and data['cpu_usage_summary']:
            vswitchd_status = next((item for item in data['cpu_usage_summary'] if item['Component'] == 'OVS vSwitchd'), {})
            if vswitchd_status.get('Status') == 'Available':
                summary.append(f"• CPU Monitoring: {vswitchd_status['Node Count']} nodes")
        
        # Top CPU performer
        if 'ovs_vswitchd_top5' in data and data['ovs_vswitchd_top5']:
            top_cpu = data['ovs_vswitchd_top5'][0]
            summary.append(f"• Top CPU: {top_cpu['Node']} ({top_cpu['Max CPU (%)']}%)")
        
        # Top memory consumer
        if 'ovs_memory_top5' in data and data['ovs_memory_top5']:
            top_memory = data['ovs_memory_top5'][0]
            summary.append(f"• Top Memory: {top_memory['Pod']} ({top_memory['Max Memory']})")
        
        # Flow metrics
        if 'dp_flows_summary' in data and data['dp_flows_summary']:
            dp_info = next((item for item in data['dp_flows_summary'] if item['Metric'] == 'Top Flow Count'), {})
            if dp_info:
                summary.append(f"• Top DP Flows: {dp_info['Value']}")
        
        return " ".join(summary)

    def _summarize_ovs_comprehensive(self, data: Dict[str, Any]) -> str:
        """Generate comprehensive OVS metrics summary"""
        summary = ["Comprehensive OVS Analysis:"]
        
        # Overview status
        if 'ovs_overview' in data:
            overview = data['ovs_overview']
            cpu_status = next((item['Value'] for item in overview if item['Property'] == 'CPU Metrics'), 'Unknown')
            memory_status = next((item['Value'] for item in overview if item['Property'] == 'Memory Metrics'), 'Unknown')
            flow_status = next((item['Value'] for item in overview if item['Property'] == 'Flow Metrics'), 'Unknown')
            
            status_indicators = []
            if cpu_status == 'Available':
                status_indicators.append("CPU✓")
            if memory_status == 'Available':
                status_indicators.append("Memory✓")
            if flow_status == 'Available':
                status_indicators.append("Flows✓")
            
            summary.append(f"• Metrics: {', '.join(status_indicators)}")
        
        # Top performers
        if 'cpu_performance' in data and data['cpu_performance']:
            top_cpu = data['cpu_performance'][0]
            summary.append(f"• Top CPU: {top_cpu['Component']} on {top_cpu['Node']} ({top_cpu['Max CPU (%)']}%)")
        
        if 'memory_performance' in data and data['memory_performance']:
            top_memory = data['memory_performance'][0]
            summary.append(f"• Top Memory: {top_memory['Component']} {top_memory['Pod']} ({top_memory['Max Memory']})")
        
        # Flow activity
        if 'flow_metrics' in data and data['flow_metrics']:
            flow_types = [item['Flow Type'] for item in data['flow_metrics']]
            summary.append(f"• Flow Types: {', '.join(flow_types[:3])}")
        
        # Connection health
        if 'connection_health' in data:
            healthy_count = sum(1 for item in data['connection_health'] if item.get('Status') == 'Healthy')
            total_count = len(data['connection_health'])
            summary.append(f"• Connection Health: {healthy_count}/{total_count} healthy")
        
        return " ".join(summary)

    def create_compact_tables(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Create compact tables optimized for readability with 2-5 columns"""
        compact_dataframes = {}
        
        for table_name, table_data in structured_data.items():
            if isinstance(table_data, list) and table_data:
                df = pd.DataFrame(table_data)
                
                if df.empty:
                    continue
                
                # Create compact version based on table type
                if 'overview' in table_name or 'summary' in table_name:
                    # For overview/summary tables, use 2 columns: Property-Value format
                    if len(df.columns) > 2:
                        # Try to find property-value pattern
                        prop_col = None
                        val_col = None
                        
                        for col in df.columns:
                            col_lower = col.lower()
                            if any(word in col_lower for word in ['property', 'metric', 'indicator', 'name']):
                                prop_col = col
                            elif any(word in col_lower for word in ['value', 'count', 'status', 'result']):
                                val_col = col
                        
                        if prop_col and val_col:
                            compact_df = df[[prop_col, val_col]].copy()
                            compact_df.columns = ['Property', 'Value']
                        else:
                            # Take first 2 columns
                            compact_df = df.iloc[:, :2].copy()
                    else:
                        compact_df = df.copy()
                
                elif 'top' in table_name or 'latency' in table_name:
                    # For ranking/performance tables, use 3-4 columns
                    important_cols = []
                    
                    # Look for rank/index column
                    for col in df.columns:
                        if any(word in col.lower() for word in ['rank', 'index', 'priority', '#']):
                            important_cols.append(col)
                            break
                    
                    # Look for name/identifier column
                    for col in df.columns:
                        if col not in important_cols and any(word in col.lower() for word in ['name', 'resource', 'operation', 'label']):
                            important_cols.append(col)
                            break
                    
                    # Look for value columns
                    for col in df.columns:
                        if col not in important_cols and any(word in col.lower() for word in ['value', 'latency', 'rate', 'usage', 'max', 'avg']):
                            important_cols.append(col)
                            if len(important_cols) >= 4:
                                break
                    
                    # Fill remaining slots
                    for col in df.columns:
                        if col not in important_cols:
                            important_cols.append(col)
                            if len(important_cols) >= 4:
                                break
                    
                    compact_df = df[important_cols[:4]].copy()
                
                else:
                    # For other tables, use up to 5 columns
                    compact_df = df.iloc[:, :self.max_columns].copy()
                
                # Limit rows to prevent overly long tables
                if len(compact_df) > 15:
                    compact_df = compact_df.head(15)
                
                compact_dataframes[table_name] = compact_df
        
        return compact_dataframes

    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames with column limits"""
        dataframes = {}
        
        try:
            for key, value in structured_data.items():
                if isinstance(value, list) and value:
                    df = pd.DataFrame(value)
                    
                    # Limit columns to max_columns
                    if len(df.columns) > self.max_columns:
                        # Keep most important columns
                        priority_cols = ['name', 'status', 'value', 'count', 'property']
                        
                        # Find priority columns that exist
                        keep_cols = []
                        for col in df.columns:
                            col_lower = col.lower()
                            if any(priority in col_lower for priority in priority_cols):
                                keep_cols.append(col)
                        
                        # Add remaining columns up to limit
                        remaining_cols = [col for col in df.columns if col not in keep_cols]
                        while len(keep_cols) < self.max_columns and remaining_cols:
                            keep_cols.append(remaining_cols.pop(0))
                        
                        df = df[keep_cols[:self.max_columns]]
                    
                    dataframes[key] = df
                    
                elif isinstance(value, dict):
                    # Handle nested dictionaries
                    for nested_key, nested_value in value.items():
                        if isinstance(nested_value, list) and nested_value:
                            df = pd.DataFrame(nested_value)
                            
                            # Apply column limit
                            if len(df.columns) > self.max_columns:
                                df = df.iloc[:, :self.max_columns]
                            
                            dataframes[f"{key}_{nested_key}"] = df
        
        except Exception as e:
            logger.error(f"Failed to transform to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames with improved styling"""
        html_tables = {}
        
        try:
            for name, df in dataframes.items():
                if not df.empty:
                    # Create styled HTML table
                    html = df.to_html(
                        index=False,
                        classes='table table-striped table-bordered table-sm',
                        escape=False,
                        table_id=f"table-{name.replace('_', '-')}",
                        border=1
                    )
                    
                    # Clean up HTML (remove newlines and extra whitespace)
                    html = re.sub(r'\s+', ' ', html.replace('\n', ' ').replace('\r', ''))
                    html = html.strip()
                    
                    # Add responsive wrapper
                    html = f'<div class="table-responsive">{html}</div>'
                    
                    html_tables[name] = html
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables: {e}")
        
        return html_tables
    
    def generate_brief_summary(self, structured_data: Dict[str, Any], data_type: str) -> str:
        """Generate a brief textual summary of the data"""
        try:
            if data_type == 'cluster_info':
                return self._summarize_cluster_info(structured_data)
            elif data_type == 'prometheus_basic_info':
                return self._summarize_prometheus_basic_info(structured_data)
            elif data_type == 'kube_api_metrics':
                return self._summarize_kube_api_metrics(structured_data)
            elif data_type == 'node_usage':
                return self._summarize_node_usage(structured_data)
            elif data_type == 'pod_status':
                return self._summarize_pod_status(structured_data)
            elif data_type == 'cluster_status':
                return self._summarize_cluster_status(structured_data)
            elif data_type == 'cluster_status_analysis':
                return self._summarize_cluster_status_analysis(structured_data)                
            elif data_type == 'ovn_sync_duration':
                return self._summarize_ovn_sync_duration(structured_data)
            elif data_type == 'pod_usage':
                return self._summarize_pod_usage(structured_data)
            elif data_type == 'ovs_usage':
                return self._summarize_ovs_usage(structured_data)
            elif data_type == 'ovs_comprehensive':
                return self._summarize_ovs_comprehensive(structured_data)                              
            else:
                return self._summarize_generic(structured_data)
        
        except Exception as e:
            logger.error(f"Failed to generate summary: {e}")
            return f"Summary generation failed: {str(e)}"
    
# Enhanced module functions
def extract_and_transform_mcp_results(mcp_results: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and transform MCP results into tables and summaries"""
    try:
        elt = PerformanceDataELT()
        
        # Extract data
        extracted = elt.extract_json_data(mcp_results)
        
        if 'error' in extracted:
            return extracted
        
        # Create both full and compact tables
        full_dataframes = elt.transform_to_dataframes(extracted['structured_data'])
        compact_dataframes = elt.create_compact_tables(extracted['structured_data'])
        
        # Generate HTML tables (use compact version for better readability)
        html_tables = elt.generate_html_tables(compact_dataframes)
        
        # Generate summary
        summary = elt.generate_brief_summary(
            extracted['structured_data'], 
            extracted['data_type']
        )
        
        return {
            'data_type': extracted['data_type'],
            'summary': summary,
            'html_tables': html_tables,
            'dataframes': full_dataframes,
            'compact_dataframes': compact_dataframes,
            'structured_data': extracted['structured_data'],
            'timestamp': extracted['timestamp']
        }
        
    except Exception as e:
        logger.error(f"Failed to extract and transform MCP results: {e}")
        return {'error': str(e)}

def format_results_as_table(results: Dict[str, Any], compact: bool = True) -> str:
    """Format results as HTML table string"""
    try:
        transformed = extract_and_transform_mcp_results(results)
        
        if 'error' in transformed:
            return f"<div class='alert alert-danger'>Error formatting table: {transformed['error']}</div>"
        
        if not transformed.get('html_tables'):
            return "<div class='alert alert-info'>No tabular data available</div>"
        
        # Create organized HTML output
        html_output = []
        
        # Add data type header
        data_type = transformed.get('data_type', 'unknown').replace('_', ' ').title()
        html_output.append(f"<h3>{data_type} Analysis</h3>")
        
        # Add summary
        if transformed.get('summary'):
            html_output.append(f"<div class='alert alert-info'><strong>Summary:</strong> {transformed['summary']}</div>")
        
        # Add tables with proper headers
        table_order = [
            'cluster_overview', 'api_summary', 'usage_summary', 'pod_overview',
            'database_sizes', 'ovn_database_metrics', 'executive_summary',
            'latency_metrics', 'group_summary', 'phase_distribution',
            'resource_counts', 'node_summary', 'cluster_health',
            'component_health', 'critical_issues', 'top_recommendations'
        ]
        
        # First add ordered tables
        for table_name in table_order:
            if table_name in transformed['html_tables']:
                table_title = table_name.replace('_', ' ').title()
                html_output.append(f"<h4>{table_title}</h4>")
                html_output.append(transformed['html_tables'][table_name])
        
        # Then add any remaining tables
        for table_name, table_html in transformed['html_tables'].items():
            if table_name not in table_order:
                table_title = table_name.replace('_', ' ').title()
                html_output.append(f"<h4>{table_title}</h4>")
                html_output.append(table_html)
        
        return ' '.join(html_output)
        
    except Exception as e:
        logger.error(f"Failed to format results as table: {e}")
        return f"<div class='alert alert-danger'>Error: {str(e)}</div>"

def convert_json_to_tables(json_data: Union[Dict[str, Any], str], 
                          table_format: str = "both",
                          compact: bool = True) -> Dict[str, Union[str, List[List]]]:
    """
    Convert JSON/dictionary data to table formats optimized for the specific modules
    
    Args:
        json_data: Input JSON data as dictionary or JSON string
        table_format: Output format - "tabular", "html", or "both" (default)
        compact: Whether to use compact table format (default True)
        
    Returns:
        Dictionary containing the requested table formats with metadata
    """
    try:
        # Parse JSON string if needed
        if isinstance(json_data, str):
            try:
                data = json.loads(json_data)
            except json.JSONDecodeError as e:
                return {
                    'error': f"Invalid JSON string: {str(e)}",
                    'metadata': {'conversion_failed': True}
                }
        else:
            data = json_data
        
        if not isinstance(data, dict):
            return {
                'error': "Input data must be a dictionary or JSON object",
                'metadata': {'conversion_failed': True}
            }
        
        # Initialize ELT processor
        elt = PerformanceDataELT()
        
        # Extract and structure the data
        extracted = elt.extract_json_data(data)
        
        if 'error' in extracted:
            return {
                'error': f"Data extraction failed: {extracted['error']}",
                'metadata': {'conversion_failed': True}
            }
        
        # Transform to DataFrames
        if compact:
            dataframes = elt.create_compact_tables(extracted['structured_data'])
        else:
            dataframes = elt.transform_to_dataframes(extracted['structured_data'])
        
        result = {
            'metadata': {
                'data_type': extracted['data_type'],
                'timestamp': extracted.get('timestamp'),
                'tables_generated': len(dataframes),
                'table_names': list(dataframes.keys()),
                'conversion_successful': True,
                'compact_mode': compact
            }
        }
        
        # Generate requested formats
        if table_format in ["tabular", "both"]:
            tabular_tables = {}
            
            for table_name, df in dataframes.items():
                if not df.empty:
                    # Convert DataFrame to list of lists
                    tabular_data = [df.columns.tolist()] + df.values.tolist()
                    
                    # Create formatted string version
                    try:
                        formatted_table = tabulate(
                            df.values.tolist(), 
                            headers=df.columns.tolist(),
                            tablefmt="grid",
                            stralign="left",
                            maxcolwidths=[30] * len(df.columns)  # Limit column width
                        )
                        tabular_tables[table_name] = {
                            'raw_data': tabular_data,
                            'formatted_string': formatted_table
                        }
                    except Exception as e:
                        logger.warning(f"Failed to format table {table_name}: {e}")
                        tabular_tables[table_name] = {
                            'raw_data': tabular_data,
                            'formatted_string': str(df)
                        }
            
            result['tabular'] = tabular_tables
        
        if table_format in ["html", "both"]:
            html_tables = elt.generate_html_tables(dataframes)
            result['html'] = html_tables
        
        # Add summary
        summary = elt.generate_brief_summary(extracted['structured_data'], extracted['data_type'])
        result['summary'] = summary
        
        return result
        
    except Exception as e:
        logger.error(f"Error converting JSON to tables: {e}")
        return {
            'error': str(e),
            'metadata': {'conversion_failed': True}
        }

def convert_cluster_info_to_tables(cluster_info_json: Union[Dict[str, Any], str]) -> Dict[str, str]:
    """
    Specialized converter for cluster info JSON (from ovnk_benchmark_openshift_cluster_info.py)
    
    Returns:
        Dictionary with HTML tables optimized for cluster info display
    """
    result = convert_json_to_tables(cluster_info_json, "html", compact=True)
    
    if 'error' in result:
        return {'error': result['error']}
    
    return result.get('html', {})

def convert_prometheus_basic_to_tables(prometheus_json: Union[Dict[str, Any], str]) -> Dict[str, str]:
    """
    Specialized converter for Prometheus basic info JSON (from ovnk_benchmark_prometheus_basicinfo.py)
    
    Returns:
        Dictionary with HTML tables optimized for Prometheus metrics display
    """
    result = convert_json_to_tables(prometheus_json, "html", compact=True)
    
    if 'error' in result:
        return {'error': result['error']}
    
    return result.get('html', {})

def convert_kube_api_to_tables(kube_api_json: Union[Dict[str, Any], str]) -> Dict[str, str]:
    """
    Specialized converter for Kubernetes API metrics JSON (from ovnk_benchmark_prometheus_kubeapi.py)
    
    Returns:
        Dictionary with HTML tables optimized for API metrics display
    """
    result = convert_json_to_tables(kube_api_json, "html", compact=True)
    
    if 'error' in result:
        return {'error': result['error']}
    
    return result.get('html', {})

def generate_brief_results(results: Dict[str, Any]) -> str:
    """Generate brief text summary of results"""
    try:
        transformed = extract_and_transform_mcp_results(results)
        
        if 'error' in transformed:
            return f"Error generating summary: {transformed['error']}"
        
        return transformed.get('summary', 'No summary available')
        
    except Exception as e:
        logger.error(f"Failed to generate brief results: {e}")
        return f"Error: {str(e)}"

def convert_dict_to_simple_table(data: Dict[str, Any], 
                                table_format: str = "both") -> Dict[str, Any]:
    """
    Convert a simple dictionary to table format with automatic key-value pair detection
    Limited to 2 columns for maximum readability
    """
    try:
        result = {
            'metadata': {
                'conversion_type': 'simple_dict_to_table',
                'original_keys': list(data.keys()),
                'conversion_successful': True
            }
        }
        
        # Create simple key-value table (2 columns)
        table_data = []
        for key, value in list(data.items())[:20]:  # Limit to 20 rows
            # Convert complex values to strings
            if isinstance(value, (dict, list)):
                if isinstance(value, dict):
                    value_str = f"Dict({len(value)} keys)" if len(value) > 5 else json.dumps(value, default=str)[:50]
                else:
                    value_str = f"List({len(value)} items)" if len(value) > 5 else str(value)[:50]
            else:
                value_str = str(value)[:80] + '...' if len(str(value)) > 80 else str(value)
            
            table_data.append([str(key).replace('_', ' ').title(), value_str])
        
        headers = ['Property', 'Value']
        
        if table_format in ["tabular", "both"]:
            raw_data = [headers] + table_data
            
            try:
                formatted_string = tabulate(
                    table_data,
                    headers=headers,
                    tablefmt="grid",
                    stralign="left",
                    maxcolwidths=[30, 50]
                )
            except Exception as e:
                formatted_string = "\n".join([f"{key}: {value}" for key, value in table_data])
            
            result['tabular'] = {
                'simple_table': {
                    'raw_data': raw_data,
                    'formatted_string': formatted_string
                }
            }
        
        if table_format in ["html", "both"]:
            df = pd.DataFrame(table_data, columns=headers)
            html_table = df.to_html(
                index=False,
                classes='table table-striped table-bordered table-sm',
                escape=False,
                table_id="simple-table"
            )
            html_table = re.sub(r'\s+', ' ', html_table.replace('\n', ' ').replace('\r', ''))
            result['html'] = {'simple_table': html_table.strip()}
        
        return result
        
    except Exception as e:
        logger.error(f"Error converting dict to simple table: {e}")
        return {
            'error': str(e),
            'metadata': {'conversion_failed': True}
        }

def auto_detect_and_convert_to_tables(data: Union[Dict[str, Any], str],
                                     table_format: str = "both",
                                     compact: bool = True) -> Dict[str, Any]:
    """
    Auto-detect data structure and convert to appropriate table format
    Optimized for OpenShift benchmark data formats
    """
    try:
        # Parse JSON string if needed
        if isinstance(data, str):
            try:
                parsed_data = json.loads(data)
            except json.JSONDecodeError:
                return {
                    'error': "Invalid JSON string provided",
                    'metadata': {'conversion_failed': True}
                }
        else:
            parsed_data = data
        
        if not isinstance(parsed_data, dict):
            return {
                'error': "Data must be a dictionary or JSON object",
                'metadata': {'conversion_failed': True}
            }
        
        # Try optimized conversion for known data types
        elt = PerformanceDataELT()
        data_type = elt._identify_data_type(parsed_data)
        
        if data_type in ['cluster_info', 'prometheus_basic_info', 'kube_api_metrics', 'node_usage', 'pod_status']:
            # Use specialized extraction
            comprehensive_result = convert_json_to_tables(parsed_data, table_format, compact)
            
            if not comprehensive_result.get('metadata', {}).get('conversion_failed', False):
                comprehensive_result['metadata']['detection_method'] = f'specialized_{data_type}'
                return comprehensive_result
        
        # Fall back to simple conversion for unknown formats
        simple_result = convert_dict_to_simple_table(parsed_data, table_format)
        
        if not simple_result.get('metadata', {}).get('conversion_failed', False):
            simple_result['metadata']['detection_method'] = 'simple_key_value'
            return simple_result
        
        # If both fail, return error
        return {
            'error': 'Failed to convert data using both specialized and simple methods',
            'metadata': {
                'conversion_failed': True,
                'data_type': data_type
            }
        }
        
    except Exception as e:
        logger.error(f"Error in auto-detect conversion: {e}")
        return {
            'error': str(e),
            'metadata': {'conversion_failed': True}
        }

def json_to_html_table(json_data: Union[Dict[str, Any], str], compact: bool = True) -> str:
    """Convert JSON data to HTML table format with compact option"""
    result = convert_json_to_tables(json_data, "html", compact)
    
    if 'error' in result:
        return f"<div class='alert alert-danger'>Error: {result['error']}</div>"
    
    html_tables = result.get('html', {})
    if not html_tables:
        return "<div class='alert alert-warning'>No tables generated</div>"
    
    # Generate organized output
    output_parts = []
    
    # Add data type info
    data_type = result.get('metadata', {}).get('data_type', 'unknown')
    output_parts.append(f"<div class='mb-3'><span class='badge badge-info'>{data_type.replace('_', ' ').title()}</span></div>")
    
    # Add summary if available
    if result.get('summary'):
        output_parts.append(f"<div class='alert alert-light'>{result['summary']}</div>")
    
    # Add tables
    for table_name, html_table in html_tables.items():
        table_title = table_name.replace('_', ' ').title()
        output_parts.append(f"<h5 class='mt-3'>{table_title}</h5>")
        output_parts.append(html_table)
    
    return ' '.join(output_parts)

def json_to_tabular_data(json_data: Union[Dict[str, Any], str], compact: bool = True) -> List[Dict[str, Any]]:
    """Convert JSON data to tabular format with compact option"""
    result = convert_json_to_tables(json_data, "tabular", compact)
    
    if 'error' in result:
        return [{'error': result['error']}]
    
    tabular_tables = result.get('tabular', {})
    table_list = []
    
    for table_name, table_data in tabular_tables.items():
        table_list.append({
            'name': table_name,
            'title': table_name.replace('_', ' ').title(),
            'headers': table_data['raw_data'][0] if table_data['raw_data'] else [],
            'rows': table_data['raw_data'][1:] if len(table_data['raw_data']) > 1 else [],
            'formatted_string': table_data.get('formatted_string', ''),
            'row_count': len(table_data['raw_data']) - 1 if table_data['raw_data'] else 0,
            'column_count': len(table_data['raw_data'][0]) if table_data['raw_data'] else 0
        })
    
    return table_list

# Validation and utility functions
def validate_json_structure(data: Union[Dict[str, Any], str]) -> Dict[str, Any]:
    """Validate and analyze JSON structure for table conversion suitability"""
    try:
        if isinstance(data, str):
            parsed_data = json.loads(data)
        else:
            parsed_data = data
        
        elt = PerformanceDataELT()
        data_type = elt._identify_data_type(parsed_data) if isinstance(parsed_data, dict) else 'unknown'
        
        analysis = {
            'is_valid': True,
            'data_type': data_type,
            'detected_format': 'specialized' if data_type != 'generic' else 'generic',
            'structure_info': {},
            'conversion_recommendations': []
        }
        
        if isinstance(parsed_data, dict):
            analysis['structure_info'] = {
                'total_keys': len(parsed_data),
                'nested_levels': _get_max_depth(parsed_data),
                'list_fields': [k for k, v in parsed_data.items() if isinstance(v, list)],
                'dict_fields': [k for k, v in parsed_data.items() if isinstance(v, dict)],
                'simple_fields': [k for k, v in parsed_data.items() if not isinstance(v, (dict, list))]
            }
            
            # Provide conversion recommendations
            if data_type != 'generic':
                analysis['conversion_recommendations'].append(f"Detected {data_type} format - using specialized extraction")
            elif analysis['structure_info']['list_fields']:
                analysis['conversion_recommendations'].append("Contains list fields suitable for tabular conversion")
            if analysis['structure_info']['nested_levels'] > 3:
                analysis['conversion_recommendations'].append("Deep nesting detected - using compact mode recommended")
            if len(analysis['structure_info']['simple_fields']) > len(analysis['structure_info']['list_fields']):
                analysis['conversion_recommendations'].append("Mostly key-value pairs - 2-column table format recommended")
        
        return analysis
        
    except json.JSONDecodeError as e:
        return {
            'is_valid': False,
            'error': f"Invalid JSON: {str(e)}",
            'conversion_recommendations': ["Fix JSON syntax before conversion"]
        }
    except Exception as e:
        return {
            'is_valid': False,
            'error': f"Validation error: {str(e)}",
            'conversion_recommendations': ["Check data format and try again"]
        }

def _get_max_depth(data: Dict[str, Any], current_depth: int = 0) -> int:
    """Calculate maximum nesting depth of a dictionary"""
    if not isinstance(data, dict):
        return current_depth
    
    max_depth = current_depth
    for value in data.values():
        if isinstance(value, dict):
            depth = _get_max_depth(value, current_depth + 1)
            max_depth = max(max_depth, depth)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    depth = _get_max_depth(item, current_depth + 1)
                    max_depth = max(max_depth, depth)
    
    return max_depth

def batch_convert_json_files(file_paths: List[str], 
                           output_format: str = "html",
                           compact: bool = True) -> Dict[str, Any]:
    """
    Convert multiple JSON files to table format
    
    Args:
        file_paths: List of JSON file paths
        output_format: Output format for tables
        compact: Use compact table format
        
    Returns:
        Dictionary with conversion results for each file
    """
    results = {}
    
    for file_path in file_paths:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            converted = convert_json_to_tables(data, output_format, compact)
            results[file_path] = {
                'success': not converted.get('metadata', {}).get('conversion_failed', False),
                'result': converted
            }
            
        except FileNotFoundError:
            results[file_path] = {
                'success': False,
                'error': f"File not found: {file_path}"
            }
        except json.JSONDecodeError as e:
            results[file_path] = {
                'success': False,
                'error': f"Invalid JSON in {file_path}: {str(e)}"
            }
        except Exception as e:
            results[file_path] = {
                'success': False,
                'error': f"Error processing {file_path}: {str(e)}"
            }
    
    return results

def create_dashboard_html(json_outputs: Dict[str, Union[Dict[str, Any], str]]) -> str:
    """
    Create a comprehensive HTML dashboard from multiple JSON outputs
    
    Args:
        json_outputs: Dictionary mapping source_name -> json_data
                     e.g., {'cluster_info': cluster_data, 'prometheus_basic': prom_data}
    
    Returns:
        Complete HTML dashboard string
    """
    try:
        dashboard_parts = []
        
        # Dashboard header
        dashboard_parts.append("""
        <div class='container-fluid'>
        <h1 class='mb-4'>OpenShift Cluster Performance Dashboard</h1>
        <div class='row'>
        """)
        
        # Process each JSON output
        for source_name, json_data in json_outputs.items():
            try:
                html_table = json_to_html_table(json_data, compact=True)
                
                # Create card for each source
                card_title = source_name.replace('_', ' ').title()
                dashboard_parts.append(f"""
                <div class='col-md-6 mb-4'>
                <div class='card'>
                <div class='card-header'>
                <h5 class='card-title'>{card_title}</h5>
                </div>
                <div class='card-body'>
                {html_table}
                </div>
                </div>
                </div>
                """)
                
            except Exception as e:
                # Add error card
                dashboard_parts.append(f"""
                <div class='col-md-6 mb-4'>
                <div class='card border-danger'>
                <div class='card-header bg-danger text-white'>
                <h5 class='card-title'>{source_name.replace('_', ' ').title()} - Error</h5>
                </div>
                <div class='card-body'>
                <div class='alert alert-danger'>Error processing {source_name}: {str(e)}</div>
                </div>
                </div>
                </div>
                """)
        
        # Dashboard footer
        dashboard_parts.append("</div></div>")
        
        # Combine with basic CSS
        full_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
        <title>OpenShift Performance Dashboard</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
        .table-responsive {{ max-height: 400px; overflow-y: auto; }}
        .card {{ box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .badge {{ font-size: 0.9em; }}
        </style>
        </head>
        <body>
        {' '.join(dashboard_parts)}
        </body>
        </html>
        """
        
        return full_html
        
    except Exception as e:
        logger.error(f"Failed to create dashboard HTML: {e}")
        return f"<div class='alert alert-danger'>Dashboard creation failed: {str(e)}</div>"

def export_tables_to_csv(json_data: Union[Dict[str, Any], str], 
                        output_dir: str = ".", 
                        prefix: str = "ovnk_") -> Dict[str, str]:
    """
    Export JSON data tables to CSV files
    
    Args:
        json_data: Input JSON data
        output_dir: Output directory for CSV files
        prefix: Filename prefix
        
    Returns:
        Dictionary mapping table_name -> csv_file_path
    """
    try:
        import os
        
        result = convert_json_to_tables(json_data, "tabular", compact=True)
        
        if 'error' in result:
            return {'error': result['error']}
        
        tabular_data = result.get('tabular', {})
        csv_files = {}
        
        for table_name, table_data in tabular_data.items():
            try:
                df = pd.DataFrame(table_data['raw_data'][1:], columns=table_data['raw_data'][0])
                
                # Create filename
                filename = f"{prefix}{table_name}.csv"
                filepath = os.path.join(output_dir, filename)
                
                # Export to CSV
                df.to_csv(filepath, index=False)
                csv_files[table_name] = filepath
                
            except Exception as e:
                logger.error(f"Failed to export table {table_name} to CSV: {e}")
                csv_files[table_name] = f"Error: {str(e)}"
        
        return csv_files
        
    except Exception as e:
        logger.error(f"Error exporting tables to CSV: {e}")
        return {'error': str(e)}

# Export main functions for external use
__all__ = [
    'PerformanceDataELT',
    'extract_and_transform_mcp_results',
    'format_results_as_table',
    'generate_brief_results',
    'convert_json_to_tables',
    'convert_dict_to_simple_table',
    'auto_detect_and_convert_to_tables',
    'json_to_html_table',
    'json_to_tabular_data',
    'validate_json_structure',
    'batch_convert_json_files',
    'convert_cluster_info_to_tables',
    'convert_prometheus_basic_to_tables', 
    'convert_kube_api_to_tables',
    'create_dashboard_html',
    'export_tables_to_csv'
]