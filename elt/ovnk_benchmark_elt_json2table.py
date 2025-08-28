"""
Extract, Load, Transform module for OpenShift Benchmark Performance Data
Converts JSON outputs to table format and generates brief results
"""

import logging
from typing import Dict, Any, List, Optional, Union
import json
import pandas as pd
from datetime import datetime
import re

logger = logging.getLogger(__name__)

class PerformanceDataELT:
    """Extract, Load, Transform class for performance data"""
    
    def __init__(self):
        self.processed_data = {}
        
    def extract_json_data(self, mcp_results: Dict[str, Any]) -> Dict[str, Any]:
        """Extract relevant data from MCP tool results"""
        try:
            extracted = {
                'timestamp': mcp_results.get('timestamp', datetime.now().isoformat()),
                'data_type': self._identify_data_type(mcp_results),
                'raw_data': mcp_results,
                'structured_data': {}
            }
            
            # Extract structured data based on type
            if extracted['data_type'] == 'cluster_info':
                extracted['structured_data'] = self._extract_cluster_info(mcp_results)
            elif extracted['data_type'] == 'node_info':
                extracted['structured_data'] = self._extract_node_info(mcp_results)
            elif extracted['data_type'] == 'api_metrics':
                extracted['structured_data'] = self._extract_api_metrics(mcp_results)
            elif extracted['data_type'] == 'prometheus_query':
                extracted['structured_data'] = self._extract_prometheus_data(mcp_results)
            else:
                extracted['structured_data'] = self._extract_generic_data(mcp_results)
            
            return extracted
            
        except Exception as e:
            logger.error(f"Failed to extract JSON data: {e}")
            return {'error': str(e), 'raw_data': mcp_results}
    
    def _identify_data_type(self, data: Dict[str, Any]) -> str:
        """Identify the type of data from MCP results"""
        if 'version' in data and 'identity' in data:
            return 'cluster_info'
        elif 'nodes_by_role' in data or 'total_nodes' in data:
            return 'node_info'
        elif 'api_server_latency' in data or 'latency_metrics' in data:
            return 'api_metrics'
        elif 'result' in data and 'query' in data:
            return 'prometheus_query'
        else:
            return 'generic'
    
    def _extract_cluster_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cluster information into structured format"""
        structured = {
            'cluster_summary': [],
            'operators': []
        }
        
        # Basic cluster info
        if 'version' in data:
            version = data['version']
            structured['cluster_summary'].append({
                'Property': 'OpenShift Version',
                'Value': version.get('openshift_version', version.get('kubernetes_version', 'Unknown'))
            })
            structured['cluster_summary'].append({
                'Property': 'Platform',
                'Value': version.get('platform', 'Unknown')
            })
        
        if 'identity' in data:
            identity = data['identity']
            structured['cluster_summary'].append({
                'Property': 'Cluster Name',
                'Value': identity.get('cluster_name', 'Unknown')
            })
            structured['cluster_summary'].append({
                'Property': 'Infrastructure Name',
                'Value': identity.get('infrastructure_name', 'Unknown')
            })
        
        # Operators info
        if 'operators' in data and 'operators' in data['operators']:
            for op in data['operators']['operators']:
                structured['operators'].append({
                    'Name': op.get('name', ''),
                    'Version': op.get('version', ''),
                    'Available': 'Yes' if op.get('available') else 'No',
                    'Degraded': 'Yes' if op.get('degraded') else 'No',
                    'Progressing': 'Yes' if op.get('progressing') else 'No'
                })
        
        return structured
    
    def _extract_node_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract node information into structured format"""
        structured = {
            'node_summary': [],
            'nodes_by_role': {},
            'resource_summary': []
        }
        
        # Node summary
        if 'summary' in data:
            summary = data['summary']
            structured['node_summary'] = [
                {'Role': 'Master', 'Count': summary.get('master', 0)},
                {'Role': 'Worker', 'Count': summary.get('worker', 0)},
                {'Role': 'Infra', 'Count': summary.get('infra', 0)}
            ]
        
        # Resource summary
        if 'resource_summary' in data:
            for role, resources in data['resource_summary'].items():
                structured['resource_summary'].append({
                    'Role': role.capitalize(),
                    'Nodes': resources.get('nodes', 0),
                    'CPU Cores': f"{resources.get('cpu_cores', 0):.1f}",
                    'Memory (GB)': f"{resources.get('memory_gb', 0):.1f}"
                })
        
        # Detailed node information
        if 'nodes_by_role' in data:
            for role, nodes in data['nodes_by_role'].items():
                structured['nodes_by_role'][role] = []
                for node in nodes:
                    structured['nodes_by_role'][role].append({
                        'Name': node.get('name', ''),
                        'Instance Type': node.get('instance_type', 'unknown'),
                        'CPU Cores': f"{node.get('resources', {}).get('cpu_cores', {}).get('capacity', 0):.1f}",
                        'Memory (GB)': f"{node.get('resources', {}).get('memory_mb', {}).get('capacity', 0) / 1024:.1f}",
                        'Status': node.get('status', {}).get('overall_status', 'Unknown')
                    })
        
        return structured
    
    def _extract_api_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract API server metrics into structured format"""
        structured = {
            'latency_summary': [],
            'request_rates': [],
            'resource_usage': []
        }
        
        # Latency metrics
        if 'latency_metrics' in data and 'api_server_latency' in data['latency_metrics']:
            latency = data['latency_metrics']['api_server_latency']
            
            if 'read_operations' in latency:
                ro = latency['read_operations']
                structured['latency_summary'].append({
                    'Operation Type': 'Read Operations',
                    'Avg 99th Percentile (s)': f"{ro.get('avg_99th_percentile', 0):.3f}",
                    'Max 99th Percentile (s)': f"{ro.get('max_99th_percentile', 0):.3f}"
                })
            
            if 'mutating_operations' in latency:
                mut = latency['mutating_operations']
                structured['latency_summary'].append({
                    'Operation Type': 'Mutating Operations',
                    'Avg 99th Percentile (s)': f"{mut.get('avg_99th_percentile', 0):.3f}",
                    'Max 99th Percentile (s)': f"{mut.get('max_99th_percentile', 0):.3f}"
                })
        
        # Request rates
        if 'rate_metrics' in data and 'analysis' in data['rate_metrics']:
            analysis = data['rate_metrics']['analysis']
            structured['request_rates'].append({
                'Metric': 'Total Requests/sec',
                'Value': f"{analysis.get('total_requests_per_second', 0):.1f}"
            })
            structured['request_rates'].append({
                'Metric': 'Error Rate (%)',
                'Value': f"{analysis.get('error_percentage', 0):.1f}"
            })
        
        # Resource usage
        if 'resource_metrics' in data and 'analysis' in data['resource_metrics']:
            analysis = data['resource_metrics']['analysis']
            structured['resource_usage'].append({
                'Resource': 'CPU Cores',
                'Usage': f"{analysis.get('cpu_usage_cores', 0):.2f}"
            })
            structured['resource_usage'].append({
                'Resource': 'Memory (MB)',
                'Usage': f"{analysis.get('memory_usage_mb', 0):.1f}"
            })
        
        return structured
    
    def _extract_prometheus_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Prometheus query results into structured format"""
        structured = {
            'query_results': []
        }
        
        if 'result' in data:
            results = data['result']
            if isinstance(results, list):
                for i, result in enumerate(results):
                    metric = result.get('metric', {})
                    value = result.get('value', [])
                    
                    if len(value) >= 2:
                        structured['query_results'].append({
                            'Index': i,
                            'Metric Labels': ', '.join([f"{k}={v}" for k, v in metric.items()]),
                            'Timestamp': value[0],
                            'Value': value[1]
                        })
        
        return structured
    
    def _extract_generic_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract generic data into a flat structure"""
        structured = {'data': []}
        
        def flatten_dict(d, parent_key='', sep='_'):
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                elif isinstance(v, list) and v and isinstance(v[0], dict):
                    # Handle list of dictionaries
                    for i, item in enumerate(v):
                        if isinstance(item, dict):
                            items.extend(flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
                        else:
                            items.append((f"{new_key}_{i}", str(item)))
                else:
                    items.append((new_key, str(v)))
            return dict(items)
        
        flattened = flatten_dict(data)
        for key, value in flattened.items():
            structured['data'].append({
                'Property': key,
                'Value': value
            })
        
        return structured
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        dataframes = {}
        
        try:
            for key, value in structured_data.items():
                if isinstance(value, list) and value:
                    # Convert list of dictionaries to DataFrame
                    dataframes[key] = pd.DataFrame(value)
                elif isinstance(value, dict):
                    # Handle nested dictionaries
                    for nested_key, nested_value in value.items():
                        if isinstance(nested_value, list) and nested_value:
                            dataframes[f"{key}_{nested_key}"] = pd.DataFrame(nested_value)
        
        except Exception as e:
            logger.error(f"Failed to transform to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            for name, df in dataframes.items():
                if not df.empty:
                    # Style the table
                    html = df.to_html(
                        index=False,
                        classes='table table-striped table-bordered',
                        escape=False,
                        table_id=f"table-{name.replace('_', '-')}"
                    )
                    html_tables[name] = html
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables: {e}")
        
        return html_tables
    
    def generate_brief_summary(self, structured_data: Dict[str, Any], data_type: str) -> str:
        """Generate a brief textual summary of the data"""
        try:
            if data_type == 'cluster_info':
                return self._summarize_cluster_info(structured_data)
            elif data_type == 'node_info':
                return self._summarize_node_info(structured_data)
            elif data_type == 'api_metrics':
                return self._summarize_api_metrics(structured_data)
            else:
                return self._summarize_generic(structured_data)
        
        except Exception as e:
            logger.error(f"Failed to generate summary: {e}")
            return f"Summary generation failed: {str(e)}"
    
    def _summarize_cluster_info(self, data: Dict[str, Any]) -> str:
        """Generate cluster info summary"""
        summary = ["Cluster Information Summary:"]
        
        if 'cluster_summary' in data:
            for item in data['cluster_summary']:
                summary.append(f"• {item['Property']}: {item['Value']}")
        
        if 'operators' in data:
            total_ops = len(data['operators'])
            available_ops = sum(1 for op in data['operators'] if op.get('Available') == 'Yes')
            degraded_ops = sum(1 for op in data['operators'] if op.get('Degraded') == 'Yes')
            
            summary.append(f"\nCluster Operators:")
            summary.append(f"• Total Operators: {total_ops}")
            summary.append(f"• Available: {available_ops}")
            summary.append(f"• Degraded: {degraded_ops}")
        
        return "\n".join(summary)
    
    def _summarize_node_info(self, data: Dict[str, Any]) -> str:
        """Generate node info summary"""
        summary = ["Node Information Summary:"]
        
        if 'node_summary' in data:
            total_nodes = sum(item['Count'] for item in data['node_summary'])
            summary.append(f"• Total Nodes: {total_nodes}")
            
            for item in data['node_summary']:
                if item['Count'] > 0:
                    summary.append(f"• {item['Role']}: {item['Count']} nodes")
        
        if 'resource_summary' in data:
            total_cpu = sum(float(item['CPU Cores']) for item in data['resource_summary'])
            total_memory = sum(float(item['Memory (GB)']) for item in data['resource_summary'])
            
            summary.append(f"\nCluster Resources:")
            summary.append(f"• Total CPU Cores: {total_cpu:.1f}")
            summary.append(f"• Total Memory: {total_memory:.1f} GB")
        
        return "\n".join(summary)
    
    def _summarize_api_metrics(self, data: Dict[str, Any]) -> str:
        """Generate API metrics summary"""
        summary = ["API Server Performance Summary:"]
        
        if 'latency_summary' in data:
            summary.append("\nLatency Metrics (99th percentile):")
            for item in data['latency_summary']:
                summary.append(f"• {item['Operation Type']}: {item['Avg 99th Percentile (s)']}s avg")
        
        if 'request_rates' in data:
            summary.append("\nRequest Rates:")
            for item in data['request_rates']:
                summary.append(f"• {item['Metric']}: {item['Value']}")
        
        if 'resource_usage' in data:
            summary.append("\nResource Usage:")
            for item in data['resource_usage']:
                summary.append(f"• {item['Resource']}: {item['Usage']}")
        
        return "\n".join(summary)
    
    def _summarize_generic(self, data: Dict[str, Any]) -> str:
        """Generate generic summary"""
        summary = ["Data Summary:"]
        
        if 'data' in data:
            summary.append(f"• Total properties: {len(data['data'])}")
            # Show first few properties
            for i, item in enumerate(data['data'][:5]):
                summary.append(f"• {item['Property']}: {item['Value'][:50]}...")
            
            if len(data['data']) > 5:
                summary.append(f"• ... and {len(data['data']) - 5} more properties")
        
        return "\n".join(summary)

# Module functions for easy use
def extract_and_transform_mcp_results(mcp_results: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and transform MCP results into tables and summaries"""
    try:
        elt = PerformanceDataELT()
        
        # Extract data
        extracted = elt.extract_json_data(mcp_results)
        
        if 'error' in extracted:
            return extracted
        
        # Transform to DataFrames
        dataframes = elt.transform_to_dataframes(extracted['structured_data'])
        
        # Generate HTML tables
        html_tables = elt.generate_html_tables(dataframes)
        
        # Generate summary
        summary = elt.generate_brief_summary(
            extracted['structured_data'], 
            extracted['data_type']
        )
        
        return {
            'data_type': extracted['data_type'],
            'summary': summary,
            'html_tables': html_tables,
            'dataframes': dataframes,
            'structured_data': extracted['structured_data'],
            'timestamp': extracted['timestamp']
        }
        
    except Exception as e:
        logger.error(f"Failed to extract and transform MCP results: {e}")
        return {'error': str(e)}

def format_results_as_table(results: Dict[str, Any]) -> str:
    """Format results as HTML table string"""
    try:
        transformed = extract_and_transform_mcp_results(results)
        
        if 'error' in transformed:
            return f"<p>Error formatting table: {transformed['error']}</p>"
        
        if not transformed.get('html_tables'):
            return f"<p>No tabular data available</p>"
        
        # Combine all tables
        html_output = []
        for table_name, table_html in transformed['html_tables'].items():
            table_title = table_name.replace('_', ' ').title()
            html_output.append(f"<h4>{table_title}</h4>")
            html_output.append(table_html)
        
        return '\n'.join(html_output)
        
    except Exception as e:
        logger.error(f"Failed to format results as table: {e}")
        return f"<p>Error: {str(e)}</p>"

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