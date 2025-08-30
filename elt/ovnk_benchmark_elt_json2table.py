"""
Extract, Load, Transform module for OpenShift Benchmark Performance Data
Converts JSON outputs to table format and generates brief results
Updated with improved table conversion functionality
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
            elif extracted['data_type'] == 'cluster_status':
                extracted['structured_data'] = self._extract_cluster_status(mcp_results)
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
        elif 'analysis_type' in data and data.get('analysis_type') == 'cluster_status_analysis':
            return 'cluster_status'
        elif 'node_status' in data and 'operator_status' in data:
            return 'cluster_status'
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
    
    def _extract_cluster_status(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cluster status analysis into structured format"""
        structured = {
            'executive_summary': [],
            'component_health': [],
            'critical_issues': [],
            'recommendations': [],
            'node_details': [],
            'operator_details': [],
            'mcp_details': []
        }
        
        try:
            # Executive Summary
            summary = data.get('summary', {})
            if summary:
                structured['executive_summary'] = [
                    {'Property': 'Cluster Status', 'Value': summary.get('cluster_status', 'Unknown').upper()},
                    {'Property': 'Health Score', 'Value': f"{summary.get('health_score', 0)}/100"},
                    {'Property': 'Critical Issues', 'Value': summary.get('critical_issues_count', 0)},
                    {'Property': 'Warnings', 'Value': summary.get('warnings_count', 0)},
                    {'Property': 'Immediate Action Required', 'Value': 'Yes' if summary.get('immediate_action_required', False) else 'No'}
                ]
            
            # Component Health
            overall_health = data.get('overall_health', {})
            component_scores = overall_health.get('component_scores', {})
            
            for component, score_info in component_scores.items():
                structured['component_health'].append({
                    'Component': component.replace('_', ' ').title(),
                    'Health Score': f"{score_info.get('score', 0):.1f}/100",
                    'Status': score_info.get('status', 'unknown').upper()
                })
            
            # Critical Issues
            critical_issues = data.get('critical_issues', [])
            for issue in critical_issues:
                structured['critical_issues'].append({
                    'Component': issue.get('component', 'Unknown').upper(),
                    'Severity': issue.get('severity', 'Unknown').upper(),
                    'Message': issue.get('message', 'No message')
                })
            
            # Recommendations (limit to top 10)
            recommendations = data.get('recommendations', [])
            for i, rec in enumerate(recommendations[:10], 1):
                structured['recommendations'].append({
                    'Priority': i,
                    'Recommendation': rec
                })
            
            # Detailed component analysis
            component_analysis = data.get('component_analysis', {})
            
            # Node details
            if 'nodes' in component_analysis:
                node_analysis = component_analysis['nodes']
                if 'error' not in node_analysis:
                    distribution = node_analysis.get('node_distribution', {})
                    for role, count in distribution.items():
                        structured['node_details'].append({
                            'Role': role.capitalize(),
                            'Count': count,
                            'Health Status': 'Healthy' if count > 0 else 'No Nodes'
                        })
            
            # Operator details
            if 'operators' in component_analysis:
                op_analysis = component_analysis['operators']
                if 'error' not in op_analysis:
                    structured['operator_details'].append({
                        'Metric': 'Total Operators',
                        'Value': op_analysis.get('total_operators', 0)
                    })
                    structured['operator_details'].append({
                        'Metric': 'Available',
                        'Value': op_analysis.get('available_operators', 0)
                    })
                    structured['operator_details'].append({
                        'Metric': 'Degraded',
                        'Value': op_analysis.get('degraded_operators', 0)
                    })
                    structured['operator_details'].append({
                        'Metric': 'Progressing',
                        'Value': op_analysis.get('progressing_operators', 0)
                    })
            
            # MCP details
            if 'machine_config_pools' in component_analysis:
                mcp_analysis = component_analysis['machine_config_pools']
                if 'error' not in mcp_analysis:
                    structured['mcp_details'].append({
                        'Metric': 'Total Pools',
                        'Value': mcp_analysis.get('total_pools', 0)
                    })
                    structured['mcp_details'].append({
                        'Metric': 'Updated',
                        'Value': mcp_analysis.get('updated_pools', 0)
                    })
                    structured['mcp_details'].append({
                        'Metric': 'Updating',
                        'Value': mcp_analysis.get('updating_pools', 0)
                    })
                    structured['mcp_details'].append({
                        'Metric': 'Degraded',
                        'Value': mcp_analysis.get('degraded_pools', 0)
                    })
        
        except Exception as e:
            logger.error(f"Error extracting cluster status: {e}")
            structured['error'] = str(e)
        
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
            elif data_type == 'cluster_status':
                return self._summarize_cluster_status(structured_data)
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
    
    def _summarize_cluster_status(self, data: Dict[str, Any]) -> str:
        """Generate cluster status summary"""
        summary = ["Cluster Status Analysis Summary:"]
        
        if 'executive_summary' in data:
            summary.append("\nExecutive Summary:")
            for item in data['executive_summary']:
                summary.append(f"• {item['Property']}: {item['Value']}")
        
        if 'component_health' in data:
            summary.append("\nComponent Health:")
            for item in data['component_health']:
                summary.append(f"• {item['Component']}: {item['Status']} ({item['Health Score']})")
        
        if 'critical_issues' in data:
            critical_count = len(data['critical_issues'])
            if critical_count > 0:
                summary.append(f"\nCritical Issues ({critical_count}):")
                for item in data['critical_issues'][:5]:  # Show first 5
                    summary.append(f"• [{item['Component']}] {item['Message']}")
                if critical_count > 5:
                    summary.append(f"• ... and {critical_count - 5} more critical issues")
        
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


def convert_json_to_tables(json_data: Union[Dict[str, Any], str], 
                          table_format: str = "both") -> Dict[str, Union[str, List[List]]]:
    """
    Convert JSON/dictionary data to table formats (tabular and/or HTML)
    
    Args:
        json_data: Input JSON data as dictionary or JSON string
        table_format: Output format - "tabular", "html", or "both" (default)
        
    Returns:
        Dictionary containing the requested table formats:
        - "tabular": List of lists representing tabular data or formatted string
        - "html": HTML table string
        - "metadata": Information about the conversion
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
        dataframes = elt.transform_to_dataframes(extracted['structured_data'])
        
        result = {
            'metadata': {
                'data_type': extracted['data_type'],
                'timestamp': extracted.get('timestamp'),
                'tables_generated': len(dataframes),
                'table_names': list(dataframes.keys()),
                'conversion_successful': True
            }
        }
        
        # Generate requested formats
        if table_format in ["tabular", "both"]:
            tabular_tables = {}
            
            for table_name, df in dataframes.items():
                if not df.empty:
                    # Convert DataFrame to list of lists (tabular format)
                    tabular_data = [df.columns.tolist()] + df.values.tolist()
                    
                    # Also create a formatted string version using tabulate
                    try:
                        formatted_table = tabulate(
                            df.values.tolist(), 
                            headers=df.columns.tolist(),
                            tablefmt="grid",
                            stralign="left"
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


def convert_dict_to_simple_table(data: Dict[str, Any], 
                                table_format: str = "both") -> Dict[str, Any]:
    """
    Convert a simple dictionary to table format with automatic key-value pair detection
    
    Args:
        data: Dictionary to convert
        table_format: Output format - "tabular", "html", or "both"
        
    Returns:
        Dictionary containing table formats and metadata
    """
    try:
        result = {
            'metadata': {
                'conversion_type': 'simple_dict_to_table',
                'original_keys': list(data.keys()),
                'conversion_successful': True
            }
        }
        
        # Create simple key-value table
        table_data = []
        for key, value in data.items():
            # Convert complex values to strings
            if isinstance(value, (dict, list)):
                value_str = json.dumps(value, default=str)[:100] + "..." if len(str(value)) > 100 else json.dumps(value, default=str)
            else:
                value_str = str(value)
            
            table_data.append([str(key), value_str])
        
        headers = ['Property', 'Value']
        
        if table_format in ["tabular", "both"]:
            # Raw tabular data
            raw_data = [headers] + table_data
            
            # Formatted string using tabulate
            try:
                formatted_string = tabulate(
                    table_data,
                    headers=headers,
                    tablefmt="grid",
                    stralign="left"
                )
            except Exception as e:
                logger.warning(f"Failed to format simple table: {e}")
                formatted_string = "\n".join([f"{key}: {value}" for key, value in table_data])
            
            result['tabular'] = {
                'simple_table': {
                    'raw_data': raw_data,
                    'formatted_string': formatted_string
                }
            }
        
        if table_format in ["html", "both"]:
            # Create DataFrame and convert to HTML
            df = pd.DataFrame(table_data, columns=headers)
            html_table = df.to_html(
                index=False,
                classes='table table-striped table-bordered',
                escape=False,
                table_id="simple-table"
            )
            result['html'] = {'simple_table': html_table}
        
        return result
        
    except Exception as e:
        logger.error(f"Error converting dict to simple table: {e}")
        return {
            'error': str(e),
            'metadata': {'conversion_failed': True}
        }


def auto_detect_and_convert_to_tables(data: Union[Dict[str, Any], str],
                                     table_format: str = "both") -> Dict[str, Any]:
    """
    Auto-detect data structure and convert to appropriate table format
    
    Args:
        data: Input data (dict, JSON string, or complex nested structure)
        table_format: Output format - "tabular", "html", or "both"
        
    Returns:
        Dictionary with converted tables and metadata
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
        
        # Try comprehensive conversion first
        comprehensive_result = convert_json_to_tables(parsed_data, table_format)
        
        if not comprehensive_result.get('metadata', {}).get('conversion_failed', False):
            comprehensive_result['metadata']['detection_method'] = 'comprehensive_extraction'
            return comprehensive_result
        
        # Fall back to simple conversion
        logger.info("Falling back to simple dictionary conversion")
        simple_result = convert_dict_to_simple_table(parsed_data, table_format)
        
        if not simple_result.get('metadata', {}).get('conversion_failed', False):
            simple_result['metadata']['detection_method'] = 'simple_key_value'
            return simple_result
        
        # If both fail, return error
        return {
            'error': 'Failed to convert data using both comprehensive and simple methods',
            'metadata': {
                'conversion_failed': True,
                'comprehensive_error': comprehensive_result.get('error'),
                'simple_error': simple_result.get('error')
            }
        }
        
    except Exception as e:
        logger.error(f"Error in auto-detect conversion: {e}")
        return {
            'error': str(e),
            'metadata': {'conversion_failed': True}
        }


# Module functions for easy use (updated)
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


# New convenience functions
def json_to_html_table(json_data: Union[Dict[str, Any], str]) -> str:
    """Convert JSON data to HTML table format"""
    result = convert_json_to_tables(json_data, "html")
    
    if 'error' in result:
        return f"<p>Error: {result['error']}</p>"
    
    html_tables = result.get('html', {})
    if not html_tables:
        return "<p>No tables generated</p>"
    
    # Combine all HTML tables
    combined_html = []
    for table_name, html_table in html_tables.items():
        table_title = table_name.replace('_', ' ').title()
        combined_html.append(f"<h3>{table_title}</h3>")
        combined_html.append(html_table)
    
    return '\n'.join(combined_html)


def json_to_tabular_data(json_data: Union[Dict[str, Any], str]) -> List[Dict[str, Any]]:
    """Convert JSON data to tabular format (list of tables with raw data)"""
    result = convert_json_to_tables(json_data, "tabular")
    
    if 'error' in result:
        return [{'error': result['error']}]
    
    tabular_tables = result.get('tabular', {})
    table_list = []
    
    for table_name, table_data in tabular_tables.items():
        table_list.append({
            'name': table_name,
            'headers': table_data['raw_data'][0] if table_data['raw_data'] else [],
            'rows': table_data['raw_data'][1:] if len(table_data['raw_data']) > 1 else [],
            'formatted_string': table_data.get('formatted_string', '')
        })
    
    return table_list