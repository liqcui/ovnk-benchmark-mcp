"""
ELT module for OpenShift Cluster Status Analysis data
Extracts, Loads, and Transforms cluster status analysis results into readable tables
"""

import logging
from typing import Dict, Any, List, Union
import pandas as pd
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class ClusterStatELT(EltUtility):
    """Extract, Load, Transform for cluster status analysis data"""
    
    def __init__(self):
        super().__init__()
    
    def extract_cluster_stat(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cluster status analysis data"""
        try:
            structured = {
                'metadata': self._extract_metadata(data.get('metadata', {})),
                'cluster_health': self._extract_cluster_health(data.get('cluster_health', {})),
                'node_groups': self._extract_node_groups(data.get('node_groups', {})),
                'resource_utilization': self._extract_resource_utilization(data.get('resource_utilization', {})),
                'network_analysis': self._extract_network_analysis(data.get('network_policy_analysis', {})),
                'cluster_operators': self._extract_cluster_operators(data.get('cluster_operators_summary', {})),
                'mcp_status': self._extract_mcp_status(data.get('mcp_summary', {})),
                'alerts': self._extract_alerts(data.get('alerts', [])),
                'recommendations': self._extract_recommendations(data.get('recommendations', []))
            }
            return structured
        except Exception as e:
            logger.error(f"Failed to extract cluster status data: {e}")
            return {'error': str(e)}
    
    def _extract_metadata(self, metadata: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract metadata information"""
        return [
            {'Property': 'Analysis Timestamp', 'Value': self.format_timestamp(metadata.get('analysis_timestamp', ''))},
            {'Property': 'Analyzer Type', 'Value': metadata.get('analyzer_type', '').replace('_', ' ').title()},
            {'Property': 'Items Analyzed', 'Value': str(metadata.get('total_items_analyzed', 0))},
            {'Property': 'Duration', 'Value': metadata.get('duration', 'Unknown')}
        ]
    
    def _extract_cluster_health(self, health: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract cluster health summary"""
        return [
            {'Metric': 'Overall Score', 'Value': f"{health.get('overall_score', 0)}/100"},
            {'Metric': 'Health Level', 'Value': health.get('health_level', 'unknown').title()},
            {'Metric': 'Critical Issues', 'Value': str(health.get('critical_issues_count', 0))},
            {'Metric': 'Warning Issues', 'Value': str(health.get('warning_issues_count', 0))},
            {'Metric': 'Healthy Components', 'Value': str(health.get('healthy_items_count', 0))}
        ]
    
    def _extract_node_groups(self, node_groups: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract node groups summary"""
        node_data = []
        for group_type, group_info in node_groups.items():
            node_data.append({
                'Node Type': group_type.title(),
                'Total': str(group_info.get('total_nodes', 0)),
                'Ready': str(group_info.get('ready_nodes', 0)),
                'Not Ready': str(group_info.get('not_ready_nodes', 0)),
                'Health Score': f"{group_info.get('health_score', 0):.1f}%",
                'CPU Cores': str(group_info.get('resource_summary', {}).get('total_cpu_cores', 0))
            })
        return node_data
    
    def _extract_resource_utilization(self, resources: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract resource utilization metrics"""
        return [
            {'Metric': 'Pod Density', 'Value': str(resources.get('pod_density', 0))},
            {'Metric': 'Namespaces', 'Value': str(resources.get('namespace_distribution', 0))},
            {'Metric': 'Service/Pod Ratio', 'Value': f"{resources.get('service_to_pod_ratio', 0):.3f}"},
            {'Metric': 'Secret Density', 'Value': f"{resources.get('secret_density', 0):.2f}"},
            {'Metric': 'ConfigMap Density', 'Value': f"{resources.get('configmap_density', 0):.2f}"}
        ]
    
    def _extract_network_analysis(self, network: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract network policy analysis"""
        breakdown = network.get('resource_breakdown', {})
        return [
            {'Metric': 'Total Resources', 'Value': str(network.get('total_network_resources', 0))},
            {'Metric': 'Policy Density', 'Value': f"{network.get('policy_density', 0):.4f}"},
            {'Metric': 'Complexity Score', 'Value': f"{network.get('network_complexity_score', 0):.1f}"},
            {'Metric': 'Network Policies', 'Value': str(breakdown.get('networkpolicies', 0))},
            {'Metric': 'Admin Policies', 'Value': str(breakdown.get('adminnetworkpolicies', 0))},
            {'Metric': 'Egress Firewalls', 'Value': str(breakdown.get('egressfirewalls', 0))}
        ]
    
    def _extract_cluster_operators(self, operators: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract cluster operators status"""
        return [
            {'Metric': 'Total Operators', 'Value': str(operators.get('total_operators_estimated', 0))},
            {'Metric': 'Available', 'Value': str(operators.get('available_operators', 0))},
            {'Metric': 'Unavailable', 'Value': str(operators.get('unavailable_operators', 0))},
            {'Metric': 'Availability %', 'Value': f"{operators.get('availability_percentage', 0):.1f}%"},
            {'Metric': 'Health Status', 'Value': operators.get('health_status', 'unknown').title()}
        ]
    
    def _extract_mcp_status(self, mcp: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract machine config pool status"""
        status_dist = mcp.get('status_distribution', {})
        return [
            {'Metric': 'Total Pools', 'Value': str(mcp.get('total_pools', 0))},
            {'Metric': 'Health Score', 'Value': f"{mcp.get('health_score', 0):.1f}%"},
            {'Metric': 'Health Status', 'Value': mcp.get('health_status', 'unknown').title()},
            {'Metric': 'Updated Pools', 'Value': str(status_dist.get('Updated', 0))},
            {'Metric': 'Degraded Pools', 'Value': str(status_dist.get('Degraded', 0))}
        ]
    
    def _extract_alerts(self, alerts: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """Extract alerts information"""
        if not alerts:
            return [{'Status': 'No Alerts', 'Message': 'System is healthy'}]
        
        alert_data = []
        for i, alert in enumerate(alerts[:10], 1):  # Limit to top 10 alerts
            alert_data.append({
                'Alert #': str(i),
                'Severity': alert.get('severity', 'unknown').upper(),
                'Component': alert.get('component_name', 'unknown'),
                'Message': self.truncate_text(alert.get('message', ''), 50)
            })
        return alert_data
    
    def _extract_recommendations(self, recommendations: List[str]) -> List[Dict[str, str]]:
        """Extract recommendations"""
        if not recommendations:
            return [{'Recommendation': 'No specific recommendations available'}]
        
        rec_data = []
        for i, rec in enumerate(recommendations[:8], 1):  # Limit to top 8 recommendations
            rec_data.append({
                'Priority': str(i),
                'Recommendation': self.truncate_text(rec, 80)
            })
        return rec_data
    
    def summarize_cluster_stat(self, structured_data: Dict[str, Any]) -> str:
        """Generate brief summary of cluster status analysis"""
        try:
            health_data = structured_data.get('cluster_health', [])
            node_groups = structured_data.get('node_groups', [])
            alerts = structured_data.get('alerts', [])
            
            # Extract key metrics
            overall_score = 0
            health_level = 'unknown'
            total_nodes = 0
            ready_nodes = 0
            
            for item in health_data:
                if item.get('Metric') == 'Overall Score':
                    overall_score = item.get('Value', '0/100').split('/')[0]
                elif item.get('Metric') == 'Health Level':
                    health_level = item.get('Value', 'unknown')
            
            for node in node_groups:
                total_nodes += int(node.get('Total', 0))
                ready_nodes += int(node.get('Ready', 0))
            
            alert_count = len(alerts) if alerts[0].get('Status') != 'No Alerts' else 0
            
            summary_parts = [
                f"Cluster Health: {health_level} ({overall_score}/100)",
                f"Nodes: {ready_nodes}/{total_nodes} ready",
                f"Alerts: {alert_count} active" if alert_count > 0 else "No active alerts"
            ]
            
            return " • ".join(summary_parts)
        
        except Exception as e:
            logger.error(f"Failed to generate cluster status summary: {e}")
            return "Cluster status analysis summary unavailable"
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        dataframes = {}
        
        try:
            # Transform each data section to DataFrame
            for key, data in structured_data.items():
                if isinstance(data, list) and data:
                    df = pd.DataFrame(data)
                    if not df.empty:
                        # Apply column limiting based on table type
                        if key in ['node_groups']:
                            df = self.limit_dataframe_columns(df, max_cols=6, table_name=key)
                        elif key in ['network_analysis']:
                            df = self.limit_dataframe_columns(df, max_cols=6, table_name=key)
                        elif key in ['alerts'] and len(df.columns) > 4:
                            df = self.limit_dataframe_columns(df, max_cols=4, table_name=key)
                        else:
                            df = self.limit_dataframe_columns(df, max_cols=2, table_name=key)
                        dataframes[key] = df
        
        except Exception as e:
            logger.error(f"Failed to transform cluster status data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            # Define table priorities and titles
            table_info = {
                'cluster_health': 'Cluster Health Overview',
                'node_groups': 'Node Groups Status',
                'resource_utilization': 'Resource Utilization',
                'cluster_operators': 'Cluster Operators',
                'mcp_status': 'Machine Config Pools',
                'network_analysis': 'Network Policy Analysis',
                'alerts': 'Active Alerts',
                'recommendations': 'Recommendations',
                'metadata': 'Analysis Metadata'
            }
            
            # Generate tables in priority order
            for table_name in table_info.keys():
                if table_name in dataframes:
                    df = dataframes[table_name]
                    if not df.empty:
                        html_tables[table_name] = self.create_html_table(df, table_name)
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for cluster status: {e}")
        
        return html_tables