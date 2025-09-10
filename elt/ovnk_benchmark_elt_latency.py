"""
ELT module for OVN-Kubernetes Latency Metrics
Handles extraction, transformation and HTML generation for latency data
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Optional, Union
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class latencyELT(EltUtility):
    """ELT module for OVN-Kubernetes latency metrics"""
    
    def __init__(self):
        super().__init__()
        self.max_columns = 6  # Allow more columns for latency data
    
    def extract_ovn_latency_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVN latency data from collected metrics"""
        try:
            structured = {
                'collection_info': self._extract_collection_info(data),
                'ready_duration_metrics': self._extract_ready_duration_metrics(data),
                'sync_duration_metrics': self._extract_sync_duration_metrics(data), 
                'cni_latency_metrics': self._extract_cni_latency_metrics(data),
                'pod_annotation_metrics': self._extract_pod_annotation_metrics(data),
                'pod_creation_metrics': self._extract_pod_creation_metrics(data),
                'service_latency_metrics': self._extract_service_latency_metrics(data),
                'network_config_metrics': self._extract_network_config_metrics(data),
                'latency_summary': self._extract_latency_summary(data)
            }
            
            return structured
            
        except Exception as e:
            logger.error(f"Failed to extract latency data: {e}")
            return {'error': str(e)}
    
    def _extract_collection_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract collection metadata"""
        return {
            'timestamp': data.get('collection_timestamp', 'Unknown'),
            'query_type': data.get('query_type', 'Unknown'),
            'duration': data.get('query_parameters', {}).get('duration', 'N/A'),
            'total_metrics': data.get('summary', {}).get('total_metrics', 0),
            'successful_metrics': data.get('summary', {}).get('successful_metrics', 0),
            'failed_metrics': data.get('summary', {}).get('failed_metrics', 0)
        }
    
    def _extract_ready_duration_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract ready duration metrics"""
        metrics = []
        ready_data = data.get('ready_duration_metrics', {})
        
        for metric_key, metric_info in ready_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)),
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0))
                }
                
                # Add top pod info
                top_entries = stats.get('top_5', [])
                if top_entries:
                    top_pod = top_entries[0]
                    metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                    metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_sync_duration_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract sync duration metrics"""
        metrics = []
        sync_data = data.get('sync_duration_metrics', {})
        
        for metric_key, metric_info in sync_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)),
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0))
                }
                
                # For sync duration, show top 20 entries
                top_entries = stats.get('top_20', stats.get('top_5', []))
                if top_entries:
                    top_pod = top_entries[0]
                    metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                    metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                    metric_entry['top_resource'] = top_pod.get('resource_name', 'N/A')
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_cni_latency_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract CNI latency metrics"""
        metrics = []
        cni_data = data.get('cni_latency_metrics', {})
        
        for metric_key, metric_info in cni_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)),
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0))
                }
                
                top_entries = stats.get('top_5', [])
                if top_entries:
                    top_pod = top_entries[0]
                    metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                    metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_pod_annotation_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract pod annotation metrics"""
        metrics = []
        pod_data = data.get('pod_annotation_metrics', {})
        
        for metric_key, metric_info in pod_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)),
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0))
                }
                
                top_entries = stats.get('top_5', [])
                if top_entries:
                    top_pod = top_entries[0]
                    metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                    metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_pod_creation_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract pod creation metrics"""
        metrics = []
        creation_data = data.get('pod_creation_metrics', {})
        
        for metric_key, metric_info in creation_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)) if stats.get('max_value') else 'N/A',
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)) if stats.get('avg_value') else 'N/A',
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0)) if stats.get('max_value') else 'unknown'
                }
                
                top_entries = stats.get('top_5', [])
                if top_entries:
                    # Find first non-null entry
                    top_pod = None
                    for entry in top_entries:
                        if entry.get('value') is not None:
                            top_pod = entry
                            break
                    
                    if top_pod:
                        metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                        metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                    else:
                        metric_entry['top_pod'] = 'N/A'
                        metric_entry['top_node'] = 'N/A'
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_service_latency_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract service latency metrics"""
        metrics = []
        service_data = data.get('service_latency_metrics', {})
        
        for metric_key, metric_info in service_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)),
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0))
                }
                
                top_entries = stats.get('top_5', [])
                if top_entries:
                    top_pod = top_entries[0]
                    metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                    metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_network_config_metrics(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract network configuration metrics"""
        metrics = []
        network_data = data.get('network_config_metrics', {})
        
        for metric_key, metric_info in network_data.items():
            if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                stats = metric_info['statistics']
                metric_entry = {
                    'metric_name': self.truncate_metric_name(metric_info.get('metric_name', metric_key)),
                    'component': metric_info.get('component', 'Unknown'),
                    'max_value': self.format_latency_value(stats.get('max_value', 0)),
                    'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
                    'data_points': stats.get('count', 0),
                    'severity': self.categorize_latency_severity(stats.get('max_value', 0))
                }
                
                top_entries = stats.get('top_5', [])
                if top_entries:
                    top_pod = top_entries[0]
                    metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                    metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                    # Handle service name for network config metrics
                    if top_pod.get('service_name'):
                        metric_entry['top_service'] = top_pod.get('service_name', 'N/A')
                
                metrics.append(metric_entry)
        
        return metrics
    
    def _extract_latency_summary(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract overall latency summary"""
        summary = data.get('summary', {})
        
        summary_data = {
            'total_metrics': summary.get('total_metrics', 0),
            'successful_metrics': summary.get('successful_metrics', 0),
            'failed_metrics': summary.get('failed_metrics', 0),
            'controller_metrics': summary.get('component_breakdown', {}).get('controller', 0),
            'node_metrics': summary.get('component_breakdown', {}).get('node', 0),
            'overall_max_latency': 'N/A',
            'overall_avg_latency': 'N/A',
            'critical_metric': 'N/A'
        }
        
        # Extract overall latency info
        if 'overall_max_latency' in summary:
            max_info = summary['overall_max_latency']
            readable = max_info.get('readable', {})
            summary_data['overall_max_latency'] = f"{readable.get('value', 0)} {readable.get('unit', '')}"
            summary_data['critical_metric'] = max_info.get('metric', 'N/A')
        
        if 'overall_avg_latency' in summary:
            avg_info = summary['overall_avg_latency']
            readable = avg_info.get('readable', {})
            summary_data['overall_avg_latency'] = f"{readable.get('value', 0)} {readable.get('unit', '')}"
        
        return summary_data
    
    def _extract_top_latencies_detail(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract detailed top latencies for comprehensive view"""
        top_latencies = []
        summary = data.get('summary', {})
        
        for metric in summary.get('top_latencies', [])[:10]:  # Top 10
            latency_entry = {
                'rank': len(top_latencies) + 1,
                'metric_name': self.truncate_metric_name(metric.get('metric_name', 'Unknown')),
                'component': metric.get('component', 'Unknown'),
                'max_latency': f"{metric.get('readable_max', {}).get('value', 0)} {metric.get('readable_max', {}).get('unit', '')}",
                'avg_latency': f"{metric.get('readable_avg', {}).get('value', 0)} {metric.get('readable_avg', {}).get('unit', '')}",
                'data_points': metric.get('data_points', 0),
                'severity': self.categorize_latency_severity(metric.get('max_value', 0))
            }
            top_latencies.append(latency_entry)
        
        return top_latencies
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured latency data to DataFrames with enhanced formatting"""
        dataframes = {}
        
        try:
            # Collection info
            if 'collection_info' in structured_data:
                collection_data = [
                    {'Property': 'Collection Time', 'Value': structured_data['collection_info']['timestamp'][:19]},
                    {'Property': 'Query Type', 'Value': structured_data['collection_info']['query_type'].title()},
                    {'Property': 'Total Metrics', 'Value': str(structured_data['collection_info']['total_metrics'])},
                    {'Property': 'Success Rate', 'Value': f"{structured_data['collection_info']['successful_metrics']}/{structured_data['collection_info']['total_metrics']}"},
                    {'Property': 'Failed Metrics', 'Value': str(structured_data['collection_info']['failed_metrics'])}
                ]
                dataframes['latencyelt_collection_info'] = pd.DataFrame(collection_data)

            # Essential results summary (if from latencyelt_extract_essential_results)
            if 'overall_summary' in structured_data:
                summary_data = []
                overall = structured_data['overall_summary']
                
                if 'max_latency' in overall:
                    max_latency = overall['max_latency']
                    if 'readable' in max_latency:
                        summary_data.append({
                            'Property': 'Maximum Latency', 
                            'Value': f"{max_latency['readable']['value']} {max_latency['readable']['unit']}"
                        })
                
                if 'avg_latency' in overall:
                    avg_latency = overall['avg_latency']
                    if 'readable' in avg_latency:
                        summary_data.append({
                            'Property': 'Average Latency', 
                            'Value': f"{avg_latency['readable']['value']} {avg_latency['readable']['unit']}"
                        })
                
                if 'critical_metric' in overall:
                    summary_data.append({
                        'Property': 'Critical Metric', 
                        'Value': self.truncate_metric_name(overall['critical_metric'], 40)
                    })
                
                if 'component_breakdown' in overall:
                    breakdown = overall['component_breakdown']
                    summary_data.append({
                        'Property': 'Controller Metrics', 
                        'Value': str(breakdown.get('controller', 0))
                    })
                    summary_data.append({
                        'Property': 'Node Metrics', 
                        'Value': str(breakdown.get('node', 0))
                    })
                
                if summary_data:
                    dataframes['latencyelt_essential_summary'] = pd.DataFrame(summary_data)

            # Top latencies ranking (from essential results)
            if 'top_latencies_ranking' in structured_data:
                df_data = []
                for entry in structured_data['top_latencies_ranking']:
                    df_data.append({
                        'Rank': entry['rank'],
                        'Metric Name': self.truncate_metric_name(entry['metric_name'], 35),
                        'Component': entry['component'].title(),
                        'Max Latency': f"{entry['readable_max'].get('value', 0)} {entry['readable_max'].get('unit', '')}",
                        'Severity': entry['severity'],
                        'Data Points': entry['data_points'],
                        'is_top1': entry.get('is_top1', False),
                        'is_critical': entry['severity'] in ['critical', 'high']
                    })
                
                if df_data:
                    dataframes['latencyelt_top_latencies_ranking'] = pd.DataFrame(df_data)

            # Controller sync top 20 (if from latencyelt_get_controller_sync_top20)
            if 'top_20_detailed' in structured_data:
                df_data = []
                for entry in structured_data['top_20_detailed']:
                    df_data.append({
                        'Rank': entry['rank'],
                        'Pod Name': self.truncate_latencyelt_pod_name(entry['pod_name'], 25),
                        'Node Name': self.truncate_node_name(entry['node_name'], 20),
                        'Resource': entry.get('resource_name', 'N/A'),
                        'Latency': entry['formatted_value'],
                        'Value (s)': entry['value'],
                        'is_top1': entry['rank'] == 1,
                        'is_top3': entry['rank'] <= 3
                    })
                
                if df_data:
                    dataframes['latencyelt_controller_sync_top20'] = pd.DataFrame(df_data)

            # All metrics top 5 detailed (if from latencyelt_get_all_metrics_top5_detailed)
            if 'metrics_details' in structured_data:
                all_top5_data = []
                
                for metric_key, metric_info in structured_data['metrics_details'].items():
                    for pod_entry in metric_info['top_5_detailed']:
                        all_top5_data.append({
                            'Metric Name': self.truncate_metric_name(metric_info['metric_name'], 30),
                            'Component': metric_info['component'].title(),
                            'Category': metric_info['category'].replace('_', ' ').title(),
                            'Rank': pod_entry['rank'],
                            'Pod Name': self.truncate_latencyelt_pod_name(pod_entry['pod_name'], 25),
                            'Node Name': self.truncate_node_name(pod_entry['node_name'], 20),
                            'Latency': pod_entry['formatted_value'],
                            'Value': pod_entry['value'],
                            'Unit': pod_entry['unit'],
                            'is_top1_in_metric': pod_entry['rank'] == 1
                        })
                
                if all_top5_data:
                    dataframes['latencyelt_all_metrics_top5'] = pd.DataFrame(all_top5_data)

            # Continue with existing dataframe creation for other categories...
            # (keeping existing code for ready_duration_metrics, sync_duration_metrics, etc.)
            
            # Ready duration metrics
            if structured_data.get('ready_duration_metrics'):
                df = pd.DataFrame(structured_data['ready_duration_metrics'])
                if not df.empty:
                    dataframes['latencyelt_ready_duration'] = df
            
            # Sync duration metrics
            if structured_data.get('sync_duration_metrics'):
                df = pd.DataFrame(structured_data['sync_duration_metrics'])
                if not df.empty:
                    dataframes['latencyelt_sync_duration'] = df
            
            # CNI latency metrics
            if structured_data.get('cni_latency_metrics'):
                df = pd.DataFrame(structured_data['cni_latency_metrics'])
                if not df.empty:
                    dataframes['latencyelt_cni_latency'] = df
            
            # Pod annotation metrics
            if structured_data.get('pod_annotation_metrics'):
                df = pd.DataFrame(structured_data['pod_annotation_metrics'])
                if not df.empty:
                    dataframes['latencyelt_pod_annotation'] = df
            
            # Pod creation metrics
            if structured_data.get('pod_creation_metrics'):
                df = pd.DataFrame(structured_data['pod_creation_metrics'])
                if not df.empty:
                    dataframes['latencyelt_pod_creation'] = df
            
            # Service latency metrics
            if structured_data.get('service_latency_metrics'):
                df = pd.DataFrame(structured_data['service_latency_metrics'])
                if not df.empty:
                    dataframes['latencyelt_service_latency'] = df
            
            # Network config metrics
            if structured_data.get('network_config_metrics'):
                df = pd.DataFrame(structured_data['network_config_metrics'])
                if not df.empty:
                    dataframes['latencyelt_network_config'] = df

            # Critical findings
            if 'critical_findings' in structured_data and structured_data['critical_findings']:
                findings_data = []
                for finding in structured_data['critical_findings']:
                    findings_data.append({
                        'Finding Type': finding['type'].replace('_', ' ').title(),
                        'Description': finding['description'],
                        'Details': finding.get('metric', finding.get('count', 'N/A'))
                    })
                
                if findings_data:
                    dataframes['latencyelt_critical_findings'] = pd.DataFrame(findings_data)
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to create latency DataFrames: {e}")
            return {}
 
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables with enhanced latency-specific styling and highlighting"""
        html_tables = {}
        
        try:
            for table_name, df in dataframes.items():
                if df.empty:
                    continue
                
                # Apply column limiting based on table type
                df_limited = self.limit_dataframe_columns(df, table_name=table_name)
                df_formatted = df_limited.copy()
                
                # Enhanced formatting for different table types
                if table_name == 'latencyelt_top_latencies_ranking':
                    # Special formatting for top latencies ranking
                    if 'Rank' in df_formatted.columns:
                        df_formatted['Rank'] = df_formatted.apply(
                            lambda row: f'<span class="badge badge-danger badge-lg pulse-animation"><i class="fas fa-exclamation-triangle"></i> #{row["Rank"]}</span>' if row.get('is_top1', False)
                            else f'<span class="badge badge-warning font-weight-bold">#{row["Rank"]}</span>' if row.get('is_critical', False)
                            else f'<span class="badge badge-info">#{row["Rank"]}</span>',
                            axis=1
                        )
                    
                    if 'Max Latency' in df_formatted.columns:
                        df_formatted['Max Latency'] = df_formatted.apply(
                            lambda row: f'<span class="text-danger font-weight-bold blink-text">{row["Max Latency"]}</span>' if row.get('is_top1', False)
                            else f'<span class="text-warning font-weight-bold">{row["Max Latency"]}</span>' if row.get('is_critical', False)
                            else row['Max Latency'],
                            axis=1
                        )
                    
                    if 'Metric Name' in df_formatted.columns:
                        df_formatted['Metric Name'] = df_formatted.apply(
                            lambda row: f'<span class="text-danger font-weight-bold">{row["Metric Name"]}</span> <span class="badge badge-danger">CRITICAL</span>' if row.get('is_top1', False)
                            else f'<span class="text-warning font-weight-bold">{row["Metric Name"]}</span>' if row.get('is_critical', False)
                            else row['Metric Name'],
                            axis=1
                        )
                    
                    # Remove helper columns
                    df_formatted = df_formatted.drop(columns=['is_top1', 'is_critical'], errors='ignore')
                
                elif table_name == 'latencyelt_controller_sync_top20':
                    # Special formatting for controller sync top 20
                    if 'Rank' in df_formatted.columns:
                        df_formatted['Rank'] = df_formatted.apply(
                            lambda row: f'<span class="badge badge-danger badge-lg"><i class="fas fa-exclamation-triangle"></i> #{row["Rank"]}</span>' if row.get('is_top1', False)
                            else f'<span class="badge badge-warning font-weight-bold">#{row["Rank"]}</span>' if row.get('is_top3', False)
                            else f'<span class="badge badge-info">#{row["Rank"]}</span>',
                            axis=1
                        )
                    
                    if 'Latency' in df_formatted.columns:
                        df_formatted['Latency'] = df_formatted.apply(
                            lambda row: f'<span class="text-danger font-weight-bold">{row["Latency"]}</span>' if row.get('is_top1', False)
                            else f'<span class="text-warning">{row["Latency"]}</span>' if row.get('is_top3', False)
                            else row['Latency'],
                            axis=1
                        )
                    
                    # Remove helper columns
                    df_formatted = df_formatted.drop(columns=['is_top1', 'is_top3'], errors='ignore')
                
                elif table_name == 'latencyelt_all_metrics_top5':
                    # Special formatting for all metrics top 5
                    if 'Rank' in df_formatted.columns:
                        df_formatted['Rank'] = df_formatted.apply(
                            lambda row: f'<span class="badge badge-danger">#{row["Rank"]}</span>' if row.get('is_top1_in_metric', False)
                            else f'<span class="badge badge-secondary">#{row["Rank"]}</span>',
                            axis=1
                        )
                    
                    if 'Latency' in df_formatted.columns:
                        df_formatted['Latency'] = df_formatted.apply(
                            lambda row: f'<span class="text-danger font-weight-bold">{row["Latency"]}</span>' if row.get('is_top1_in_metric', False)
                            else row['Latency'],
                            axis=1
                        )
                    
                    # Remove helper columns
                    df_formatted = df_formatted.drop(columns=['is_top1_in_metric'], errors='ignore')
                
                elif 'severity' in df_formatted.columns:
                    # Apply severity-based formatting for general latency tables
                    if 'max_value' in df_formatted.columns:
                        df_formatted['max_value'] = df_formatted.apply(
                            lambda row: self._create_latency_badge_html(row['max_value'], row.get('severity', 'unknown')), 
                            axis=1
                        )
                    
                    if 'component' in df_formatted.columns:
                        df_formatted['component'] = df_formatted['component'].apply(
                            lambda x: f'<span class="badge badge-primary">{x}</span>' if x.lower() == 'controller'
                            else f'<span class="badge badge-info">{x}</span>' if x.lower() == 'node'
                            else f'<span class="badge badge-secondary">{x}</span>'
                        )
                
                elif table_name == 'latencyelt_critical_findings':
                    # Special formatting for critical findings
                    if 'Finding Type' in df_formatted.columns:
                        df_formatted['Finding Type'] = df_formatted['Finding Type'].apply(
                            lambda x: f'<span class="badge badge-danger">{x}</span>'
                        )
                
                # Create HTML table with appropriate styling
                css_class = 'table table-striped table-bordered table-sm'
                if 'top_latencies_ranking' in table_name or 'critical' in table_name:
                    css_class += ' table-hover alert-table'
                elif 'summary' in table_name or 'collection_info' in table_name:
                    css_class += ' table-info'
                
                html = df_formatted.to_html(
                    index=False,
                    classes=css_class,
                    escape=False,
                    table_id=f"table-{table_name.replace('_', '-')}",
                    border=1
                )
                
                # Clean up HTML
                html = self.clean_html(html)
                
                # Add responsive wrapper with enhanced styling for critical tables
                if 'top_latencies_ranking' in table_name or 'critical' in table_name:
                    html = f'<div class="table-responsive critical-table-wrapper">{html}</div>'
                else:
                    html = f'<div class="table-responsive">{html}</div>'
                
                html_tables[table_name] = html
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate latency HTML tables: {e}")
            return {}

    def _create_latency_badge_html(self, value: str, severity: str) -> str:
        """Create HTML badge for latency value with severity color"""
        badge_colors = {
            'critical': 'danger',
            'high': 'warning', 
            'medium': 'info',
            'low': 'success',
            'unknown': 'secondary'
        }
        
        color = badge_colors.get(severity, 'secondary')
        return f'<span class="badge badge-{color}">{value}</span>'
    
    def _create_enhanced_html_table(self, df: pd.DataFrame, table_name: str) -> str:
        """Create enhanced HTML table with custom styling for latency data"""
        try:
            # Add custom CSS class based on table type
            css_class = 'table table-striped table-bordered table-sm'
            if 'top_latencies' in table_name:
                css_class += ' table-hover'
            elif 'summary' in table_name:
                css_class += ' table-info'
            
            html = df.to_html(
                index=False,
                classes=css_class,
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
            logger.error(f"Failed to create enhanced HTML table for {table_name}: {e}")
            return f'<div class="alert alert-danger">Error generating table: {str(e)}</div>'
    
    def summarize_ovn_latency_data(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary text for OVN latency data"""
        try:
            summary_parts = []
            
            # Collection info
            if 'collection_info' in structured_data:
                info = structured_data['collection_info']
                summary_parts.append(f"Collected {info['successful_metrics']}/{info['total_metrics']} latency metrics")
                if info['duration'] != 'N/A':
                    summary_parts.append(f"over {info['duration']}")
            
            # Overall latency summary
            if 'latency_summary' in structured_data:
                summary = structured_data['latency_summary']
                if summary['overall_max_latency'] != 'N/A':
                    summary_parts.append(f"• Maximum latency: {summary['overall_max_latency']}")
                if summary['critical_metric'] != 'N/A':
                    metric_name = self.truncate_metric_name(summary['critical_metric'], 25)
                    summary_parts.append(f"• Critical metric: {metric_name}")
                
                summary_parts.append(f"• Controller metrics: {summary['controller_metrics']}")
                summary_parts.append(f"• Node metrics: {summary['node_metrics']}")
            
            # Component breakdown
            component_counts = {}
            for category in ['ready_duration_metrics', 'sync_duration_metrics', 'cni_latency_metrics',
                           'pod_annotation_metrics', 'pod_creation_metrics', 'service_latency_metrics', 
                           'network_config_metrics']:
                if category in structured_data and structured_data[category]:
                    count = len(structured_data[category])
                    category_name = category.replace('_metrics', '').replace('_', ' ').title()
                    component_counts[category_name] = count
            
            if component_counts:
                summary_parts.append("• Metrics by category: " + 
                                   ", ".join([f"{k}: {v}" for k, v in component_counts.items()]))
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate latency summary: {e}")
            return f"Latency summary generation failed: {str(e)}"

    async def latencyelt_get_controller_sync_top20(prometheus_client, time: str = None, duration: str = None, end_time: str = None):
        """Get top 20 controller sync duration metrics with detailed pod/node/resource info"""
        from tools.ovnk_benchmark_prometheus_ovnk_latency import OVNLatencyCollector
        
        collector = OVNLatencyCollector(prometheus_client)
        result = await collector.collect_controller_sync_duration(time, duration, end_time)
        
        # Extract top 20 detailed entries for ovnkube_controller_sync_duration_seconds only
        if ('metric_name' in result and 
            result['metric_name'] == 'ovnkube_controller_sync_duration_seconds' and
            'statistics' in result and 'top_20' in result['statistics']):
            
            top_20_entries = []
            for i, entry in enumerate(result['statistics']['top_20'][:20]):  # Ensure max 20
                detailed_entry = {
                    'rank': i + 1,
                    'pod_name': entry.get('pod_name', 'N/A'),
                    'node_name': entry.get('node_name', 'N/A'),
                    'resource_name': entry.get('resource_name', 'N/A'),
                    'value': entry.get('value', 0),
                    'unit': 'seconds',
                    'readable_value': entry.get('readable_value', {}),
                    'formatted_value': f"{entry.get('readable_value', {}).get('value', 0)} {entry.get('readable_value', {}).get('unit', 'ms')}"
                }
                top_20_entries.append(detailed_entry)
            
            return {
                'metric_name': 'ovnkube_controller_sync_duration_seconds',
                'component': 'controller',
                'total_entries': len(top_20_entries),
                'max_value': result['statistics'].get('max_value', 0),
                'avg_value': result['statistics'].get('avg_value', 0),
                'readable_max': result['statistics'].get('readable_max', {}),
                'readable_avg': result['statistics'].get('readable_avg', {}),
                'top_20_detailed': top_20_entries,
                'collection_timestamp': datetime.now().isoformat(),
                'query_type': 'controller_sync_top20'
            }
        
        return {'error': 'No controller sync duration data available for ovnkube_controller_sync_duration_seconds'}

    async def latencyelt_get_all_metrics_top5_detailed(prometheus_client, time: str = None, duration: str = None, end_time: str = None):
        """Get detailed top 5 entries for all latency metrics with pod/node/value/unit info"""
        comprehensive_data = await latencyelt_get_comprehensive_metrics(prometheus_client, time, duration, end_time)
        
        if 'error' in comprehensive_data:
            return comprehensive_data
        
        metrics_top5_detailed = {}
        
        # Process all metric categories
        categories = [
            'ready_duration_metrics', 'sync_duration_metrics', 'cni_latency_metrics',
            'pod_annotation_metrics', 'pod_creation_metrics', 'service_latency_metrics', 
            'network_config_metrics'
        ]
        
        for category in categories:
            if category in comprehensive_data:
                category_data = comprehensive_data[category]
                
                for metric_key, metric_info in category_data.items():
                    if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                        stats = metric_info['statistics']
                        
                        # Extract detailed top 5 pod info
                        top_5_details = []
                        top_entries = stats.get('top_5', stats.get('top_20', [])[:5])
                        
                        for i, entry in enumerate(top_entries[:5]):
                            if entry.get('value') is not None:
                                detail = {
                                    'rank': i + 1,
                                    'pod_name': entry.get('pod_name', 'N/A'),
                                    'node_name': entry.get('node_name', 'N/A'),
                                    'value': entry.get('value', 0),
                                    'unit': metric_info.get('unit', 'seconds'),
                                    'readable_value': entry.get('readable_value', {}),
                                    'formatted_value': f"{entry.get('readable_value', {}).get('value', 0)} {entry.get('readable_value', {}).get('unit', 'ms')}"
                                }
                                
                                # Add additional fields if present
                                if 'resource_name' in entry:
                                    detail['resource_name'] = entry.get('resource_name', 'N/A')
                                if 'service_name' in entry:
                                    detail['service_name'] = entry.get('service_name', 'N/A')
                                
                                top_5_details.append(detail)
                        
                        metrics_top5_detailed[metric_key] = {
                            'metric_name': metric_info.get('metric_name', metric_key),
                            'component': metric_info.get('component', 'Unknown'),
                            'unit': metric_info.get('unit', 'seconds'),
                            'category': category.replace('_metrics', ''),
                            'max_value': stats.get('max_value', 0),
                            'avg_value': stats.get('avg_value', 0),
                            'readable_max': stats.get('readable_max', {}),
                            'readable_avg': stats.get('readable_avg', {}),
                            'data_points': stats.get('count', 0),
                            'top_5_detailed': top_5_details
                        }
        
        return {
            'collection_timestamp': comprehensive_data.get('collection_timestamp', ''),
            'query_type': 'all_metrics_top5_detailed',
            'total_metrics': len(metrics_top5_detailed),
            'metrics_details': metrics_top5_detailed,
            'summary': comprehensive_data.get('summary', {})
        }

    def latencyelt_extract_essential_results(data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract all essential results from latency.json format data"""
        try:
            structured = {
                'collection_info': {
                    'timestamp': data.get('collection_timestamp', 'Unknown'),
                    'timezone': data.get('timezone', 'UTC'),
                    'query_type': data.get('query_type', 'Unknown'),
                    'total_metrics': data.get('summary', {}).get('total_metrics', 0),
                    'successful_metrics': data.get('summary', {}).get('successful_metrics', 0),
                    'failed_metrics': data.get('summary', {}).get('failed_metrics', 0)
                },
                'overall_summary': {
                    'max_latency': data.get('summary', {}).get('overall_max_latency', {}),
                    'avg_latency': data.get('summary', {}).get('overall_avg_latency', {}),
                    'component_breakdown': data.get('summary', {}).get('component_breakdown', {}),
                    'critical_metric': data.get('summary', {}).get('overall_max_latency', {}).get('metric', 'N/A')
                },
                'top_latencies_ranking': [],
                'metrics_by_category': {},
                'critical_findings': []
            }
            
            # Extract top latencies ranking
            top_latencies = data.get('summary', {}).get('top_latencies', [])
            for i, metric in enumerate(top_latencies[:10]):
                entry = {
                    'rank': i + 1,
                    'metric_name': metric.get('metric_name', 'Unknown'),
                    'component': metric.get('component', 'Unknown'),
                    'max_value': metric.get('max_value', 0),
                    'avg_value': metric.get('avg_value', 0),
                    'readable_max': metric.get('readable_max', {}),
                    'readable_avg': metric.get('readable_avg', {}),
                    'data_points': metric.get('data_points', 0),
                    'severity': 'critical' if i == 0 else 'high' if i < 3 else 'medium',
                    'is_top1': i == 0
                }
                structured['top_latencies_ranking'].append(entry)
            
            # Extract metrics by category
            categories = [
                'ready_duration_metrics', 'sync_duration_metrics', 'cni_latency_metrics',
                'pod_annotation_metrics', 'pod_creation_metrics', 'service_latency_metrics', 
                'network_config_metrics'
            ]
            
            for category in categories:
                if category in data:
                    category_name = category.replace('_metrics', '').replace('_', ' ').title()
                    structured['metrics_by_category'][category_name] = []
                    
                    for metric_key, metric_info in data[category].items():
                        if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                            stats = metric_info['statistics']
                            
                            # Get top pod info
                            top_pod_info = {}
                            top_entries = stats.get('top_5', stats.get('top_20', []))
                            if top_entries:
                                top_entry = top_entries[0]
                                top_pod_info = {
                                    'pod_name': top_entry.get('pod_name', 'N/A'),
                                    'node_name': top_entry.get('node_name', 'N/A'),
                                    'value': top_entry.get('value', 0),
                                    'formatted_value': f"{top_entry.get('readable_value', {}).get('value', 0)} {top_entry.get('readable_value', {}).get('unit', 'ms')}"
                                }
                                if 'resource_name' in top_entry:
                                    top_pod_info['resource_name'] = top_entry.get('resource_name', 'N/A')
                            
                            metric_summary = {
                                'metric_name': metric_info.get('metric_name', metric_key),
                                'component': metric_info.get('component', 'Unknown'),
                                'unit': metric_info.get('unit', 'seconds'),
                                'max_value': stats.get('max_value', 0),
                                'avg_value': stats.get('avg_value', 0),
                                'readable_max': stats.get('readable_max', {}),
                                'readable_avg': stats.get('readable_avg', {}),
                                'data_points': stats.get('count', 0),
                                'severity': elt_utility.categorize_latency_severity(stats.get('max_value', 0)),
                                'top_pod': top_pod_info
                            }
                            structured['metrics_by_category'][category_name].append(metric_summary)
            
            # Identify critical findings
            if top_latencies:
                # Critical metric (top 1)
                top_metric = top_latencies[0]
                if top_metric.get('max_value', 0) > 60:  # > 1 minute
                    structured['critical_findings'].append({
                        'type': 'critical_latency',
                        'metric': top_metric.get('metric_name', 'Unknown'),
                        'component': top_metric.get('component', 'Unknown'),
                        'value': top_metric.get('readable_max', {}),
                        'description': f"Extremely high latency detected in {top_metric.get('metric_name', 'Unknown')}"
                    })
                
                # High controller latencies
                controller_metrics = [m for m in top_latencies if m.get('component') == 'controller' and m.get('max_value', 0) > 5]
                if len(controller_metrics) > 2:
                    structured['critical_findings'].append({
                        'type': 'controller_performance',
                        'count': len(controller_metrics),
                        'description': f"Multiple controller metrics showing high latency ({len(controller_metrics)} metrics > 5s)"
                    })
            
            return structured
            
        except Exception as e:
            logger.error(f"Failed to extract essential results: {e}")
            return {'error': str(e)}

    def create_latencyelt_critical_badge(self, value: str, is_critical: bool = False, is_top1: bool = False) -> str:
        """Create critical badge for latencyELT with enhanced styling"""
        if is_top1:
            return f'<span class="badge badge-danger badge-lg pulse-animation"><i class="fas fa-exclamation-triangle"></i> {value}</span>'
        elif is_critical:
            return f'<span class="badge badge-warning font-weight-bold">{value}</span>'
        else:
            return f'<span class="badge badge-info">{value}</span>'

    def format_latencyelt_metric_name(self, metric_name: str, is_critical: bool = False, is_top1: bool = False) -> str:
        """Format metric name with critical highlighting for latencyELT"""
        truncated_name = self.truncate_metric_name(metric_name, 35)
        
        if is_top1:
            return f'<span class="text-danger font-weight-bold">{truncated_name}</span> <span class="badge badge-danger">CRITICAL</span>'
        elif is_critical:
            return f'<span class="text-warning font-weight-bold">{truncated_name}</span>'
        else:
            return truncated_name

    def categorize_latencyelt_severity_enhanced(self, max_value: float, avg_value: float, data_points: int, unit: str = 'seconds') -> Dict[str, str]:
        """Enhanced severity categorization for latencyELT metrics"""
        try:
            if unit == 'seconds':
                # Enhanced thresholds based on metric impact
                if max_value > 300:  # > 5 minutes - system critical
                    max_severity = 'critical'
                    impact = 'system_critical'
                elif max_value > 60:  # > 1 minute - service critical
                    max_severity = 'critical' 
                    impact = 'service_critical'
                elif max_value > 10:  # > 10 seconds - performance critical
                    max_severity = 'high'
                    impact = 'performance_critical'
                elif max_value > 2:   # > 2 seconds - noticeable delay
                    max_severity = 'medium'
                    impact = 'noticeable'
                elif max_value > 0.5: # > 500ms - minor impact
                    max_severity = 'low'
                    impact = 'minor'
                else:
                    max_severity = 'excellent'
                    impact = 'minimal'
                
                # Average value assessment
                if avg_value > 30:
                    avg_severity = 'critical'
                elif avg_value > 5:
                    avg_severity = 'high' 
                elif avg_value > 1:
                    avg_severity = 'medium'
                elif avg_value > 0.2:
                    avg_severity = 'low'
                else:
                    avg_severity = 'excellent'
                
                # Data reliability factor
                reliability = 'high' if data_points > 10 else 'medium' if data_points > 3 else 'low'
                
                return {
                    'max_severity': max_severity,
                    'avg_severity': avg_severity,
                    'overall': max_severity if max_severity in ['critical'] else avg_severity,
                    'impact_level': impact,
                    'data_reliability': reliability,
                    'is_critical': max_severity == 'critical' or avg_severity == 'critical'
                }
            else:
                return {
                    'max_severity': 'unknown',
                    'avg_severity': 'unknown', 
                    'overall': 'unknown',
                    'impact_level': 'unknown',
                    'data_reliability': 'unknown',
                    'is_critical': False
                }
        except (ValueError, TypeError):
            return {
                'max_severity': 'unknown',
                'avg_severity': 'unknown',
                'overall': 'unknown', 
                'impact_level': 'unknown',
                'data_reliability': 'unknown',
                'is_critical': False
            }

    # New HTML conversion functions for latencyELT data

    async def latencyelt_convert_json_to_html_tables(latency_json_data: Dict[str, Any]) -> str:
        """Convert latency.json format data to comprehensive HTML tables"""
        try:
            # Initialize latencyELT processor
            elt = latencyELT()
            
            # Extract essential results
            essential_data = latencyelt_extract_essential_results(latency_json_data)
            
            if 'error' in essential_data:
                return f"<div class='alert alert-danger'>Error: {essential_data['error']}</div>"
            
            # Create DataFrames using the updated transform method
            dataframes = elt.transform_to_dataframes(essential_data)
            
            # Generate HTML tables with enhanced styling
            html_tables = elt.generate_html_tables(dataframes)
            
            # Generate organized HTML output
            output_parts = []
            
            # Add header with critical alert if needed
            if essential_data.get('critical_findings'):
                output_parts.append(f"""
                <div class="alert alert-danger alert-dismissible fade show critical-alert" role="alert">
                    <h4 class="alert-heading"><i class="fas fa-exclamation-triangle"></i> Critical Latency Issues Detected!</h4>
                    <p>High-impact latency problems have been identified in your OVN-Kubernetes cluster.</p>
                    <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                        <span aria-hidden="true">&times;</span>
                    </button>
                </div>
                """)
            
            # Add title and metadata
            output_parts.append(f"""
            <div class="mb-4">
                <h3><span class="badge badge-primary">OVN-Kubernetes Latency Analysis</span></h3>
                <small class="text-muted">Generated from latency metrics data</small>
            </div>
            """)
            
            # Define table display order for logical presentation
            table_order = [
                ('latencyelt_essential_summary', 'Overall Summary'),
                ('latencyelt_collection_info', 'Collection Information'),
                ('latencyelt_critical_findings', 'Critical Findings'),
                ('latencyelt_top_latencies_ranking', 'Top Latency Metrics (Critical First)'),
                ('latencyelt_controller_sync_top20', 'Controller Sync Duration Top 20'),
                ('latencyelt_all_metrics_top5', 'All Metrics Top 5 Details'),
                ('latencyelt_ready_duration', 'Ready Duration Metrics'),
                ('latencyelt_sync_duration', 'Sync Duration Metrics'),
                ('latencyelt_cni_latency', 'CNI Latency Metrics'),
                ('latencyelt_pod_annotation', 'Pod Annotation Metrics'),
                ('latencyelt_pod_creation', 'Pod Creation Metrics'),
                ('latencyelt_service_latency', 'Service Latency Metrics'),
                ('latencyelt_network_config', 'Network Configuration Metrics')
            ]
            
            # Add tables in logical order
            for table_name, display_title in table_order:
                if table_name in html_tables:
                    # Special styling for critical tables
                    if table_name in ['latencyelt_critical_findings', 'latencyelt_top_latencies_ranking']:
                        output_parts.append(f"""
                        <div class="critical-section mt-4">
                            <h4 class="text-danger"><i class="fas fa-exclamation-circle"></i> {display_title}</h4>
                            {html_tables[table_name]}
                        </div>
                        """)
                    else:
                        output_parts.append(f"<h5 class='mt-4'>{display_title}</h5>")
                        output_parts.append(html_tables[table_name])
            
            # Add any remaining tables not in the predefined order
            for table_name, html_table in html_tables.items():
                if not any(table_name == order_item[0] for order_item in table_order):
                    table_title = table_name.replace('latencyelt_', '').replace('_', ' ').title()
                    output_parts.append(f"<h5 class='mt-4'>{table_title}</h5>")
                    output_parts.append(html_table)
            
            # Add custom CSS for enhanced styling
            custom_css = """
            <style>
                .critical-alert { 
                    border-left: 5px solid #dc3545; 
                    animation: pulse 2s infinite; 
                }
                .critical-table-wrapper { 
                    border: 2px solid #dc3545; 
                    border-radius: 5px; 
                    margin: 10px 0; 
                }
                .critical-section { 
                    background-color: #f8f9fa; 
                    border: 1px solid #dee2e6; 
                    border-radius: 5px; 
                    padding: 15px; 
                    margin: 10px 0; 
                }
                .pulse-animation { 
                    animation: pulse 1.5s infinite; 
                }
                .blink-text { 
                    animation: blink 2s infinite; 
                }
                .alert-table { 
                    background-color: #fff3cd; 
                }
                @keyframes pulse {
                    0% { opacity: 1; }
                    50% { opacity: 0.5; }
                    100% { opacity: 1; }
                }
                @keyframes blink {
                    0%, 50% { opacity: 1; }
                    51%, 100% { opacity: 0.3; }
                }
                .badge-lg { 
                    font-size: 0.9em; 
                    padding: 0.5em 0.8em; 
                }
            </style>
            """
            
            return custom_css + ' '.join(output_parts)
            
        except Exception as e:
            return f"<div class='alert alert-danger'>Error processing latency data: {str(e)}</div>"

    # Updated main conversion functions for integration

    async def latencyelt_process_and_convert(latency_data: Union[Dict[str, Any], str], output_format: str = "html") -> Dict[str, Any]:
        """Process latency data and convert to requested format with enhanced features"""
        try:
            # Parse input data
            if isinstance(latency_data, str):
                try:
                    data = json.loads(latency_data)
                except json.JSONDecodeError as e:
                    return {'error': f"Invalid JSON: {str(e)}"}
            else:
                data = latency_data
            
            # Extract essential results
            essential_data = latencyelt_extract_essential_results(data)
            
            if 'error' in essential_data:
                return essential_data
            
            # Initialize processor
            elt = latencyELT()
            
            # Create summary
            summary = elt.summarize_ovn_latency_data(essential_data)
            
            result = {
                'data_type': 'latencyelt_metrics',
                'summary': summary,
                'essential_data': essential_data,
                'timestamp': data.get('collection_timestamp', datetime.now().isoformat()),
                'processing_successful': True,
                'critical_issues_found': len(essential_data.get('critical_findings', [])) > 0,
                'total_metrics': essential_data.get('collection_info', {}).get('total_metrics', 0)
            }
            
            if output_format in ["html", "both"]:
                html_output = await latencyelt_convert_json_to_html_tables(data)
                result['html'] = html_output
            
            if output_format in ["dataframes", "both"]:
                dataframes = elt.transform_to_dataframes(essential_data)
                result['dataframes'] = dataframes
            
            if output_format in ["tabular", "both"]:
                dataframes = elt.transform_to_dataframes(essential_data)
                tabular_tables = {}
                
                for table_name, df in dataframes.items():
                    if not df.empty:
                        try:
                            formatted_table = tabulate(
                                df.values.tolist(),
                                headers=df.columns.tolist(),
                                tablefmt="grid",
                                stralign="left",
                                maxcolwidths=[30] * len(df.columns)
                            )
                            tabular_tables[table_name] = {
                                'raw_data': [df.columns.tolist()] + df.values.tolist(),
                                'formatted_string': formatted_table
                            }
                        except Exception as e:
                            tabular_tables[table_name] = {
                                'raw_data': [df.columns.tolist()] + df.values.tolist(),
                                'formatted_string': str(df)
                            }
                
                result['tabular'] = tabular_tables
            
            return result
            
        except Exception as e:
            return {'error': f"Processing failed: {str(e)}", 'processing_successful': False}    

# Query functions for latency data collection
async def latencyelt_get_controller_sync_top20(prometheus_client, time: str = None, duration: str = None, end_time: str = None):
    """Get top 20 controller sync duration metrics with detailed pod/node/resource info"""
    from tools.ovnk_benchmark_prometheus_ovnk_latency import OVNLatencyCollector
    
    collector = OVNLatencyCollector(prometheus_client)
    result = await collector.collect_controller_sync_duration(time, duration, end_time)
    
    # Extract top 20 detailed entries
    if 'statistics' in result and 'top_20' in result['statistics']:
        top_20_entries = []
        for entry in result['statistics']['top_20']:
            detailed_entry = {
                'pod_name': entry.get('pod_name', 'N/A'),
                'node_name': entry.get('node_name', 'N/A'),
                'resource_name': entry.get('resource_name', 'N/A'),
                'value_seconds': entry.get('value', 0),
                'readable_value': entry.get('readable_value', {}),
                'formatted_value': f"{entry.get('readable_value', {}).get('value', 0)} {entry.get('readable_value', {}).get('unit', '')}"
            }
            top_20_entries.append(detailed_entry)
        
        return {
            'metric_name': 'ovnkube_controller_sync_duration_seconds',
            'total_entries': len(top_20_entries),
            'max_value': result['statistics'].get('max_value', 0),
            'avg_value': result['statistics'].get('avg_value', 0),
            'top_20_detailed': top_20_entries
        }
    
    return {'error': 'No sync duration data available'}

async def latencyelt_get_comprehensive_metrics(prometheus_client, time: str = None, duration: str = None, end_time: str = None):
    """Get comprehensive latency metrics for all components"""
    from tools.ovnk_benchmark_prometheus_ovnk_latency import collect_enhanced_ovn_latency_metrics
    
    return await collect_enhanced_ovn_latency_metrics(
        prometheus_client, time, duration, end_time,
        include_controller_metrics=True,
        include_node_metrics=True, 
        include_extended_metrics=True
    )

async def latencyelt_get_metric_top5_detail(prometheus_client, metric_name: str, time: str = None, duration: str = None, end_time: str = None):
    """Get detailed top 5 entries for any specific metric"""
    from tools.ovnk_benchmark_prometheus_ovnk_latency import OVNLatencyCollector
    
    collector = OVNLatencyCollector(prometheus_client)
    result = await collector._collect_metric(metric_name, time, duration, end_time)
    
    if 'statistics' in result and result['statistics'].get('count', 0) > 0:
        stats = result['statistics']
        top_entries = stats.get('top_5', [])
        
        detailed_entries = []
        for i, entry in enumerate(top_entries[:5]):
            detailed_entry = {
                'rank': i + 1,
                'pod_name': entry.get('pod_name', 'N/A'),
                'node_name': entry.get('node_name', 'N/A'),
                'value_seconds': entry.get('value', 0),
                'readable_value': entry.get('readable_value', {}),
                'formatted_value': f"{entry.get('readable_value', {}).get('value', 0)} {entry.get('readable_value', {}).get('unit', '')}"
            }
            
            # Add resource name for sync metrics
            if 'resource_name' in entry:
                detailed_entry['resource_name'] = entry.get('resource_name', 'N/A')
            
            # Add service name for network config metrics
            if 'service_name' in entry:
                detailed_entry['service_name'] = entry.get('service_name', 'N/A')
            
            detailed_entries.append(detailed_entry)
        
        return {
            'metric_name': metric_name,
            'component': result.get('component', 'Unknown'),
            'unit': result.get('unit', 'seconds'),
            'total_entries': stats.get('count', 0),
            'max_value': stats.get('max_value', 0),
            'avg_value': stats.get('avg_value', 0),
            'top_5_detailed': detailed_entries
        }
    
    return {'error': f'No data available for metric: {metric_name}'}

async def latencyelt_get_all_metrics_avg_max(prometheus_client, time: str = None, duration: str = None, end_time: str = None):
    """Get avg/max values for all latency metrics with top 5 details"""
    comprehensive_data = await latencyelt_get_comprehensive_metrics(prometheus_client, time, duration, end_time)
    
    if 'error' in comprehensive_data:
        return comprehensive_data
    
    metrics_summary = {}
    
    # Process all metric categories
    categories = [
        'ready_duration_metrics', 'sync_duration_metrics', 'cni_latency_metrics',
        'pod_annotation_metrics', 'pod_creation_metrics', 'service_latency_metrics', 
        'network_config_metrics'
    ]
    
    for category in categories:
        if category in comprehensive_data:
            category_data = comprehensive_data[category]
            
            for metric_key, metric_info in category_data.items():
                if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                    stats = metric_info['statistics']
                    
                    # Extract top 5 pod details
                    top_5_details = []
                    top_entries = stats.get('top_5', stats.get('top_20', [])[:5])
                    
                    for i, entry in enumerate(top_entries[:5]):
                        if entry.get('value') is not None:
                            detail = {
                                'rank': i + 1,
                                'pod_name': entry.get('pod_name', 'N/A'),
                                'node_name': entry.get('node_name', 'N/A'),
                                'value': entry.get('value', 0),
                                'unit': metric_info.get('unit', 'seconds'),
                                'formatted_value': f"{entry.get('readable_value', {}).get('value', 0)} {entry.get('readable_value', {}).get('unit', '')}"
                            }
                            
                            # Add additional fields if present
                            if 'resource_name' in entry:
                                detail['resource_name'] = entry.get('resource_name', 'N/A')
                            if 'service_name' in entry:
                                detail['service_name'] = entry.get('service_name', 'N/A')
                            
                            top_5_details.append(detail)
                    
                    metrics_summary[metric_key] = {
                        'metric_name': metric_info.get('metric_name', metric_key),
                        'component': metric_info.get('component', 'Unknown'),
                        'unit': metric_info.get('unit', 'seconds'),
                        'max_value': stats.get('max_value', 0),
                        'avg_value': stats.get('avg_value', 0),
                        'readable_max': stats.get('readable_max', {}),
                        'readable_avg': stats.get('readable_avg', {}),
                        'data_points': stats.get('count', 0),
                        'top_5_pods': top_5_details
                    }
    
    return {
        'collection_timestamp': comprehensive_data.get('collection_timestamp', ''),
        'query_type': comprehensive_data.get('query_type', 'instant'),
        'total_metrics': len(metrics_summary),
        'metrics_details': metrics_summary,
        'overall_summary': comprehensive_data.get('summary', {})
    }

# New functions to add to ovnk_benchmark_elt_latency.py



    

    # Additional utility functions for latencyELT

