"""
Extract, Load, Transform module for OVN Deep Drive Performance Data
Handles comprehensive deep dive analysis results from ovnk_benchmark_performance_ovnk_deepdrive.py
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class deepDriveELT(EltUtility):
    """ELT module for OVN Deep Drive performance analysis data"""
    
    def __init__(self):
        super().__init__()

    def extract_deepdrive_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract deep drive analysis data into structured format"""
        try:
            structured = {
                'metadata': self._extract_metadata(data),
                'basic_info': self._extract_basic_info(data),
                'ovnkube_pods': self._extract_ovnkube_pods(data),
                'ovn_containers': self._extract_ovn_containers(data),
                'ovs_metrics': self._extract_ovs_metrics(data),
                'latency_metrics': self._extract_latency_metrics(data),
                'nodes_usage': self._extract_nodes_usage(data),
                'performance_analysis': self._extract_performance_analysis(data)
            }
            return structured
        except Exception as e:
            logger.error(f"Failed to extract deep drive data: {e}")
            return {'error': str(e)}

    def _extract_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata information"""
        return {
            'analysis_timestamp': data.get('analysis_timestamp', 'N/A'),
            'analysis_type': data.get('analysis_type', 'N/A'),
            'query_duration': data.get('query_duration', 'N/A'),
            'timezone': data.get('timezone', 'N/A'),
            'components_analyzed': data.get('execution_metadata', {}).get('components_analyzed', 0),
            'tool_name': data.get('execution_metadata', {}).get('tool_name', 'N/A')
        }

    def _extract_basic_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract basic cluster information"""
        basic_info = data.get('basic_info', {})
        if basic_info.get('error'):
            return {'error': basic_info['error']}
        
        extracted = {
            'pod_counts': basic_info.get('pod_counts', {}),
            'database_sizes': basic_info.get('database_sizes', {}),
            'alerts_summary': basic_info.get('alerts_summary', {}),
            'pod_distribution': basic_info.get('pod_distribution', {})
        }
        return extracted

    def _extract_ovnkube_pods(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVN Kubernetes pods usage data"""
        pods_data = data.get('ovnkube_pods_cpu', {})
        if pods_data.get('error'):
            return {'error': pods_data['error']}
        
        return {
            'node_pods': pods_data.get('ovnkube_node_pods', {}),
            'control_plane_pods': pods_data.get('ovnkube_control_plane_pods', {})
        }

    def _extract_ovn_containers(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVN containers usage data"""
        containers_data = data.get('ovn_containers', {})
        if containers_data.get('error'):
            return {'error': containers_data['error']}
        
        return containers_data.get('containers', {})

    def _extract_ovs_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVS metrics data"""
        ovs_data = data.get('ovs_metrics', {})
        if ovs_data.get('error'):
            return {'error': ovs_data['error']}
        
        return {
            'cpu_usage': ovs_data.get('cpu_usage', {}),
            'memory_usage': ovs_data.get('memory_usage', {}),
            'flows_metrics': ovs_data.get('flows_metrics', {}),
            'connection_metrics': ovs_data.get('connection_metrics', {})
        }

    def _extract_latency_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract latency metrics data"""
        latency_data = data.get('latency_metrics', {})
        if latency_data.get('error'):
            return {'error': latency_data['error']}
        
        return latency_data.get('categories', {})

    def _extract_nodes_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract nodes usage data"""
        nodes_data = data.get('nodes_usage', {})
        if nodes_data.get('error'):
            return {'error': nodes_data['error']}
        
        return {
            'controlplane_nodes': nodes_data.get('controlplane_nodes', {}),
            'infra_nodes': nodes_data.get('infra_nodes', {}),
            'top5_worker_nodes': nodes_data.get('top5_worker_nodes', {})
        }

    def _extract_performance_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract performance analysis results"""
        perf_data = data.get('performance_analysis', {})
        return {
            'performance_summary': perf_data.get('performance_summary', {}),
            'key_findings': perf_data.get('key_findings', []),
            'recommendations': perf_data.get('recommendations', []),
            'resource_hotspots': perf_data.get('resource_hotspots', {}),
            'latency_analysis': perf_data.get('latency_analysis', {}),
            'node_analysis': perf_data.get('node_analysis', {})
        }

    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into DataFrames"""
        dataframes = {}
        
        try:
            # Metadata table
            if 'metadata' in structured_data:
                metadata_rows = []
                for key, value in structured_data['metadata'].items():
                    metadata_rows.append({
                        'Property': key.replace('_', ' ').title(),
                        'Value': str(value)
                    })
                if metadata_rows:
                    dataframes['metadata'] = pd.DataFrame(metadata_rows)

            # Basic info tables
            if 'basic_info' in structured_data and not structured_data['basic_info'].get('error'):
                basic_info = structured_data['basic_info']
                
                # Pod counts
                pod_counts = basic_info.get('pod_counts', {})
                if pod_counts:
                    pod_rows = [
                        {'Property': 'Total Pods', 'Value': str(pod_counts.get('total_pods', 0))}
                    ]
                    phases = pod_counts.get('phases', {})
                    for phase, count in phases.items():
                        pod_rows.append({'Property': f'{phase} Pods', 'Value': str(count)})
                    dataframes['pod_status'] = pd.DataFrame(pod_rows)

                # Database sizes
                db_sizes = basic_info.get('database_sizes', {})
                if db_sizes:
                    db_rows = []
                    for db_name, db_info in db_sizes.items():
                        if isinstance(db_info, dict):
                            db_rows.append({
                                'Database': db_name.replace('_', ' ').title(),
                                'Size (MB)': f"{db_info.get('size_mb', 0):.2f}"
                            })
                    if db_rows:
                        dataframes['database_sizes'] = pd.DataFrame(db_rows)

                # Alerts summary
                alerts = basic_info.get('alerts_summary', {})
                if alerts and alerts.get('top_alerts'):
                    alert_rows = []
                    for i, alert in enumerate(alerts['top_alerts'][:5], 1):
                        alert_rows.append({
                            'Rank': i,
                            'Alert': alert.get('alert_name', 'N/A'),
                            'Severity': alert.get('severity', 'N/A'),
                            'Count': alert.get('count', 0)
                        })
                    if alert_rows:
                        dataframes['alerts'] = pd.DataFrame(alert_rows)

                # Pod distribution
                pod_dist = basic_info.get('pod_distribution', {})
                if pod_dist and pod_dist.get('top_nodes'):
                    node_rows = []
                    for i, node in enumerate(pod_dist['top_nodes'][:5], 1):
                        node_rows.append({
                            'Rank': i,
                            'Node': self.truncate_node_name(node.get('node_name', 'N/A')),
                            'Role': node.get('node_role', 'N/A'),
                            'Pod Count': node.get('pod_count', 0)
                        })
                    if node_rows:
                        dataframes['pod_distribution'] = pd.DataFrame(node_rows)

            # OVN Kubernetes pods
            if 'ovnkube_pods' in structured_data and not structured_data['ovnkube_pods'].get('error'):
                pods_data = structured_data['ovnkube_pods']
                
                # Node pods CPU
                node_pods = pods_data.get('node_pods', {})
                if node_pods.get('top_5_cpu'):
                    cpu_rows = []
                    for i, pod in enumerate(node_pods['top_5_cpu'][:5], 1):
                        metrics = pod.get('metrics', {}).get('cpu_usage', {})
                        cpu_rows.append({
                            'Rank': i,
                            'Pod': self.truncate_text(pod.get('pod_name', 'N/A'), 20),
                            'Node': self.truncate_node_name(pod.get('node_name', 'N/A'), 20),
                            'Avg CPU %': f"{metrics.get('avg', 0):.2f}",
                            'Max CPU %': f"{metrics.get('max', 0):.2f}"
                        })
                    if cpu_rows:
                        dataframes['ovnkube_node_cpu'] = pd.DataFrame(cpu_rows)

                # Node pods Memory
                if node_pods.get('top_5_memory'):
                    mem_rows = []
                    for i, pod in enumerate(node_pods['top_5_memory'][:5], 1):
                        metrics = pod.get('metrics', {}).get('memory_usage', {})
                        mem_rows.append({
                            'Rank': i,
                            'Pod': self.truncate_text(pod.get('pod_name', 'N/A'), 20),
                            'Node': self.truncate_node_name(pod.get('node_name', 'N/A'), 20),
                            'Avg MB': f"{metrics.get('avg', 0):.1f}",
                            'Max MB': f"{metrics.get('max', 0):.1f}"
                        })
                    if mem_rows:
                        dataframes['ovnkube_node_memory'] = pd.DataFrame(mem_rows)

                # Control plane pods
                cp_pods = pods_data.get('control_plane_pods', {})
                if cp_pods.get('top_5_cpu'):
                    cp_rows = []
                    for i, pod in enumerate(cp_pods['top_5_cpu'][:5], 1):
                        metrics = pod.get('metrics', {}).get('cpu_usage', {})
                        cp_rows.append({
                            'Rank': i,
                            'Pod': self.truncate_text(pod.get('pod_name', 'N/A'), 25),
                            'Avg CPU %': f"{metrics.get('avg', 0):.2f}",
                            'Max CPU %': f"{metrics.get('max', 0):.2f}"
                        })
                    if cp_rows:
                        dataframes['control_plane_cpu'] = pd.DataFrame(cp_rows)

            # OVS metrics
            if 'ovs_metrics' in structured_data and not structured_data['ovs_metrics'].get('error'):
                ovs_data = structured_data['ovs_metrics']
                
                # OVS CPU usage
                cpu_usage = ovs_data.get('cpu_usage', {})
                if cpu_usage.get('ovs_vswitchd_top5'):
                    ovs_cpu_rows = []
                    for i, node in enumerate(cpu_usage['ovs_vswitchd_top5'][:5], 1):
                        ovs_cpu_rows.append({
                            'Rank': i,
                            'Node': self.truncate_node_name(node.get('node_name', 'N/A'), 25),
                            'Avg CPU %': f"{node.get('avg', 0):.2f}",
                            'Max CPU %': f"{node.get('max', 0):.2f}"
                        })
                    if ovs_cpu_rows:
                        dataframes['ovs_cpu'] = pd.DataFrame(ovs_cpu_rows)

                # OVS Memory usage
                mem_usage = ovs_data.get('memory_usage', {})
                if mem_usage.get('ovs_vswitchd_top5'):
                    ovs_mem_rows = []
                    for i, pod in enumerate(mem_usage['ovs_vswitchd_top5'][:5], 1):
                        ovs_mem_rows.append({
                            'Rank': i,
                            'Pod': self.truncate_text(pod.get('pod_name', 'N/A'), 20),
                            'Avg MB': f"{pod.get('avg', 0):.1f}",
                            'Max MB': f"{pod.get('max', 0):.1f}"
                        })
                    if ovs_mem_rows:
                        dataframes['ovs_memory'] = pd.DataFrame(ovs_mem_rows)

                # Flow metrics
                flows = ovs_data.get('flows_metrics', {})
                if flows.get('dp_flows_top5'):
                    flow_rows = []
                    for i, flow in enumerate(flows['dp_flows_top5'][:5], 1):
                        flow_rows.append({
                            'Rank': i,
                            'Instance': self.truncate_text(flow.get('instance', 'N/A'), 20),
                            'Avg Flows': flow.get('avg', 0),
                            'Max Flows': flow.get('max', 0)
                        })
                    if flow_rows:
                        dataframes['datapath_flows'] = pd.DataFrame(flow_rows)

            # Nodes usage
            if 'nodes_usage' in structured_data and not structured_data['nodes_usage'].get('error'):
                nodes_data = structured_data['nodes_usage']
                
                # Control plane nodes
                cp_nodes = nodes_data.get('controlplane_nodes', {})
                if cp_nodes.get('individual_nodes'):
                    cp_node_rows = []
                    for node in cp_nodes['individual_nodes'][:3]:
                        cpu_usage = node.get('cpu_usage', {})
                        mem_usage = node.get('memory_usage', {})
                        cp_node_rows.append({
                            'Node': self.truncate_node_name(node.get('name', 'N/A'), 25),
                            'CPU %': f"{cpu_usage.get('avg', 0):.1f}",
                            'Memory GB': f"{mem_usage.get('avg', 0)/1024:.1f}" if mem_usage.get('avg') else '0.0'
                        })
                    if cp_node_rows:
                        dataframes['controlplane_nodes'] = pd.DataFrame(cp_node_rows)

                # Top worker nodes
                worker_nodes = nodes_data.get('top5_worker_nodes', {})
                if worker_nodes.get('individual_nodes'):
                    worker_rows = []
                    for node in worker_nodes['individual_nodes'][:5]:
                        cpu_usage = node.get('cpu_usage', {})
                        mem_usage = node.get('memory_usage', {})
                        worker_rows.append({
                            'Rank': node.get('rank', 0),
                            'Node': self.truncate_node_name(node.get('name', 'N/A'), 20),
                            'CPU %': f"{cpu_usage.get('avg', 0):.1f}",
                            'Memory GB': f"{mem_usage.get('avg', 0)/1024:.1f}" if mem_usage.get('avg') else '0.0'
                        })
                    if worker_rows:
                        dataframes['worker_nodes'] = pd.DataFrame(worker_rows)

            # Performance analysis
            if 'performance_analysis' in structured_data:
                perf_data = structured_data['performance_analysis']
                
                # Performance summary
                perf_summary = perf_data.get('performance_summary', {})
                if perf_summary:
                    summary_rows = [
                        {'Metric': 'Overall Score', 'Value': str(perf_summary.get('overall_score', 0))},
                        {'Metric': 'Performance Grade', 'Value': perf_summary.get('performance_grade', 'N/A')}
                    ]
                    component_scores = perf_summary.get('component_scores', {})
                    for comp, score in component_scores.items():
                        summary_rows.append({
                            'Metric': comp.replace('_', ' ').title(),
                            'Value': str(score)
                        })
                    if summary_rows:
                        dataframes['performance_summary'] = pd.DataFrame(summary_rows)

                # Key findings
                findings = perf_data.get('key_findings', [])
                if findings:
                    finding_rows = []
                    for i, finding in enumerate(findings[:5], 1):
                        finding_rows.append({
                            'Rank': i,
                            'Finding': str(finding)
                        })
                    dataframes['key_findings'] = pd.DataFrame(finding_rows)

                # Recommendations
                recommendations = perf_data.get('recommendations', [])
                if recommendations:
                    rec_rows = []
                    for i, rec in enumerate(recommendations[:5], 1):
                        rec_rows.append({
                            'Rank': i,
                            'Recommendation': str(rec)
                        })
                    dataframes['recommendations'] = pd.DataFrame(rec_rows)

            # Apply column limits
            limited_dataframes = {}
            for name, df in dataframes.items():
                if not df.empty:
                    limited_df = self.limit_dataframe_columns(df, table_name=name)
                    limited_dataframes[name] = limited_df

            return limited_dataframes

        except Exception as e:
            logger.error(f"Failed to transform deep drive data to DataFrames: {e}")
            return {}

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            for table_name, df in dataframes.items():
                if not df.empty:
                    html_tables[table_name] = self.create_html_table(df, table_name)
            
            return html_tables
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for deep drive data: {e}")
            return {}

    def summarize_deepdrive_data(self, structured_data: Dict[str, Any]) -> str:
        """Generate a brief summary of the deep drive analysis"""
        try:
            summary_parts = ["OVN Deep Drive Analysis:"]
            
            # Basic cluster info
            basic_info = structured_data.get('basic_info', {})
            if not basic_info.get('error'):
                pod_counts = basic_info.get('pod_counts', {})
                total_pods = pod_counts.get('total_pods', 0)
                running_pods = pod_counts.get('phases', {}).get('Running', 0)
                summary_parts.append(f"• Cluster: {total_pods} total pods ({running_pods} running)")
                
                alerts = basic_info.get('alerts_summary', {})
                if alerts:
                    alert_count = alerts.get('total_alert_types', 0)
                    summary_parts.append(f"• Alerts: {alert_count} active alert types")

            # Performance analysis
            perf_analysis = structured_data.get('performance_analysis', {})
            perf_summary = perf_analysis.get('performance_summary', {})
            if perf_summary:
                overall_score = perf_summary.get('overall_score', 0)
                grade = perf_summary.get('performance_grade', 'N/A')
                summary_parts.append(f"• Performance: {overall_score}/100 (Grade: {grade})")

            # Key findings
            findings = perf_analysis.get('key_findings', [])
            if findings:
                summary_parts.append(f"• Key findings: {len(findings)} identified")

            # Recommendations
            recommendations = perf_analysis.get('recommendations', [])
            if recommendations:
                summary_parts.append(f"• Recommendations: {len(recommendations)} provided")

            # Node analysis
            nodes_data = structured_data.get('nodes_usage', {})
            if not nodes_data.get('error'):
                cp_nodes = nodes_data.get('controlplane_nodes', {})
                worker_nodes = nodes_data.get('top5_worker_nodes', {})
                cp_count = cp_nodes.get('count', 0)
                worker_count = worker_nodes.get('count', 0)
                summary_parts.append(f"• Infrastructure: {cp_count} control plane, {worker_count} worker nodes analyzed")

            return " ".join(summary_parts)

        except Exception as e:
            logger.error(f"Failed to generate deep drive summary: {e}")
            return f"Deep drive analysis summary generation failed: {str(e)}"