"""
Deep Drive ELT module for OVN-Kubernetes comprehensive performance analysis
Extract, Load, Transform module for deep drive analysis results
"""

import logging
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime

from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class deepDriveELT(EltUtility):
    """Extract, Load, Transform class for Deep Drive analysis data"""
    
    def __init__(self):
        super().__init__()

    def _to_float_safe(self, value, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            if isinstance(value, (int, float)):
                return float(value)
            return float(str(value).strip())
        except Exception:
            return default

    def _extract_stat(self, metrics: Dict[str, Any], metric_key: str, stat_candidates: List[str]) -> float:
        try:
            metric = metrics.get(metric_key, {}) if isinstance(metrics, dict) else {}
            for key in stat_candidates:
                if key in metric and metric.get(key) is not None:
                    return self._to_float_safe(metric.get(key), 0.0)
        except Exception:
            pass
        return 0.0

    def extract_deepdrive_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract deep drive analysis data from JSON"""
        try:
            extracted = {
                'metadata': self._extract_metadata(data),
                'basic_info': self._extract_basic_info(data),
                'resource_usage': self._extract_resource_usage(data),
                'latency_analysis': self._extract_latency_analysis(data),
                'performance_insights': self._extract_performance_insights(data),
                'node_analysis': self._extract_node_analysis(data),
                'ovs_metrics': self._extract_ovs_summary(data)
            }
            return extracted
            
        except Exception as e:
            logger.error(f"Failed to extract deep drive data: {e}")
            return {'error': str(e)}

    def _extract_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata information"""
        return {
            'analysis_timestamp': data.get('analysis_timestamp', ''),
            'analysis_type': data.get('analysis_type', ''),
            'query_duration': data.get('query_duration', ''),
            'timezone': data.get('timezone', 'UTC'),
            'components_analyzed': data.get('execution_metadata', {}).get('components_analyzed', 0),
            'tool_name': data.get('execution_metadata', {}).get('tool_name', ''),
            'timeout_seconds': data.get('execution_metadata', {}).get('timeout_seconds', 0)
        }

    def _extract_basic_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract basic cluster information"""
        basic_info = data.get('basic_info', {})
        
        # Pod counts and phases
        pod_info = []
        pod_counts = basic_info.get('pod_counts', {})
        if pod_counts:
            pod_info.append({
                'Metric': 'Total Pods',
                'Value': pod_counts.get('total_pods', 0),
                'Status': 'info'
            })
            
            phases = pod_counts.get('phases', {})
            for phase, count in phases.items():
                status = 'success' if phase == 'Running' else 'warning' if phase == 'Failed' else 'info'
                pod_info.append({
                    'Metric': f'Pods {phase}',
                    'Value': count,
                    'Status': status
                })

        # Database sizes
        db_info = []
        db_sizes = basic_info.get('database_sizes', {})
        for db_name, db_data in db_sizes.items():
            size_mb = db_data.get('size_mb', 0)
            db_info.append({
                'Database': db_name.replace('_', ' ').title(),
                'Max DB Size': f"{size_mb:.1f} MB" if isinstance(size_mb, (int, float)) else size_mb,
                'Status': 'success' if (isinstance(size_mb, (int, float)) and size_mb < 10) else 'warning'
            })

        # Alerts summary
        alerts_info = []
        alerts = basic_info.get('alerts_summary', {}).get('top_alerts', [])
        for idx, alert in enumerate(alerts[:5], 1):
            severity = alert.get('severity', 'unknown')
            status = 'danger' if severity == 'critical' else 'warning' if severity == 'warning' else 'info'
            alerts_info.append({
                'Rank': f"🔥 {idx}" if idx == 1 else idx,  # Highlight top alert
                'Alert': alert.get('alert_name', ''),
                'Severity': severity.upper(),
                'Status': status
            })

        return {
            'pod_status': pod_info,
            'database_sizes': db_info,
            'alerts': alerts_info
        }

    def _extract_resource_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract resource usage data"""
        # OVNKube pods CPU usage
        ovnkube_pods = data.get('ovnkube_pods_cpu', {})
        top_cpu_pods = []
        pods_usage_detailed: List[Dict[str, Any]] = []
        
        # Node pods
        node_pods_cpu = ovnkube_pods.get('ovnkube_node_pods', {}).get('top_5_cpu', [])
        for pod in node_pods_cpu[:5]:
            cpu_usage = pod.get('metrics', {}).get('cpu_usage', {})
            memory_usage = pod.get('metrics', {}).get('memory_usage', {})  # Get memory data
            rank = pod.get('rank', 0)
            top_cpu_pods.append({
                'Rank': f"🏆 {rank}" if rank == 1 else rank,  # Highlight top pod
                'Pod': self.truncate_text(pod.get('pod_name', ''), 25),
                'Node': self.truncate_node_name(pod.get('node_name', ''), 20),
                'CPU %': f"{cpu_usage.get('avg', 0):.2f}",
                'Memory MB': f"{memory_usage.get('avg', 0):.1f}" if memory_usage else "N/A"
            })

            # Detailed entry with avg/max CPU and RAM
            pods_usage_detailed.append({
                'Scope': 'Node Pod',
                'Pod': pod.get('pod_name', ''),
                'Node': pod.get('node_name', ''),
                'Avg CPU %': round(cpu_usage.get('avg', 0.0), 2),
                'Max CPU %': round(cpu_usage.get('max', 0.0), 2),
                'Avg Mem MB': round(memory_usage.get('avg', 0.0), 1) if memory_usage else 0.0,
                'Max Mem MB': round(memory_usage.get('max', 0.0), 1) if memory_usage else 0.0
            })

        # Control plane pods
        cp_pods_cpu = ovnkube_pods.get('ovnkube_control_plane_pods', {}).get('top_5_cpu', [])
        for pod in cp_pods_cpu:
            cpu_usage = pod.get('metrics', {}).get('cpu_usage', {})
            memory_usage = pod.get('metrics', {}).get('memory_usage', {})  # Get memory data
            rank = len(top_cpu_pods) + 1
            top_cpu_pods.append({
                'Rank': rank,
                'Pod': self.truncate_text(pod.get('pod_name', ''), 25),
                'Node': self.truncate_node_name(pod.get('node_name', ''), 20),
                'CPU %': f"{cpu_usage.get('avg', 0):.2f}",
                'Memory MB': f"{memory_usage.get('avg', 0):.1f}" if memory_usage else "N/A"
            })

            pods_usage_detailed.append({
                'Scope': 'Control Pod',
                'Pod': pod.get('pod_name', ''),
                'Node': pod.get('node_name', ''),
                'Avg CPU %': round(cpu_usage.get('avg', 0.0), 2),
                'Max CPU %': round(cpu_usage.get('max', 0.0), 2),
                'Avg Mem MB': round(memory_usage.get('avg', 0.0), 1) if memory_usage else 0.0,
                'Max Mem MB': round(memory_usage.get('max', 0.0), 1) if memory_usage else 0.0
            })

        # OVN containers usage
        container_usage = []
        containers_usage_detailed: List[Dict[str, Any]] = []
        ovn_containers = data.get('ovn_containers', {}).get('containers', {})
        
        for container_name, container_data in ovn_containers.items():
            if 'error' not in container_data:
                cpu_data = container_data.get('top_5_cpu', [])
                mem_data = container_data.get('top_5_memory', [])
                
                if cpu_data:
                    top_cpu = cpu_data[0]
                    cpu_metrics = top_cpu.get('metrics', {}).get('cpu_usage', {})
                    
                    # Get memory metrics from the same pod or find matching memory entry
                    mem_metrics = top_cpu.get('metrics', {}).get('memory_usage', {})
                    if not mem_metrics and mem_data:
                        # Try to find matching memory entry by pod name
                        pod_name = top_cpu.get('pod_name', '')
                        for mem_entry in mem_data:
                            if mem_entry.get('pod_name', '') == pod_name:
                                mem_metrics = mem_entry.get('metrics', {}).get('memory_usage', {})
                                break
                        # If still not found, use first memory entry
                        if not mem_metrics and mem_data:
                            mem_metrics = mem_data[0].get('metrics', {}).get('memory_usage', {})
                    
                    status = 'danger' if container_name == 'ovnkube_controller' and cpu_metrics.get('avg', 0) > 0.5 else 'success'
                    
                    container_usage.append({
                        'Container': container_name.replace('_', ' ').title(),
                        'CPU %': f"{cpu_metrics.get('avg', 0):.3f}",
                        'Memory MB': f"{mem_metrics.get('avg', 0):.1f}" if mem_metrics and mem_metrics.get('avg', 0) > 0 else "N/A",
                        'Status': status
                    })

                    # Detailed entry with avg/max CPU and RAM
                    containers_usage_detailed.append({
                        'Container': container_name,
                        'Pod': top_cpu.get('pod_name', ''),
                        'Node': top_cpu.get('node_name', ''),
                        'Avg CPU %': round(cpu_metrics.get('avg', 0.0), 3),
                        'Max CPU %': round(cpu_metrics.get('max', 0.0), 3),
                        'Avg Mem MB': round(mem_metrics.get('avg', 0.0), 1) if mem_metrics else 0.0,
                        'Max Mem MB': round(mem_metrics.get('max', 0.0), 1) if mem_metrics else 0.0
                    })

        # Nodes usage detailed (per-node avg/max)
        nodes_usage = data.get('nodes_usage', {})
        nodes_usage_detailed: List[Dict[str, Any]] = []
        if nodes_usage:
            # Controlplane nodes (per node)
            cp = nodes_usage.get('controlplane_nodes', {})
            for node in (cp.get('individual_nodes', []) or []):
                metrics = node.get('metrics', {})
                avg_cpu = self._extract_stat(metrics, 'cpu_usage', ['avg', 'value', 'max'])
                max_cpu = self._extract_stat(metrics, 'cpu_usage', ['max', 'avg', 'value'])
                avg_mem = self._extract_stat(metrics, 'memory_usage', ['avg', 'value', 'max'])
                max_mem = self._extract_stat(metrics, 'memory_usage', ['max', 'avg', 'value'])
                nodes_usage_detailed.append({
                    'Node Group': 'Control Plane',
                    'Node Name': node.get('name') or node.get('instance', ''),
                    'Avg CPU %': round(avg_cpu, 2),
                    'Max CPU %': round(max_cpu, 2),
                    'Avg Mem MB': round(avg_mem, 1),
                    'Max Mem MB': round(max_mem, 1)
                })
            # Infra nodes (per node)
            infra = nodes_usage.get('infra_nodes', {})
            for node in (infra.get('individual_nodes', []) or []):
                metrics = node.get('metrics', {})
                avg_cpu = self._extract_stat(metrics, 'cpu_usage', ['avg', 'value', 'max'])
                max_cpu = self._extract_stat(metrics, 'cpu_usage', ['max', 'avg', 'value'])
                avg_mem = self._extract_stat(metrics, 'memory_usage', ['avg', 'value', 'max'])
                max_mem = self._extract_stat(metrics, 'memory_usage', ['max', 'avg', 'value'])
                nodes_usage_detailed.append({
                    'Node Group': 'Infra',
                    'Node Name': node.get('name') or node.get('instance', ''),
                    'Avg CPU %': round(avg_cpu, 2),
                    'Max CPU %': round(max_cpu, 2),
                    'Avg Mem MB': round(avg_mem, 1),
                    'Max Mem MB': round(max_mem, 1)
                })
            # Top5 worker nodes (per node)
            top5 = nodes_usage.get('top5_worker_nodes', {})
            for node in (top5.get('individual_nodes', []) or []):
                metrics = node.get('metrics', {})
                avg_cpu = self._extract_stat(metrics, 'cpu_usage', ['avg', 'value', 'max'])
                max_cpu = self._extract_stat(metrics, 'cpu_usage', ['max', 'avg', 'value'])
                avg_mem = self._extract_stat(metrics, 'memory_usage', ['avg', 'value', 'max'])
                max_mem = self._extract_stat(metrics, 'memory_usage', ['max', 'avg', 'value'])
                nodes_usage_detailed.append({
                    'Node Group': 'Top5 Workers',
                    'Node Name': node.get('name') or node.get('instance', ''),
                    'Avg CPU %': round(avg_cpu, 2),
                    'Max CPU %': round(max_cpu, 2),
                    'Avg Mem MB': round(avg_mem, 1),
                    'Max Mem MB': round(max_mem, 1)
                })

        return {
            'top_cpu_pods': top_cpu_pods,
            'container_usage': container_usage,
            'pods_usage_detailed': pods_usage_detailed,
            'containers_usage_detailed': containers_usage_detailed,
            'nodes_usage_detailed': nodes_usage_detailed
        }            

    def _extract_latency_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract latency analysis data"""
        latency_data = data.get('latency_metrics', {}).get('categories', {})
        top_latencies = []
        
        # Collect all latency metrics with values
        all_metrics = []
        for category, metrics in latency_data.items():
            for metric_name, metric_info in metrics.items():
                max_val = metric_info.get('max_value', 0)
                if max_val > 0:
                    all_metrics.append({
                        'category': category,
                        'metric_name': metric_name,
                        'max_value': max_val,
                        'component': metric_info.get('component', ''),
                        'unit': metric_info.get('unit', 'seconds')
                    })
        
        # Sort by max_value and take top 10
        all_metrics.sort(key=lambda x: x['max_value'], reverse=True)
        
        for idx, metric in enumerate(all_metrics[:10], 1):
            formatted_value = self.format_latency_value(metric['max_value'], metric['unit'])
            severity = self.categorize_latency_severity(metric['max_value'], metric['unit'])
            
            # Highlight critical latencies
            rank_display = f"⚠️ {idx}" if severity in ['critical', 'high'] and idx <= 3 else idx
            
            top_latencies.append({
                'Rank': rank_display,
                'Metric': self.truncate_metric_name(metric['metric_name'], 30),
                'Component': metric['component'].title(),
                'Latency': formatted_value,
                'Severity': severity.title()
            })

        # Category summaries
        category_summary = []
        for category, metrics in latency_data.items():
            if metrics:
                valid_values = [m.get('max_value', 0) for m in metrics.values() if m.get('max_value', 0) > 0]
                if not valid_values:
                    continue
                max_latency = max(valid_values)
                if max_latency > 0:
                    formatted = self.format_latency_value(max_latency, 'seconds')
                    severity = self.categorize_latency_severity(max_latency, 'seconds')
                    
                    category_summary.append({
                        'Category': category.replace('_', ' ').title(),
                        'Max Latency': formatted,
                        'Severity': severity.title()
                    })

        return {
            'top_latencies': top_latencies,
            'category_summary': category_summary
        }

    def _extract_performance_insights(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract performance insights"""
        perf_analysis = data.get('performance_analysis', {})
        perf_summary = perf_analysis.get('performance_summary', {})
        
        # Performance summary
        summary_data = []
        summary_data.append({
            'Metric': 'Overall Score',
            'Value': f"{perf_summary.get('overall_score', 0)}/100"
        })
        summary_data.append({
            'Metric': 'Performance Grade',
            'Value': perf_summary.get('performance_grade', 'D')
        })
        
        component_scores = perf_summary.get('component_scores', {})
        for component, score in component_scores.items():
            summary_data.append({
                'Metric': component.replace('_', ' ').title(),
                'Value': f"{score}/100"
            })

        # Key findings and recommendations
        findings = []
        key_findings = perf_analysis.get('key_findings', [])
        recommendations = perf_analysis.get('recommendations', [])
        
        for idx, finding in enumerate(key_findings[:5], 1):
            findings.append({
                'Type': 'Finding',
                'Description': finding
            })
        
        for idx, rec in enumerate(recommendations[:5], 1):
            findings.append({
                'Type': 'Recommendation',
                'Description': rec
            })

        return {
            'performance_summary': summary_data,
            'insights': findings
        }

    def _extract_node_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract node analysis data"""
        nodes_usage = data.get('nodes_usage', {})
        node_summary = []
        
        # Control plane nodes
        cp_nodes = nodes_usage.get('controlplane_nodes', {})
        cp_summary = cp_nodes.get('summary', {})
        if cp_summary:
            cpu_max = cp_summary.get('cpu_usage', {}).get('max', 0)
            mem_max = cp_summary.get('memory_usage', {}).get('max', 0)
            status = 'warning' if cpu_max > 70 else 'success'
            
            node_summary.append({
                'Node Type': 'Control Plane',
                'Count': cp_nodes.get('count', 0),
                'Max CPU %': f"{cpu_max:.1f}",
                'Max Memory MB': f"{mem_max:.0f}",
                'Status': status
            })

        # Worker nodes
        worker_nodes = nodes_usage.get('top5_worker_nodes', {})
        worker_summary = worker_nodes.get('summary', {})
        if worker_summary:
            cpu_max = worker_summary.get('cpu_usage', {}).get('max', 0)
            mem_max = worker_summary.get('memory_usage', {}).get('max', 0)
            status = 'danger' if cpu_max > 80 else 'warning' if cpu_max > 70 else 'success'
            
            highlight = "🔥 Worker" if cpu_max > 80 else "Worker"
            
            node_summary.append({
                'Node Type': highlight,
                'Count': worker_nodes.get('count', 0),
                'Max CPU %': f"{cpu_max:.1f}",
                'Max Memory MB': f"{mem_max:.0f}",
                'Status': status
            })

        # Individual worker nodes
        individual_workers = []
        worker_nodes_list = worker_nodes.get('individual_nodes', [])
        for node in worker_nodes_list[:5]:
            cpu_max = node.get('cpu_usage', {}).get('max', 0)
            rank = node.get('rank', 0)
            
            rank_display = f"🏆 {rank}" if rank == 1 else rank
            
            individual_workers.append({
                'Rank': rank_display,
                'Node': self.truncate_node_name(node.get('name', ''), 25),
                'CPU %': f"{cpu_max:.1f}",
                'Memory MB': f"{node.get('memory_usage', {}).get('max', 0):.0f}"
            })

        return {
            'node_summary': node_summary,
            'top_worker_nodes': individual_workers
        }

    def _extract_ovs_summary(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVS metrics summary"""
        ovs_data = data.get('ovs_metrics', {})
        
        # CPU usage summary
        cpu_summary = []
        cpu_usage = ovs_data.get('cpu_usage', {})
        
        vswitchd_top = cpu_usage.get('ovs_vswitchd_top5', [])
        for idx, node_data in enumerate(vswitchd_top[:3], 1):
            status = 'warning' if node_data.get('avg', 0) > 2 else 'success'
            rank_display = f"🔥 {idx}" if idx == 1 and node_data.get('avg', 0) > 2 else idx
            
            cpu_summary.append({
                'Rank': rank_display,
                'Component': 'OVS vSwitchd',
                'Node': self.truncate_node_name(node_data.get('node_name', ''), 20),
                'CPU %': f"{node_data.get('avg', 0):.2f}"
            })

        # Flow metrics
        flows_summary = []
        flows_data = ovs_data.get('flows_metrics', {})
        
        dp_flows = flows_data.get('dp_flows_top5', [])
        for idx, flow_data in enumerate(dp_flows[:3], 1):
            flows_summary.append({
                'Type': 'DP Flows',
                'Instance': flow_data.get('instance', ''),
                'Avg Flows': f"{flow_data.get('avg', 0):.0f}",
                'Max Flows': f"{flow_data.get('max', 0):.0f}"
            })

        return {
            'cpu_usage_summary': cpu_summary,
            'flows_summary': flows_summary
        }

    def summarize_deepdrive_data(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary for deep drive analysis data"""
        try:
            summary_parts = []
            
            # Basic cluster info
            metadata = structured_data.get('metadata', {})
            if metadata:
                analysis_type = metadata.get('analysis_type', 'unknown')
                duration = metadata.get('query_duration', 'unknown')
                components = metadata.get('components_analyzed', 0)
                summary_parts.append(f"Comprehensive {analysis_type.replace('_', ' ')} analysis over {duration} covering {components} components")
            
            # Top latency issues
            latency_analysis = structured_data.get('latency_analysis', {})
            top_latencies = latency_analysis.get('top_latencies', [])
            if top_latencies:
                top_latency = top_latencies[0]
                summary_parts.append(f"Highest latency: {top_latency.get('Latency', 'unknown')} ({top_latency.get('Metric', 'unknown')})")
            
            # Resource usage
            resource_usage = structured_data.get('resource_usage', {})
            top_cpu_pods = resource_usage.get('top_cpu_pods', [])
            if top_cpu_pods:
                top_pod = top_cpu_pods[0]
                summary_parts.append(f"Top CPU consuming pod: {top_pod.get('Pod', 'unknown')} ({top_pod.get('CPU %', '0')}%)")
            
            return " • ".join(summary_parts) if summary_parts else "Deep drive analysis completed with limited data available"

            # Node analysis
            node_analysis = structured_data.get('node_analysis', {})
            top_workers = node_analysis.get('top_worker_nodes', [])
            if top_workers:
                top_worker = top_workers[0]
                cpu_usage = top_worker.get('CPU %', '0')
                summary_parts.append(f"Highest worker CPU usage: {cpu_usage}% on {top_worker.get('Node', 'unknown')}")
            
            # Performance insights
            perf_insights = structured_data.get('performance_insights', {})
            perf_summary = perf_insights.get('performance_summary', [])
            
            overall_score = None
            grade = None
            for item in perf_summary:
                if item.get('Metric') == 'Overall Score':
                    overall_score = item.get('Value', '0/100')
                elif item.get('Metric') == 'Performance Grade':
                    grade = item.get('Value', 'D')
            
            if overall_score and grade:
                summary_parts.append(f"Overall performance score: {overall_score} (Grade: {grade})")
                        
        except Exception as e:
            logger.error(f"Failed to generate deep drive summary: {e}")
            return f"Deep drive analysis summary generation failed: {str(e)}"

    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data to DataFrames"""
        try:
            dataframes = {}
            
            # Analysis metadata
            metadata = structured_data.get('metadata', {})
            if metadata:
                metadata_list = []
                for key, value in metadata.items():
                    if value:  # Only include non-empty values
                        metadata_list.append({
                            'Property': key.replace('_', ' ').title(),
                            'Value': str(value)
                        })
                if metadata_list:
                    df = pd.DataFrame(metadata_list)
                    dataframes['analysis_metadata'] = self.limit_dataframe_columns(df, 2, 'analysis_metadata')
            
            # Basic info tables
            basic_info = structured_data.get('basic_info', {})
            
            # Pod status
            pod_status = basic_info.get('pod_status', [])
            if pod_status:
                df = pd.DataFrame(pod_status)
                dataframes['cluster_overview'] = self.limit_dataframe_columns(df, 2, 'cluster_overview')
            
            # OVN DB Size (renamed from Database sizes)
            db_sizes = basic_info.get('database_sizes', [])
            if db_sizes:
                df = pd.DataFrame(db_sizes)
                # Keep all three columns: Database, Max DB Size, Status
                dataframes['ovn_db_size'] = self.limit_dataframe_columns(df, 3, 'ovn_db_size')
            
            # Alerts
            alerts = basic_info.get('alerts', [])
            if alerts:
                df = pd.DataFrame(alerts)
                dataframes['alerts'] = self.limit_dataframe_columns(df, 4, 'alerts')
            
            # Performance insights
            perf_insights = structured_data.get('performance_insights', {})
            
            # Performance summary
            perf_summary = perf_insights.get('performance_summary', [])
            if perf_summary:
                df = pd.DataFrame(perf_summary)
                dataframes['performance_summary'] = self.limit_dataframe_columns(df, 2, 'performance_summary')
            
            # Insights and recommendations
            insights = perf_insights.get('insights', [])
            if insights:
                df = pd.DataFrame(insights)
                dataframes['insights'] = self.limit_dataframe_columns(df, 2, 'insights')
            
            # Resource usage
            resource_usage = structured_data.get('resource_usage', {})
            
            # Removed Top CPU Pods table (redundant with detailed pods/containers tables)
            
            # Detailed pods usage table: avg/max CPU and RAM
            pods_usage_detailed = resource_usage.get('pods_usage_detailed', [])
            if pods_usage_detailed:
                df = pd.DataFrame(pods_usage_detailed)
                dataframes['pods_usage_detailed'] = df

            # Detailed containers usage table
            containers_usage_detailed = resource_usage.get('containers_usage_detailed', [])
            if containers_usage_detailed:
                df = pd.DataFrame(containers_usage_detailed)
                dataframes['containers_usage_detailed'] = df

            # Nodes usage detailed
            nodes_usage_detailed = resource_usage.get('nodes_usage_detailed', [])
            if nodes_usage_detailed:
                df = pd.DataFrame(nodes_usage_detailed)
                dataframes['nodes_usage_detailed'] = df
            
            # Latency analysis
            latency_analysis = structured_data.get('latency_analysis', {})
            
            # Top latencies
            top_latencies = latency_analysis.get('top_latencies', [])
            if top_latencies:
                df = pd.DataFrame(top_latencies)
                dataframes['top_latencies'] = df  # Don't limit columns for full metric visibility
            
            # Category summary
            category_summary = latency_analysis.get('category_summary', [])
            if category_summary:
                df = pd.DataFrame(category_summary)
                dataframes['latency_categories'] = self.limit_dataframe_columns(df, 3, 'latency_categories')
            
            # Node analysis
            node_analysis = structured_data.get('node_analysis', {})
            
            # Node summary
            node_summary = node_analysis.get('node_summary', [])
            if node_summary:
                df = pd.DataFrame(node_summary)
                dataframes['node_summary'] = self.limit_dataframe_columns(df, 5, 'node_summary')
            
            # Remove 'Top Worker Nodes' table (duplicate with detailed per-node table)
            
            # OVS metrics
            ovs_metrics = structured_data.get('ovs_metrics', {})
            
            # CPU usage summary
            cpu_summary = ovs_metrics.get('cpu_usage_summary', [])
            if cpu_summary:
                df = pd.DataFrame(cpu_summary)
                dataframes['ovs_cpu_usage'] = self.limit_dataframe_columns(df, 4, 'ovs_cpu_usage')
            
            # Flows summary
            flows_summary = ovs_metrics.get('flows_summary', [])
            if flows_summary:
                df = pd.DataFrame(flows_summary)
                dataframes['ovs_flows'] = self.limit_dataframe_columns(df, 4, 'ovs_flows')
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform deep drive data to DataFrames: {e}")
            return {}

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables with enhanced styling for deep drive analysis"""
        try:
            html_tables = {}
            
            # Define table priorities and styling
            table_priorities = {
                'analysis_metadata': 0,      # Move Analysis Metadata to the top
                'cluster_overview': 1,       # Then Cluster Overview
                'node_summary': 2,           # Node Summary above detailed
                'nodes_usage_detailed': 3,   # Then Nodes Usage Detailed
                'ovn_db_size': 4,            # OVN DB Size
                'pods_usage_detailed': 5,    # Pods Usage Detailed under OVN DB Size
                'containers_usage_detailed': 6, # Containers Usage Detailed under OVN DB Size
                'latency_categories': 7,
                'top_latencies': 8,
                'alerts': 10,
                'performance_summary': 999   # Bottom
            }
            
            # Sort tables by priority
            sorted_tables = sorted(
                dataframes.items(),
                key=lambda x: table_priorities.get(x[0], 999)
            )
            
            for table_name, df in sorted_tables:
                if df.empty:
                    continue
                
                # Add status-based styling
                styled_df = df.copy()
                
                # Apply highlighting for critical metrics and top rankings
                if table_name == 'top_latencies':
                    # Highlight critical latencies
                    for idx, row in styled_df.iterrows():
                        if 'Critical' in str(row.get('Severity', '')):
                            styled_df.at[idx, 'Severity'] = f'<span class="badge badge-danger">{row.get("Severity", "")}</span>'
                        elif 'High' in str(row.get('Severity', '')):
                            styled_df.at[idx, 'Severity'] = f'<span class="badge badge-warning">{row.get("Severity", "")}</span>'
                        elif 'Medium' in str(row.get('Severity', '')):
                            styled_df.at[idx, 'Severity'] = f'<span class="badge badge-info">{row.get("Severity", "")}</span>'
                        else:
                            styled_df.at[idx, 'Severity'] = f'<span class="badge badge-success">{row.get("Severity", "")}</span>'
                
                elif table_name == 'alerts':
                    # Style alert severity
                    for idx, row in styled_df.iterrows():
                        severity = str(row.get('Severity', ''))
                        if 'CRITICAL' in severity:
                            styled_df.at[idx, 'Severity'] = f'<span class="badge badge-danger">{severity}</span>'
                        elif 'WARNING' in severity:
                            styled_df.at[idx, 'Severity'] = f'<span class="badge badge-warning">{severity}</span>'
                        else:
                            styled_df.at[idx, 'Severity'] = f'<span class="badge badge-info">{severity}</span>'
                
                elif table_name == 'node_summary':
                    # Style node status
                    for idx, row in styled_df.iterrows():
                        status = str(row.get('Status', ''))
                        if status == 'danger':
                            styled_df.at[idx, 'Status'] = f'<span class="badge badge-danger">High Load</span>'
                        elif status == 'warning':
                            styled_df.at[idx, 'Status'] = f'<span class="badge badge-warning">Medium Load</span>'
                        else:
                            styled_df.at[idx, 'Status'] = f'<span class="badge badge-success">Normal</span>'
                
                # Generate HTML table
                html_table = self.create_html_table(styled_df, table_name)
                
                # Add custom styling for important tables
                if table_name in ['performance_summary', 'top_latencies', 'alerts']:
                    html_table = f'<div class="border border-primary rounded p-2 mb-3">{html_table}</div>'
                
                html_tables[table_name] = html_table
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate deep drive HTML tables: {e}")
            return {}

