#!/usr/bin/env python3
"""
OVNK Benchmark Prometheus Node Usage Collector
Collects CPU, RAM, and network usage metrics for nodes grouped by role
"""

import asyncio
import json
import yaml
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import sys

# Import dependencies
from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError
from tools.ovnk_benchmark_prometheus_utility import mcpToolsUtility
from ocauth.ovnk_benchmark_auth import OpenShiftAuth


class nodeUsageCollector:
    """Collect node usage metrics from Prometheus with role-based grouping"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery, auth_client: Optional[OpenShiftAuth] = None):
        self.prometheus_client = prometheus_client
        self.auth_client = auth_client
        self.utility = mcpToolsUtility(auth_client)
        
        # Default PromQL queries (hardcoded fallback)
        self.default_queries = {
            'cpu_usage': '100 - (avg by (instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)',
            'memory_usage': '(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / 1024 / 1024',
            'network_rx': 'sum by (instance) (rate(node_network_receive_bytes_total{device!~"lo|veth.*|docker.*|virbr.*|cni.*|flannel.*|br.*|vxlan.*|genev.*|tun.*|tap.*|ovs.*"}[5m]))',
            'network_tx': 'sum by (instance) (rate(node_network_transmit_bytes_total{device!~"lo|veth.*|docker.*|virbr.*|cni.*|flannel.*|br.*|vxlan.*|genev.*|tun.*|tap.*|ovs.*"}[5m]))'
        }
        
        # Load queries from metrics.yaml or use defaults
        self.queries = self.load_metrics_config()
        
        # Metric units
        self.units = {
            'cpu_usage': '%',
            'memory_usage': 'MB', 
            'network_rx': 'bytes/s',
            'network_tx': 'bytes/s'
        }
    
    def load_metrics_config(self) -> Dict[str, str]:
        """Load metrics from metrics.yaml or use hardcoded defaults"""
        metrics_file = 'metrics.yaml'
        
        if os.path.exists(metrics_file):
            try:
                with open(metrics_file, 'r') as f:
                    config = yaml.safe_load(f)
                    
                if isinstance(config, dict) and 'metrics' in config:
                    metrics = config['metrics']
                    queries = {}
                    
                    # Extract relevant queries
                    for metric_name in ['cpu_usage', 'memory_usage', 'network_rx', 'network_tx']:
                        if metric_name in metrics:
                            queries[metric_name] = metrics[metric_name].get('query', self.default_queries[metric_name])
                        else:
                            queries[metric_name] = self.default_queries[metric_name]
                    
                    return queries
            except Exception:
                pass
        
        # Return defaults if config loading fails
        return self.default_queries
    
    async def get_instance_node_mapping(self) -> Dict[str, str]:
        """Get mapping from Prometheus instance to node name"""
        try:
            result = await self.prometheus_client.query_instant('up{job="node-exporter"}')
            instance_to_node = {}
            
            if 'result' in result:
                for item in result['result']:
                    if 'metric' in item:
                        instance = item['metric'].get('instance', '')
                        node_name = (item['metric'].get('node') or 
                                   item['metric'].get('nodename') or 
                                   item['metric'].get('kubernetes_node') or
                                   instance.split(':')[0])
                        
                        if instance and node_name:
                            instance_to_node[instance] = node_name
            
            return instance_to_node
        except Exception:
            return {}
    
    async def query_metric_stats(self, metric_query: str, start_time: str, end_time: str) -> Dict[str, Dict[str, float]]:
        """Query metric and calculate min/avg/max for each instance"""
        try:
            result = await self.prometheus_client.query_range(metric_query, start_time, end_time, '15s')
            stats = {}
            
            if 'result' in result:
                for item in result['result']:
                    instance = item.get('metric', {}).get('instance', '')
                    if not instance:
                        continue
                    
                    # Extract values and calculate statistics
                    if 'values' in item and item['values']:
                        values = []
                        for timestamp, value in item['values']:
                            try:
                                val = float(value)
                                if not (val != val):  # Check for NaN
                                    values.append(val)
                            except (ValueError, TypeError):
                                continue
                        
                        if values:
                            stats[instance] = {
                                'min': round(min(values), 2),
                                'avg': round(sum(values) / len(values), 2),
                                'max': round(max(values), 2)
                            }
            
            return stats
        except Exception:
            return {}
    
    async def debug_role_detection(self) -> Dict[str, Any]:
        """Debug method to check role detection"""
        debug_info = {
            'node_labels_sources': {},
            'role_assignments': {},
            'label_samples': {}
        }
        
        # Try each source
        if self.auth_client:
            k8s_labels = await self.utility.get_node_labels_from_kubernetes()
            debug_info['node_labels_sources']['kubernetes_api'] = len(k8s_labels)
            debug_info['label_samples']['kubernetes_api'] = {
                node: labels for node, labels in list(k8s_labels.items())[:2]
            }
        
        oc_labels = await self.utility.get_node_labels_via_oc()
        debug_info['node_labels_sources']['oc_cli'] = len(oc_labels)
        debug_info['label_samples']['oc_cli'] = {
            node: labels for node, labels in list(oc_labels.items())[:2]
        }
        
        prom_labels = await self.utility.get_node_labels_from_prometheus(self.prometheus_client)
        debug_info['node_labels_sources']['prometheus'] = len(prom_labels)
        debug_info['label_samples']['prometheus'] = {
            node: labels for node, labels in list(prom_labels.items())[:2]
        }
        
        # Get final labels and role assignments
        final_labels = await self.utility.get_all_node_labels(self.prometheus_client)
        debug_info['final_node_count'] = len(final_labels)
        
        for node_name, labels in final_labels.items():
            role = self.utility.determine_node_role(node_name, labels)
            debug_info['role_assignments'][node_name] = {
                'role': role,
                'role_labels': {k: v for k, v in labels.items() if 'role' in k.lower()},
                'has_controlplane_label': any('control-plane' in k for k in labels.keys()),
                'has_master_label': any('master' in k for k in labels.keys()),
                'has_worker_label': any('worker' in k for k in labels.keys())
            }
        
        return debug_info

    async def collect_usage_data(self, duration: str = '1h', end_time: Optional[str] = None, debug: bool = False) -> Dict[str, Any]:
        """Main method to collect node usage data"""
        
        # Get time range
        start_time, end_time = self.prometheus_client.get_time_range_from_duration(duration, end_time)
        
        # Get node information and mappings - pass prometheus_client for fallback
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        instance_mapping = await self.get_instance_node_mapping()
        
        # Query all metrics
        all_metrics = {}
        for metric_name, query in self.queries.items():
            all_metrics[metric_name] = await self.query_metric_stats(query, start_time, end_time)
        
        # Initialize result structure
        result = {
            'metadata': {
                'query_time': datetime.now(timezone.utc).isoformat(),
                'duration': duration,
                'start_time': start_time,
                'end_time': end_time,
                'timezone': 'UTC'
            },
            'groups': {
                'controlplane': {'nodes': [], 'summary': {}},
                'worker': {'nodes': [], 'summary': {}},
                'infra': {'nodes': [], 'summary': {}},
                'workload': {'nodes': [], 'summary': {}}
            },
            'top_usage': {
                'cpu': [],
                'memory': []
            }
        }
        
        # Collect all instances with metrics
        all_instances = set()
        for metric_data in all_metrics.values():
            all_instances.update(metric_data.keys())
        
        # Process each instance
        node_usage_data = []
        for instance in all_instances:
            node_name = instance_mapping.get(instance, instance.split(':')[0])
            
            # Find node role from groups using improved matching
            node_role = 'worker'  # default
            for role, nodes in node_groups.items():
                for node in nodes:
                    # Check exact match or short name match
                    if (node['name'] == node_name or 
                        node['name'].startswith(node_name + '.') or
                        node_name.startswith(node['name'] + '.')):
                        node_role = role
                        break
                if node_role != 'worker':
                    break
            
            # If still not found, use utility's role detection directly
            if node_role == 'worker':
                # Get labels for this specific node
                all_node_labels = await self.utility.get_all_node_labels(self.prometheus_client)
                node_labels = all_node_labels.get(node_name, {})
                if not node_labels:
                    # Try short name
                    for full_name, labels in all_node_labels.items():
                        if full_name.startswith(node_name + '.') or node_name.startswith(full_name):
                            node_labels = labels
                            break
                node_role = self.utility.determine_node_role(node_name, node_labels, instance)
            
            # Build node data
            node_data = {
                'instance': instance,
                'name': node_name,
                'role': node_role,
                'metrics': {}
            }
            
            # Add metrics
            for metric_name in self.queries.keys():
                if instance in all_metrics[metric_name]:
                    stats = all_metrics[metric_name][instance].copy()
                    stats['unit'] = self.units[metric_name]
                    node_data['metrics'][metric_name] = stats
                else:
                    node_data['metrics'][metric_name] = {
                        'min': None, 'avg': None, 'max': None,
                        'unit': self.units[metric_name]
                    }
            
            node_usage_data.append(node_data)
            result['groups'][node_role]['nodes'].append(node_data)
        
        # Calculate group summaries
        for role, group in result['groups'].items():
            if group['nodes']:
                group['summary'] = self.calculate_group_summary(group['nodes'])
            group['count'] = len(group['nodes'])
        
        # Get top 5 worker nodes for CPU and memory
        worker_nodes = [node for node in node_usage_data if node['role'] == 'worker']
        
        # Top 5 CPU usage (by max value)
        cpu_sorted = sorted(
            [node for node in worker_nodes if node['metrics']['cpu_usage']['max'] is not None],
            key=lambda x: x['metrics']['cpu_usage']['max'],
            reverse=True
        )[:5]
        
        result['top_usage']['cpu'] = [
            {
                'name': node['name'],
                'instance': node['instance'], 
                'cpu_max': node['metrics']['cpu_usage']['max'],
                'cpu_avg': node['metrics']['cpu_usage']['avg']
            }
            for node in cpu_sorted
        ]
        
        # Top 5 memory usage (by max value)
        memory_sorted = sorted(
            [node for node in worker_nodes if node['metrics']['memory_usage']['max'] is not None],
            key=lambda x: x['metrics']['memory_usage']['max'],
            reverse=True
        )[:5]
        
        result['top_usage']['memory'] = [
            {
                'name': node['name'],
                'instance': node['instance'],
                'memory_max': node['metrics']['memory_usage']['max'],
                'memory_avg': node['metrics']['memory_usage']['avg']
            }
            for node in memory_sorted
        ]
        
        # Add debug information if requested
        if debug:
            debug_data = await self.debug_role_detection()
            result['debug'] = debug_data
        
        return result
    
    def calculate_group_summary(self, nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate summary statistics for a group of nodes"""
        summary = {}
        
        for metric_name in self.queries.keys():
            summary[metric_name] = {}
            
            # Collect values for each statistic
            for stat in ['min', 'avg', 'max']:
                values = []
                for node in nodes:
                    if metric_name in node['metrics']:
                        value = node['metrics'][metric_name].get(stat)
                        if value is not None:
                            values.append(value)
                
                # Calculate group statistic
                if values:
                    if stat == 'min':
                        summary[metric_name][stat] = round(min(values), 2)
                    elif stat == 'max':
                        summary[metric_name][stat] = round(max(values), 2)
                    else:  # avg
                        summary[metric_name][stat] = round(sum(values) / len(values), 2)
                else:
                    summary[metric_name][stat] = None
            
            summary[metric_name]['unit'] = self.units[metric_name]
        
        return summary


async def main():
    """Main function for testing"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect node usage metrics')
    parser.add_argument('--prometheus-url', help='Prometheus server URL')
    parser.add_argument('--token', help='Bearer token for authentication')
    parser.add_argument('--kubeconfig', help='Path to kubeconfig file') 
    parser.add_argument('--duration', default='1h', help='Duration to query')
    parser.add_argument('--end-time', help='End time (ISO format)')
    parser.add_argument('--debug', action='store_true', help='Enable debug output for role detection')
    parser.add_argument('--pretty', action='store_true', help='Pretty print JSON')
    
    args = parser.parse_args()
    
    # Initialize authentication
    auth_client = None
    try:
        auth_client = OpenShiftAuth(kubeconfig_path=args.kubeconfig)
        await auth_client.initialize()
    except Exception:
        pass
    
    # Determine Prometheus configuration
    prometheus_url = args.prometheus_url or (auth_client.prometheus_url if auth_client else None)
    token = args.token or (auth_client.prometheus_token if auth_client else None)
    
    if not prometheus_url:
        print("Error: Prometheus URL required", file=sys.stderr)
        return 1
    
    # Create collector and run
    prometheus_client = PrometheusBaseQuery(prometheus_url, token)
    
    try:
        async with prometheus_client:
            if not await prometheus_client.test_connection():
                print("Error: Cannot connect to Prometheus", file=sys.stderr)
                return 1
            
            collector = nodeUsageCollector(prometheus_client, auth_client)
            result = await collector.collect_usage_data(args.duration, args.end_time, args.debug)
            
            print(json.dumps(result, indent=2 if args.pretty else None))
            return 0
    
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    exit_code = asyncio.run(main())
    sys.exit(exit_code)