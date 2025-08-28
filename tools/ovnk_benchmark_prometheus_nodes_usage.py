#!/usr/bin/env python3
"""
OVNK Benchmark Prometheus Node Usage Query
Queries CPU, RAM, and network usage metrics for nodes grouped by role (master/infra/worker)
Uses standard Kubernetes node role labels for role detection
"""

import asyncio
import json
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import sys
import re
import subprocess

# Import the base query class and authentication
from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError
from ocauth.ovnk_benchmark_auth import OpenShiftAuth
from kubernetes import client
from kubernetes.client.rest import ApiException


class NodeUsageQuery:
    """Query node usage metrics from Prometheus with node role detection"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery, auth_client: Optional[OpenShiftAuth] = None):
        self.prometheus_client = prometheus_client
        self.auth_client = auth_client
        
        # Base metric queries without role grouping
        self.base_queries = {
            'cpu_usage': '100 - (avg by (instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)',
            'memory_usage': '(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / 1024 / 1024',
            'network_rx': 'sum by (instance) (rate(node_network_receive_bytes_total{device!~"lo|veth.*|docker.*|virbr.*|cni.*|flannel.*|br.*|vxlan.*|genev.*|tun.*|tap.*|ovs.*"}[5m]))',
            'network_tx': 'sum by (instance) (rate(node_network_transmit_bytes_total{device!~"lo|veth.*|docker.*|virbr.*|cni.*|flannel.*|br.*|vxlan.*|genev.*|tun.*|tap.*|ovs.*"}[5m]))'
        }
        
        # Metric units
        self.units = {
            'cpu_usage': '%',
            'memory_usage': 'MB',
            'network_rx': 'bytes/s',
            'network_tx': 'bytes/s'
        }
        
        # Standard Kubernetes node role labels in order of priority
        self.role_labels = [
            'node-role.kubernetes.io/control-plane',
            'node-role.kubernetes.io/master', 
            'node-role.kubernetes.io/infra',
            'node-role.kubernetes.io/worker'
        ]
    
    async def get_node_labels_from_kubernetes(self) -> Dict[str, Dict[str, str]]:
        """Get node labels directly from Kubernetes API"""
        if not self.auth_client or not self.auth_client.kube_client:
            return {}
        
        try:
            v1 = client.CoreV1Api(self.auth_client.kube_client)
            nodes = v1.list_node()
            
            labels_map = {}
            for node in nodes.items:
                node_name = node.metadata.name
                labels = node.metadata.labels or {}
                
                # Store labels for this node and potential name variations
                candidates = [node_name]
                if '.' in node_name:
                    candidates.append(node_name.split('.')[0])
                
                for key in candidates:
                    if key not in labels_map:
                        labels_map[key] = labels
            
            return labels_map
            
        except Exception as e:
            print(f"Warning: Could not fetch node labels from Kubernetes API: {e}", file=sys.stderr)
            return {}

    async def get_node_labels_via_oc(self) -> Dict[str, Dict[str, str]]:
        """Get node labels using oc CLI as a fallback."""
        try:
            completed = subprocess.run(
                ["oc", "get", "nodes", "-o", "json"],
                check=True,
                capture_output=True,
                text=True
            )
            data = json.loads(completed.stdout)
            items = data.get("items", []) if isinstance(data, dict) else []
            labels_map: Dict[str, Dict[str, str]] = {}
            for node in items:
                metadata = node.get("metadata", {})
                node_name = metadata.get("name", "")
                labels = metadata.get("labels", {}) or {}
                candidates = [node_name]
                if '.' in node_name:
                    candidates.append(node_name.split('.')[0])
                for key in candidates:
                    if key and key not in labels_map:
                        labels_map[key] = labels
            if labels_map:
                print(f"Retrieved node labels via oc for {len(labels_map)} nodes", file=sys.stderr)
            return labels_map
        except Exception as e:
            print(f"Warning: Could not fetch node labels via oc CLI: {e}", file=sys.stderr)
            return {}
    
    async def get_node_labels(self) -> Dict[str, Dict[str, str]]:
        """Get node labels from Kubernetes API first, fallback to Prometheus metric"""
        # Try Kubernetes API first if available
        if self.auth_client:
            k8s_labels = await self.get_node_labels_from_kubernetes()
            if k8s_labels:
                print(f"Retrieved node labels from Kubernetes API for {len(k8s_labels)} nodes", file=sys.stderr)
                return k8s_labels
        
        # Try oc CLI as a secondary fallback
        oc_labels = await self.get_node_labels_via_oc()
        if oc_labels:
            return oc_labels
        
        # Fallback to Prometheus metric
        print("Falling back to Prometheus kube_node_labels metric", file=sys.stderr)
        try:
            result = await self.prometheus_client.query_instant('kube_node_labels')
            labels_map: Dict[str, Dict[str, str]] = {}
            
            if 'result' in result:
                for item in result['result']:
                    if 'metric' not in item:
                        continue
                    metric_labels = item['metric']
                    node_name = metric_labels.get('node') or metric_labels.get('kubernetes_node') or metric_labels.get('nodename')
                    instance = metric_labels.get('instance', '')
                    host = instance.split(':')[0] if instance else ''
                    
                    # Keep all labels except __name__
                    labels = {k: v for k, v in metric_labels.items() if k != '__name__'}
                    
                    candidates: List[str] = []
                    if node_name:
                        candidates.append(node_name)
                        if '.' in node_name:
                            candidates.append(node_name.split('.')[0])
                    if host:
                        candidates.append(host)
                        if '.' in host:
                            candidates.append(host.split('.')[0])
                    
                    for key in candidates:
                        if key and key not in labels_map:
                            labels_map[key] = labels
            
            return labels_map
        except Exception as e:
            print(f"Warning: Could not fetch node labels: {e}", file=sys.stderr)
            return {}
    
    async def get_node_instance_mapping(self) -> Dict[str, str]:
        """Get mapping from instance to node name"""
        try:
            result = await self.prometheus_client.query_instant('up{job="node-exporter"}')
            instance_to_node = {}
            
            if 'result' in result:
                for item in result['result']:
                    if 'metric' in item:
                        instance = item['metric'].get('instance', '')
                        # Try to get node name from various possible labels
                        node_name = (item['metric'].get('node') or 
                                   item['metric'].get('nodename') or 
                                   item['metric'].get('kubernetes_node') or
                                   instance.split(':')[0])  # fallback to instance without port
                        
                        if instance and node_name:
                            instance_to_node[instance] = node_name
            
            return instance_to_node
        except Exception as e:
            print(f"Warning: Could not get instance mapping: {e}", file=sys.stderr)
            return {}
    
    def determine_node_role(self, node_name: str, labels: Dict[str, str], instance: str) -> str:
        """Determine node role from standard Kubernetes node role labels"""
        
        # Check for standard Kubernetes node role labels in priority order
        for role_label in self.role_labels:
            if role_label in labels:
                # Extract role from the label name
                if 'control-plane' in role_label:
                    return 'master'
                elif 'master' in role_label:
                    return 'master'
                elif 'infra' in role_label:
                    return 'infra'
                elif 'worker' in role_label:
                    return 'worker'
        
        # Fallback: Check other node labels for role indicators
        for label_key, label_value in labels.items():
            label_key_lower = label_key.lower()
            label_value_lower = label_value.lower()
            
            # Check label keys
            if 'master' in label_key_lower or 'control-plane' in label_key_lower or 'controlplane' in label_key_lower:
                return 'master'
            elif 'infra' in label_key_lower:
                return 'infra'  
            elif 'worker' in label_key_lower:
                return 'worker'
            
            # Check label values
            if 'master' in label_value_lower or 'control' in label_value_lower:
                return 'master'
            elif 'infra' in label_value_lower:
                return 'infra'
            elif 'worker' in label_value_lower:
                return 'worker'
        
        # Final fallback to name pattern matching
        name_lower = node_name.lower()
        instance_lower = instance.lower().split(':')[0]
        
        # Check node name patterns
        if any(pattern in name_lower for pattern in ['master', 'control', 'cp']):
            return 'master'
        elif any(pattern in name_lower for pattern in ['infra', 'infrastructure']):
            return 'infra'
        elif any(pattern in name_lower for pattern in ['worker', 'node', 'compute']):
            return 'worker'
        
        # Check instance patterns
        if any(pattern in instance_lower for pattern in ['master', 'control', 'cp']):
            return 'master'
        elif any(pattern in instance_lower for pattern in ['infra', 'infrastructure']):
            return 'infra'
        elif any(pattern in instance_lower for pattern in ['worker', 'node', 'compute']):
            return 'worker'
        
        return 'unknown'
    
    async def query_metric_stats(self, metric_query: str, start_time: str, end_time: str) -> Dict[str, Dict[str, float]]:
        """Query a metric and calculate min/avg/max for each instance"""
        try:
            result = await self.prometheus_client.query_range(metric_query, start_time, end_time, '15s')
            stats = {}
            
            if 'result' in result:
                for item in result['result']:
                    # In case query is not aggregated, aggregate per instance here
                    instance = item.get('metric', {}).get('instance', 'unknown')
                    if not instance and 'device' in item.get('metric', {}):
                        # Skip per-device series without instance label
                        continue
                    if instance == 'unknown':
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
                        # Defensive: if values all zeros and per-device series slipped through, continue
                        if not values:
                            continue
                        if values:
                            stats[instance] = {
                                'min': round(min(values), 2),
                                'avg': round(sum(values) / len(values), 2),
                                'max': round(max(values), 2)
                            }
            
            return stats
        except Exception as e:
            print(f"Warning: Failed to query metric {metric_query}: {e}", file=sys.stderr)
            return {}
    
    async def query_node_usage(self, duration: str, end_time: Optional[str] = None, debug: bool = False) -> Dict[str, Any]:
        """Main method to query node usage metrics"""
        
        # Get time range
        start_time, end_time = self.prometheus_client.get_time_range_from_duration(duration, end_time)
        
        # Get node information
        print("Getting node labels and mappings...", file=sys.stderr)
        node_labels = await self.get_node_labels()
        instance_mapping = await self.get_node_instance_mapping()
        
        # Determine label source for metadata
        label_source = "kubernetes_api" if (self.auth_client and node_labels) else "prometheus_metric"
        
        # Initialize result structure
        result = {
            'metadata': {
                'query_time': datetime.now(timezone.utc).isoformat(),
                'duration': duration,
                'start_time': start_time,
                'end_time': end_time,
                'timezone': 'UTC',
                'role_detection_method': 'kubernetes_node_role_labels',
                'label_source': label_source,
                'standard_role_labels': self.role_labels,
                'units': {
                    'cpu_usage': 'percentage (%)',
                    'memory_usage': 'megabytes (MB)',
                    'network_rx': 'bytes per second (bytes/s)',
                    'network_tx': 'bytes per second (bytes/s)'
                }
            },
            'node_groups': {
                'master': {'nodes': [], 'summary': {}},
                'infra': {'nodes': [], 'summary': {}},
                'worker': {'nodes': [], 'summary': {}},
                'unknown': {'nodes': [], 'summary': {}}
            }
        }
        
        # Query all metrics
        print("Querying metrics...", file=sys.stderr)
        all_metrics = {}
        for metric_name, query in self.base_queries.items():
            print(f"  Querying {metric_name}...", file=sys.stderr)
            all_metrics[metric_name] = await self.query_metric_stats(query, start_time, end_time)
        
        # Process results by instance
        all_instances = set()
        for metric_data in all_metrics.values():
            all_instances.update(metric_data.keys())
        
        print(f"Processing {len(all_instances)} instances...", file=sys.stderr)
        
        for instance in all_instances:
            # Get node name and role
            node_name = instance_mapping.get(instance, instance.split(':')[0])
            labels = node_labels.get(node_name, {})
            role = self.determine_node_role(node_name, labels, instance)
            
            # Build node data
            node_data = {
                'instance': instance,
                'node_name': node_name,
                'node_role': role,
                'kubernetes_labels': {
                    label: value for label, value in labels.items()
                    if label.startswith('node-role.kubernetes.io/')
                },
                'metrics': {}
            }
            
            # Add metrics with units
            for metric_name in self.base_queries.keys():
                if instance in all_metrics[metric_name]:
                    stats = all_metrics[metric_name][instance]
                    stats['unit'] = self.units[metric_name]
                    node_data['metrics'][metric_name] = stats
                else:
                    node_data['metrics'][metric_name] = {
                        'min': None, 'avg': None, 'max': None, 
                        'unit': self.units[metric_name]
                    }
            
            # Add to appropriate group
            result['node_groups'][role]['nodes'].append(node_data)
        
        # Calculate group summaries
        print("Calculating summaries...", file=sys.stderr)
        for role, group in result['node_groups'].items():
            if not group['nodes']:
                continue
                
            group['node_count'] = len(group['nodes'])
            group['summary'] = self.calculate_group_summary(group['nodes'])
        
        # Add debug info
        if debug:
            role_label_usage = {}
            for role, group in result['node_groups'].items():
                role_label_usage[role] = {}
                for node in group['nodes']:
                    for label in self.role_labels:
                        if label in node['kubernetes_labels']:
                            role_label_usage[role][label] = role_label_usage[role].get(label, 0) + 1
            
            result['debug'] = {
                'total_nodes': len(all_instances),
                'node_labels_found': len(node_labels),
                'instance_mappings': len(instance_mapping),
                'nodes_by_role': {
                    role: len(group['nodes']) 
                    for role, group in result['node_groups'].items()
                },
                'role_label_usage': role_label_usage,
                'sample_instances': list(all_instances)[:5],
                'sample_node_labels': {
                    node: labels for node, labels in list(node_labels.items())[:3]
                }
            }
        
        return result
    
    def calculate_group_summary(self, nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate summary statistics for a group of nodes"""
        summary = {}
        
        for metric_name in self.base_queries.keys():
            summary[metric_name] = {'min': [], 'avg': [], 'max': []}
            
            # Collect all values
            for node in nodes:
                if metric_name in node['metrics']:
                    for stat in ['min', 'avg', 'max']:
                        value = node['metrics'][metric_name].get(stat)
                        if value is not None:
                            summary[metric_name][stat].append(value)
            
            # Calculate group statistics
            for stat in ['min', 'avg', 'max']:
                values = summary[metric_name][stat]
                if values:
                    if stat == 'min':
                        summary[metric_name][stat] = round(min(values), 2)
                    elif stat == 'max':
                        summary[metric_name][stat] = round(max(values), 2)
                    else:  # avg
                        summary[metric_name][stat] = round(sum(values) / len(values), 2)
                else:
                    summary[metric_name][stat] = None
            
            # Add unit
            summary[metric_name]['unit'] = self.units[metric_name]
        
        return summary
    
    async def get_available_metrics(self) -> List[str]:
        """Get list of available node exporter metrics"""
        try:
            metadata = await self.prometheus_client.get_metrics_metadata()
            node_metrics = [metric for metric in metadata.keys() if metric.startswith('node_')]
            return sorted(node_metrics)
        except Exception:
            return []


async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Query node usage metrics from Prometheus')
    parser.add_argument('--prometheus-url', help='Prometheus server URL (auto-discovered if not provided)')
    parser.add_argument('--token', help='Bearer token for authentication (auto-discovered if not provided)')
    parser.add_argument('--kubeconfig', help='Path to kubeconfig file')
    parser.add_argument('--duration', default='1h', help='Duration to query (e.g., 5m, 1h, 1d)')
    parser.add_argument('--end-time', help='End time (ISO format), defaults to now')
    parser.add_argument('--output', help='Output file path, defaults to stdout')
    parser.add_argument('--pretty', action='store_true', help='Pretty print JSON output')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    parser.add_argument('--list-metrics', action='store_true', help='List available node metrics and exit')
    parser.add_argument('--quiet', action='store_true', help='Suppress progress messages')
    parser.add_argument('--no-auth', action='store_true', help='Skip Kubernetes authentication')
    
    args = parser.parse_args()
    
    # Suppress progress messages if quiet
    if args.quiet:
        sys.stderr = open('/dev/null', 'w')
    
    # Initialize authentication if not disabled
    auth_client = None
    if not args.no_auth:
        try:
            auth_client = OpenShiftAuth(kubeconfig_path=args.kubeconfig)
            await auth_client.initialize()
        except Exception as e:
            print(f"Warning: Authentication failed, proceeding without Kubernetes API access: {e}", file=sys.stderr)
            auth_client = None
    
    # Determine Prometheus URL and token
    prometheus_url = args.prometheus_url
    token = args.token
    
    if auth_client and auth_client.prometheus_url and not prometheus_url:
        prometheus_url = auth_client.prometheus_url
        print(f"Using auto-discovered Prometheus URL: {prometheus_url}", file=sys.stderr)
    
    if auth_client and auth_client.prometheus_token and not token:
        token = auth_client.prometheus_token
        print("Using auto-discovered authentication token", file=sys.stderr)
    
    if not prometheus_url:
        print("Error: Prometheus URL must be provided via --prometheus-url or auto-discovery", file=sys.stderr)
        return 1
    
    # Create Prometheus client
    prometheus_client = PrometheusBaseQuery(prometheus_url, token)
    
    try:
        async with prometheus_client:
            # Test connection
            if not await prometheus_client.test_connection():
                print("Error: Cannot connect to Prometheus server", file=sys.stderr)
                return 1
            
            # Create node usage query instance
            node_query = NodeUsageQuery(prometheus_client, auth_client)
            
            # List metrics if requested
            if args.list_metrics:
                metrics = await node_query.get_available_metrics()
                print(json.dumps(metrics, indent=2 if args.pretty else None))
                return 0
            
            # Query node usage
            result = await node_query.query_node_usage(args.duration, args.end_time, args.debug)
            
            # Format output
            json_output = json.dumps(result, indent=2 if args.pretty else None)
            
            # Write output
            if args.output:
                with open(args.output, 'w') as f:
                    f.write(json_output)
                if not args.quiet:
                    print(f"Results written to {args.output}", file=sys.stderr)
            else:
                print(json_output)
            
            return 0
    
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1
    finally:
        # Restore stderr if it was suppressed
        if args.quiet and hasattr(sys.stderr, 'close'):
            sys.stderr.close()
            sys.stderr = sys.__stderr__


if __name__ == '__main__':
    exit_code = asyncio.run(main())
    sys.exit(exit_code)