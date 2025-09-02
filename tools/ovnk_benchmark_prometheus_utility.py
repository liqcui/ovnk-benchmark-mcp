#!/usr/bin/env python3
"""
OVNK Benchmark Prometheus Utilities
Provides utility functions for node information and grouping
"""

import asyncio
import json
import subprocess
from typing import Dict, List, Any, Optional
from kubernetes import client
from kubernetes.client.rest import ApiException


class mcpToolsUtility:
    """Utility class for node information and grouping operations"""
    
    def __init__(self, auth_client=None):
        self.auth_client = auth_client
        self.node_cache = {}
        self.cache_timestamp = None
        
        # Node role detection labels in priority order
        self.role_labels = [
            'node-role.kubernetes.io/control-plane',
            'node-role.kubernetes.io/master', 
            'node-role.kubernetes.io/infra',
            'node-role.kubernetes.io/worker'
        ]
        
        # Role mapping for consistency
        self.role_mapping = {
            'control-plane': 'controlplane',
            'master': 'controlplane',
            'infra': 'infra',
            'worker': 'worker',
            'workload': 'workload'
        }
    
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
            
        except Exception:
            return {}

    async def get_node_labels_via_oc(self) -> Dict[str, Dict[str, str]]:
        """Get node labels using oc CLI as fallback"""
        try:
            completed = subprocess.run(
                ["oc", "get", "nodes", "-o", "json"],
                check=True,
                capture_output=True,
                text=True
            )
            data = json.loads(completed.stdout)
            items = data.get("items", []) if isinstance(data, dict) else []
            labels_map = {}
            
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
            
            return labels_map
            
        except Exception:
            return {}
    
    async def get_node_labels_from_prometheus(self, prometheus_client) -> Dict[str, Dict[str, str]]:
        """Get node labels from Prometheus kube_node_labels metric"""
        try:
            result = await prometheus_client.query_instant('kube_node_labels')
            labels_map = {}
            
            if 'result' in result:
                for item in result['result']:
                    if 'metric' not in item:
                        continue
                    metric_labels = item['metric']
                    node_name = (metric_labels.get('node') or 
                                metric_labels.get('kubernetes_node') or 
                                metric_labels.get('nodename'))
                    
                    if not node_name:
                        continue
                    
                    # Keep all labels except __name__ and prometheus-specific labels
                    labels = {k: v for k, v in metric_labels.items() 
                             if k not in ['__name__', 'job', 'instance']}
                    
                    # Store labels for node name variations
                    candidates = [node_name]
                    if '.' in node_name:
                        candidates.append(node_name.split('.')[0])
                    
                    for key in candidates:
                        if key not in labels_map:
                            labels_map[key] = labels
            
            return labels_map
        except Exception:
            return {}

    async def get_all_node_labels(self, prometheus_client=None) -> Dict[str, Dict[str, str]]:
        """Get node labels with fallback strategies"""
        # Try Kubernetes API first
        if self.auth_client:
            k8s_labels = await self.get_node_labels_from_kubernetes()
            if k8s_labels:
                return k8s_labels
        
        # Try oc CLI as fallback
        oc_labels = await self.get_node_labels_via_oc()
        if oc_labels:
            return oc_labels
        
        # Try Prometheus kube_node_labels as third fallback
        if prometheus_client:
            prom_labels = await self.get_node_labels_from_prometheus(prometheus_client)
            if prom_labels:
                return prom_labels
        
        return {}
    
    def determine_node_role(self, node_name: str, labels: Dict[str, str], instance: str = "") -> str:
        """Determine node role from labels with improved detection"""
        
        # Check standard Kubernetes node role labels with exact matching
        if 'node-role.kubernetes.io/control-plane' in labels:
            return 'controlplane'
        if 'node-role.kubernetes.io/master' in labels:
            return 'controlplane'
        if 'node-role.kubernetes.io/infra' in labels:
            return 'infra'
        if 'node-role.kubernetes.io/worker' in labels:
            return 'worker'
        
        # Check for older/alternative role labels
        for label_key, label_value in labels.items():
            label_key_lower = label_key.lower()
            
            # Check for role in label keys
            if 'kubernetes.io/role' in label_key_lower:
                if label_value.lower() in ['master', 'control-plane']:
                    return 'controlplane'
                elif label_value.lower() == 'infra':
                    return 'infra'
                elif label_value.lower() == 'worker':
                    return 'worker'
            
            # Check for role indicators in any label
            if any(term in label_key_lower for term in ['master', 'control-plane', 'controlplane']):
                return 'controlplane'
            elif 'infra' in label_key_lower:
                return 'infra'
            elif 'worker' in label_key_lower:
                return 'worker'
            elif 'workload' in label_key_lower:
                return 'workload'
            
            # Check label values for role indicators
            label_value_lower = label_value.lower() if isinstance(label_value, str) else ""
            if any(term in label_value_lower for term in ['master', 'control']):
                return 'controlplane'
            elif 'infra' in label_value_lower:
                return 'infra'
            elif 'worker' in label_value_lower:
                return 'worker'
            elif 'workload' in label_value_lower:
                return 'workload'
        
        # Name-based detection as fallback
        name_lower = node_name.lower()
        instance_lower = instance.lower().split(':')[0] if instance else ""
        
        # Check for master/control-plane patterns
        if any(pattern in name_lower for pattern in ['master', 'control', 'cp-', '-cp']):
            return 'controlplane'
        if instance_lower and any(pattern in instance_lower for pattern in ['master', 'control', 'cp-', '-cp']):
            return 'controlplane'
        
        # Check for infra patterns
        if 'infra' in name_lower or 'infrastructure' in name_lower:
            return 'infra'
        if instance_lower and ('infra' in instance_lower or 'infrastructure' in instance_lower):
            return 'infra'
        
        # Check for workload patterns
        if 'workload' in name_lower:
            return 'workload'
        if instance_lower and 'workload' in instance_lower:
            return 'workload'
        
        # Default to worker for compute nodes
        return 'worker'
    
    async def get_node_groups(self, prometheus_client=None) -> Dict[str, List[Dict[str, Any]]]:
        """Get nodes grouped by role (controlplane/worker/infra/workload)"""
        node_labels = await self.get_all_node_labels(prometheus_client)
        
        groups = {
            'controlplane': [],
            'worker': [],
            'infra': [],
            'workload': []
        }
        
        for node_name, labels in node_labels.items():
            role = self.determine_node_role(node_name, labels)
            
            node_info = {
                'name': node_name,
                'role': role,
                'labels': {k: v for k, v in labels.items() if k.startswith('node-role.kubernetes.io/')}
            }
            
            groups[role].append(node_info)
        
        return groups
    
    async def get_node_info(self, node_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific node"""
        node_labels = await self.get_all_node_labels()
        
        if node_name not in node_labels:
            # Try short name lookup
            for full_name, labels in node_labels.items():
                if full_name.startswith(node_name + '.'):
                    node_name = full_name
                    break
            else:
                return None
        
        labels = node_labels[node_name]
        role = self.determine_node_role(node_name, labels)
        
        return {
            'name': node_name,
            'role': role,
            'labels': labels,
            'role_labels': {k: v for k, v in labels.items() if k.startswith('node-role.kubernetes.io/')}
        }
    
    async def get_nodes_by_role(self, role: str) -> List[Dict[str, Any]]:
        """Get all nodes with specified role"""
        groups = await self.get_node_groups()
        return groups.get(role, [])
    
    async def get_cluster_summary(self) -> Dict[str, Any]:
        """Get cluster node summary"""
        groups = await self.get_node_groups()
        
        summary = {
            'total_nodes': sum(len(nodes) for nodes in groups.values()),
            'groups': {}
        }
        
        for role, nodes in groups.items():
            summary['groups'][role] = {
                'count': len(nodes),
                'nodes': [node['name'] for node in nodes]
            }
        
        return summary

    def get_pod_node_names_via_oc(self, namespace: str = "openshift-multus", label_selector: Optional[str] = None) -> Dict[str, Any]:
        """Get the nodeName for pods in a namespace using oc jsonpath.

        Returns a JSON result with list of node names where the pods are scheduled.
        """
        try:
            cmd = ["oc", "-n", namespace, "get", "pods"]
            if label_selector:
                cmd.extend(["-l", label_selector])
            jsonpath = "{range .items[*]}{.spec.nodeName}{'\\n'}{end}"
            cmd.extend(["-o", f"jsonpath={jsonpath}"])
            completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
            lines = [line.strip() for line in completed.stdout.splitlines()]
            node_names = [ln for ln in lines if ln]
            
            # Return JSON format with metadata
            return {
                'namespace': namespace,
                'label_selector': label_selector,
                'node_names': node_names,
                'count': len(node_names)
            }
        except Exception as e:
            return {
                'namespace': namespace,
                'label_selector': label_selector,
                'error': str(e),
                'node_names': [],
                'count': 0
            }

    def get_pod_to_node_mapping_via_oc(self, namespace: str = "openshift-multus", label_selector: Optional[str] = None) -> Dict[str, str]:
        """Get a mapping of pod name -> nodeName via oc jsonpath.

        Enhanced to return JSON result with pod-node mapping and metadata.
        Uses: oc -n <ns> get pods -o jsonpath to extract pod metadata.name and spec.nodeName.
        """
        try:
            cmd = ["oc", "-n", namespace, "get", "pods"]
            if label_selector:
                cmd.extend(["-l", label_selector])
            jsonpath = "{range .items[*]}{.metadata.name}={.spec.nodeName}{'\\n'}{end}"
            cmd.extend(["-o", f"jsonpath={jsonpath}"])
            completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
            mapping: Dict[str, str] = {}
            for line in completed.stdout.splitlines():
                if not line.strip():
                    continue
                if "=" in line:
                    pod, node = line.split("=", 1)
                    pod = pod.strip()
                    node = node.strip()
                    if pod and node:
                        mapping[pod] = node
            return mapping
        except Exception:
            return {}
    
    def get_pod_full_info_via_oc(self, namespace: str = "openshift-multus", label_selector: Optional[str] = None) -> Dict[str, Dict[str, str]]:
        """Get full pod info including namespace via oc jsonpath.
        
        Returns: Dict[pod_name, Dict[str, str]] with pod_name -> {node_name, namespace}
        """
        try:
            cmd = ["oc", "-n", namespace, "get", "pods"]
            if label_selector:
                cmd.extend(["-l", label_selector])
            jsonpath = "{range .items[*]}{.metadata.name}={.spec.nodeName}={.metadata.namespace}{'\\n'}{end}"
            cmd.extend(["-o", f"jsonpath={jsonpath}"])
            completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
            
            pod_info: Dict[str, Dict[str, str]] = {}
            for line in completed.stdout.splitlines():
                if not line.strip():
                    continue
                parts = line.split("=")
                if len(parts) >= 3:
                    pod = parts[0].strip()
                    node = parts[1].strip()
                    ns = parts[2].strip()
                    if pod and node:
                        pod_info[pod] = {
                            'node_name': node,
                            'namespace': ns if ns else namespace
                        }
            return pod_info
        except Exception:
            return {}
    
    def get_all_pods_info_across_namespaces(self, namespaces: List[str] = None) -> Dict[str, Dict[str, str]]:
        """Get pod info across multiple namespaces.
        
        Returns: Dict[pod_name, Dict[str, str]] with pod_name -> {node_name, namespace}
        """
        if namespaces is None:
            namespaces = ['default', 'openshift-ovn-kubernetes', 'kube-system', 'openshift-monitoring', 'openshift-multus']
        
        all_pod_info: Dict[str, Dict[str, str]] = {}
        
        for namespace in namespaces:
            try:
                pod_info = self.get_pod_full_info_via_oc(namespace=namespace)
                all_pod_info.update(pod_info)
            except Exception:
                continue
        
        return all_pod_info
    
    def get_pod_to_node_mapping_with_metadata_via_oc(self, namespace: str = "openshift-multus", label_selector: Optional[str] = None) -> Dict[str, Any]:
        """Get pod-node mapping with metadata in JSON format.
        
        This is the enhanced version that returns JSON result format as requested.
        """
        try:
            cmd = ["oc", "-n", namespace, "get", "pods"]
            if label_selector:
                cmd.extend(["-l", label_selector])
            jsonpath = "{range .items[*]}{.metadata.name}={.spec.nodeName}={'\\n'}{end}"
            cmd.extend(["-o", f"jsonpath={jsonpath}"])
            completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
            
            mapping: Dict[str, str] = {}
            pod_details: List[Dict[str, str]] = []
            
            for line in completed.stdout.splitlines():
                if not line.strip():
                    continue
                if "=" in line:
                    pod, node = line.split("=", 1)
                    pod = pod.strip()
                    node = node.strip()
                    if pod and node:
                        mapping[pod] = node
                        pod_details.append({
                            'pod_name': pod,
                            'node_name': node
                        })
            
            return {
                'namespace': namespace,
                'label_selector': label_selector,
                'pod_node_mapping': mapping,
                'pod_details': pod_details,
                'count': len(mapping)
            }
            
        except Exception as e:
            return {
                'namespace': namespace,
                'label_selector': label_selector,
                'error': str(e),
                'pod_node_mapping': {},
                'pod_details': [],
                'count': 0
            }