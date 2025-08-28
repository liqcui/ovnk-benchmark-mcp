#!/usr/bin/env python3
"""
OpenShift Cluster Information Collection Tool
Gathers comprehensive cluster information including nodes, resources, and network policies
"""

import json
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

from kubernetes import client
from kubernetes.client.rest import ApiException

from ocauth.ovnk_benchmark_auth import OpenShiftAuth

logger = logging.getLogger(__name__)


@dataclass
class NodeInfo:
    """Node information structure"""
    name: str
    node_type: str  # master, infra, worker
    cpu_capacity: str
    memory_capacity: str
    architecture: str
    kernel_version: str
    container_runtime: str
    kubelet_version: str
    os_image: str
    ready_status: bool
    schedulable: bool
    creation_timestamp: str


@dataclass
class ClusterInfo:
    """Complete cluster information structure"""
    cluster_name: str
    cluster_version: str
    platform: str
    api_server_url: str
    total_nodes: int
    master_nodes: List[NodeInfo]
    infra_nodes: List[NodeInfo]
    worker_nodes: List[NodeInfo]
    namespaces_count: int
    pods_count: int
    services_count: int
    secrets_count: int
    configmaps_count: int
    networkpolicies_count: int
    adminnetworkpolicies_count: int
    egressfirewalls_count: int
    collection_timestamp: str
    cluster_operators_status: Dict[str, str]


class OpenShiftGeneralInfo:
    """Collects comprehensive OpenShift cluster information"""
    
    def __init__(self):
        self.auth_manager = None
        self.k8s_client = None
        
    async def initialize(self):
        """Initialize the collector with authentication"""
        self.auth_manager = OpenShiftAuth()
        await self.auth_manager.initialize()
        self.k8s_client = self.auth_manager.kube_client
        
    async def collect_cluster_info(self) -> ClusterInfo:
        """Collect all cluster information"""
        try:
            if not self.k8s_client:
                await self.initialize()
                
            logger.info("Starting cluster information collection...")
            
            # Basic cluster info
            cluster_name = await self._get_cluster_name()
            cluster_version = await self._get_cluster_version()
            platform = await self._get_infrastructure_platform()
            api_server_url = getattr(getattr(self.k8s_client, "configuration", None), "host", "") or ""
            
            # Node information
            nodes_info = await self._collect_nodes_info()
            
            # Resource counts
            namespaces_count = await self._count_namespaces()
            pods_count = await self._count_pods()
            services_count = await self._count_services()
            secrets_count = await self._count_secrets()
            configmaps_count = await self._count_configmaps()
            
            # Network policy counts
            networkpolicies_count = await self._count_networkpolicies()
            adminnetworkpolicies_count = await self._count_admin_networkpolicies()
            egressfirewalls_count = await self._count_egressfirewalls()
            
            # Cluster operators status
            cluster_operators_status = await self._get_cluster_operators_status()
            
            cluster_info = ClusterInfo(
                cluster_name=cluster_name,
                cluster_version=cluster_version,
                platform=platform,
                api_server_url=api_server_url,
                total_nodes=len(nodes_info["all_nodes"]),
                master_nodes=nodes_info["master_nodes"],
                infra_nodes=nodes_info["infra_nodes"], 
                worker_nodes=nodes_info["worker_nodes"],
                namespaces_count=namespaces_count,
                pods_count=pods_count,
                services_count=services_count,
                secrets_count=secrets_count,
                configmaps_count=configmaps_count,
                networkpolicies_count=networkpolicies_count,
                adminnetworkpolicies_count=adminnetworkpolicies_count,
                egressfirewalls_count=egressfirewalls_count,
                collection_timestamp=datetime.now(timezone.utc).isoformat(),
                cluster_operators_status=cluster_operators_status
            )
            
            logger.info("Cluster information collection completed successfully")
            return cluster_info
            
        except Exception as e:
            logger.error(f"Failed to collect cluster info: {e}")
            raise
            
    async def _get_cluster_name(self) -> str:
        """Get cluster name from infrastructure config"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            infrastructure = custom_api.get_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="infrastructures",
                name="cluster"
            )
            return infrastructure.get("status", {}).get("infrastructureName", "unknown")
        except Exception as e:
            logger.warning(f"Could not get cluster name: {e}")
            return "unknown"
            
    async def _get_cluster_version(self) -> str:
        """Get OpenShift cluster version"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            cluster_version = custom_api.get_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="clusterversions",
                name="version"
            )
            
            history = cluster_version.get("status", {}).get("history", [])
            if history:
                return history[0].get("version", "unknown")
                
        except Exception as e:
            logger.warning(f"Could not get cluster version: {e}")
            
        return "unknown"
        
    async def _get_infrastructure_platform(self) -> str:
        """Get infrastructure platform (AWS, Azure, GCP, etc.)"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            infrastructure = custom_api.get_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="infrastructures",
                name="cluster"
            )
            return infrastructure.get("status", {}).get("platform", "unknown")
        except Exception as e:
            logger.warning(f"Could not get infrastructure platform: {e}")
            return "unknown"
            
    async def _collect_nodes_info(self) -> Dict[str, List[NodeInfo]]:
        """Collect detailed information about all nodes"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            nodes = v1.list_node()
            
            master_nodes = []
            infra_nodes = []
            worker_nodes = []
            all_nodes = []
            
            for node in nodes.items:
                node_info = self._parse_node_info(node)
                all_nodes.append(node_info)
                
                # Categorize nodes based on labels
                labels = node.metadata.labels or {}
                if labels.get("node-role.kubernetes.io/master") == "" or \
                   labels.get("node-role.kubernetes.io/control-plane") == "":
                    node_info.node_type = "master"
                    master_nodes.append(node_info)
                elif labels.get("node-role.kubernetes.io/infra") == "":
                    node_info.node_type = "infra"
                    infra_nodes.append(node_info)
                else:
                    node_info.node_type = "worker"
                    worker_nodes.append(node_info)
                    
            return {
                "all_nodes": all_nodes,
                "master_nodes": master_nodes,
                "infra_nodes": infra_nodes,
                "worker_nodes": worker_nodes
            }
            
        except Exception as e:
            logger.error(f"Failed to collect nodes info: {e}")
            raise
            
    def _parse_node_info(self, node) -> NodeInfo:
        """Parse Kubernetes node object into NodeInfo"""
        status = node.status
        spec = node.spec
        metadata = node.metadata
        
        # Get node conditions
        ready_status = False
        for condition in status.conditions or []:
            if condition.type == "Ready" and condition.status == "True":
                ready_status = True
                break
                
        return NodeInfo(
            name=metadata.name,
            node_type="unknown",  # Will be set by categorization logic
            cpu_capacity=status.capacity.get("cpu", "unknown"),
            memory_capacity=status.capacity.get("memory", "unknown"),
            architecture=status.node_info.architecture,
            kernel_version=status.node_info.kernel_version,
            container_runtime=status.node_info.container_runtime_version,
            kubelet_version=status.node_info.kubelet_version,
            os_image=status.node_info.os_image,
            ready_status=ready_status,
            schedulable=not spec.unschedulable if spec.unschedulable is not None else True,
            creation_timestamp=metadata.creation_timestamp.isoformat() if metadata.creation_timestamp else ""
        )
        
    async def _count_namespaces(self) -> int:
        """Count total number of namespaces"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            namespaces = v1.list_namespace()
            return len(namespaces.items)
        except Exception as e:
            logger.warning(f"Could not count namespaces: {e}")
            return 0
            
    async def _count_pods(self) -> int:
        """Count total number of pods across all namespaces"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            pods = v1.list_pod_for_all_namespaces()
            return len(pods.items)
        except Exception as e:
            logger.warning(f"Could not count pods: {e}")
            return 0
            
    async def _count_services(self) -> int:
        """Count total number of services"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            services = v1.list_service_for_all_namespaces()
            return len(services.items)
        except Exception as e:
            logger.warning(f"Could not count services: {e}")
            return 0
            
    async def _count_secrets(self) -> int:
        """Count total number of secrets"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            secrets = v1.list_secret_for_all_namespaces()
            return len(secrets.items)
        except Exception as e:
            logger.warning(f"Could not count secrets: {e}")
            return 0
            
    async def _count_configmaps(self) -> int:
        """Count total number of configmaps"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            configmaps = v1.list_config_map_for_all_namespaces()
            return len(configmaps.items)
        except Exception as e:
            logger.warning(f"Could not count configmaps: {e}")
            return 0
            
    async def _count_networkpolicies(self) -> int:
        """Count total number of network policies"""
        try:
            networking_v1 = client.NetworkingV1Api(self.k8s_client)
            network_policies = networking_v1.list_network_policy_for_all_namespaces()
            return len(network_policies.items)
        except Exception as e:
            logger.warning(f"Could not count network policies: {e}")
            return 0
            
    async def _count_admin_networkpolicies(self) -> int:
        """Count total number of admin network policies"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            admin_policies = custom_api.list_cluster_custom_object(
                group="policy.networking.k8s.io",
                version="v1alpha1",
                plural="adminnetworkpolicies"
            )
            return len(admin_policies.get("items", []))
        except Exception as e:
            logger.warning(f"Could not count admin network policies: {e}")
            return 0
            
    async def _count_egressfirewalls(self) -> int:
        """Count total number of egress firewalls"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            egress_firewalls = custom_api.list_cluster_custom_object(
                group="k8s.ovn.org",
                version="v1",
                plural="egressfirewalls"
            )
            return len(egress_firewalls.get("items", []))
        except Exception as e:
            logger.warning(f"Could not count egress firewalls: {e}")
            return 0
            
    async def _get_cluster_operators_status(self) -> Dict[str, str]:
        """Get status of cluster operators"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            operators = custom_api.list_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="clusteroperators"
            )
            
            operator_status = {}
            for operator in operators.get("items", []):
                name = operator["metadata"]["name"]
                conditions = operator.get("status", {}).get("conditions", [])
                
                # Find the "Available" condition
                status = "Unknown"
                for condition in conditions:
                    if condition["type"] == "Available":
                        status = "Available" if condition["status"] == "True" else "Unavailable"
                        break
                        
                operator_status[name] = status
                
            return operator_status
            
        except Exception as e:
            logger.warning(f"Could not get cluster operators status: {e}")
            return {}
            
    def to_json(self, cluster_info: ClusterInfo) -> str:
        """Convert cluster info to JSON string"""
        return json.dumps(asdict(cluster_info), indent=2, default=str)
        
    def to_dict(self, cluster_info: ClusterInfo) -> Dict[str, Any]:
        """Convert cluster info to dictionary"""
        return asdict(cluster_info)


# Convenience functions for MCP server
async def collect_cluster_information() -> Dict[str, Any]:
    """Collect and return cluster information as dictionary"""
    collector = OpenShiftGeneralInfo()
    cluster_info = await collector.collect_cluster_info()
    return collector.to_dict(cluster_info)


async def get_cluster_info_json() -> str:
    """Collect and return cluster information as JSON string"""
    collector = OpenShiftGeneralInfo()
    cluster_info = await collector.collect_cluster_info()
    return collector.to_json(cluster_info)