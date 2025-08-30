#!/usr/bin/env python3
"""
OpenShift Cluster Status Collector
Collects comprehensive cluster status including node status, cluster operators, and machine config pools
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import json
from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class ClusterStatCollector:
    """Collector for comprehensive cluster status information"""
    
    def __init__(self, kubeconfig_path: Optional[str] = None):
        """Initialize the cluster status collector"""
        self.kubeconfig_path = kubeconfig_path
        self.core_v1_api = None
        self.custom_objects_api = None
        self.apps_v1_api = None
        self._initialized = False
        
    async def initialize(self):
        """Initialize Kubernetes client connection"""
        try:
            if self.kubeconfig_path:
                config.load_kube_config(config_file=self.kubeconfig_path)
            else:
                try:
                    config.load_incluster_config()
                except config.ConfigException:
                    config.load_kube_config()
            
            self.core_v1_api = client.CoreV1Api()
            self.custom_objects_api = client.CustomObjectsApi()
            self.apps_v1_api = client.AppsV1Api()
            self._initialized = True
            
            logger.info("ClusterStatCollector initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize ClusterStatCollector: {e}")
            raise
    
    def _ensure_initialized(self):
        """Ensure the collector is initialized"""
        if not self._initialized:
            raise RuntimeError("ClusterStatCollector not initialized. Call initialize() first.")
    
    async def get_node_status(self) -> Dict[str, Any]:
        """Get comprehensive node status information"""
        self._ensure_initialized()
        
        try:
            # Get all nodes
            nodes_response = self.core_v1_api.list_node()
            nodes = nodes_response.items
            
            node_status = {
                'total_nodes': len(nodes),
                'nodes_by_status': {},
                'nodes_by_role': {'master': [], 'worker': [], 'infra': []},
                'detailed_status': [],
                'summary': {
                    'ready': 0,
                    'not_ready': 0,
                    'scheduling_disabled': 0,
                    'unknown': 0
                }
            }
            
            for node in nodes:
                node_name = node.metadata.name
                node_info = {
                    'name': node_name,
                    'ready': False,
                    'schedulable': not node.spec.unschedulable if node.spec.unschedulable is not None else True,
                    'roles': [],
                    'conditions': [],
                    'taints': [],
                    'creation_timestamp': node.metadata.creation_timestamp.isoformat() if node.metadata.creation_timestamp else None,
                    'kubelet_version': node.status.node_info.kubelet_version if node.status.node_info else None,
                    'os_image': node.status.node_info.os_image if node.status.node_info else None,
                    'kernel_version': node.status.node_info.kernel_version if node.status.node_info else None
                }
                
                # Extract node roles from labels
                if node.metadata.labels:
                    for label_key in node.metadata.labels:
                        if 'node-role.kubernetes.io/' in label_key:
                            role = label_key.split('/')[-1]
                            node_info['roles'].append(role)
                            
                            # Categorize nodes by role
                            if role in ['master', 'control-plane']:
                                node_status['nodes_by_role']['master'].append(node_name)
                            elif role == 'worker':
                                node_status['nodes_by_role']['worker'].append(node_name)
                            elif role == 'infra':
                                node_status['nodes_by_role']['infra'].append(node_name)
                
                # Process node conditions
                if node.status.conditions:
                    for condition in node.status.conditions:
                        condition_info = {
                            'type': condition.type,
                            'status': condition.status,
                            'reason': condition.reason,
                            'message': condition.message,
                            'last_transition_time': condition.last_transition_time.isoformat() if condition.last_transition_time else None
                        }
                        node_info['conditions'].append(condition_info)
                        
                        # Check if node is ready
                        if condition.type == 'Ready' and condition.status == 'True':
                            node_info['ready'] = True
                
                # Process taints
                if node.spec.taints:
                    for taint in node.spec.taints:
                        taint_info = {
                            'key': taint.key,
                            'value': taint.value,
                            'effect': taint.effect
                        }
                        node_info['taints'].append(taint_info)
                
                # Update summary counts
                if node_info['ready']:
                    node_status['summary']['ready'] += 1
                else:
                    node_status['summary']['not_ready'] += 1
                    
                if not node_info['schedulable']:
                    node_status['summary']['scheduling_disabled'] += 1
                
                # Determine overall node status
                if node_info['ready'] and node_info['schedulable']:
                    status = 'Ready'
                elif node_info['ready'] and not node_info['schedulable']:
                    status = 'Ready,SchedulingDisabled'
                elif not node_info['ready'] and node_info['schedulable']:
                    status = 'NotReady'
                elif not node_info['ready'] and not node_info['schedulable']:
                    status = 'NotReady,SchedulingDisabled'
                else:
                    status = 'Unknown'
                    node_status['summary']['unknown'] += 1
                
                node_info['overall_status'] = status
                
                # Group nodes by status
                if status not in node_status['nodes_by_status']:
                    node_status['nodes_by_status'][status] = []
                node_status['nodes_by_status'][status].append(node_name)
                
                node_status['detailed_status'].append(node_info)
            
            return node_status
            
        except ApiException as e:
            logger.error(f"Kubernetes API error getting node status: {e}")
            raise
        except Exception as e:
            logger.error(f"Error getting node status: {e}")
            raise
    
    async def get_cluster_operator_status(self) -> Dict[str, Any]:
        """Get cluster operator status information"""
        self._ensure_initialized()
        
        try:
            # Get cluster operators using custom objects API
            operators_response = self.custom_objects_api.list_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="clusteroperators"
            )
            
            operators = operators_response.get('items', [])
            
            operator_status = {
                'total_operators': len(operators),
                'operators': [],
                'summary': {
                    'available': 0,
                    'degraded': 0,
                    'progressing': 0,
                    'not_available': 0
                },
                'operators_by_status': {
                    'healthy': [],
                    'degraded': [],
                    'progressing': [],
                    'unavailable': []
                }
            }
            
            for operator in operators:
                op_name = operator.get('metadata', {}).get('name', 'unknown')
                op_info = {
                    'name': op_name,
                    'available': False,
                    'degraded': False,
                    'progressing': False,
                    'conditions': [],
                    'version': None,
                    'related_objects': []
                }
                
                # Extract version information
                if 'status' in operator and 'versions' in operator['status']:
                    versions = operator['status']['versions']
                    if versions:
                        op_info['version'] = versions[0].get('version', 'unknown')
                
                # Process conditions
                if 'status' in operator and 'conditions' in operator['status']:
                    for condition in operator['status']['conditions']:
                        condition_info = {
                            'type': condition.get('type', ''),
                            'status': condition.get('status', ''),
                            'reason': condition.get('reason', ''),
                            'message': condition.get('message', ''),
                            'last_transition_time': condition.get('lastTransitionTime', '')
                        }
                        op_info['conditions'].append(condition_info)
                        
                        # Update operator flags based on conditions
                        if condition['type'] == 'Available' and condition['status'] == 'True':
                            op_info['available'] = True
                        elif condition['type'] == 'Degraded' and condition['status'] == 'True':
                            op_info['degraded'] = True
                        elif condition['type'] == 'Progressing' and condition['status'] == 'True':
                            op_info['progressing'] = True
                
                # Process related objects
                if 'status' in operator and 'relatedObjects' in operator['status']:
                    for related_obj in operator['status']['relatedObjects']:
                        obj_info = {
                            'group': related_obj.get('group', ''),
                            'resource': related_obj.get('resource', ''),
                            'name': related_obj.get('name', ''),
                            'namespace': related_obj.get('namespace', '')
                        }
                        op_info['related_objects'].append(obj_info)
                
                # Update summary counts
                if op_info['available']:
                    operator_status['summary']['available'] += 1
                else:
                    operator_status['summary']['not_available'] += 1
                    
                if op_info['degraded']:
                    operator_status['summary']['degraded'] += 1
                    
                if op_info['progressing']:
                    operator_status['summary']['progressing'] += 1
                
                # Categorize operators by health status
                if op_info['available'] and not op_info['degraded']:
                    operator_status['operators_by_status']['healthy'].append(op_name)
                elif op_info['degraded']:
                    operator_status['operators_by_status']['degraded'].append(op_name)
                elif op_info['progressing']:
                    operator_status['operators_by_status']['progressing'].append(op_name)
                else:
                    operator_status['operators_by_status']['unavailable'].append(op_name)
                
                operator_status['operators'].append(op_info)
            
            return operator_status
            
        except ApiException as e:
            logger.error(f"Kubernetes API error getting cluster operators: {e}")
            raise
        except Exception as e:
            logger.error(f"Error getting cluster operators: {e}")
            raise
    
    async def get_machine_config_pool_status(self) -> Dict[str, Any]:
        """Get machine config pool (MCP) status information"""
        self._ensure_initialized()
        
        try:
            # Get machine config pools using custom objects API
            mcp_response = self.custom_objects_api.list_cluster_custom_object(
                group="machineconfiguration.openshift.io",
                version="v1",
                plural="machineconfigpools"
            )
            
            mcps = mcp_response.get('items', [])
            
            mcp_status = {
                'total_pools': len(mcps),
                'pools': [],
                'summary': {
                    'updated': 0,
                    'updating': 0,
                    'degraded': 0,
                    'ready': 0
                },
                'pools_by_status': {
                    'healthy': [],
                    'updating': [],
                    'degraded': [],
                    'not_ready': []
                }
            }
            
            for mcp in mcps:
                pool_name = mcp.get('metadata', {}).get('name', 'unknown')
                pool_info = {
                    'name': pool_name,
                    'updated': False,
                    'updating': False,
                    'degraded': False,
                    'machine_count': 0,
                    'ready_machine_count': 0,
                    'updated_machine_count': 0,
                    'unavailable_machine_count': 0,
                    'conditions': [],
                    'current_config': None,
                    'node_selector': {}
                }
                
                # Extract machine counts
                if 'status' in mcp:
                    status = mcp['status']
                    pool_info['machine_count'] = status.get('machineCount', 0)
                    pool_info['ready_machine_count'] = status.get('readyMachineCount', 0)
                    pool_info['updated_machine_count'] = status.get('updatedMachineCount', 0)
                    pool_info['unavailable_machine_count'] = status.get('unavailableMachineCount', 0)
                    
                    # Get current configuration
                    if 'configuration' in status:
                        pool_info['current_config'] = status['configuration'].get('name', '')
                    
                    # Process conditions
                    if 'conditions' in status:
                        for condition in status['conditions']:
                            condition_info = {
                                'type': condition.get('type', ''),
                                'status': condition.get('status', ''),
                                'reason': condition.get('reason', ''),
                                'message': condition.get('message', ''),
                                'last_transition_time': condition.get('lastTransitionTime', '')
                            }
                            pool_info['conditions'].append(condition_info)
                            
                            # Update pool flags based on conditions
                            if condition['type'] == 'Updated' and condition['status'] == 'True':
                                pool_info['updated'] = True
                            elif condition['type'] == 'Updating' and condition['status'] == 'True':
                                pool_info['updating'] = True
                            elif condition['type'] == 'Degraded' and condition['status'] == 'True':
                                pool_info['degraded'] = True
                
                # Extract node selector
                if 'spec' in mcp and 'nodeSelector' in mcp['spec']:
                    pool_info['node_selector'] = mcp['spec']['nodeSelector'].get('matchLabels', {})
                
                # Update summary counts
                if pool_info['updated']:
                    mcp_status['summary']['updated'] += 1
                    mcp_status['summary']['ready'] += 1
                    
                if pool_info['updating']:
                    mcp_status['summary']['updating'] += 1
                    
                if pool_info['degraded']:
                    mcp_status['summary']['degraded'] += 1
                
                # Categorize pools by status
                if pool_info['updated'] and not pool_info['degraded']:
                    mcp_status['pools_by_status']['healthy'].append(pool_name)
                elif pool_info['updating']:
                    mcp_status['pools_by_status']['updating'].append(pool_name)
                elif pool_info['degraded']:
                    mcp_status['pools_by_status']['degraded'].append(pool_name)
                else:
                    mcp_status['pools_by_status']['not_ready'].append(pool_name)
                
                mcp_status['pools'].append(pool_info)
            
            return mcp_status
            
        except ApiException as e:
            logger.error(f"Kubernetes API error getting machine config pools: {e}")
            raise
        except Exception as e:
            logger.error(f"Error getting machine config pools: {e}")
            raise
    
    async def collect_comprehensive_cluster_status(self) -> Dict[str, Any]:
        """Collect comprehensive cluster status information"""
        self._ensure_initialized()
        
        try:
            logger.info("Starting comprehensive cluster status collection...")
            
            # Collect all status information concurrently
            node_status_task = asyncio.create_task(self.get_node_status())
            operator_status_task = asyncio.create_task(self.get_cluster_operator_status())
            mcp_status_task = asyncio.create_task(self.get_machine_config_pool_status())
            
            # Wait for all tasks to complete
            node_status, operator_status, mcp_status = await asyncio.gather(
                node_status_task,
                operator_status_task,
                mcp_status_task,
                return_exceptions=True
            )
            
            # Handle exceptions
            cluster_status = {
                'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                'collection_type': 'comprehensive_cluster_status'
            }
            
            if isinstance(node_status, Exception):
                cluster_status['node_status'] = {'error': str(node_status)}
                logger.error(f"Node status collection failed: {node_status}")
            else:
                cluster_status['node_status'] = node_status
                
            if isinstance(operator_status, Exception):
                cluster_status['operator_status'] = {'error': str(operator_status)}
                logger.error(f"Operator status collection failed: {operator_status}")
            else:
                cluster_status['operator_status'] = operator_status
                
            if isinstance(mcp_status, Exception):
                cluster_status['mcp_status'] = {'error': str(mcp_status)}
                logger.error(f"MCP status collection failed: {mcp_status}")
            else:
                cluster_status['mcp_status'] = mcp_status
            
            # Generate overall cluster health summary
            cluster_status['overall_summary'] = self._generate_overall_summary(
                cluster_status
            )
            
            logger.info("Comprehensive cluster status collection completed")
            return cluster_status
            
        except Exception as e:
            logger.error(f"Error collecting comprehensive cluster status: {e}")
            return {
                'error': str(e),
                'collection_timestamp': datetime.now(timezone.utc).isoformat(),
                'collection_type': 'comprehensive_cluster_status'
            }
    
    def _generate_overall_summary(self, cluster_status: Dict[str, Any]) -> Dict[str, Any]:
        """Generate overall cluster health summary"""
        summary = {
            'cluster_health': 'unknown',
            'critical_issues': [],
            'warnings': [],
            'healthy_components': []
        }
        
        issues = []
        warnings = []
        healthy = []
        
        try:
            # Analyze node status
            if 'node_status' in cluster_status and 'error' not in cluster_status['node_status']:
                node_data = cluster_status['node_status']
                node_summary = node_data.get('summary', {})
                
                if node_summary.get('not_ready', 0) > 0:
                    issues.append(f"{node_summary['not_ready']} nodes are not ready")
                
                if node_summary.get('scheduling_disabled', 0) > 0:
                    warnings.append(f"{node_summary['scheduling_disabled']} nodes have scheduling disabled")
                
                if node_summary.get('ready', 0) > 0:
                    healthy.append(f"{node_summary['ready']} nodes are ready")
            
            # Analyze operator status
            if 'operator_status' in cluster_status and 'error' not in cluster_status['operator_status']:
                op_data = cluster_status['operator_status']
                op_summary = op_data.get('summary', {})
                
                if op_summary.get('not_available', 0) > 0:
                    issues.append(f"{op_summary['not_available']} cluster operators are not available")
                
                if op_summary.get('degraded', 0) > 0:
                    issues.append(f"{op_summary['degraded']} cluster operators are degraded")
                
                if op_summary.get('progressing', 0) > 0:
                    warnings.append(f"{op_summary['progressing']} cluster operators are progressing")
                
                if op_summary.get('available', 0) > 0:
                    healthy.append(f"{op_summary['available']} cluster operators are available")
            
            # Analyze MCP status
            if 'mcp_status' in cluster_status and 'error' not in cluster_status['mcp_status']:
                mcp_data = cluster_status['mcp_status']
                mcp_summary = mcp_data.get('summary', {})
                
                if mcp_summary.get('degraded', 0) > 0:
                    issues.append(f"{mcp_summary['degraded']} machine config pools are degraded")
                
                if mcp_summary.get('updating', 0) > 0:
                    warnings.append(f"{mcp_summary['updating']} machine config pools are updating")
                
                if mcp_summary.get('ready', 0) > 0:
                    healthy.append(f"{mcp_summary['ready']} machine config pools are ready")
            
            # Determine overall health
            if issues:
                summary['cluster_health'] = 'critical' if len(issues) >= 3 else 'degraded'
            elif warnings:
                summary['cluster_health'] = 'warning'
            else:
                summary['cluster_health'] = 'healthy'
            
            summary['critical_issues'] = issues
            summary['warnings'] = warnings
            summary['healthy_components'] = healthy
            
        except Exception as e:
            logger.error(f"Error generating overall summary: {e}")
            summary['error'] = str(e)
        
        return summary
    
    async def cleanup(self):
        """Cleanup resources"""
        self._initialized = False
        logger.info("ClusterStatCollector cleanup completed")