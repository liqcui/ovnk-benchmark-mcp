"""
OpenShift Authentication Module
Handles authentication and service discovery for OpenShift/Kubernetes clusters
"""

import os
import base64
import json
from typing import Optional, Dict, Any
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import aiohttp
import asyncio


class OpenShiftAuth:
    """Authentication and service discovery for OpenShift clusters"""
    
    def __init__(self, kubeconfig_path: Optional[str] = None):
        self.kubeconfig_path = kubeconfig_path or os.getenv('KUBECONFIG')
        self.kube_client: Optional[client.ApiClient] = None
        self.prometheus_url: Optional[str] = None
        self.prometheus_token: Optional[str] = None
        self.cluster_info: Dict[str, Any] = {}
        
    async def initialize(self) -> None:
        """Initialize authentication and discover services"""
        try:
            # Load kubeconfig
            if self.kubeconfig_path and os.path.exists(self.kubeconfig_path):
                config.load_kube_config(config_file=self.kubeconfig_path)
            elif os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount/token'):
                # Running in-cluster
                config.load_incluster_config()
            else:
                # Try default kubeconfig location
                config.load_kube_config()
            
            # Create API client
            self.kube_client = client.ApiClient()
            
            # Get cluster information
            await self._get_cluster_info()
            
            # Discover Prometheus
            await self._discover_prometheus()
            
            print(f"✅ Successfully authenticated to OpenShift cluster")
            print(f"✅ Prometheus URL: {self.prometheus_url}")
            
        except Exception as e:
            print(f"❌ Failed to initialize authentication: {e}")
            raise
    
    async def _get_cluster_info(self) -> None:
        """Get basic cluster information"""
        try:
            v1 = client.CoreV1Api(self.kube_client)
            
            # Get cluster version (OpenShift specific)
            try:
                custom_api = client.CustomObjectsApi(self.kube_client)
                cluster_version = custom_api.get_cluster_custom_object(
                    group="config.openshift.io",
                    version="v1",
                    plural="clusterversions",
                    name="version"
                )
                self.cluster_info['openshift_version'] = cluster_version.get('status', {}).get('desired', {}).get('version', 'unknown')
                self.cluster_info['is_openshift'] = True
            except ApiException:
                self.cluster_info['is_openshift'] = False
            
            # Get nodes info
            nodes = v1.list_node()
            self.cluster_info['node_count'] = len(nodes.items)
            self.cluster_info['nodes'] = []
            for node in nodes.items:
                labels = node.metadata.labels or {}
                roles_set = set()
                for label_key, label_value in labels.items():
                    if label_key.startswith('node-role.kubernetes.io/'):
                        role_suffix = label_key.split('/', 1)[1]
                        roles_set.add(role_suffix or (label_value or 'unknown'))
                    elif label_key == 'kubernetes.io/role' and label_value:
                        roles_set.add(label_value)

                self.cluster_info['nodes'].append({
                    'name': node.metadata.name,
                    'roles': sorted(list(roles_set)),
                    'version': node.status.node_info.kubelet_version
                })
            
            # Get namespaces
            namespaces = v1.list_namespace()
            self.cluster_info['namespace_count'] = len(namespaces.items)
            
        except Exception as e:
            print(f"⚠️  Warning: Could not get cluster info: {e}")
    
    async def _discover_prometheus(self) -> None:
        """Discover Prometheus service and get access token"""
        try:
            v1 = client.CoreV1Api(self.kube_client)
            
            # Try to find Prometheus in common OpenShift namespaces
            prometheus_namespaces = [
                'openshift-monitoring',
                'monitoring',
                'prometheus',
                'kube-system'
            ]
            
            prometheus_service = None
            prometheus_namespace = None
            
            for namespace in prometheus_namespaces:
                try:
                    services = v1.list_namespaced_service(namespace=namespace)
                    for service in services.items:
                        if 'prometheus' in service.metadata.name.lower():
                            prometheus_service = service
                            prometheus_namespace = namespace
                            break
                    if prometheus_service:
                        break
                except ApiException:
                    continue
            
            if not prometheus_service:
                # Try to find thanos-querier (OpenShift 4.x)
                for namespace in prometheus_namespaces:
                    try:
                        services = v1.list_namespaced_service(namespace=namespace)
                        for service in services.items:
                            if 'thanos-querier' in service.metadata.name.lower():
                                prometheus_service = service
                                prometheus_namespace = namespace
                                break
                        if prometheus_service:
                            break
                    except ApiException:
                        continue
            
            if prometheus_service:
                # Get service details
                port = 9090  # Default Prometheus port
                if prometheus_service.spec.ports:
                    for p in prometheus_service.spec.ports:
                        if p.name in ['web', 'http', 'prometheus'] or p.port in [9090, 9091]:
                            port = p.port
                            break
                
                # Try to get route (OpenShift) or construct service URL
                if self.cluster_info.get('is_openshift', False):
                    self.prometheus_url = await self._get_openshift_route_url(
                        prometheus_service.metadata.name,
                        prometheus_namespace
                    )
                
                if not self.prometheus_url:
                    # Fallback to service URL
                    self.prometheus_url = f"http://{prometheus_service.metadata.name}.{prometheus_namespace}.svc.cluster.local:{port}"
            
            # Get service account token
            await self._get_prometheus_token()
            
        except Exception as e:
            print(f"⚠️  Warning: Could not discover Prometheus: {e}")
            # Fallback to default URLs
            self.prometheus_url = "http://prometheus-k8s.openshift-monitoring.svc.cluster.local:9090"
    
    async def _get_openshift_route_url(self, service_name: str, namespace: str) -> Optional[str]:
        """Get OpenShift route URL for Prometheus"""
        try:
            custom_api = client.CustomObjectsApi(self.kube_client)
            
            # Look for existing route
            routes = custom_api.list_namespaced_custom_object(
                group="route.openshift.io",
                version="v1",
                namespace=namespace,
                plural="routes"
            )
            
            for route in routes.get('items', []):
                if service_name in route['metadata']['name']:
                    host = route['spec'].get('host')
                    tls = route['spec'].get('tls')
                    protocol = 'https' if tls else 'http'
                    return f"{protocol}://{host}"
            
            return None
            
        except Exception as e:
            print(f"Could not get OpenShift route: {e}")
            return None
    
    async def _get_prometheus_token(self) -> None:
        """Get service account token for Prometheus access"""
        try:
            # Try to get token from service account
            v1 = client.CoreV1Api(self.kube_client)
            
            # First, try to get the current context token
            if hasattr(self.kube_client.configuration, 'api_key') and self.kube_client.configuration.api_key:
                self.prometheus_token = list(self.kube_client.configuration.api_key.values())[0]
                if self.prometheus_token.startswith('Bearer '):
                    self.prometheus_token = self.prometheus_token[7:]
                return
            
            # Try to read token from service account (in-cluster)
            token_path = '/var/run/secrets/kubernetes.io/serviceaccount/token'
            if os.path.exists(token_path):
                with open(token_path, 'r') as f:
                    self.prometheus_token = f.read().strip()
                return
            
            # Try to create a service account token
            try:
                # Create or get monitoring service account
                sa_name = "ovnk-benchmark-monitor"
                namespace = "default"
                
                try:
                    v1.read_namespaced_service_account(name=sa_name, namespace=namespace)
                except ApiException as e:
                    if e.status == 404:
                        # Create service account
                        sa_body = client.V1ServiceAccount(
                            metadata=client.V1ObjectMeta(name=sa_name)
                        )
                        v1.create_namespaced_service_account(namespace=namespace, body=sa_body)
                
                # Create token request (Kubernetes 1.24+)
                auth_v1 = client.AuthenticationV1Api(self.kube_client)
                token_request = client.V1TokenRequest(
                    spec=client.V1TokenRequestSpec(
                        expiration_seconds=3600  # 1 hour
                    )
                )
                
                token_response = auth_v1.create_service_account_token(
                    name=sa_name,
                    namespace=namespace,
                    body=token_request
                )
                
                self.prometheus_token = token_response.status.token
                
            except Exception as e:
                print(f"Could not create service account token: {e}")
                self.prometheus_token = None
            
        except Exception as e:
            print(f"⚠️  Warning: Could not get Prometheus token: {e}")
            self.prometheus_token = None
    
    async def test_prometheus_connection(self) -> bool:
        """Test connection to Prometheus"""
        if not self.prometheus_url:
            return False
        
        try:
            headers = {}
            if self.prometheus_token:
                headers['Authorization'] = f'Bearer {self.prometheus_token}'
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.prometheus_url}/api/v1/query",
                    params={'query': 'up'},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('status') == 'success'
                    return False
                    
        except Exception as e:
            print(f"Prometheus connection test failed: {e}")
            return False
    
    def get_cluster_summary(self) -> Dict[str, Any]:
        """Get a summary of cluster information"""
        return {
            'cluster_info': self.cluster_info,
            'prometheus_url': self.prometheus_url,
            'prometheus_authenticated': bool(self.prometheus_token),
            'auth_status': 'authenticated' if self.kube_client else 'not_authenticated'
        }
# Global authenticator instance
auth = OpenShiftAuth()    