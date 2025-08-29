"""
OpenShift Authentication Module
Handles authentication and service discovery for OpenShift/Kubernetes clusters
"""

import os
import base64
import json
from typing import Optional, Dict, Any, List
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import aiohttp
import asyncio
import logging


class OpenShiftAuth:
    """Authentication and service discovery for OpenShift clusters"""
    
    def __init__(self, kubeconfig_path: Optional[str] = None):
        self.kubeconfig_path = kubeconfig_path or os.getenv('KUBECONFIG')
        self.kube_client: Optional[client.ApiClient] = None
        self.prometheus_url: Optional[str] = None
        self.prometheus_token: Optional[str] = None
        self.cluster_info: Dict[str, Any] = {}
        self.prometheus_alt_urls: List[str] = []
        
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
            self.prometheus_alt_urls = []
            
            for namespace in prometheus_namespaces:
                try:
                    services = v1.list_namespaced_service(namespace=namespace)
                    for service in services.items:
                        if 'prometheus' in service.metadata.name.lower():
                            prometheus_service = service
                            prometheus_namespace = namespace
                            # Collect candidate in-cluster URLs (prefer non-OAuth services)
                            try:
                                svc_name = service.metadata.name
                                # Derive ports
                                ports = [p.port for p in (service.spec.ports or [])]
                                # Heuristic: prefer 9090 (Prometheus web) if present
                                candidate_ports = []
                                if 9090 in ports:
                                    candidate_ports.append(9090)
                                candidate_ports.extend([p for p in ports if p not in candidate_ports])
                                for port in candidate_ports:
                                    url = f"http://{svc_name}.{namespace}.svc.cluster.local:{port}"
                                    if url not in self.prometheus_alt_urls:
                                        self.prometheus_alt_urls.append(url)
                            except Exception:
                                pass
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
                                # Collect candidate in-cluster URLs
                                try:
                                    svc_name = service.metadata.name
                                    ports = [p.port for p in (service.spec.ports or [])]
                                    candidate_ports = []
                                    if 9090 in ports:
                                        candidate_ports.append(9090)
                                    candidate_ports.extend([p for p in ports if p not in candidate_ports])
                                    for port in candidate_ports:
                                        url = f"http://{svc_name}.{namespace}.svc.cluster.local:{port}"
                                        if url not in self.prometheus_alt_urls:
                                            self.prometheus_alt_urls.append(url)
                                except Exception:
                                    pass
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
                    route_url = await self._get_openshift_route_url(
                        prometheus_service.metadata.name,
                        prometheus_namespace
                    )
                    # Decide whether to prefer internal service over Route
                    prefer_internal = os.getenv('OVNK_PREFER_INTERNAL_PROMETHEUS', 'true').lower() in ['1', 'true', 'yes']
                    if prefer_internal and self.prometheus_alt_urls:
                        self.prometheus_url = self.prometheus_alt_urls[0]
                    else:
                        self.prometheus_url = route_url
                
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
    
    def _create_token_request_object(self, expiration_seconds: int = 3600):
        """Create a TokenRequest object using dynamic approach"""
        # Try multiple ways to create the TokenRequest object
        
        # Method 1: Direct class access
        try:
            return client.V1TokenRequest(
                spec=client.V1TokenRequestSpec(expiration_seconds=expiration_seconds)
            )
        except (AttributeError, NameError):
            pass
        
        # Method 2: Via client.models
        try:
            return client.models.V1TokenRequest(
                spec=client.models.V1TokenRequestSpec(expiration_seconds=expiration_seconds)
            )
        except (AttributeError, NameError):
            pass
        
        # Method 3: Via authentication_v1 models
        try:
            from kubernetes.client.models import V1TokenRequest, V1TokenRequestSpec
            return V1TokenRequest(
                spec=V1TokenRequestSpec(expiration_seconds=expiration_seconds)
            )
        except ImportError:
            pass
        
        # Method 4: Dynamic object creation (fallback)
        try:
            # Create the objects dynamically using dict-like structure
            token_request = {
                'apiVersion': 'authentication.k8s.io/v1',
                'kind': 'TokenRequest',
                'spec': {
                    'expirationSeconds': expiration_seconds
                }
            }
            return token_request
        except Exception:
            pass
        
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
            
            # Try to create a service account token using multiple approaches
            try:
                # Create or get monitoring service account
                sa_name = "ovnk-benchmark-monitor"
                namespace = "default"

                try:
                    v1.read_namespaced_service_account(name=sa_name, namespace=namespace)
                except ApiException as e:
                    if e.status == 404:
                        sa_body = client.V1ServiceAccount(
                            metadata=client.V1ObjectMeta(name=sa_name)
                        )
                        v1.create_namespaced_service_account(namespace=namespace, body=sa_body)

                # Try to create token request with multiple approaches
                token_request = self._create_token_request_object(3600)
                
                if token_request is None:
                    print("⚠️  TokenRequest API not available in this kubernetes client version")
                    # Fallback: try to find existing secrets
                    await self._get_token_from_secrets(sa_name, namespace, v1)
                    return
                
                # Create the token request
                try:
                    auth_v1 = client.AuthenticationV1Api(self.kube_client)
                    
                    if isinstance(token_request, dict):
                        # Handle dynamic object creation
                        import urllib3
                        import yaml
                        
                        # Convert to proper API call
                        response = self.kube_client.call_api(
                            resource_path=f'/api/v1/namespaces/{namespace}/serviceaccounts/{sa_name}/token',
                            method='POST',
                            body=token_request,
                            header_params={'Content-Type': 'application/json'},
                            response_type='object'
                        )
                        if response and len(response) > 0:
                            token_data = response[0]
                            if hasattr(token_data, 'status') and hasattr(token_data.status, 'token'):
                                self.prometheus_token = token_data.status.token
                            elif isinstance(token_data, dict) and 'status' in token_data:
                                self.prometheus_token = token_data['status'].get('token')
                    else:
                        # Use standard API
                        token_response = auth_v1.create_service_account_token(
                            name=sa_name,
                            namespace=namespace,
                            body=token_request
                        )
                        self.prometheus_token = token_response.status.token
                        
                except Exception as e:
                    print(f"Failed to create token via TokenRequest API: {e}")
                    # Fallback to secret-based approach
                    await self._get_token_from_secrets(sa_name, namespace, v1)

            except Exception as e:
                print(f"Could not create service account or token: {e}")
                self.prometheus_token = None
            
        except Exception as e:
            print(f"⚠️  Warning: Could not get Prometheus token: {e}")
            self.prometheus_token = None
    
    async def _get_token_from_secrets(self, sa_name: str, namespace: str, v1_api) -> None:
        """Fallback method: get token from service account secrets (older K8s versions)"""
        try:
            # Get service account
            service_account = v1_api.read_namespaced_service_account(name=sa_name, namespace=namespace)
            
            # Look for token secrets
            if hasattr(service_account, 'secrets') and service_account.secrets:
                for secret_ref in service_account.secrets:
                    try:
                        secret = v1_api.read_namespaced_secret(
                            name=secret_ref.name,
                            namespace=namespace
                        )
                        
                        if secret.type == 'kubernetes.io/service-account-token':
                            token_data = secret.data.get('token')
                            if token_data:
                                self.prometheus_token = base64.b64decode(token_data).decode('utf-8')
                                print("✅ Retrieved token from service account secret")
                                return
                    except Exception as e:
                        print(f"Could not read secret {secret_ref.name}: {e}")
                        continue
            
            # If no existing secrets, try to create a token secret (pre-1.24 K8s)
            await self._create_token_secret(sa_name, namespace, v1_api)
            
        except Exception as e:
            print(f"Could not get token from secrets: {e}")
    
    async def _create_token_secret(self, sa_name: str, namespace: str, v1_api) -> None:
        """Create a token secret for service account (pre-K8s 1.24)"""
        try:
            secret_name = f"{sa_name}-token"
            
            # Check if secret already exists
            try:
                existing_secret = v1_api.read_namespaced_secret(name=secret_name, namespace=namespace)
                token_data = existing_secret.data.get('token')
                if token_data:
                    self.prometheus_token = base64.b64decode(token_data).decode('utf-8')
                    print("✅ Retrieved token from existing token secret")
                    return
            except ApiException as e:
                if e.status != 404:
                    raise
            
            # Create token secret
            secret_body = client.V1Secret(
                metadata=client.V1ObjectMeta(
                    name=secret_name,
                    annotations={
                        'kubernetes.io/service-account.name': sa_name
                    }
                ),
                type='kubernetes.io/service-account-token'
            )
            
            created_secret = v1_api.create_namespaced_secret(namespace=namespace, body=secret_body)
            print(f"Created token secret: {created_secret.metadata.name}")
            
            # Wait a bit for the token to be populated
            await asyncio.sleep(2)
            
            # Retrieve the token
            secret = v1_api.read_namespaced_secret(name=secret_name, namespace=namespace)
            token_data = secret.data.get('token')
            if token_data:
                self.prometheus_token = base64.b64decode(token_data).decode('utf-8')
                print("✅ Successfully created and retrieved token from secret")
            else:
                print("⚠️  Token secret created but token not yet populated")
                
        except Exception as e:
            print(f"Could not create token secret: {e}")
    
    async def test_prometheus_connection(self) -> bool:
        """Test connection to Prometheus"""
        if not self.prometheus_url:
            return False
        
        try:
            headers = {}
            if self.prometheus_token:
                headers['Authorization'] = f'Bearer {self.prometheus_token}'
            
            # Disable SSL verification to allow self-signed certificate chains
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                logging.getLogger(__name__).debug(
                    f"Testing Prometheus connection url={self.prometheus_url}/api/v1/query headers_keys={list(headers.keys())} ssl_verification=False"
                )
                async with session.get(
                    f"{self.prometheus_url}/api/v1/query",
                    params={'query': 'up'},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    logging.getLogger(__name__).debug(
                        f"Prometheus test response status={response.status}"
                    )
                    if response.status == 200:
                        text = await response.text()
                        logging.getLogger(__name__).debug(
                            f"Prometheus test response body_snippet={text[:500]}"
                        )
                        try:
                            data = json.loads(text)
                        except Exception:
                            logging.getLogger(__name__).warning("Prometheus test: failed to parse JSON, returning False")
                            return False
                        return data.get('status') == 'success'
                    # Handle 403 by trying alternate internal endpoints if any
                    if response.status == 403 and self.prometheus_alt_urls:
                        logging.getLogger(__name__).warning(
                            f"Prometheus route access forbidden (403). Trying internal endpoints: {self.prometheus_alt_urls}"
                        )
                        for alt in self.prometheus_alt_urls:
                            try:
                                async with session.get(
                                    f"{alt}/api/v1/query",
                                    params={'query': 'up'},
                                    headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=10)
                                ) as alt_resp:
                                    if alt_resp.status == 200:
                                        alt_text = await alt_resp.text()
                                        try:
                                            alt_data = json.loads(alt_text)
                                            if alt_data.get('status') == 'success':
                                                self.prometheus_url = alt
                                                logging.getLogger(__name__).info(f"Switched Prometheus URL to internal endpoint: {alt}")
                                                return True
                                        except Exception:
                                            continue
                            except Exception:
                                continue
                        return False
                    return False
                    
        except Exception as e:
            print(f"Prometheus connection test failed: {e}")
            return False
    
    async def test_kubeapi_connection(self) -> bool:
        """Test connectivity to the Kubernetes API server"""
        try:
            if not self.kube_client:
                if self.kubeconfig_path and os.path.exists(self.kubeconfig_path):
                    config.load_kube_config(config_file=self.kubeconfig_path)
                elif os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount/token'):
                    config.load_incluster_config()
                else:
                    config.load_kube_config()
                self.kube_client = client.ApiClient()

            v1 = client.CoreV1Api(self.kube_client)
            v1.list_namespace(limit=1)
            return True
        except Exception as e:
            print(f"KubeAPI connection test failed: {e}")
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