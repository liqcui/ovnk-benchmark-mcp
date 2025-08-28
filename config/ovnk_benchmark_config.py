"""
Configuration module for OpenShift OVN-Kubernetes Benchmark
"""

import os
import yaml
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field, ConfigDict
from pathlib import Path


class MetricConfig(BaseModel):
    """Configuration for a single metric"""
    query: str
    metricName: str
    instant: Optional[bool] = False
    unit: Optional[str] = None
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


class PrometheusConfig(BaseModel):
    """Prometheus configuration"""
    url: Optional[str] = None
    token: Optional[str] = None
    timeout: int = 30
    verify_ssl: bool = False
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


class DatabaseConfig(BaseModel):
    """Database configuration"""
    type: str = "duckdb"
    path: str = "storage/ovnk_benchmark.db"
    backup_path: Optional[str] = "storage/backups/"
    retention_days: int = 30
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


class ReportConfig(BaseModel):
    """Report generation configuration"""
    output_dir: str = "exports"
    formats: List[str] = ["excel", "pdf"]
    template_dir: str = "templates"
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


class Config(BaseModel):
    """Main configuration class"""
    kubeconfig_path: Optional[str] = Field(default=None)
    metrics_file: str = Field(default="config/metrics.yml")
    prometheus: PrometheusConfig = Field(default_factory=PrometheusConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    reports: ReportConfig = Field(default_factory=ReportConfig)
    timezone: str = Field(default="UTC")
    log_level: str = Field(default="INFO")
    
    # Metrics organization
    metrics_config: Dict[str, List[MetricConfig]] = Field(default_factory=dict)
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
    
    def __init__(self, **data):
        super().__init__(**data)
        self._load_environment()
        self._load_metrics()
        self._create_directories()
    
    def _load_environment(self) -> None:
        """Load configuration from environment variables"""
        # Kubeconfig
        if not self.kubeconfig_path:
            self.kubeconfig_path = os.getenv('KUBECONFIG', os.path.expanduser('~/.kube/config'))
        
        # Prometheus
        if prometheus_url := os.getenv('PROMETHEUS_URL'):
            self.prometheus.url = prometheus_url
        if prometheus_token := os.getenv('PROMETHEUS_TOKEN'):
            self.prometheus.token = prometheus_token
        
        # Database
        if db_path := os.getenv('DATABASE_PATH'):
            self.database.path = db_path
        
        # Reports
        if output_dir := os.getenv('REPORT_OUTPUT_DIR'):
            self.reports.output_dir = output_dir
        
        # Logging
        if log_level := os.getenv('LOG_LEVEL'):
            self.log_level = log_level
    
    def _load_metrics(self) -> None:
        """Load metrics configuration from YAML file"""
        try:
            metrics_path = Path(self.metrics_file)
            if not metrics_path.exists():
                print(f"⚠️  Warning: Metrics file not found: {self.metrics_file}")
                return
            
            with open(metrics_path, 'r') as f:
                metrics_data = yaml.safe_load(f)
            
            if not isinstance(metrics_data, list):
                print(f"⚠️  Warning: Invalid metrics file format")
                return
            
            # Organize metrics by categories based on comments or metric names
            self.metrics_config = {
                'general': [],
                'api_server': [],
                'multus': [],
                'ovn_control_plane': [],
                'ovn_node': [],
                'ovn_containers': [],
                'ovs_containers': [],
                'ovn_sync': []
            }
            
            for metric_data in metrics_data:
                if not isinstance(metric_data, dict):
                    continue
                
                try:
                    metric = MetricConfig(**metric_data)
                    category = self._categorize_metric(metric)
                    self.metrics_config[category].append(metric)
                except Exception as e:
                    print(f"⚠️  Warning: Invalid metric config: {e}")
            
            print(f"✅ Loaded {sum(len(metrics) for metrics in self.metrics_config.values())} metrics")
            
        except Exception as e:
            print(f"❌ Error loading metrics configuration: {e}")
    
    def _categorize_metric(self, metric: MetricConfig) -> str:
        """Categorize metric based on name and query"""
        metric_name = metric.metricName.lower()
        query = metric.query.lower()
        
        if any(keyword in metric_name for keyword in ['pod-status', 'namespace-status']):
            return 'general'
        elif any(keyword in metric_name for keyword in ['apicalls', 'apiserver']):
            return 'api_server'
        elif 'multus' in metric_name or 'multus' in query:
            return 'multus'
        elif any(keyword in metric_name for keyword in ['ovn-control-plane', 'ovnkube-master']):
            return 'ovn_control_plane'
        elif 'ovnkube-node' in metric_name or 'ovnkube-node' in query:
            return 'ovn_node'
        elif any(keyword in metric_name for keyword in ['nbdb', 'sdb', 'northd', 'ovnkube-controller', 'ovn-controller']):
            return 'ovn_containers'
        elif any(keyword in metric_name for keyword in ['ovs-vswitchd', 'ovsdb-server', 'ovs_db']):
            return 'ovs_containers'
        elif any(keyword in metric_name for keyword in ['sync_duration', 'ready_duration']):
            return 'ovn_sync'
        else:
            return 'general'
    
    def _create_directories(self) -> None:
        """Create necessary directories"""
        directories = [
            Path(self.database.path).parent,
            Path(self.reports.output_dir),
            'storage',
            'exports',
            'logs'
        ]
        
        if self.database.backup_path:
            directories.append(Path(self.database.backup_path))
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def get_metrics_by_category(self, category: str) -> List[MetricConfig]:
        """Get metrics by category"""
        return self.metrics_config.get(category, [])
    
    def get_all_metrics(self) -> Dict[str, List[MetricConfig]]:
        """Get all metrics organized by category"""
        return self.metrics_config
    
    def validate_config(self) -> Dict[str, Any]:
        """Validate configuration and return status"""
        status = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check kubeconfig
        if self.kubeconfig_path and not Path(self.kubeconfig_path).exists():
            status['errors'].append(f"Kubeconfig file not found: {self.kubeconfig_path}")
            status['valid'] = False
        
        # Check metrics file
        if not Path(self.metrics_file).exists():
            status['errors'].append(f"Metrics file not found: {self.metrics_file}")
            status['valid'] = False
        
        # Check if we have any metrics loaded
        total_metrics = sum(len(metrics) for metrics in self.metrics_config.values())
        if total_metrics == 0:
            status['warnings'].append("No metrics loaded from configuration")
        
        return status