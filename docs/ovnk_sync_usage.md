# OVN-Kubernetes Sync Duration Monitoring

A comprehensive monitoring and analysis solution for OVN-Kubernetes sync duration metrics. This toolset collects, analyzes, and reports on the performance of OVN-Kubernetes components in OpenShift/Kubernetes clusters.

## 📁 File Structure

```
├── tools/
│   ├── ovnk_benchmark_auth.py                    # Authentication module (provided)
│   ├── ovnk_benchmark_prometheus_basequery.py    # Prometheus base query (provided)
│   └── ovnk_benchmark_prometheus_ovnk_sync.py    # Main collector module
├── analysis/
│   └── ovnk_benchmark_performance_analysis_ovnk_sync.py  # Analysis module
├── metrics.yaml                                   # Metrics configuration
├── usage_example.py                              # Usage demonstration
└── README.md                                     # This file
```

## 🚀 Features

### Data Collection (`OVNSyncDurationCollector`)
- **Instant Metrics**: Collect current sync duration metrics
- **Duration Metrics**: Collect metrics over specified time periods
- **Flexible Configuration**: Support for custom metrics via `metrics.yaml`
- **Node Information**: Automatically retrieves pod-to-node mappings
- **Error Handling**: Robust error handling and fallbacks

### Performance Analysis (`OVNKubeSyncDurationAnalyzer`)
- **Statistical Analysis**: Mean, median, percentiles, standard deviation
- **Performance Classification**: Excellent, Good, Moderate, Poor, Critical
- **Outlier Detection**: Identify unusual performance patterns
- **Anomaly Detection**: Detect performance spikes and degradations
- **Component-Specific Thresholds**: Different performance expectations per component
- **Comprehensive Reports**: JSON reports with actionable recommendations

### Monitoring (`OVNKubePerformanceMonitor`)
- **Combined Workflow**: Seamlessly combines collection and analysis
- **Automated Reporting**: Generates comprehensive performance reports
- **File Management**: Automatic timestamped file saving
- **Console Summaries**: Human-readable performance summaries

## 📊 Monitored Metrics

### Default Metrics (when no `metrics.yaml` is provided):

1. **Controller Ready Duration**
   ```promql
   topk(10, ovnkube_controller_ready_duration_seconds)
   ```
   - Measures time for ovnkube-controller pods to become ready
   - Unit: seconds

2. **Node Ready Duration**
   ```promql
   topk(10, ovnkube_node_ready_duration_seconds)
   ```
   - Measures time for ovnkube-node pods to become ready
   - Unit: seconds

3. **Controller Sync Duration**
   ```promql
   ovnkube_controller_sync_duration_seconds{namespace="openshift-ovn-kubernetes"}
   ```
   - Measures duration of controller sync operations
   - Unit: seconds

### Custom Metrics Configuration

Create a `metrics.yaml` file to define custom metrics:

```yaml
metrics:
  - query: topk(10, ovnkube_controller_ready_duration_seconds)
    metricName: ovnkube_controller_ready_duration_seconds
    unit: seconds
    description: "Time taken for ovnkube-controller to become ready"
    component: controller
    
  - query: histogram_quantile(0.95, rate(ovnkube_controller_sync_duration_seconds_bucket[5m]))
    metricName: ovnkube_controller_sync_duration_p95
    unit: seconds
    description: "95th percentile of controller sync duration"
    component: controller
```

## 🛠️ Installation & Setup

### Prerequisites

- Python 3.8+
- Access to OpenShift/Kubernetes cluster
- Prometheus with OVN-Kubernetes metrics
- Required Python packages:
  ```bash
  pip install kubernetes aiohttp pyyaml numpy
  ```

### Configuration

1. **Authentication Setup**
   - Ensure `KUBECONFIG` environment variable is set, or
   - Place kubeconfig file in default location, or
   - Run from within a cluster pod (uses service account)

2. **Prometheus Access**
   - The tool auto-discovers Prometheus in common namespaces:
     - `openshift-monitoring`
     - `monitoring` 
     - `prometheus`
     - `kube-system`

3. **Metrics Configuration** (Optional)
   - Create `metrics.yaml` in the working directory
   - Define custom PromQL queries and metadata

## 📖 Usage Examples

### Basic Collection

```python
import asyncio
from ovnk_benchmark_auth import auth
from ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery
from ovnk_benchmark_prometheus_ovnk_sync import OVNSyncDurationCollector

async def collect_metrics():
    # Initialize authentication
    await auth.initialize()
    
    # Create Prometheus client
    prometheus_client = PrometheusBaseQuery(
        prometheus_url=auth.prometheus_url,
        token=auth.prometheus_token
    )
    
    # Create collector
    collector = OVNSyncDurationCollector(prometheus_client)
    
    try:
        # Collect instant metrics
        instant_results = await collector.collect_instant_metrics()
        collector.save_results(instant_results)
        
        # Collect duration metrics (5 minutes)
        duration_results = await collector.collect_duration_metrics('5m')
        collector.save_results(duration_results)
        
    finally:
        await prometheus_client.close()

# Run collection
asyncio.run(collect_metrics())
```

### Performance Analysis

```python
import asyncio
from analysis.ovnk_benchmark_performance_analysis_ovnk_sync import (
    OVNKubePerformanceMonitor,
    print_analysis_summary
)

async def analyze_performance():
    await auth.initialize()
    prometheus_client = PrometheusBaseQuery(
        prometheus_url=auth.prometheus_url,
        token=auth.prometheus_token
    )
    
    # Create monitor (combines collection + analysis)
    monitor = OVNKubePerformanceMonitor(prometheus_client)
    
    try:
        # Perform complete analysis
        report = await monitor.monitor_duration_performance('10m')
        
        # Display summary
        print_analysis_summary(report)
        
    finally:
        await prometheus_client.close()

asyncio.run(analyze_performance())
```

### Command Line Usage

```bash
# Run the usage example
python usage_example.py

# Run individual modules
python tools/ovnk_benchmark_prometheus_ovnk_sync.py
python analysis/ovnk_benchmark_performance_analysis_ovnk_sync.py
```

## 📊 Output Formats

### Collection Results (JSON)

```json
{
  "collection_type": "instant|duration",
  "timestamp": "2024-01-15T10:30:00.000000Z",
  "metrics": {
    "ovnkube_controller_ready_duration_seconds": {
      "unit": "seconds",
      "data": [...],
      "count": 5
    }
  },
  "top_10_sync_durations": [
    {
      "metric_name": "ovnkube_controller_ready_duration_seconds",
      "value": 2.5,
      "readable_duration": {"value": 2.5, "unit": "s"},
      "pod_name": "ovnkube-controller-abc123",
      "node_name": "worker-node-1",
      "namespace": "openshift-ovn-kubernetes"
    }
  ],
  "summary": {
    "total_metrics": 15,
    "max_duration": {...},
    "average_duration": {...}
  }
}
```

### Analysis Report (JSON)

```json
{
  "report_metadata": {
    "generated_at": "2024-01-15T10:35:00.000000Z",
    "collection_type": "duration",
    "analysis_period": {...}
  },
  "overall_assessment": {
    "overall_health": "good|moderate|poor|critical",
    "total_metrics_analyzed": 3,
    "performance_distribution": {
      "excellent": 1,
      "good": 2,
      "moderate": 0,
      "poor": 0,
      "critical": 0
    },
    "total_outliers": 2,
    "total_anomalies": 0
  },
  "metric_analyses": {...},
  "key_findings": [
    "✅ Best performing component: ovnkube_controller_sync_duration_seconds",
    "🔴 Worst performing component: ovnkube_controller_ready_duration_seconds"
  ],
  "priority_recommendations": [
    "Monitor trends to ensure performance doesn't degrade",
    "Check for intermittent issues causing occasional high durations"
  ],
  "component_health": {
    "ovnkube-controller": {
      "overall_health": "good",
      "average_p95": 0.245,
      "metrics_count": 2
    }
  }
}
```

## 🎯 Performance Thresholds

### Default Thresholds

| Component | Excellent | Good | Moderate | Poor | Critical |
|-----------|-----------|------|----------|------|----------|
| Controller Ready | < 1.0s | < 5.0s | < 15.0s | < 30.0s | ≥ 30.0s |
| Node Ready | < 0.5s | < 2.0s | < 10.0s | < 20.0s | ≥ 20.0s |
| Controller Sync | < 0.05s | < 0.2s | < 1.0s | < 5.0s | ≥ 5.0s |

### Custom Thresholds

You can customize thresholds by modifying the `OVNKubeSyncDurationAnalyzer`:

```python
from analysis.ovnk_benchmark_performance_analysis_ovnk_sync import (
    PerformanceThresholds,
    OVNKubeSyncDurationAnalyzer
)

# Create custom thresholds
custom_thresholds = PerformanceThresholds(
    excellent_threshold=0.1,
    good_threshold=0.5,
    moderate_threshold=2.0,
    poor_threshold=10.0
)

# Create analyzer with custom thresholds
analyzer = OVNKubeSyncDurationAnalyzer(thresholds=custom_thresholds)
```

## 🔍 Analysis Features

### Statistical Metrics
- **Count**: Number of measurements
- **Min/Max**: Minimum and maximum values
- **Mean/Median**: Central tendency measures
- **Standard Deviation**: Variability measure
- **Percentiles**: P50, P75, P90, P95, P99

### Performance Classification
Each measurement is classified into performance levels:
- **🟢 Excellent**: Optimal performance
- **🟡 Good**: Acceptable performance
- **🟠 Moderate**: Should be monitored
- **🔴 Poor**: Needs attention
- **🚨 Critical**: Immediate action required

### Anomaly Detection
- **Outliers**: Using IQR (Interquartile Range) method
- **Spikes**: Values exceeding 3 standard deviations
- **Sustained Issues**: Consistently high values over time

### Recommendations Engine
Generates actionable recommendations based on:
- Performance levels
- Statistical patterns
- Component-specific best practices
- Historical trends

## 🚨 Troubleshooting

### Common Issues

1. **Authentication Failed**
   ```
   Error: Could not initialize authentication
   ```
   - Ensure KUBECONFIG is set correctly
   - Check cluster connectivity
   - Verify service account permissions

2. **Prometheus Connection Failed**
   ```
   Error: Prometheus connection test failed
   ```
   - Check if Prometheus is running
   - Verify service discovery in common namespaces
   - Check network policies/firewalls

3. **No Metrics Found**
   ```
   Warning: No data found for metric
   ```
   - Verify OVN-Kubernetes is deployed
   - Check metric names in Prometheus
   - Ensure metrics are being exported

4. **Permission Denied**
   ```
   Error: 403 Forbidden
   ```
   - Check RBAC permissions
   - Ensure service account has monitoring access
   - Verify token validity

### Debug Mode

Enable debug output by setting environment variable:
```bash
export DEBUG=1
python tools/ovnk_benchmark_prometheus_ovnk_sync.py
```

## 🔧 Advanced Configuration

### Custom Service Account

Create a dedicated service account with monitoring permissions:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ovnk-benchmark-monitor
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ovnk-benchmark-monitor
rules:
- apiGroups: [""]
  resources: ["pods", "nodes", "services"]
  verbs: ["get", "list"]
- apiGroups: ["config.openshift.io"]
  resources: ["clusterversions"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ovnk-benchmark-monitor
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ovnk-benchmark-monitor
subjects:
- kind: ServiceAccount
  name: ovnk-benchmark-monitor
  namespace: default
```

### Prometheus Configuration

Ensure Prometheus is scraping OVN-Kubernetes metrics:

```yaml
- job_name: 'ovn-kubernetes'
  kubernetes_sd_configs:
  - role: pod
  relabel_configs:
  - source_labels: [__meta_kubernetes_pod_label_app]
    action: keep
    regex: ovnkube-.*
  - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
    action: keep
    regex: true
```

## 📈 Integration Examples

### Grafana Dashboard

Create alerts based on analysis results:

```json
{
  "alert": {
    "name": "OVN Sync Duration High",
    "condition": "avg(ovnkube_controller_sync_duration_seconds) > 1.0",
    "frequency": "10s",
    "executionErrorState": "alerting",
    "noDataState": "no_data",
    "for": "5m"
  }
}
```

### Monitoring Pipeline

Integrate with monitoring systems:

```bash
#!/bin/bash
# monitoring_pipeline.sh

# Run collection and analysis
python usage_example.py

# Process results
LATEST_REPORT=$(ls -t ovnk_sync_analysis_*.json | head -n1)

# Extract health status
HEALTH=$(jq -r '.overall_assessment.overall_health' $LATEST_REPORT)

# Send alerts if needed
if [[ "$HEALTH" == "critical" ]] || [[ "$HEALTH" == "poor" ]]; then
    # Send alert to monitoring system
    curl -X POST "https://monitoring-system/alert" \
         -H "Content-Type: application/json" \
         -d '{"status": "'$HEALTH'", "report": "'$LATEST_REPORT'"}'
fi
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Update documentation
6. Submit a pull request

## 📄 License

This project is licensed under the Apache License 2.0.

## 🔗 Related Projects

- [OVN-Kubernetes](https://github.com/ovn-org/ovn-kubernetes)
- [OpenShift Container Platform](https://www.openshift.com/)
- [Prometheus](https://prometheus.io/)
- [Kubernetes](https://kubernetes.io/)

---

For questions, issues, or contributions, please refer to the project repository.