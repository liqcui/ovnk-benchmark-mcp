# OVN Kubernetes Container Usage Monitoring Project

## Project Structure

```
project_root/
├── ovnk_benchmark_auth.py                              # Authentication module (provided)
├── ovnk_benchmark_prometheus_basequery.py             # Base Prometheus query module (provided)
├── metrics-base.yml                                   # Metrics configuration file (optional)
├── tools/
│   └── ovnk_benchmark_prometheus_ovnk_containers.py   # Main collector module
└── analysis/
    └── ovnk_benchmark_performance_analysis_ovnk_containers.py  # Performance analysis module
```

## Files Created

### 1. `/tools/ovnk_benchmark_prometheus_ovnk_containers.py`
**Class: `OVNKContainersUsageCollector`**

Features:
- **YAML Configuration Support**: Reads PromQL queries from `metrics-base.yml` file
- **Fallback to Hardcoded Queries**: Uses built-in PromQL if YAML file is missing/invalid
- Collects CPU, memory, and DB size metrics for OVN Kubernetes containers
- Supports both instant and duration-based collection
- Uses PrometheusBaseQuery for Prometheus integration
- Integrates with OpenShiftAuth for cluster information
- Exports results to JSON format
- Concurrent query execution for performance

Key Methods:
- `__init__(prometheus_client, metrics_config_file)` - Initialize with optional YAML file path
- `collect_instant_metrics()` - Collect metrics at a specific point in time
- `collect_duration_metrics()` - Collect metrics over a time duration with min/max/avg
- `reload_metrics_config()` - Dynamically reload configuration from YAML
- `get_metrics_summary()` - Get summary of loaded configuration
- `export_to_json()` - Export collected data to JSON file

### 2. `/analysis/ovnk_benchmark_performance_analysis_ovnk_containers.py`
**Class: `OVNKPerformanceAnalyzer`**

Features:
- Analyzes performance data from the collector
- Generates performance alerts with severity levels
- Identifies top resource consumers
- Calculates cluster health score (0-100)
- Provides container-specific analysis
- Generates human-readable performance reports
- Configurable thresholds for different resource types

### 3. `metrics-base.yml` - Configuration File
**Format**: YAML with metrics definitions

Features:
- **Flexible Configuration**: Define custom PromQL queries
- **Automatic Fallback**: Uses hardcoded queries if file missing
- **Dynamic Reloading**: Can be reloaded at runtime
- **Rich Metadata**: Supports metricName, unit, description fields

## Metrics Configuration Priority

1. **YAML File** (`metrics-base.yml`): Primary source if file exists and is valid
2. **Hardcoded Queries**: Fallback if YAML file is missing, empty, or invalid
3. **Runtime Reload**: Configuration can be reloaded without restarting

## YAML Configuration Format

```yaml
metrics:
  - query: 'your_promql_query_here'
    metricName: YOUR_METRIC_NAME
    unit: bytes|percentage|count|etc
    description: "# OVN Kubernetes Container Usage Monitoring Project

## Project Structure

```
project_root/
├── ovnk_benchmark_auth.py                              # Authentication module (provided)
├── ovnk_benchmark_prometheus_basequery.py             # Base Prometheus query module (provided)
├── tools/
│   └── ovnk_benchmark_prometheus_ovnk_containers.py   # Main collector module
└── analysis/
    └── ovnk_benchmark_performance_analysis_ovnk_containers.py  # Performance analysis module
```

## Files Created

### 1. `/tools/ovnk_benchmark_prometheus_ovnk_containers.py`
**Class: `OVNKContainersUsageCollector`**

Features:
- Collects CPU, memory, and DB size metrics for OVN Kubernetes containers
- Supports both instant and duration-based collection
- Uses PrometheusBaseQuery for Prometheus integration
- Integrates with OpenShiftAuth for cluster information
- Exports results to JSON format
- Built-in metrics configuration with default PromQL queries
- Concurrent query execution for performance

Key Methods:
- `collect_instant_metrics()` - Collect metrics at a specific point in time
- `collect_duration_metrics()` - Collect metrics over a time duration with min/max/avg
- `export_to_json()` - Export collected data to JSON file
- `get_cluster_info()` - Get cluster information via OpenShiftAuth

### 2. `/analysis/ovnk_benchmark_performance_analysis_ovnk_containers.py`
**Class: `OVNKPerformanceAnalyzer`**

Features:
- Analyzes performance data from the collector
- Generates performance alerts with severity levels
- Identifies top resource consumers
- Calculates cluster health score (0-100)
- Provides container-specific analysis
- Generates human-readable performance reports
- Configurable thresholds for different resource types

Key Methods:
- `analyze_performance_data()` - Main analysis function
- `generate_performance_report()` - Human-readable report generation
- `get_container_analysis()` - Detailed analysis for specific containers
- `export_analysis()` - Export analysis results to JSON

## Default Metrics Configuration

The collector includes these built-in PromQL queries:

### CPU Metrics
- Container CPU usage percentage: `rate(container_cpu_usage_seconds_total{pod=~"ovnkube-master.*|ovnkube-node.*", container!="POD", container!=""}[5m]) * 100`

### Memory Metrics
- Container memory RSS: `container_memory_rss{pod=~"ovnkube-master.*|ovnkube-node.*", container!="POD", container!=""}`
- Container memory working set: `container_memory_working_set_bytes{pod=~"ovnkube-master.*|ovnkube-node.*", container!="POD", container!=""}`

### OVN Database Size Metrics
- NBDB size: `ovn_db_db_size_bytes{db_name=~"OVN_Northbound"}`
- SBDB size: `ovn_db_db_size_bytes{db_name=~"OVN_Southbound"}`

### OVN Container-Specific Memory Metrics
- NBDB memory: `sum by(pod) (container_memory_rss{pod=~"ovnkube-master.*|ovnkube-node.*", container=~"nbdb"})`
- SBDB memory: `sum by(pod) (container_memory_rss{pod=~"ovnkube-master.*|ovnkube-node.*", container=~"sbdb"})`
- Northd memory: `sum by(pod) (container_memory_rss{pod=~"ovnkube-master.*|ovnkube-node.*", container=~"northd"})`
- OVN Kube Controller memory: `sum by(pod) (container_memory_rss{pod=~"ovnkube-master.*|ovnkube-node.*", container=~"ovnkube-controller"})`
- OVN Controller memory: `sum by(pod) (container_memory_rss{pod=~"ovnkube-master.*|ovnkube-node.*", container=~"ovn-controller"})`

## Usage Examples

### Basic Usage

```python
import asyncio
from ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery
from ovnk_benchmark_auth import auth
from tools.ovnk_benchmark_prometheus_ovnk_containers import OVNKContainersUsageCollector
from analysis.ovnk_benchmark_performance_analysis_ovnk_containers import OVNKPerformanceAnalyzer

async def main():
    # Initialize authentication
    await auth.initialize()
    
    # Create Prometheus client
    prometheus_client = PrometheusBaseQuery(
        prometheus_url=auth.prometheus_url,
        token=auth.prometheus_token
    )
    
    # Create collector
    collector = OVNKContainersUsageCollector(prometheus_client)
    
    # Collect instant metrics
    instant_data = await collector.collect_instant_metrics()
    
    # Collect duration metrics (last 5 minutes)
    duration_data = await collector.collect_duration_metrics("5m")
    
    # Export to JSON
    instant_file = collector.export_to_json(instant_data)
    duration_file = collector.export_to_json(duration_data)
    
    # Analyze performance
    analyzer = OVNKPerformanceAnalyzer()
    analysis = analyzer.analyze_performance_data(duration_data)
    
    # Generate report
    report = analyzer.generate_performance_report(analysis)
    print(report)
    
    # Export analysis
    analysis_file = analyzer.export_analysis(analysis)
    
    await prometheus_client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### Command Line Usage

```bash
# Run the collector
python tools/ovnk_benchmark_prometheus_ovnk_containers.py

# Analyze existing metrics file
python analysis/ovnk_benchmark_performance_analysis_ovnk_containers.py metrics_file.json
```

## JSON Output Format

### Collector Output
```json
{
  "collection_timestamp": "2025-08-25T10:30:00.000Z",
  "collection_type": "duration",
  "start_time": "2025-08-25T10:25:00.000Z", 
  "end_time": "2025-08-25T10:30:00.000Z",
  "duration": "5m",
  "metrics": {
    "container_cpu_usage": {
      "config": {...},
      "result_type": "duration",
      "data": [
        {
          "pod_name": "ovnkube-master-xyz",
          "container_name": "nbdb",
          "min": 2.5,
          "max": 15.8,
          "avg": 8.2,
          "unit": "percentage",
          "sample_count": 20,
          "labels": {...}
        }
      ]
    }
  },
  "summary": {
    "total_metrics_collected": 10,
    "successful_metrics": 9,
    "failed_metrics": 1,
    "top_usage": {
      "cpu": [...],
      "memory": [...],
      "db_size": [...]
    },
    "containers_summary": {...},
    "db_summary": {...}
  }
}
```

### Analysis Output
```json
{
  "analysis_timestamp": "2025-08-25T10:31:00.000Z",
  "total_containers": 25,
  "total_pods": 8,
  "cluster_health_score": 87.5,
  "performance_alerts": [
    {
      "severity": "high",
      "metric_type": "memory",
      "pod_name": "ovnkube-master-abc",
      "container_name": "nbdb",
      "message": "High memory usage detected",
      "current_value": 1536000000,
      "threshold": 1073741824,
      "unit": "bytes",
      "timestamp": "2025-08-25T10:31:00.000Z"
    }
  ],
  "top_performers": {...},
  "resource_utilization": {...},
  "recommendations": [
    "📊 OVN Database containers showing high memory usage. Consider OVN database compaction or cleanup.",
    "⚖️ Resource imbalance detected in pods: ovnkube-node-xyz. Consider container resource limit tuning."
  ]
}
```

## Performance Thresholds

### CPU Usage
- High: > 80%
- Critical: > 95%

### Memory Usage  
- High: > 1GB
- Critical: > 2GB

### Database Size
- High: > 100MB
- Critical: > 500MB

## Features

1. **Concurrent Metrics Collection** - All Prometheus queries run in parallel for efficiency
2. **Flexible Time Ranges** - Support for both instant and duration-based collection
3. **Comprehensive Analysis** - Performance alerts, health scoring, and recommendations
4. **OVN-Aware** - Special handling for OVN Kubernetes components
5. **Export Capabilities** - JSON export for both metrics and analysis
6. **Human-Readable Reports** - Formatted text reports for easy consumption
7. **Error Handling** - Graceful handling of failed queries and missing data
8. **Extensible Configuration** - Easy to add new metrics or modify thresholds