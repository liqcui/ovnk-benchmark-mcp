# OVS Performance Monitoring and Analysis System

A comprehensive monitoring and analysis system for Open vSwitch (OVS) performance in OpenShift/Kubernetes environments.

## 🏗️ System Architecture

```
┌─────────────────────┐    ┌──────────────────────┐    ┌─────────────────────┐
│   OpenShift/K8s     │    │   Prometheus         │    │   OVS Monitoring    │
│   ┌─────────────┐   │    │   ┌──────────────┐   │    │   ┌─────────────┐   │
│   │ OVS Nodes   │───┼────┼──▶│   Metrics    │───┼────┼──▶│  Collector  │   │
│   │             │   │    │   │   Storage    │   │    │   │             │   │
│   └─────────────┘   │    │   └──────────────┘   │    │   └─────────────┘   │
│                     │    │                      │    │          │          │
│   ┌─────────────┐   │    │   ┌──────────────┐   │    │   ┌─────▼─────────┐ │
│   │ OVSDB       │───┼────┼──▶│   PromQL     │   │    │   │   Analyzer    │ │
│   │ Servers     │   │    │   │   Queries    │   │    │   │               │ │
│   └─────────────┘   │    │   └──────────────┘   │    │   └───────────────┘ │
└─────────────────────┘    └──────────────────────┘    └─────────────────────┘
```

## 📋 Components

### 1. Core Modules

- **`ovnk_benchmark_prometheus_basequery.py`**: Base Prometheus query functionality
- **`ovnk_benchmark_auth.py`**: OpenShift/Kubernetes authentication and service discovery
- **`ovnk_benchmark_prometheus_ovnk_ovs.py`**: OVS metrics collector (main implementation)
- **`ovnk_benchmark_performance_analysis_ovs.py`**: Performance analysis and alerting

### 2. Configuration

- **`metrics.yaml`**: Metrics configuration with PromQL queries and thresholds
- **Main script**: Orchestrator for running various analysis modes

## 🚀 Features

### Metrics Collected

#### CPU Usage
- ✅ OVS vswitchd CPU usage per node
- ✅ OVSDB server CPU usage per node
- ✅ Top 10 highest CPU consumers
- ✅ Min/Max/Average statistics

#### Memory Usage
- ✅ OVS database process memory (with unit conversion)
- ✅ OVS vswitchd process memory (with unit conversion)
- ✅ Top 10 highest memory consumers
- ✅ Automatic unit conversion (bytes → KB/MB/GB)

#### Flow Metrics
- ✅ Datapath flows total (`ovs_vswitchd_dp_flows_total`)
- ✅ Bridge flows for br-int and br-ex
- ✅ Flow efficiency analysis
- ✅ Top 10 instances by flow count

#### Connection Health
- ✅ Stream connections open
- ✅ Connection overflows
- ✅ Discarded connections
- ✅ Connection efficiency metrics

### Analysis Capabilities

#### Performance Analysis
- 🚨 **Alerting**: Critical/Warning/Info levels
- 📊 **Insights**: High-confidence performance insights
- 📈 **Trends**: Variance analysis and growth patterns
- 🎯 **Recommendations**: Actionable optimization suggestions

#### Alert Categories
- **CPU Usage**: High utilization alerts with thresholds
- **Memory Usage**: Memory leak detection and growth alerts
- **Flow Table**: Flow table size optimization alerts
- **Connection Health**: Network connectivity issue detection

## 🛠️ Installation and Setup

### Prerequisites

```bash
# Required Python packages
pip install aiohttp asyncio kubernetes pyyaml statistics
```

### File Structure

```
project/
├── tools/
│   ├── ovnk_benchmark_prometheus_basequery.py
│   ├── ovnk_benchmark_auth.py
│   └── ovnk_benchmark_prometheus_ovnk_ovs.py
├── analysis/
│   └── ovnk_benchmark_performance_analysis_ovs.py
├── metrics.yaml
└── main.py  # Main orchestrator script
```

### Configuration

1. **Kubeconfig Setup**:
   ```bash
   export KUBECONFIG=/path/to/your/kubeconfig
   ```

2. **Metrics Configuration**:
   - Place `metrics.yaml` in the project root
   - Customize thresholds and queries as needed

## 📖 Usage

### Command Line Interface

#### Instant Analysis (Point-in-time)
```bash
python main.py --mode instant --output ovs_report.json
```

#### Duration Analysis (Time Range)
```bash
# 5-minute analysis
python main.py --mode duration --duration 5m --output ovs_5min_report.json

# 1-hour analysis
python main.py --mode duration --duration 1h --output ovs_1hour_report.json

# 1-day analysis
python main.py --mode duration --duration 1d --output ovs_daily_report.json
```

#### Individual Metrics
```bash
# CPU metrics only
python main.py --mode metric --metric cpu --duration 5m

# Memory metrics only  
python main.py --mode metric --metric memory --duration 10m

# Flow metrics only
python main.py --mode metric --metric dp_flows

# Bridge flow metrics
python main.py --mode metric --metric bridge_flows

# Connection metrics
python main.py --mode metric --metric connections
```

#### Continuous Monitoring
```bash
# Monitor every 5 minutes with 2-minute duration windows
python main.py --mode monitor --interval 300 --duration 2m

# Limited monitoring (10 iterations)
python main.py --mode monitor --interval 300 --duration 2m --max-iterations 10
```

### Programmatic Usage

#### Basic Usage
```python
import asyncio
from ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery
from ovnk_benchmark_auth import OpenShiftAuth
from ovnk_benchmark_prometheus_ovnk_ovs import OVSUsageCollector
from ovnk_benchmark_performance_analysis_ovs import OVSPerformanceAnalyzer

async def main():
    # Initialize authentication
    auth = OpenShiftAuth()
    await auth.initialize()
    
    # Create Prometheus client
    async with PrometheusBaseQuery(auth.prometheus_url, auth.prometheus_token) as prometheus_client:
        # Initialize OVS collector
        ovs_collector = OVSUsageCollector(prometheus_client, auth)
        
        # Collect metrics
        cpu_metrics = await ovs_collector.query_ovs_cpu_usage(duration="5m")
        memory_metrics = await ovs_collector.query_ovs_memory_usage(duration="5m")
        
        # Analyze performance
        analyzer = OVSPerformanceAnalyzer()
        analysis = analyzer.analyze_comprehensive_ovs_metrics({
            'cpu_usage': cpu_metrics,
            'memory_usage': memory_metrics
        })
        
        print(f"Performance Status: {analysis['performance_summary']['overall_status']}")

asyncio.run(main())
```

#### Advanced Usage
```python
async def advanced_monitoring():
    auth = OpenShiftAuth()
    await auth.initialize()
    
    async with PrometheusBaseQuery(auth.prometheus_url, auth.prometheus_token) as prometheus_client:
        ovs_collector = OVSUsageCollector(prometheus_client, auth)
        
        # Comprehensive metrics collection
        all_metrics = await ovs_collector.collect_all_ovs_metrics(duration="10m")
        
        # Detailed analysis
        analyzer = OVSPerformanceAnalyzer()
        detailed_analysis = analyzer.analyze_comprehensive_ovs_metrics(all_metrics)
        
        # Process alerts
        for alert in detailed_analysis['detailed_alerts']:
            if alert['level'] == 'critical':
                print(f"🚨 CRITICAL: {alert['message']}")
                print(f"💡 Recommendation: {alert['recommendation']}")
```

## 📊 Output Examples

### Instant Analysis Output
```json
{
  "report_type": "instant_analysis",
  "timestamp": "2025-08-27T10:30:00.000Z",
  "cluster_info": {
    "cluster_info": {
      "openshift_version": "4.12.0",
      "node_count": 6,
      "is_openshift": true
    }
  },
  "performance_analysis": {
    "overall_status": "good",
    "summary_metrics": {
      "total_alerts": 2,
      "critical_alerts": 0,
      "warning_alerts": 2,
      "total_insights": 3
    },
    "top_issues": [
      {
        "level": "warning",
        "message": "High CPU usage on node worker-1: 78.5%",
        "recommendation": "Monitor closely and consider optimization if trend continues"
      }
    ]
  }
}
```

### CPU Usage Top 10 Output
```json
{
  "ovs_vswitchd_cpu": [
    {
      "node_name": "worker-1",
      "min": 45.2,
      "avg": 67.8,
      "max": 89.5,
      "unit": "%"
    },
    {
      "node_name": "worker-2", 
      "min": 12.1,
      "avg": 25.3,
      "max": 38.7,
      "unit": "%"
    }
  ],
  "summary": {
    "ovs_vswitchd_top10": [
      {
        "node_name": "worker-1",
        "max": 89.5,
        "unit": "%"
      }
    ]
  }
}
```

### Memory Usage with Unit Conversion
```json
{
  "ovs_db_memory": [
    {
      "pod_name": "ovs-db-worker-1",
      "min": 256.5,
      "avg": 445.2, 
      "max": 678.9,
      "unit": "MB"
    }
  ],
  "ovs_vswitchd_memory": [
    {
      "pod_name": "ovs-vswitchd-worker-1",
      "min": 0.5,
      "avg": 0.74,
      "max": 1.02,
      "unit": "GB"
    }
  ]
}
```

## 🔧 Configuration Options

### metrics.yaml Configuration

```yaml
metrics:
  - query: sum by(node) (irate(container_cpu_usage_seconds_total{id=~"/system.slice/ovs-vswitchd.service"}[5m])*100)
    metricName: ovs-vswitchd-cpu-usage
    unit: percent
    threshold:
      warning: 70
      critical: 85

global:
  scrape_interval: 15s
  query_timeout: 30s

analysis:
  cpu_thresholds:
    warning_percent: 70
    critical_percent: 85
  memory_thresholds:
    warning_mb: 500
    critical_mb: 1000
```

### Performance Thresholds

#### CPU Thresholds
- **Warning**: 70% CPU usage
- **Critical**: 85% CPU usage

#### Memory Thresholds  
- **Warning**: 500 MB memory usage
- **Critical**: 1 GB memory usage

#### Flow Thresholds
- **Datapath Warning**: 5,000 flows
- **Datapath Critical**: 20,000 flows
- **Bridge Warning**: 10,000 flows  
- **Bridge Critical**: 50,000 flows

#### Connection Thresholds
- **Overflow Warning**: 100 events
- **Overflow Critical**: 1,000 events
- **Discarded Warning**: 50 connections
- **Discarded Critical**: 500 connections

## 🚨 Alert Types and Recommendations

### CPU Usage Alerts
- **High vswitchd CPU**: Review flow rules, optimize table structure
- **High OVSDB CPU**: Check database size, client connections
- **CPU Variance**: Investigate traffic patterns, flow optimization

### Memory Usage Alerts  
- **Memory Growth**: Check for leaks, review cleanup policies
- **High Memory**: Optimize flow tables, database maintenance
- **Memory Spikes**: Monitor traffic patterns, review caching

### Flow Table Alerts
- **High Flow Count**: Optimize rules, review segmentation
- **Flow Efficiency**: Check megaflow cache, rule conflicts
- **Bridge Imbalance**: Review network policies, traffic distribution

### Connection Alerts
- **Overflow Events**: Check controller connectivity, reduce load  
- **Discarded Connections**: Review network stability, controller health
- **High Overflow Rate**: Tune connection buffers, check processing capacity

## 📈 Performance Insights

### Insight Categories

#### CPU Variance Analysis
- Detects inconsistent CPU usage patterns
- Identifies potential traffic spikes or processing inefficiencies
- Confidence scoring based on variance magnitude

#### Memory Growth Tracking  
- Monitors memory usage trends over time
- Detects potential memory leaks or capacity issues
- Tracks growth rates and patterns

#### Flow Efficiency Analysis
- Analyzes datapath to bridge flow ratios
- Identifies megaflow cache inefficiencies  
- Detects flow rule optimization opportunities

#### Connection Health Monitoring
- Tracks connection stability metrics
- Identifies controller communication issues
- Monitors overflow and discard rates

## 🔍 Troubleshooting

### Common Issues

#### Authentication Problems
```bash
# Check kubeconfig
kubectl config current-context

# Verify cluster access  
kubectl get nodes

# Test Prometheus connectivity
kubectl get pods -n openshift-monitoring | grep prometheus
```

#### Prometheus Connection Issues
```bash
# Check Prometheus service
kubectl get svc -n openshift-monitoring | grep prometheus

# Test route accessibility (OpenShift)
oc get route -n openshift-monitoring | grep prometheus

# Verify token permissions
kubectl auth can-i get --subresource=metrics '*' --all-namespaces
```

#### Missing Metrics
```bash
# Check OVS pods
kubectl get pods -A | grep ovs

# Verify OVS metrics exporters
kubectl get pods -n openshift-ovn-kubernetes | grep ovs

# Check metric endpoints
curl http://prometheus-url/api/v1/label/__name__/values | grep ovs
```

### Debug Mode

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Run with verbose output
python main.py --mode instant --output debug_report.json
```

## 🔄 Integration Options

### Grafana Dashboard
- Import provided dashboard configuration from `metrics.yaml`
- Visualize real-time OVS performance metrics
- Set up alerting rules for critical thresholds

### Prometheus AlertManager
- Use alerting rules from `metrics.yaml`  
- Configure notification channels (Slack, email, PagerDuty)
- Set up escalation policies for critical alerts

### CI/CD Integration
```yaml
# GitHub Actions example
- name: OVS Performance Check
  run: |
    python main.py --mode duration --duration 5m --output ovs_check.json
    # Parse results and fail if critical alerts found
```

### Automation Scripts
```bash
#!/bin/bash
# Daily OVS health check
python main.py --mode duration --duration 1h --output "daily_$(date +%Y%m%d).json"

# Alert on critical issues
if grep -q '"level": "critical"' daily_*.json; then
    echo "🚨 Critical OVS issues detected!" 
    # Send notifications
fi
```

## 📚 API Reference

### OVSUsageCollector Methods

#### `query_ovs_cpu_usage(duration=None, time=None)`
- Collects CPU usage for OVS components
- Returns top 10 consumers with min/avg/max stats
- Supports both instant and range queries

#### `query_ovs_memory_usage(duration=None, time=None)`  
- Collects memory usage with automatic unit conversion
- Returns pod-level memory statistics
- Provides top 10 memory consumers

#### `query_ovs_dp_flows_total(duration=None, time=None)`
- Queries datapath flow counts
- Returns per-instance flow statistics
- Includes top 10 flow consumers

#### `query_ovs_bridge_flows_total(duration=None, time=None)`
- Queries bridge-specific flow counts (br-int, br-ex)
- Provides separate top 10 lists per bridge
- Includes flow efficiency metrics

#### `query_ovs_connection_metrics(duration=None, time=None)`
- Collects connection health metrics
- Monitors overflows, discarded connections
- Tracks stream connection counts

### OVSPerformanceAnalyzer Methods

#### `analyze_comprehensive_ovs_metrics(metrics_data)`
- Performs complete analysis of all collected metrics
- Returns alerts, insights, and recommendations
- Provides overall performance status assessment

## 🤝 Contributing

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Development Guidelines

- Follow Python PEP 8 style guidelines
- Add unit tests for new functionality
- Update documentation for API changes
- Test with multiple OpenShift/Kubernetes versions

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

- **Issues**: Report bugs and feature requests via GitHub Issues
- **Discussions**: Join community discussions for questions and ideas
- **Documentation**: Check the wiki for additional examples and tutorials

## 🏷️ Version History

- **v1.0.0**: Initial release with comprehensive OVS monitoring
- **v1.1.0**: Added performance analysis and alerting
- **v1.2.0**: Enhanced metrics configuration and dashboard integration

---

**📊 Start monitoring your OVS performance today!** 

```bash
python main.py --mode instant
```

CPU Usage Monitoring (query_ovs_cpu_usage)

✅ Min/Max/Average for ovs-vswitchd-cpu-usage and ovsdb-server-cpu-usage
✅ Per-node breakdown with top 10 ranking
✅ Supports both instant and duration queries


Memory Usage Monitoring (query_ovs_memory_usage)

✅ ovs_db_process_resident_memory_bytes and ovs_vswitchd_process_resident_memory_bytes
✅ Automatic unit conversion (bytes → KB/MB/GB)
✅ Pod-level tracking with top 10 consumers


Flow Metrics

✅ ovs_vswitchd_dp_flows_total with min/max/avg stats
✅ ovs_vswitchd_bridge_flows_total for br-int and br-ex bridges
✅ Top 10 results for each metric separately


Connection Metrics (query_ovs_connection_metrics)

✅ sum(ovs_vswitchd_stream_open), sum(ovs_vswitchd_rconn_overflow), sum(ovs_vswitchd_rconn_discarded)
✅ Statistical analysis with thresholds


OVSUsageCollector Class

✅ Supports both instant and duration scenarios
✅ Uses PrometheusBaseQuery client instead of direct prometheus_url
✅ Integrates with OpenShiftAuth for Kubernetes operations



🧠 Advanced Analysis Features:

Performance Analysis Module (ovnk_benchmark_performance_analysis_ovs.py)

✅ Comprehensive alert system (Critical/Warning/Info levels)
✅ Performance insights with confidence scoring
✅ Actionable recommendations for optimization
✅ Overall performance status determination


Configuration Management

✅ metrics.yaml support with fallback to default PromQL queries
✅ Configurable thresholds and alerting rules
✅ Flexible metric definitions



🚀 System Capabilities:

Multi-mode Operation: Instant queries, duration-based analysis, individual metrics, continuous monitoring
Intelligent Analysis: Detects performance issues, memory leaks, flow inefficiencies, connection problems
Comprehensive Reporting: JSON output with detailed metrics, analysis, and recommendations
Top 10 Rankings: Separate rankings for each metric type as requested
Unit Conversion: Automatic memory unit conversion for readability
Error Handling: Robust error handling with graceful degradation