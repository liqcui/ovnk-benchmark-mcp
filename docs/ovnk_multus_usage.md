/tools/ovnk_benchmark_prometheus_ovnk_multus.py
This is the main collector class MultusPODsUsageCollector that:

Inherits from the uploaded modules: Uses PrometheusBaseQuery for Prometheus queries and OpenShiftAuth for Kubernetes authentication
Reads metrics.yaml first: Checks for existing PromQL queries in metrics.yaml, falls back to default queries if not found
Supports both scenarios:

collect_instant_usage() - for instant metrics
collect_duration_usage() - for time-series metrics over a duration
collect_usage() - unified method that calls appropriate method based on duration parameter


Gets pod-node mapping: Uses Kubernetes API via OpenShiftAuth to map pod names to node names
Formats output: Converts memory units (bytes/KB/MB/GB) appropriately and provides top 10 pod usage summary
Timezone support: All timestamps in UTC
Error handling: Comprehensive error handling with detailed error messages

Key features:

Uses the default PromQL queries you provided for Multus pods
Concurrent execution of multiple Prometheus queries
Proper memory unit conversion and formatting
Top 10 CPU usage ranking
Complete JSON summary with pod names, node names, min/avg/max usage

2. analysis/ovnk_benchmark_performance_analysis_ovnk_multus.py
This is the performance analysis module that:

Analyzes pod usage data: Takes JSON input from the collector and performs comprehensive analysis
No file I/O: Pure in-memory processing, returns JSON data
Comprehensive metrics:

Individual pod analysis with efficiency scores
Aggregate cluster statistics
Usage level categorization (low/medium/high/very_high)
Performance variability analysis
Resource utilization balance


Smart recommendations: Generates actionable recommendations based on usage patterns
Cluster insights: Node distribution, performance health, bottleneck detection
Trend analysis: For duration-based collections, analyzes performance trends over time

Key analysis features:

CPU and memory efficiency scoring
Usage variance and stability analysis
Resource balance detection (CPU-heavy vs memory-heavy workloads)
Node distribution balance analysis
Performance health assessment
Global and pod-specific recommendations