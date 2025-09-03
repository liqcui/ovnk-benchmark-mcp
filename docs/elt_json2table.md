Key Updates:
1. Enhanced Data Type Detection

Added specific detection for cluster_info (from ovnk_benchmark_openshift_cluster_info.py)
Added detection for prometheus_basic_info (from ovnk_benchmark_prometheus_basicinfo.py)
Added detection for kube_api_metrics (from ovnk_benchmark_prometheus_kubeapi.py)
Added detection for node_usage and pod_status data

2. Specialized Extraction Methods

_extract_cluster_info(): Extracts cluster overview, resource counts, node summary, and health indicators
_extract_prometheus_basic_info(): Handles OVN database size metrics with automatic byte-to-MB conversion
_extract_kube_api_metrics(): Processes API latency metrics, top operations, and activity metrics
_extract_node_usage(): Handles node usage data with role-based grouping
_extract_pod_status(): Processes pod phase distribution

3. Smart Column Management

Maximum 5 columns per table with intelligent column selection
2-column mode for overview/summary tables (Property-Value format)
3-4 columns for ranking/performance tables
Priority-based column selection (keeps most important columns like name, status, value)

4. Improved Table Formatting

Compact table creation with create_compact_tables() method
Row limiting (max 15 rows) to prevent overly long tables
Value truncation for readability (max 80 characters for text fields)
Bootstrap styling for responsive HTML tables

5. New Specialized Functions

convert_cluster_info_to_tables() - Optimized for cluster info JSON
convert_prometheus_basic_to_tables() - Optimized for Prometheus metrics
convert_kube_api_to_tables() - Optimized for API metrics
create_dashboard_html() - Creates complete HTML dashboard from multiple sources
export_tables_to_csv() - Exports tables to CSV files

6. Enhanced Error Handling

Better error messages for different failure modes
Graceful degradation when specialized extraction fails
Detailed logging for debugging

##Usage Examples:
# For cluster info
cluster_tables = convert_cluster_info_to_tables(cluster_info_json)

# For Prometheus basic info  
prom_tables = convert_prometheus_basic_to_tables(prometheus_json)

# For API metrics
api_tables = convert_kube_api_to_tables(kube_api_json)

# Create complete dashboard
dashboard_html = create_dashboard_html({
    'cluster_info': cluster_data,
    'prometheus_basic': prom_data,
    'kube_api': api_data
})
