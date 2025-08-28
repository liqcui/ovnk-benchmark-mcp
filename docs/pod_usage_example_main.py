#!/usr/bin/env python3
"""
Main Example: Pod Usage Collection and Analysis
Demonstrates how to use the PodsUsageCollector and PerformanceAnalyzer modules
"""

import asyncio
import sys
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any

# Import the modules we created
sys.path.append('/tools')
sys.path.append('/tools/analysis')

from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError
from ocauth.ovnk_benchmark_auth import OpenShiftAuth
from tools.ovnk_benchmark_prometheus_pods_usage import PodsUsageCollector
from analysis.ovnk_benchmark_performance_analysis_pods import PODsPerformanceAnalyzer, analyze_pod_performance


async def main():
    """Main execution function"""
    print("🚀 Starting Pod Usage Collection and Analysis")
    print("=" * 60)
    
    # Configuration - you can modify these as needed
    config = {
        'prometheus_url': os.getenv('PROMETHEUS_URL', 'http://prometheus-k8s.openshift-monitoring.svc.cluster.local:9090'),
        'prometheus_token': os.getenv('PROMETHEUS_TOKEN'),
        'kubeconfig_path': os.getenv('KUBECONFIG'),
        'output_dir': '/tmp',
        'duration': '1h',  # For duration-based queries
        'pod_pattern': '.*',  # All pods
        'container_pattern': '.*',  # All containers
        'namespace_pattern': '.*',  # All namespaces
        'use_ovn_queries': True,  # Use predefined OVN queries
    }
    
    # Initialize authentication (optional)
    auth = None
    try:
        print("🔐 Initializing OpenShift authentication...")
        auth = OpenShiftAuth(config['kubeconfig_path'])
        await auth.initialize()
        
        # Update prometheus config from discovery
        if auth.prometheus_url:
            config['prometheus_url'] = auth.prometheus_url
        if auth.prometheus_token:
            config['prometheus_token'] = auth.prometheus_token
            
    except Exception as e:
        print(f"⚠️ Warning: Authentication failed: {e}")
        print("💡 Continuing with manual configuration...")
    
    # Initialize Prometheus client
    print(f"🔗 Connecting to Prometheus: {config['prometheus_url']}")
    
    async with PrometheusBaseQuery(
        prometheus_url=config['prometheus_url'],
        token=config['prometheus_token']
    ) as prometheus_client:
        
        # Test connection
        print("🧪 Testing Prometheus connection...")
        if await prometheus_client.test_connection():
            print("✅ Prometheus connection successful")
        else:
            print("❌ Prometheus connection failed")
            return 1
        
        # Initialize collector
        collector = PodsUsageCollector(prometheus_client, auth)
        
        # Scenario 1: Instant Usage Collection
        print("\n📊 Scenario 1: Collecting Instant Pod Usage")
        print("-" * 50)
        
        try:
            if config['use_ovn_queries']:
                print("Using predefined OVN queries...")
                instant_summary = await collector.collect_instant_usage(use_ovn_queries=True)
            else:
                print(f"Using pattern-based queries (pod: {config['pod_pattern']}, container: {config['container_pattern']})")
                instant_summary = await collector.collect_instant_usage(
                    pod_pattern=config['pod_pattern'],
                    container_pattern=config['container_pattern'],
                    namespace_pattern=config['namespace_pattern']
                )
            
            print(f"✅ Collected instant usage for {instant_summary['total_pods_analyzed']} pods")
            
            # Export instant summary
            instant_json_file = f"{config['output_dir']}/pod_usage_instant_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            collector.export_summary_json(instant_summary, instant_json_file)
            
            # Analyze instant usage
            print("🔬 Analyzing instant usage performance...")
            instant_analysis = analyze_pod_performance(instant_summary)
            
            # Export analysis
            instant_analysis_file = f"{config['output_dir']}/pod_analysis_instant_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            analyzer = PODsPerformanceAnalyzer()
            analyzer.export_analysis_json(instant_analysis, instant_analysis_file)
            
            # Generate report
            instant_report_file = f"{config['output_dir']}/pod_report_instant_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            analyzer.generate_performance_report(instant_analysis, instant_report_file)
            
            print(f"📈 Instant Analysis Summary:")
            print(f"   Health Score: {instant_analysis['cluster_health']['overall_score']:.1f}/100")
            print(f"   Health Level: {instant_analysis['cluster_health']['health_level']}")
            print(f"   Critical Issues: {instant_analysis['cluster_health']['critical_issues_count']}")
            
        except PrometheusQueryError as e:
            print(f"❌ Instant usage collection failed: {e}")
        
        # Scenario 2: Duration Usage Collection
        print(f"\n📊 Scenario 2: Collecting Duration Pod Usage ({config['duration']})")
        print("-" * 50)
        
        try:
            if config['use_ovn_queries']:
                print("Using predefined OVN queries...")
                duration_summary = await collector.collect_duration_usage(
                    duration=config['duration'],
                    use_ovn_queries=True
                )
            else:
                print(f"Using pattern-based queries (pod: {config['pod_pattern']}, container: {config['container_pattern']})")
                duration_summary = await collector.collect_duration_usage(
                    duration=config['duration'],
                    pod_pattern=config['pod_pattern'],
                    container_pattern=config['container_pattern'],
                    namespace_pattern=config['namespace_pattern']
                )
            
            print(f"✅ Collected duration usage for {duration_summary['total_pods_analyzed']} pods")
            
            # Export duration summary
            duration_json_file = f"{config['output_dir']}/pod_usage_duration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            collector.export_summary_json(duration_summary, duration_json_file)
            
            # Analyze duration usage
            print("🔬 Analyzing duration usage performance...")
            duration_analysis = analyze_pod_performance(duration_summary)
            
            # Export analysis
            duration_analysis_file = f"{config['output_dir']}/pod_analysis_duration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            analyzer.export_analysis_json(duration_analysis, duration_analysis_file)
            
            # Generate report
            duration_report_file = f"{config['output_dir']}/pod_report_duration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            analyzer.generate_performance_report(duration_analysis, duration_report_file)
            
            print(f"📈 Duration Analysis Summary:")
            print(f"   Health Score: {duration_analysis['cluster_health']['overall_score']:.1f}/100")
            print(f"   Health Level: {duration_analysis['cluster_health']['health_level']}")
            print(f"   Critical Issues: {duration_analysis['cluster_health']['critical_issues_count']}")
            print(f"   Query Duration: {duration_summary['query_info']['duration']}")
            print(f"   Time Range: {duration_summary['query_info']['start_time']} to {duration_summary['query_info']['end_time']}")
            
        except PrometheusQueryError as e:
            print(f"❌ Duration usage collection failed: {e}")
        
        # Scenario 3: Custom Queries Example
        print("\n📊 Scenario 3: Custom Queries Example")
        print("-" * 50)
        
        try:
            custom_queries = {
                'custom_cpu': 'sum by(pod, node) (irate(container_cpu_usage_seconds_total{container!="POD"}[5m])) * 100',
                'custom_memory': 'sum by(pod, node) (container_memory_working_set_bytes{container!="POD"})',
                'custom_network_rx': 'sum by(pod, node) (irate(container_network_receive_bytes_total[5m]))',
                'custom_network_tx': 'sum by(pod, node) (irate(container_network_transmit_bytes_total[5m]))'
            }
            
            print("Using custom queries for comprehensive metrics...")
            custom_summary = await collector.collect_instant_usage(custom_queries=custom_queries)
            
            print(f"✅ Collected custom metrics for {custom_summary['total_pods_analyzed']} pods")
            
            # Export custom summary
            custom_json_file = f"{config['output_dir']}/pod_usage_custom_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            collector.export_summary_json(custom_summary, custom_json_file)
            
        except PrometheusQueryError as e:
            print(f"❌ Custom usage collection failed: {e}")
    
    # Close authentication if used
    if auth:
        await auth.test_prometheus_connection()
    
    print("\n🎉 Pod Usage Collection and Analysis Complete!")
    print("=" * 60)
    print(f"📁 Output files saved to: {config['output_dir']}")
    print("\nGenerated files:")
    print("  • pod_usage_*.json - Raw usage data")
    print("  • pod_analysis_*.json - Performance analysis")
    print("  • pod_report_*.txt - Human-readable reports")
    
    return 0


async def demo_ovn_specific():
    """Demo function specifically for OVN pod monitoring"""
    print("🔧 OVN-Specific Pod Monitoring Demo")
    print("=" * 40)
    
    # This demonstrates the exact use case from the requirements
    prometheus_url = os.getenv('PROMETHEUS_URL', 'http://prometheus-k8s.openshift-monitoring.svc.cluster.local:9090')
    prometheus_token = os.getenv('PROMETHEUS_TOKEN')
    
    async with PrometheusBaseQuery(prometheus_url, prometheus_token) as client:
        collector = PodsUsageCollector(client)
        
        # Use the exact queries from requirements
        ovn_queries = {
            'memory-ovnkube-node-pods': 'sum by(pod) (container_memory_rss{pod=~"ovnkube-node-.*", container=~"kube-rbac-proxy-node|kube-rbac-proxy-ovn-metrics|nbdb|northd|ovn-acl-logging|ovn-controller|ovnkube-controller|sbdb"})',
            'cpu-ovnkube-node-pods': 'topk(10, sum by(pod) (irate(container_cpu_usage_seconds_total{container!="POD", pod=~"ovnkube-node.*", namespace=~"openshift-ovn-kubernetes"}[5m]))) * 100'
        }
        
        # Instant collection
        summary = await collector.collect_instant_usage(custom_queries=ovn_queries)
        
        # Analysis
        analysis = analyze_pod_performance(summary)
        
        print(f"✅ OVN Analysis Complete:")
        print(f"   Pods Analyzed: {summary['total_pods_analyzed']}")
        print(f"   Health Score: {analysis['cluster_health']['overall_score']:.1f}/100")
        
        return summary, analysis


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)