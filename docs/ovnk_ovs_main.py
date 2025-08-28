"""
OVS Benchmark Main Script
Comprehensive OVS monitoring and analysis for OpenShift/Kubernetes
"""

import asyncio
import json
import argparse
import sys
import os
from pathlib import Path
from datetime import datetime, timezone

# Add paths for imports
sys.path.append('/tools')
sys.path.append('/analysis')

from ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery
from ovnk_benchmark_auth import OpenShiftAuth
from ovnk_benchmark_prometheus_ovnk_ovs import OVSUsageCollector
from ovnk_benchmark_performance_analysis_ovs import OVSPerformanceAnalyzer


class OVSBenchmarkRunner:
    """Main orchestrator for OVS benchmarking and analysis"""
    
    def __init__(self):
        self.auth_client = None
        self.prometheus_client = None
        self.ovs_collector = None
        self.performance_analyzer = None
        
    async def initialize(self, kubeconfig_path: str = None):
        """Initialize all components"""
        try:
            print("🚀 Initializing OVS Benchmark Runner...")
            
            # Initialize authentication
            self.auth_client = OpenShiftAuth(kubeconfig_path)
            await self.auth_client.initialize()
            
            # Test Prometheus connection
            if not await self.auth_client.test_prometheus_connection():
                raise Exception("Cannot connect to Prometheus")
            
            # Initialize Prometheus client
            self.prometheus_client = PrometheusBaseQuery(
                self.auth_client.prometheus_url, 
                self.auth_client.prometheus_token
            )
            
            # Initialize OVS collector and analyzer
            self.ovs_collector = OVSUsageCollector(self.prometheus_client, self.auth_client)
            self.performance_analyzer = OVSPerformanceAnalyzer()
            
            print("✅ All components initialized successfully")
            
        except Exception as e:
            print(f"❌ Initialization failed: {e}")
            raise
    
    async def run_instant_analysis(self, output_file: str = None):
        """Run instant OVS metrics collection and analysis"""
        try:
            print("\n🔍 Running instant OVS metrics analysis...")
            
            # Collect all OVS metrics
            async with self.prometheus_client:
                metrics_data = await self.ovs_collector.collect_all_ovs_metrics()
            
            # Perform analysis
            analysis_result = self.performance_analyzer.analyze_comprehensive_ovs_metrics(metrics_data)
            
            # Combine data and analysis
            comprehensive_report = {
                'report_type': 'instant_analysis',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'cluster_info': self.auth_client.get_cluster_summary(),
                'raw_metrics': metrics_data,
                'performance_analysis': analysis_result
            }
            
            # Output results
            if output_file:
                self._save_report(comprehensive_report, output_file)
                print(f"📄 Report saved to: {output_file}")
            else:
                print("\n" + "="*80)
                print("OVS INSTANT ANALYSIS REPORT")
                print("="*80)
                self._print_summary(comprehensive_report)
            
            return comprehensive_report
            
        except Exception as e:
            print(f"❌ Instant analysis failed: {e}")
            return None
    
    async def run_duration_analysis(self, duration: str, output_file: str = None):
        """Run duration-based OVS metrics collection and analysis"""
        try:
            print(f"\n🔍 Running OVS metrics analysis for duration: {duration}...")
            
            # Collect all OVS metrics over duration
            async with self.prometheus_client:
                metrics_data = await self.ovs_collector.collect_all_ovs_metrics(duration=duration)
            
            # Perform analysis
            analysis_result = self.performance_analyzer.analyze_comprehensive_ovs_metrics(metrics_data)
            
            # Combine data and analysis
            comprehensive_report = {
                'report_type': f'duration_analysis_{duration}',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'duration': duration,
                'cluster_info': self.auth_client.get_cluster_summary(),
                'raw_metrics': metrics_data,
                'performance_analysis': analysis_result
            }
            
            # Output results
            if output_file:
                self._save_report(comprehensive_report, output_file)
                print(f"📄 Report saved to: {output_file}")
            else:
                print("\n" + "="*80)
                print(f"OVS DURATION ANALYSIS REPORT ({duration})")
                print("="*80)
                self._print_summary(comprehensive_report)
            
            return comprehensive_report
            
        except Exception as e:
            print(f"❌ Duration analysis failed: {e}")
            return None
    
    async def run_individual_metrics(self, metric_type: str, duration: str = None):
        """Run individual metric collection"""
        try:
            async with self.prometheus_client:
                if metric_type == "cpu":
                    result = await self.ovs_collector.query_ovs_cpu_usage(duration)
                elif metric_type == "memory":
                    result = await self.ovs_collector.query_ovs_memory_usage(duration)
                elif metric_type == "dp_flows":
                    result = await self.ovs_collector.query_ovs_dp_flows_total(duration)
                elif metric_type == "bridge_flows":
                    result = await self.ovs_collector.query_ovs_bridge_flows_total(duration)
                elif metric_type == "connections":
                    result = await self.ovs_collector.query_ovs_connection_metrics(duration)
                else:
                    print(f"❌ Unknown metric type: {metric_type}")
                    return None
            
            print(f"\n📊 {metric_type.upper()} Metrics Results:")
            print(json.dumps(result, indent=2))
            return result
            
        except Exception as e:
            print(f"❌ Individual metric collection failed: {e}")
            return None
    
    def _save_report(self, report: dict, filename: str):
        """Save report to JSON file"""
        try:
            output_path = Path(filename)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2)
                
        except Exception as e:
            print(f"⚠️ Failed to save report: {e}")
    
    def _print_summary(self, report: dict):
        """Print a summary of the analysis report"""
        try:
            # Cluster info
            cluster_info = report.get('cluster_info', {}).get('cluster_info', {})
            print(f"🏗️  Cluster: {cluster_info.get('openshift_version', 'Unknown')} ({cluster_info.get('node_count', 'Unknown')} nodes)")
            
            # Performance analysis summary
            perf_analysis = report.get('performance_analysis', {})
            perf_summary = perf_analysis.get('performance_summary', {})
            
            print(f"\n📈 Overall Status: {perf_summary.get('overall_status', 'Unknown').upper()}")
            
            summary_metrics = perf_summary.get('summary_metrics', {})
            print(f"🚨 Alerts: {summary_metrics.get('critical_alerts', 0)} Critical, {summary_metrics.get('warning_alerts', 0)} Warning")
            print(f"💡 Insights: {summary_metrics.get('total_insights', 0)} Total, {summary_metrics.get('high_confidence_insights', 0)} High Confidence")
            
            # Top issues
            top_issues = perf_summary.get('top_issues', [])
            if top_issues:
                print(f"\n🔴 Top Issues:")
                for i, issue in enumerate(top_issues[:3], 1):
                    print(f"  {i}. [{issue['level'].upper()}] {issue['message']}")
                    print(f"     💡 {issue['recommendation']}")
            
            # Key insights
            key_insights = perf_summary.get('key_insights', [])
            if key_insights:
                print(f"\n🔍 Key Insights:")
                for i, insight in enumerate(key_insights[:3], 1):
                    print(f"  {i}. {insight['title']} (Confidence: {insight['confidence']:.0%})")
                    print(f"     📊 {insight['impact']}")
            
            # Raw metrics summary
            raw_metrics = report.get('raw_metrics', {})
            
            # CPU usage summary
            cpu_data = raw_metrics.get('cpu_usage', {})
            if 'summary' in cpu_data:
                print(f"\n🖥️  CPU Usage (Top Consumers):")
                vswitchd_top = cpu_data['summary'].get('ovs_vswitchd_top10', [])[:3]
                for node in vswitchd_top:
                    print(f"  • ovs-vswitchd@{node['node_name']}: {node['max']}% max")
                
                ovsdb_top = cpu_data['summary'].get('ovsdb_server_top10', [])[:3]
                for node in ovsdb_top:
                    print(f"  • ovsdb-server@{node['node_name']}: {node['max']}% max")
            
            # Memory usage summary
            memory_data = raw_metrics.get('memory_usage', {})
            if 'summary' in memory_data:
                print(f"\n💾 Memory Usage (Top Consumers):")
                db_top = memory_data['summary'].get('ovs_db_top10', [])[:3]
                for pod in db_top:
                    print(f"  • {pod['pod_name']}: {pod['max']} {pod['unit']} max")
                
                vswitchd_top = memory_data['summary'].get('ovs_vswitchd_top10', [])[:3]
                for pod in vswitchd_top:
                    print(f"  • {pod['pod_name']}: {pod['max']} {pod['unit']} max")
            
            # Flow metrics summary
            dp_flows = raw_metrics.get('dp_flows', {})
            if 'top_10' in dp_flows:
                print(f"\n🌊 Flow Metrics (Top Instances):")
                for flow in dp_flows['top_10'][:3]:
                    print(f"  • DP Flows@{flow['instance']}: {flow['max']} flows max")
            
            bridge_flows = raw_metrics.get('bridge_flows', {})
            if 'top_10' in bridge_flows:
                br_int_top = bridge_flows['top_10'].get('br_int', [])[:2]
                for flow in br_int_top:
                    print(f"  • br-int@{flow['instance']}: {flow['max']} flows max")
                
                br_ex_top = bridge_flows['top_10'].get('br_ex', [])[:2]
                for flow in br_ex_top:
                    print(f"  • br-ex@{flow['instance']}: {flow['max']} flows max")
            
            print(f"\n📊 Full report contains {len(json.dumps(report))} characters of detailed data")
            
        except Exception as e:
            print(f"⚠️ Error printing summary: {e}")
            print("📄 Raw report data available in output file")

    async def run_continuous_monitoring(self, interval: int = 300, duration: str = "5m", max_iterations: int = None):
        """Run continuous monitoring with specified interval"""
        try:
            print(f"🔄 Starting continuous monitoring (interval: {interval}s, duration: {duration})")
            
            iteration = 0
            while max_iterations is None or iteration < max_iterations:
                iteration += 1
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                
                print(f"\n{'='*60}")
                print(f"MONITORING ITERATION {iteration} - {timestamp}")
                print(f"{'='*60}")
                
                # Run analysis
                report = await self.run_duration_analysis(
                    duration, 
                    output_file=f"ovs_monitoring_{timestamp}.json"
                )
                
                if report:
                    # Quick status check
                    perf_status = report.get('performance_analysis', {}).get('performance_summary', {}).get('overall_status', 'unknown')
                    critical_alerts = report.get('performance_analysis', {}).get('performance_summary', {}).get('summary_metrics', {}).get('critical_alerts', 0)
                    
                    print(f"📊 Status: {perf_status.upper()} | Critical Alerts: {critical_alerts}")
                    
                    if critical_alerts > 0:
                        print("🚨 CRITICAL ISSUES DETECTED - Check detailed report!")
                
                # Wait for next iteration
                if max_iterations is None or iteration < max_iterations:
                    print(f"⏱️  Waiting {interval}s for next iteration...")
                    await asyncio.sleep(interval)
                    
        except KeyboardInterrupt:
            print("\n⏹️  Monitoring stopped by user")
        except Exception as e:
            print(f"❌ Continuous monitoring failed: {e}")


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="OVS Performance Benchmark and Analysis Tool")
    parser.add_argument("--kubeconfig", help="Path to kubeconfig file")
    parser.add_argument("--mode", choices=["instant", "duration", "metric", "monitor"], 
                       default="instant", help="Analysis mode")
    parser.add_argument("--duration", default="5m", help="Duration for range queries (e.g., 5m, 1h, 1d)")
    parser.add_argument("--metric", choices=["cpu", "memory", "dp_flows", "bridge_flows", "connections"], 
                       help="Specific metric to collect")
    parser.add_argument("--output", help="Output file for results (JSON format)")
    parser.add_argument("--interval", type=int, default=300, help="Monitoring interval in seconds")
    parser.add_argument("--max-iterations", type=int, help="Maximum monitoring iterations")
    
    args = parser.parse_args()
    
    try:
        # Initialize benchmark runner
        runner = OVSBenchmarkRunner()
        await runner.initialize(args.kubeconfig)
        
        # Execute based on mode
        if args.mode == "instant":
            await runner.run_instant_analysis(args.output)
            
        elif args.mode == "duration":
            await runner.run_duration_analysis(args.duration, args.output)
            
        elif args.mode == "metric":
            if not args.metric:
                print("❌ --metric required for metric mode")
                return
            await runner.run_individual_metrics(args.metric, args.duration)
            
        elif args.mode == "monitor":
            await runner.run_continuous_monitoring(
                interval=args.interval,
                duration=args.duration,
                max_iterations=args.max_iterations
            )
        
        print("\n✅ OVS benchmark completed successfully")
        
    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())