#!/usr/bin/env python3
"""
Usage Example for OVN-Kubernetes Sync Duration Monitoring

This script demonstrates how to use the OVN sync duration collector and analyzer.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add the tools directory to the path (adjust as needed)
sys.path.append(str(Path(__file__).parent / 'tools'))

from ovnk_benchmark_auth import auth
from ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery
from ovnk_benchmark_prometheus_ovnk_sync import OVNSyncDurationCollector

# Import analysis module
sys.path.append(str(Path(__file__).parent / 'analysis'))
from ovnk_benchmark_performance_analysis_ovnk_sync import (
    OVNKubeSyncDurationAnalyzer, 
    OVNKubePerformanceMonitor,
    print_analysis_summary
)


async def demonstrate_instant_collection():
    """Demonstrate instant metrics collection"""
    print("=" * 60)
    print("🚀 DEMONSTRATING INSTANT METRICS COLLECTION")
    print("=" * 60)
    
    # Initialize authentication
    print("🔐 Initializing authentication...")
    await auth.initialize()
    
    # Create Prometheus client
    prometheus_client = PrometheusBaseQuery(
        prometheus_url=auth.prometheus_url,
        token=auth.prometheus_token
    )
    
    try:
        # Test connection
        print("🔗 Testing Prometheus connection...")
        if await prometheus_client.test_connection():
            print("✅ Prometheus connection successful")
        else:
            print("❌ Prometheus connection failed")
            return
        
        # Create collector
        collector = OVNSyncDurationCollector(prometheus_client)
        
        # Collect instant metrics
        print("📊 Collecting instant metrics...")
        results = await collector.collect_instant_metrics()
        
        # Save results
        results_file = collector.save_results(results)
        
        # Display summary
        print(f"\n📋 COLLECTION SUMMARY:")
        print(f"   Timestamp: {results.get('timestamp')}")
        print(f"   Metrics collected: {len(results.get('metrics', {}))}")
        print(f"   Top sync durations found: {len(results.get('top_10_sync_durations', []))}")
        
        # Show top 3 durations
        top_durations = results.get('top_10_sync_durations', [])
        if top_durations:
            print(f"\n🏆 TOP 3 SYNC DURATIONS:")
            for i, duration in enumerate(top_durations[:3], 1):
                readable = duration.get('readable_duration', {})
                print(f"   {i}. Pod: {duration.get('pod_name', 'unknown')}")
                print(f"      Node: {duration.get('node_name', 'unknown')}")
                print(f"      Duration: {readable.get('value', 0)} {readable.get('unit', 's')}")
                print(f"      Metric: {duration.get('metric_name', 'unknown')}")
        
        return results
        
    finally:
        await prometheus_client.close()


async def demonstrate_duration_collection():
    """Demonstrate duration metrics collection"""
    print("\n" + "=" * 60)
    print("⏱️  DEMONSTRATING DURATION METRICS COLLECTION")
    print("=" * 60)
    
    # Initialize authentication
    await auth.initialize()
    
    # Create Prometheus client
    prometheus_client = PrometheusBaseQuery(
        prometheus_url=auth.prometheus_url,
        token=auth.prometheus_token
    )
    
    try:
        # Create collector
        collector = OVNSyncDurationCollector(prometheus_client)
        
        # Collect duration metrics (5 minutes)
        duration = "5m"
        print(f"📊 Collecting metrics for duration: {duration}")
        results = await collector.collect_sync_duration_seconds_metrics(duration)
        
        # Save results
        results_file = collector.save_results(results)
        
        # Display summary
        print(f"\n📋 COLLECTION SUMMARY:")
        print(f"   Duration: {results.get('duration')}")
        print(f"   Start time: {results.get('start_time')}")
        print(f"   End time: {results.get('end_time')}")
        print(f"   Metrics collected: {len(results.get('metrics', {}))}")
        print(f"   Top max durations found: {len(results.get('top_10_max_durations', []))}")
        
        # Show top 3 max durations
        top_durations = results.get('top_10_max_durations', [])
        if top_durations:
            print(f"\n🏆 TOP 3 MAX SYNC DURATIONS (in {duration} period):")
            for i, duration_info in enumerate(top_durations[:3], 1):
                readable = duration_info.get('readable_duration', {})
                print(f"   {i}. Pod: {duration_info.get('pod_name', 'unknown')}")
                print(f"      Node: {duration_info.get('node_name', 'unknown')}")
                print(f"      Max Duration: {readable.get('value', 0)} {readable.get('unit', 's')}")
                print(f"      Metric: {duration_info.get('metric_name', 'unknown')}")
                print(f"      Data Points: {duration_info.get('data_points', 0)}")
        
        return results
        
    finally:
        await prometheus_client.close()


async def demonstrate_performance_analysis():
    """Demonstrate performance analysis"""
    print("\n" + "=" * 60)
    print("🔬 DEMONSTRATING PERFORMANCE ANALYSIS")
    print("=" * 60)
    
    # Initialize authentication
    await auth.initialize()
    
    # Create Prometheus client
    prometheus_client = PrometheusBaseQuery(
        prometheus_url=auth.prometheus_url,
        token=auth.prometheus_token
    )
    
    try:
        # Create performance monitor (combines collection + analysis)
        monitor = OVNKubePerformanceMonitor(prometheus_client)
        
        # Perform instant analysis
        print("📊 Performing instant performance analysis...")
        instant_report = await monitor.monitor_instant_performance(save_results=True)
        
        # Display analysis summary
        print_analysis_summary(instant_report)
        
        # Perform duration analysis
        print(f"\n📊 Performing duration performance analysis (10m)...")
        duration_report = await monitor.monitor_duration_performance('10m', save_results=True)
        
        # Display analysis summary
        print_analysis_summary(duration_report)
        
        return instant_report, duration_report
        
    finally:
        await prometheus_client.close()


async def main():
    """Main demonstration function"""
    print("🎯 OVN-KUBERNETES SYNC DURATION MONITORING DEMONSTRATION")
    print("=" * 70)
    
    try:
        # Demonstrate instant collection
        instant_results = await demonstrate_instant_collection()
        
        # Demonstrate duration collection
        duration_results = await demonstrate_duration_collection()
        
        # Demonstrate performance analysis
        analysis_reports = await demonstrate_performance_analysis()
        
        print("\n" + "=" * 70)
        print("✅ DEMONSTRATION COMPLETED SUCCESSFULLY")
        print("=" * 70)
        
        print("\n📁 Generated Files:")
        print("   - Instant metrics JSON file")
        print("   - Duration metrics JSON file") 
        print("   - Analysis reports JSON files")
        print("   - Check current directory for timestamped files")
        
    except Exception as e:
        print(f"\n❌ ERROR DURING DEMONSTRATION: {e}")
        raise


if __name__ == "__main__":
    # Run the demonstration
    asyncio.run(main())