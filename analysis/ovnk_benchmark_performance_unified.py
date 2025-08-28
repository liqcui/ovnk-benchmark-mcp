"""
OVNK Benchmark Performance Analysis - Unified Entry Point (Updated)
Provides a single interface to all optimized performance analyzers using utility classes
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Union
from dataclasses import asdict

from .ovnk_benchmark_performance_utility import UnifiedAnalysisInterface, ReportGenerator, ClusterHealth, AnalysisMetadata

# Import updated analyzers - some imports may fail if modules don't exist
try:
    from .ovnk_benchmark_performance_analysis_sync import OVNKubeSyncDurationAnalyzer
except ImportError:
    OVNKubeSyncDurationAnalyzer = None

try:
    from .ovnk_benchmark_performance_analysis_multus import MultusPerformanceAnalyzer
except ImportError:
    MultusPerformanceAnalyzer = None

try:
    from .ovnk_benchmark_performance_analysis_containers import ContainersPerformanceAnalyzer
except ImportError:
    ContainersPerformanceAnalyzer = None

# Import the updated analyzers
from .ovnk_benchmark_performance_analysis_pods import PODsPerformanceAnalyzer as PodsPerformanceAnalyzer
from .ovnk_benchmark_performance_analysis_ovs import OVSPerformanceAnalyzer


class UnifiedPerformanceAnalyzer:
    """Unified performance analyzer that handles all OVNK components"""
    
    def __init__(self):
        self.interface = UnifiedAnalysisInterface()
        self._register_analyzers()
        self.analysis_timestamp = datetime.now(timezone.utc)
    
    def _register_analyzers(self):
        """Register all available analyzers"""
        # Register available analyzers, skip those that aren't available
        if OVNKubeSyncDurationAnalyzer:
            self.interface.register_analyzer("sync_duration", OVNKubeSyncDurationAnalyzer())
        
        if MultusPerformanceAnalyzer:
            self.interface.register_analyzer("multus", MultusPerformanceAnalyzer())
        
        if ContainersPerformanceAnalyzer:
            self.interface.register_analyzer("containers", ContainersPerformanceAnalyzer())
        
        # Register the updated analyzers
        self.interface.register_analyzer("pods", PodsPerformanceAnalyzer())
        self.interface.register_analyzer("ovs", OVSPerformanceAnalyzer())
    
    def analyze_component(self, component: str, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze a specific component"""
        if component not in self.interface.get_available_analyzers():
            raise ValueError(f"Unknown component: {component}. Available: {self.interface.get_available_analyzers()}")
        
        try:
            return self.interface.analyze(component, metrics_data)
        except Exception as e:
            return {
                "error": f"Analysis failed for {component}: {str(e)}",
                "timestamp": self.analysis_timestamp.isoformat()
            }
    
    def analyze_all_components(self, components_data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Analyze multiple components at once"""
        results = {}
        
        for component, metrics_data in components_data.items():
            if component in self.interface.get_available_analyzers():
                results[component] = self.analyze_component(component, metrics_data)
            else:
                results[component] = {
                    "error": f"Unknown component: {component}",
                    "timestamp": self.analysis_timestamp.isoformat()
                }
        
        return results
    
    def generate_comprehensive_report(self, analysis_results: Dict[str, Dict[str, Any]], 
                                    save_json: bool = True,
                                    save_text: bool = True,
                                    output_dir: str = ".") -> Dict[str, str]:
        """Generate comprehensive report across all analyzed components"""
        
        # Create comprehensive report
        comprehensive_report = {
            "report_metadata": {
                "generated_at": self.analysis_timestamp.isoformat(),
                "report_type": "unified_ovnk_performance_analysis",
                "analyzer_version": "2.1",
                "components_analyzed": list(analysis_results.keys())
            },
            "executive_summary": self._generate_executive_summary(analysis_results),
            "component_analyses": analysis_results,
            "cross_component_insights": self._generate_cross_component_insights(analysis_results),
            "unified_recommendations": self._generate_unified_recommendations(analysis_results)
        }
        
        saved_files = {}
        timestamp_str = self.analysis_timestamp.strftime('%Y%m%d_%H%M%S')
        
        # Save JSON report
        if save_json:
            json_filename = f"{output_dir}/ovnk_unified_analysis_{timestamp_str}.json"
            if ReportGenerator.save_json_report(comprehensive_report, json_filename):
                saved_files["json"] = json_filename
        
        # Generate and save text report
        if save_text:
            text_report = self._generate_text_report(comprehensive_report)
            text_filename = f"{output_dir}/ovnk_unified_report_{timestamp_str}.txt"
            if ReportGenerator.save_text_report(text_report, text_filename):
                saved_files["text"] = text_filename
        
        return saved_files
    
    def _generate_executive_summary(self, analysis_results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Generate executive summary across all components"""
        summary = {
            "overall_cluster_health": "unknown",
            "total_components_analyzed": len(analysis_results),
            "components_with_issues": 0,
            "critical_issues_count": 0,
            "warning_issues_count": 0,
            "key_findings": [],
            "immediate_action_required": False
        }
        
        health_scores = []
        critical_components = []
        warning_components = []
        
        for component, result in analysis_results.items():
            if "error" in result:
                continue
            
            # Extract health information based on component type (updated for new structure)
            health_score = self._extract_health_score(component, result)
            if health_score is not None:
                health_scores.append(health_score)
                
                if health_score < 40:
                    critical_components.append(component)
                    summary["critical_issues_count"] += 1
                elif health_score < 70:
                    warning_components.append(component)
                    summary["warning_issues_count"] += 1
        
        # Calculate overall health
        if health_scores:
            avg_health = sum(health_scores) / len(health_scores)
            if avg_health >= 85:
                summary["overall_cluster_health"] = "excellent"
            elif avg_health >= 70:
                summary["overall_cluster_health"] = "good"
            elif avg_health >= 50:
                summary["overall_cluster_health"] = "moderate"
            elif avg_health >= 30:
                summary["overall_cluster_health"] = "poor"
            else:
                summary["overall_cluster_health"] = "critical"
        
        summary["components_with_issues"] = len(critical_components) + len(warning_components)
        summary["immediate_action_required"] = len(critical_components) > 0
        
        # Generate key findings
        if critical_components:
            summary["key_findings"].append(f"Critical performance issues detected in: {', '.join(critical_components)}")
        if warning_components:
            summary["key_findings"].append(f"Performance warnings in: {', '.join(warning_components)}")
        if not critical_components and not warning_components:
            summary["key_findings"].append("All analyzed components are performing within acceptable parameters")
        
        return summary
    
    def _extract_health_score(self, component: str, result: Dict[str, Any]) -> Optional[float]:
        """Extract health score from component analysis result (updated for new structure)"""
        # Both pods and ovs analyzers now use the standardized cluster_health structure
        if component in ["pods", "ovs"]:
            cluster_health = result.get("cluster_health", {})
            return cluster_health.get("overall_score")
        
        # For other components that might still use old structure
        elif component == "sync_duration":
            cluster_health = result.get("cluster_health", {})
            return cluster_health.get("overall_score")
        elif component == "multus":
            cluster_health = result.get("cluster_health", {})
            return cluster_health.get("overall_score")
        elif component == "containers":
            # This might still use the old structure
            return result.get("cluster_health_score")
        
        return None
    
    def _generate_cross_component_insights(self, analysis_results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Generate insights that span multiple components"""
        insights = {
            "resource_correlation": {},
            "bottleneck_analysis": {},
            "performance_patterns": {},
            "component_dependencies": {}
        }
        
        # Analyze resource usage patterns across components
        cpu_issues = []
        memory_issues = []
        sync_issues = []
        
        for component, result in analysis_results.items():
            if "error" in result:
                continue
            
            # Look for resource-related issues using the new standardized structure
            if self._has_resource_issues(component, result, "cpu"):
                cpu_issues.append(component)
            
            if self._has_resource_issues(component, result, "memory"):
                memory_issues.append(component)
            
            # Look for sync/latency issues
            if component == "sync_duration" and self._has_sync_issues(result):
                sync_issues.append(component)
        
        insights["resource_correlation"] = {
            "components_with_cpu_issues": cpu_issues,
            "components_with_memory_issues": memory_issues,
            "components_with_sync_issues": sync_issues
        }
        
        # Generate bottleneck analysis
        if len(cpu_issues) > 1:
            insights["bottleneck_analysis"]["cluster_wide_cpu_pressure"] = True
        if len(memory_issues) > 1:
            insights["bottleneck_analysis"]["cluster_wide_memory_pressure"] = True
        if sync_issues:
            insights["bottleneck_analysis"]["ovn_performance_degradation"] = True
        
        return insights
    
    def _has_resource_issues(self, component: str, result: Dict[str, Any], resource_type: str) -> bool:
        """Check if component has resource-related issues (updated for new structure)"""
        if component in ["pods", "ovs"]:
            # Use the new alerts structure
            alerts = result.get("alerts", [])
            return any(alert.get("resource_type") == resource_type for alert in alerts)
        elif component in ["multus", "containers"]:
            # Fallback to old structure if available
            alerts = result.get("alerts", [])
            return any(alert.get("resource_type") == resource_type for alert in alerts)
        return False
    
    def _has_sync_issues(self, result: Dict[str, Any]) -> bool:
        """Check if sync duration component has issues"""
        cluster_health = result.get("cluster_health", {})
        return cluster_health.get("critical_issues_count", 0) > 0 or cluster_health.get("warning_issues_count", 0) > 0
    
    def _generate_unified_recommendations(self, analysis_results: Dict[str, Dict[str, Any]]) -> List[str]:
        """Generate unified recommendations across all components"""
        unified_recommendations = []
        all_recommendations = set()
        
        # Collect all recommendations from components
        for component, result in analysis_results.items():
            if "error" in result:
                continue
            
            recommendations = result.get("cluster_recommendations", [])
            if isinstance(recommendations, list):
                all_recommendations.update(recommendations)
        
        # Add cross-component recommendations
        cross_insights = self._generate_cross_component_insights(analysis_results)
        bottlenecks = cross_insights.get("bottleneck_analysis", {})
        
        if bottlenecks.get("cluster_wide_cpu_pressure"):
            unified_recommendations.append(
                "CLUSTER-WIDE: CPU pressure detected across multiple components. Consider cluster scaling or workload optimization."
            )
        
        if bottlenecks.get("cluster_wide_memory_pressure"):
            unified_recommendations.append(
                "CLUSTER-WIDE: Memory pressure detected across multiple components. Review memory limits and consider adding nodes."
            )
        
        if bottlenecks.get("ovn_performance_degradation"):
            unified_recommendations.append(
                "OVN-SPECIFIC: Performance degradation in OVN components. Review OVN configuration and database performance."
            )
        
        # Add most common component-specific recommendations
        common_recommendations = []
        for rec in all_recommendations:
            if any(keyword in rec.lower() for keyword in ["critical", "urgent", "immediate"]):
                common_recommendations.append(rec)
        
        # Combine and deduplicate
        final_recommendations = unified_recommendations + list(common_recommendations)
        
        if not final_recommendations:
            final_recommendations.append("All components are performing well. Continue regular monitoring.")
        
    
    def _generate_text_report(self, comprehensive_report: Dict[str, Any]) -> str:
        """Generate human-readable text report"""
        lines = []
        
        # Header
        metadata = comprehensive_report["report_metadata"]
        lines.extend([
            "=" * 100,
            "OVNK UNIFIED PERFORMANCE ANALYSIS REPORT",
            "=" * 100,
            f"Generated: {metadata['generated_at']}",
            f"Report Type: {metadata['report_type']}",
            f"Components Analyzed: {', '.join(metadata['components_analyzed'])}",
            ""
        ])
        
        # Executive Summary
        summary = comprehensive_report["executive_summary"]
        lines.extend([
            "EXECUTIVE SUMMARY",
            "-" * 50,
            f"Overall Cluster Health: {summary['overall_cluster_health'].upper()}",
            f"Total Components Analyzed: {summary['total_components_analyzed']}",
            f"Components with Issues: {summary['components_with_issues']}",
            f"Critical Issues: {summary['critical_issues_count']}",
            f"Warning Issues: {summary['warning_issues_count']}",
            f"Immediate Action Required: {'YES' if summary['immediate_action_required'] else 'NO'}",
            ""
        ])
        
        # Key Findings
        if summary.get("key_findings"):
            lines.extend(["KEY FINDINGS:", "-" * 20])
            for finding in summary["key_findings"]:
                lines.append(f"• {finding}")
            lines.append("")
        
        # Cross-Component Insights
        cross_insights = comprehensive_report.get("cross_component_insights", {})
        if cross_insights:
            lines.extend(["CROSS-COMPONENT ANALYSIS", "-" * 30])
            
            resource_corr = cross_insights.get("resource_correlation", {})
            for issue_type, components in resource_corr.items():
                if components:
                    lines.append(f"{issue_type.replace('_', ' ').title()}: {', '.join(components)}")
            lines.append("")
        
        # Unified Recommendations
        recommendations = comprehensive_report.get("unified_recommendations", [])
        if recommendations:
            lines.extend(["UNIFIED RECOMMENDATIONS", "-" * 30])
            for i, rec in enumerate(recommendations, 1):
                lines.append(f"{i}. {rec}")
            lines.append("")
        
        # Component Summaries
        lines.extend(["COMPONENT ANALYSIS SUMMARIES", "-" * 40])
        component_analyses = comprehensive_report.get("component_analyses", {})
        
        for component, analysis in component_analyses.items():
            if "error" in analysis:
                lines.extend([
                    f"{component.upper()}: ERROR",
                    f"  Error: {analysis['error']}",
                    ""
                ])
            else:
                health_score = self._extract_health_score(component, analysis)
                health_str = f" (Score: {health_score:.1f}/100)" if health_score else ""
                status_icon = "✓ OK" if health_score and health_score > 70 else "⚠ NEEDS ATTENTION"
                lines.extend([
                    f"{component.upper()}{health_str}:",
                    f"  Status: {status_icon}",
                    ""
                ])
        
        lines.extend([
            "=" * 100,
            "END OF UNIFIED REPORT",
            "=" * 100
        ])
        
        return "\n".join(lines)


# Example usage functions
def example_pods_analysis():
    """Example of pods analysis using updated analyzer"""
    sample_pods_data = {
        "collection_type": "instant",
        "top_10_pods": [
            {
                "pod_name": "ovnkube-node-abc",
                "node_name": "worker-1",
                "usage_metrics": {
                    "cpu_usage": {"value": 85.5, "unit": "%"},
                    "memory_usage": {"value": 2048, "unit": "MB"}
                }
            },
            {
                "pod_name": "ovs-daemon-xyz",
                "node_name": "worker-2",
                "usage_metrics": {
                    "cpu_usage": {"value": 25.0, "unit": "%"},
                    "memory_usage": {"value": 512, "unit": "MB"}
                }
            }
        ]
    }
    
    analyzer = UnifiedPerformanceAnalyzer()
    result = analyzer.analyze_component("pods", sample_pods_data)
    
    print("Pods Analysis:")
    print(f"Status: {'✓' if 'error' not in result else '✗'}")
    if 'error' not in result:
        health = result.get("cluster_health", {})
        print(f"Health Score: {health.get('overall_score', 'N/A')}/100")
        print(f"Critical Issues: {health.get('critical_issues_count', 0)}")
    
    return result


def example_ovs_analysis():
    """Example of OVS analysis using updated analyzer"""
    sample_ovs_data = {
        'collection_type': 'duration',
        'pod_metrics': {
            'openshift-sdn/ovs-node-abc': {
                'pod_name': 'ovs-node-abc',
                'namespace': 'openshift-sdn',
                'node': 'worker-1',
                'cpu_usage': {'min': 45.2, 'avg': 67.8, 'max': 89.1, 'unit': 'percent'},
                'memory_usage': {
                    'min': {'value': 800, 'unit': 'MB'},
                    'avg': {'value': 1.2, 'unit': 'GB'},
                    'max': {'value': 1.8, 'unit': 'GB'}
                }
            }
        }
    }
    
    analyzer = UnifiedPerformanceAnalyzer()
    result = analyzer.analyze_component("ovs", sample_ovs_data)
    
    print("OVS Analysis:")
    print(f"Status: {'✓' if 'error' not in result else '✗'}")
    if 'error' not in result:
        health = result.get("cluster_health", {})
        print(f"Health Score: {health.get('overall_score', 'N/A')}/100")
        print(f"Critical Issues: {health.get('critical_issues_count', 0)}")
    
    return result


def example_unified_analysis():
    """Example of unified analysis across multiple components"""
    # Sample data for multiple components
    components_data = {
        "pods": {
            "collection_type": "instant",
            "top_10_pods": [
                {
                    "pod_name": "ovnkube-node-abc",
                    "node_name": "worker-1",
                    "usage_metrics": {
                        "cpu_usage": {"value": 75.0, "unit": "%"},
                        "memory_usage": {"value": 1536, "unit": "MB"}
                    }
                }
            ]
        },
        "ovs": {
            'collection_type': 'duration',
            'pod_metrics': {
                'openshift-sdn/ovs-node-xyz': {
                    'pod_name': 'ovs-node-xyz',
                    'namespace': 'openshift-sdn',
                    'node': 'worker-2',
                    'cpu_usage': {'min': 30.0, 'avg': 45.0, 'max': 65.0, 'unit': 'percent'},
                    'memory_usage': {
                        'min': {'value': 400, 'unit': 'MB'},
                        'avg': {'value': 600, 'unit': 'MB'},
                        'max': {'value': 800, 'unit': 'MB'}
                    }
                }
            }
        }
    }
    
    analyzer = UnifiedPerformanceAnalyzer()
    
    # Analyze all components
    results = analyzer.analyze_all_components(components_data)
    
    # Generate comprehensive report
    saved_files = analyzer.generate_comprehensive_report(results, save_json=True, save_text=True)
    
    print("Unified Analysis Complete:")
    print(f"Components analyzed: {list(results.keys())}")
    print(f"Saved files: {saved_files}")
    
    return results, saved_files


def load_and_analyze_from_files(file_paths: Dict[str, str]) -> Dict[str, Any]:
    """Load metrics data from files and perform unified analysis"""
    analyzer = UnifiedPerformanceAnalyzer()
    components_data = {}
    
    for component, file_path in file_paths.items():
        try:
            with open(file_path, 'r') as f:
                components_data[component] = json.load(f)
        except Exception as e:
            print(f"Error loading {component} data from {file_path}: {e}")
            components_data[component] = {"error": f"Failed to load data: {str(e)}"}
    
    # Analyze all loaded components
    results = analyzer.analyze_all_components(components_data)
    
    # Generate comprehensive report
    saved_files = analyzer.generate_comprehensive_report(results)
    
    print(f"Analysis complete for components: {list(components_data.keys())}")
    print(f"Report files saved: {saved_files}")
    
    return {
        "analysis_results": results,
        "saved_files": saved_files,
        "summary": analyzer._generate_executive_summary(results)
    }


def quick_health_check(components_data: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """Perform a quick health check across components"""
    analyzer = UnifiedPerformanceAnalyzer()
    results = analyzer.analyze_all_components(components_data)
    
    health_status = {}
    
    for component, result in results.items():
        if "error" in result:
            health_status[component] = "ERROR"
        else:
            health_score = analyzer._extract_health_score(component, result)
            if health_score is None:
                health_status[component] = "UNKNOWN"
            elif health_score >= 85:
                health_status[component] = "EXCELLENT"
            elif health_score >= 70:
                health_status[component] = "GOOD"
            elif health_score >= 50:
                health_status[component] = "MODERATE"
            elif health_score >= 30:
                health_status[component] = "POOR"
            else:
                health_status[component] = "CRITICAL"
    
    return health_status


def print_health_summary(health_status: Dict[str, str]):
    """Print a formatted health summary"""
    print("\nOVNK Component Health Summary:")
    print("-" * 40)
    
    status_icons = {
        "EXCELLENT": "✅",
        "GOOD": "✅", 
        "MODERATE": "⚠️",
        "POOR": "🔶",
        "CRITICAL": "🚨",
        "ERROR": "❌",
        "UNKNOWN": "❓"
    }
    
    for component, status in health_status.items():
        icon = status_icons.get(status, "❓")
        print(f"{icon} {component.ljust(15)}: {status}")
    
    # Summary counts
    status_counts = {}
    for status in health_status.values():
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print(f"\nSummary: {len(health_status)} components analyzed")
    for status, count in status_counts.items():
        if count > 0:
            print(f"  {status}: {count}")


# Main execution functions
async def main():
    """Main function demonstrating unified analysis capabilities"""
    print("OVNK Unified Performance Analysis Demo (Updated)")
    print("=" * 60)
    
    # Example 1: Quick health check
    print("\n1. Quick Health Check Example:")
    sample_data = {
        "pods": {
            "collection_type": "instant",
            "top_10_pods": [
                {
                    "pod_name": "test-pod",
                    "node_name": "worker-1",
                    "usage_metrics": {
                        "cpu_usage": {"value": 25.0, "unit": "%"},
                        "memory_usage": {"value": 512, "unit": "MB"}
                    }
                }
            ]
        }
    }
    
    health_status = quick_health_check(sample_data)
    print_health_summary(health_status)
    
    # Example 2: Individual component analysis
    print("\n2. Pods Component Analysis:")
    try:
        pods_result = example_pods_analysis()
        print("✅ Pods analysis completed successfully")
    except Exception as e:
        print(f"❌ Pods analysis failed: {e}")
    
    print("\n3. OVS Component Analysis:")
    try:
        ovs_result = example_ovs_analysis()
        print("✅ OVS analysis completed successfully")
    except Exception as e:
        print(f"❌ OVS analysis failed: {e}")
    
    # Example 4: Comprehensive unified analysis
    print("\n4. Comprehensive Unified Analysis:")
    try:
        results, saved_files = example_unified_analysis()
        print("✅ Comprehensive analysis completed successfully")
        if saved_files:
            print(f"Reports saved: {list(saved_files.keys())}")
    except Exception as e:
        print(f"❌ Comprehensive analysis failed: {e}")
    
    print(f"\n{'='*60}")
    print("Demo completed. Check generated report files for detailed analysis.")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Command-line usage for file analysis
        if sys.argv[1] == "--files" and len(sys.argv) > 2:
            # Example: python ovnk_benchmark_performance_unified.py --files pods:/path/to/pods.json ovs:/path/to/ovs.json
            file_mappings = {}
            for arg in sys.argv[2:]:
                if ":" in arg:
                    component, filepath = arg.split(":", 1)
                    file_mappings[component] = filepath
            
            if file_mappings:
                result = load_and_analyze_from_files(file_mappings)
                print("File analysis completed!")
                
                summary = result["summary"]
                print(f"\nExecutive Summary:")
                print(f"Overall Health: {summary['overall_cluster_health']}")
                print(f"Components with Issues: {summary['components_with_issues']}")
                print(f"Critical Issues: {summary['critical_issues_count']}")
            else:
                print("Usage: python ovnk_benchmark_performance_unified.py --files component1:file1.json component2:file2.json")
        elif sys.argv[1] == "--health-check":
            # Quick health check mode
            print("Available components for analysis:")
            analyzer = UnifiedPerformanceAnalyzer()
            available = analyzer.interface.get_available_analyzers()
            for comp in available:
                print(f"  - {comp}")
        else:
            print("Usage:")
            print("  python ovnk_benchmark_performance_unified.py")
            print("  python ovnk_benchmark_performance_unified.py --files component:file.json ...")
            print("  python ovnk_benchmark_performance_unified.py --health-check")
            print(f"\nAvailable components: {', '.join(['pods', 'ovs'] + (['sync_duration', 'multus', 'containers'] if any([OVNKubeSyncDurationAnalyzer, MultusPerformanceAnalyzer, ContainersPerformanceAnalyzer]) else []))}")
    else:
        # Run interactive demo
        asyncio.run(main())