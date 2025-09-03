#!/usr/bin/env python3
"""
OVNK Benchmark Cluster Status Analysis Module
Analyzes cluster information and provides performance insights
"""

import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from dataclasses import dataclass, asdict

from analysis.ovnk_benchmark_performance_utility import (
    BasePerformanceAnalyzer,
    PerformanceAlert,
    AnalysisMetadata,
    ClusterHealth,
    AlertLevel,
    ResourceType,
    PerformanceLevel,
    RecommendationEngine,
    HealthScoreCalculator,
    create_performance_alert
)

logger = logging.getLogger(__name__)


@dataclass
class NodeGroupSummary:
    """Summary for a group of nodes (master/infra/worker)"""
    node_type: str
    total_nodes: int
    ready_nodes: int
    not_ready_nodes: int
    scheduling_disabled: int
    health_score: float
    resource_summary: Dict[str, Any]
    problematic_nodes: List[str]


@dataclass
class ClusterStatusAnalysis:
    """Complete cluster status analysis result"""
    metadata: AnalysisMetadata
    cluster_health: ClusterHealth
    node_groups: Dict[str, NodeGroupSummary]
    resource_utilization: Dict[str, Any]
    network_policy_analysis: Dict[str, Any]
    cluster_operators_summary: Dict[str, Any]
    mcp_summary: Dict[str, Any]
    alerts: List[PerformanceAlert]
    recommendations: List[str]


class ClusterStatAnalyzer(BasePerformanceAnalyzer):
    """Analyzer for OpenShift cluster status and health"""
    
    def __init__(self):
        super().__init__("cluster_status")
        
    def analyze_metrics_data(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Main analysis method for cluster information"""
        try:
            # Start timing
            start_time = datetime.now(timezone.utc)
            
            # Analyze different aspects of the cluster
            node_analysis = self._analyze_node_groups(cluster_data)
            resource_analysis = self._analyze_resource_utilization(cluster_data)
            network_analysis = self._analyze_network_policies(cluster_data)
            operator_analysis = self._analyze_cluster_operators(cluster_data)
            mcp_analysis = self._analyze_mcp_status(cluster_data)
            
            # Generate alerts
            alerts = self._generate_alerts(node_analysis, operator_analysis, mcp_analysis)
            
            # Calculate cluster health
            total_components = cluster_data.get('total_nodes', 0) + len(cluster_data.get('unavailable_cluster_operators', []))
            cluster_health = self.calculate_cluster_health_score(alerts, total_components)
            
            # Generate recommendations
            recommendations = self._generate_recommendations(
                node_analysis, operator_analysis, mcp_analysis, alerts
            )
            
            # Calculate duration
            duration = str(datetime.now(timezone.utc) - start_time)
            
            # Create metadata
            metadata = self.generate_metadata(
                collection_type="cluster_status",
                total_items=cluster_data.get('total_nodes', 0),
                duration=duration
            )
            
            # Create complete analysis
            analysis = ClusterStatusAnalysis(
                metadata=metadata,
                cluster_health=cluster_health,
                node_groups=node_analysis,
                resource_utilization=resource_analysis,
                network_policy_analysis=network_analysis,
                cluster_operators_summary=operator_analysis,
                mcp_summary=mcp_analysis,
                alerts=alerts,
                recommendations=recommendations
            )
            
            return asdict(analysis)
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            raise
    
    def _analyze_node_groups(self, cluster_data: Dict[str, Any]) -> Dict[str, NodeGroupSummary]:
        """Analyze node groups (master, infra, worker)"""
        node_groups = {}
        
        for node_type in ['master_nodes', 'infra_nodes', 'worker_nodes']:
            nodes = cluster_data.get(node_type, [])
            if not nodes:
                continue
                
            group_type = node_type.replace('_nodes', '')
            
            # Count node statuses
            ready_count = 0
            not_ready_count = 0
            scheduling_disabled_count = 0
            problematic_nodes = []
            
            total_cpu_capacity = 0
            total_memory_mb = 0
            
            for node in nodes:
                status = node.get('ready_status', '')
                
                if 'Ready' in status and 'SchedulingDisabled' not in status:
                    ready_count += 1
                elif 'NotReady' in status:
                    not_ready_count += 1
                    problematic_nodes.append(node.get('name', 'unknown'))
                
                if 'SchedulingDisabled' in status:
                    scheduling_disabled_count += 1
                
                # Parse resource capacity
                cpu_str = node.get('cpu_capacity', '0')
                try:
                    cpu_cores = float(cpu_str.replace('m', '')) / 1000 if 'm' in cpu_str else float(cpu_str)
                    total_cpu_capacity += cpu_cores
                except:
                    pass
                
                memory_str = node.get('memory_capacity', '0')
                try:
                    if 'Ki' in memory_str:
                        memory_kb = float(memory_str.replace('Ki', ''))
                        total_memory_mb += memory_kb / 1024
                    elif 'Mi' in memory_str:
                        total_memory_mb += float(memory_str.replace('Mi', ''))
                    elif 'Gi' in memory_str:
                        total_memory_mb += float(memory_str.replace('Gi', '')) * 1024
                except:
                    pass
            
            # Calculate health score for this group
            health_score = self._calculate_node_group_health(
                len(nodes), ready_count, not_ready_count, scheduling_disabled_count
            )
            
            node_groups[group_type] = NodeGroupSummary(
                node_type=group_type,
                total_nodes=len(nodes),
                ready_nodes=ready_count,
                not_ready_nodes=not_ready_count,
                scheduling_disabled=scheduling_disabled_count,
                health_score=health_score,
                resource_summary={
                    'total_cpu_cores': round(total_cpu_capacity, 2),
                    'total_memory_gb': round(total_memory_mb / 1024, 2),
                    'avg_cpu_per_node': round(total_cpu_capacity / len(nodes), 2) if nodes else 0,
                    'avg_memory_gb_per_node': round(total_memory_mb / 1024 / len(nodes), 2) if nodes else 0
                },
                problematic_nodes=problematic_nodes
            )
        
        return node_groups
    
    def _calculate_node_group_health(self, total: int, ready: int, not_ready: int, disabled: int) -> float:
        """Calculate health score for a node group"""
        if total == 0:
            return 0.0
        
        # Base score from ready nodes
        base_score = (ready / total) * 100
        
        # Penalties for problematic nodes
        not_ready_penalty = (not_ready / total) * 50  # Not ready is severe
        disabled_penalty = (disabled / total) * 20     # Scheduling disabled is moderate
        
        score = base_score - not_ready_penalty - disabled_penalty
        return max(0.0, min(100.0, score))
    
    def _analyze_resource_utilization(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze cluster resource utilization patterns"""
        return {
            'pod_density': round(cluster_data.get('pods_count', 0) / max(cluster_data.get('total_nodes', 1), 1), 2),
            'namespace_distribution': cluster_data.get('namespaces_count', 0),
            'service_to_pod_ratio': round(
                cluster_data.get('services_count', 0) / max(cluster_data.get('pods_count', 1), 1), 3
            ),
            'secret_density': round(cluster_data.get('secrets_count', 0) / max(cluster_data.get('namespaces_count', 1), 1), 2),
            'configmap_density': round(cluster_data.get('configmaps_count', 0) / max(cluster_data.get('namespaces_count', 1), 1), 2)
        }
    
    def _analyze_network_policies(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze network policy configuration and impact"""
        total_pods = cluster_data.get('pods_count', 0)
        networkpolicies = cluster_data.get('networkpolicies_count', 0)
        adminnetworkpolicies = cluster_data.get('adminnetworkpolicies_count', 0)
        egressfirewalls = cluster_data.get('egressfirewalls_count', 0)
        egressips = cluster_data.get('egressips_count', 0)
        udn = cluster_data.get('udn_count', 0)
        
        total_network_resources = networkpolicies + adminnetworkpolicies + egressfirewalls + egressips + udn
        
        return {
            'total_network_resources': total_network_resources,
            'policy_density': round(networkpolicies / max(total_pods, 1), 4),
            'network_complexity_score': self._calculate_network_complexity(
                networkpolicies, adminnetworkpolicies, egressfirewalls, egressips, udn
            ),
            'resource_breakdown': {
                'networkpolicies': networkpolicies,
                'adminnetworkpolicies': adminnetworkpolicies,
                'egressfirewalls': egressfirewalls,
                'egressips': egressips,
                'user_defined_networks': udn
            }
        }
    
    def _calculate_network_complexity(self, np: int, anp: int, ef: int, eip: int, udn: int) -> float:
        """Calculate network complexity score (0-100)"""
        # Weight different types of network resources by complexity
        weights = {'np': 1.0, 'anp': 3.0, 'ef': 2.0, 'eip': 1.5, 'udn': 2.5}
        
        weighted_score = (np * weights['np'] + anp * weights['anp'] + 
                         ef * weights['ef'] + eip * weights['eip'] + udn * weights['udn'])
        
        # Normalize to 0-100 scale (arbitrary scaling)
        normalized_score = min(weighted_score / 10, 100)
        return round(normalized_score, 2)
    
    def _analyze_cluster_operators(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze cluster operators status"""
        unavailable_operators = cluster_data.get('unavailable_cluster_operators', [])
        
        # Estimate total operators (common OpenShift deployment has ~30-35 operators)
        estimated_total = max(30, len(unavailable_operators) + 25)
        available_count = estimated_total - len(unavailable_operators)
        
        return {
            'total_operators_estimated': estimated_total,
            'available_operators': available_count,
            'unavailable_operators': len(unavailable_operators),
            'availability_percentage': round((available_count / estimated_total) * 100, 1),
            'unavailable_operator_names': unavailable_operators,
            'health_status': 'healthy' if len(unavailable_operators) == 0 else 
                           'degraded' if len(unavailable_operators) <= 2 else 'critical'
        }
    
    def _analyze_mcp_status(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze machine config pool status"""
        mcp_status = cluster_data.get('mcp_status', {})
        
        status_counts = {}
        for pool_name, status in mcp_status.items():
            status_counts[status] = status_counts.get(status, 0) + 1
        
        total_pools = len(mcp_status)
        degraded_pools = status_counts.get('Degraded', 0)
        updating_pools = status_counts.get('Updating', 0)
        updated_pools = status_counts.get('Updated', 0)
        
        health_score = 100.0
        if total_pools > 0:
            health_score = ((updated_pools / total_pools) * 100) - (degraded_pools * 30) - (updating_pools * 10)
            health_score = max(0.0, min(100.0, health_score))
        
        return {
            'total_pools': total_pools,
            'status_distribution': status_counts,
            'health_score': round(health_score, 2),
            'health_status': 'healthy' if degraded_pools == 0 else 'degraded' if degraded_pools <= 1 else 'critical'
        }
    
    def _generate_alerts(self, node_analysis: Dict[str, NodeGroupSummary], 
                        operator_analysis: Dict[str, Any], 
                        mcp_analysis: Dict[str, Any]) -> List[PerformanceAlert]:
        """Generate performance alerts based on analysis"""
        alerts = []
        
        # Node-related alerts
        for group_type, group_summary in node_analysis.items():
            if group_summary.not_ready_nodes > 0:
                severity = AlertLevel.CRITICAL if group_type == 'master' else AlertLevel.HIGH
                alerts.append(create_performance_alert(
                    severity=severity.value,
                    resource_type='cpu',  # Generic resource type
                    component_name=f"{group_type}_nodes",
                    message=f"{group_summary.not_ready_nodes} {group_type} node(s) not ready",
                    current_value=group_summary.not_ready_nodes,
                    threshold_value=0,
                    unit="nodes"
                ))
            
            if group_summary.health_score < 80:
                alerts.append(create_performance_alert(
                    severity=AlertLevel.MEDIUM.value,
                    resource_type='cpu',
                    component_name=f"{group_type}_nodes",
                    message=f"{group_type} node group health score below threshold",
                    current_value=group_summary.health_score,
                    threshold_value=80,
                    unit="score"
                ))
        
        # Cluster operator alerts
        if operator_analysis['unavailable_operators'] > 0:
            severity = AlertLevel.CRITICAL if operator_analysis['unavailable_operators'] > 2 else AlertLevel.HIGH
            alerts.append(create_performance_alert(
                severity=severity.value,
                resource_type='cpu',
                component_name="cluster_operators",
                message=f"{operator_analysis['unavailable_operators']} cluster operator(s) unavailable",
                current_value=operator_analysis['unavailable_operators'],
                threshold_value=0,
                unit="operators"
            ))
        
        # MCP alerts
        if mcp_analysis['health_status'] in ['degraded', 'critical']:
            alerts.append(create_performance_alert(
                severity=AlertLevel.HIGH.value if mcp_analysis['health_status'] == 'degraded' else AlertLevel.CRITICAL.value,
                resource_type='cpu',
                component_name="machine_config_pools",
                message=f"Machine config pools in {mcp_analysis['health_status']} state",
                current_value=mcp_analysis['health_score'],
                threshold_value=80,
                unit="score"
            ))
        
        return alerts
    
    def _generate_recommendations(self, node_analysis: Dict[str, NodeGroupSummary],
                                operator_analysis: Dict[str, Any],
                                mcp_analysis: Dict[str, Any],
                                alerts: List[PerformanceAlert]) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        # Critical issues first
        critical_alerts = [a for a in alerts if a.severity == AlertLevel.CRITICAL]
        if critical_alerts:
            recommendations.append("URGENT: Critical cluster issues detected requiring immediate attention")
        
        # Node-specific recommendations
        for group_type, group_summary in node_analysis.items():
            if group_summary.not_ready_nodes > 0:
                recommendations.append(f"Investigate and resolve {group_summary.not_ready_nodes} not ready {group_type} node(s)")
            
            if group_summary.health_score < 60:
                recommendations.append(f"Review {group_type} node group health - score: {group_summary.health_score}%")
        
        # Cluster operator recommendations
        if operator_analysis['unavailable_operators'] > 0:
            recommendations.append(f"Check and restore {operator_analysis['unavailable_operators']} unavailable cluster operator(s)")
            if operator_analysis['unavailable_operator_names']:
                recommendations.append(f"Focus on operators: {', '.join(operator_analysis['unavailable_operator_names'][:3])}")
        
        # MCP recommendations
        if mcp_analysis['health_status'] != 'healthy':
            recommendations.append("Review machine config pool status and resolve any configuration issues")
        
        # Resource utilization recommendations
        if not recommendations:
            recommendations.append("Cluster appears healthy - continue monitoring key metrics")
        
        return recommendations
    
    def generate_report(self, analysis_result: Dict[str, Any]) -> str:
        """Generate human-readable analysis report"""
        lines = []
        
        # Header
        metadata = analysis_result.get('metadata', {})
        lines.extend([
            "=" * 80,
            "OPENSHIFT CLUSTER STATUS ANALYSIS REPORT",
            "=" * 80,
            f"Analysis Timestamp: {metadata.get('analysis_timestamp', 'unknown')}",
            f"Total Items Analyzed: {metadata.get('total_items_analyzed', 0)}",
            f"Analysis Duration: {metadata.get('duration', 'unknown')}",
            ""
        ])
        
        # Cluster health summary
        health = analysis_result.get('cluster_health', {})
        lines.extend([
            "CLUSTER HEALTH SUMMARY",
            "-" * 40,
            f"Overall Score: {health.get('overall_score', 0)}/100",
            f"Health Level: {health.get('health_level', 'unknown').title()}",
            f"Critical Issues: {health.get('critical_issues_count', 0)}",
            f"Warning Issues: {health.get('warning_issues_count', 0)}",
            f"Healthy Components: {health.get('healthy_items_count', 0)}",
            ""
        ])
        
        # Node groups analysis
        node_groups = analysis_result.get('node_groups', {})
        lines.extend(["NODE GROUPS ANALYSIS", "-" * 40])
        for group_type, group_data in node_groups.items():
            lines.extend([
                f"{group_type.upper()} NODES:",
                f"  Total: {group_data.get('total_nodes', 0)}",
                f"  Ready: {group_data.get('ready_nodes', 0)}",
                f"  Not Ready: {group_data.get('not_ready_nodes', 0)}",
                f"  Health Score: {group_data.get('health_score', 0):.1f}%",
                f"  CPU Cores: {group_data.get('resource_summary', {}).get('total_cpu_cores', 0)}",
                f"  Memory (GB): {group_data.get('resource_summary', {}).get('total_memory_gb', 0)}",
                ""
            ])
        
        # Alerts section
        alerts = analysis_result.get('alerts', [])
        if alerts:
            lines.extend(["PERFORMANCE ALERTS", "-" * 40])
            for alert in alerts[:10]:  # Show top 10 alerts
                lines.append(f"  [{alert.get('severity', 'unknown').upper()}] {alert.get('message', '')}")
            lines.append("")
        
        # Recommendations
        recommendations = analysis_result.get('recommendations', [])
        if recommendations:
            lines.extend(["RECOMMENDATIONS", "-" * 40])
            for i, rec in enumerate(recommendations, 1):
                lines.append(f"{i}. {rec}")
            lines.append("")
        
        return "\n".join(lines)


# Convenience functions
async def analyze_cluster_status(cluster_data: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze cluster status data and return results"""
    analyzer = ClusterStatAnalyzer()
    return analyzer.analyze_metrics_data(cluster_data)


async def generate_cluster_status_report(cluster_data: Dict[str, Any]) -> str:
    """Generate and return cluster status report as text"""
    analyzer = ClusterStatAnalyzer()
    analysis_result = analyzer.analyze_metrics_data(cluster_data)
    return analyzer.generate_report(analysis_result)


if __name__ == "__main__":
    # Example usage
    import asyncio
    from tools.ovnk_benchmark_openshift_cluster_info import collect_cluster_information
    
    async def main():
        try:
            # Collect cluster information
            print("Collecting cluster information...")
            cluster_data = await collect_cluster_information()
            
            # Analyze the data
            print("Analyzing cluster status...")
            analyzer = ClusterStatAnalyzer()
            analysis_result = analyzer.analyze_metrics_data(cluster_data)
            
            # Generate and display report
            report = analyzer.generate_report(analysis_result)
            print(report)
            
            # Return the analysis result as JSON/dict
            print("\nAnalysis completed successfully!")
            return analysis_result
            
        except Exception as e:
            print(f"Error: {e}")
            return None
    
    asyncio.run(main())