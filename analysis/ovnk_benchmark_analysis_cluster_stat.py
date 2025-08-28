#!/usr/bin/env python3
"""
OpenShift Cluster Status Analyzer
Analyzes cluster status data and provides comprehensive health assessment and recommendations
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import json

logger = logging.getLogger(__name__)


class ClusterStatAnalyzer:
    """Analyzer for cluster status information with health scoring and recommendations"""
    
    def __init__(self):
        """Initialize the cluster status analyzer"""
        self.analysis_timestamp = None
        
    def analyze_cluster_status(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze comprehensive cluster status data and provide health assessment
        
        Args:
            cluster_data: Cluster status data from ClusterStatCollector
            
        Returns:
            Comprehensive analysis with health scores and recommendations
        """
        try:
            self.analysis_timestamp = datetime.now(timezone.utc).isoformat()
            
            analysis_result = {
                'analysis_timestamp': self.analysis_timestamp,
                'analysis_type': 'cluster_status_analysis',
                'overall_health': {},
                'component_analysis': {},
                'recommendations': [],
                'critical_issues': [],
                'warnings': [],
                'summary': {}
            }
            
            # Analyze each component
            if 'node_status' in cluster_data and 'error' not in cluster_data['node_status']:
                analysis_result['component_analysis']['nodes'] = self._analyze_node_status(
                    cluster_data['node_status']
                )
            
            if 'operator_status' in cluster_data and 'error' not in cluster_data['operator_status']:
                analysis_result['component_analysis']['operators'] = self._analyze_operator_status(
                    cluster_data['operator_status']
                )
            
            if 'mcp_status' in cluster_data and 'error' not in cluster_data['mcp_status']:
                analysis_result['component_analysis']['machine_config_pools'] = self._analyze_mcp_status(
                    cluster_data['mcp_status']
                )
            
            # Generate overall health assessment
            analysis_result['overall_health'] = self._generate_overall_health_assessment(
                analysis_result['component_analysis']
            )
            
            # Generate recommendations
            analysis_result['recommendations'] = self._generate_recommendations(
                analysis_result['component_analysis'],
                analysis_result['overall_health']
            )
            
            # Extract critical issues and warnings
            analysis_result['critical_issues'] = self._extract_critical_issues(
                analysis_result['component_analysis']
            )
            
            analysis_result['warnings'] = self._extract_warnings(
                analysis_result['component_analysis']
            )
            
            # Generate executive summary
            analysis_result['summary'] = self._generate_executive_summary(
                analysis_result
            )
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"Error analyzing cluster status: {e}")
            return {
                'error': str(e),
                'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
                'analysis_type': 'cluster_status_analysis'
            }
    
    def _analyze_node_status(self, node_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze node status information"""
        try:
            total_nodes = node_data.get('total_nodes', 0)
            summary = node_data.get('summary', {})
            ready_nodes = summary.get('ready', 0)
            not_ready_nodes = summary.get('not_ready', 0)
            scheduling_disabled = summary.get('scheduling_disabled', 0)
            
            # Calculate health score (0-100)
            if total_nodes == 0:
                health_score = 0
            else:
                ready_percentage = (ready_nodes / total_nodes) * 100
                scheduling_penalty = (scheduling_disabled / total_nodes) * 20
                health_score = max(0, min(100, ready_percentage - scheduling_penalty))
            
            # Determine health status
            if health_score >= 95:
                health_status = 'excellent'
            elif health_score >= 85:
                health_status = 'good'
            elif health_score >= 70:
                health_status = 'fair'
            elif health_score >= 50:
                health_status = 'poor'
            else:
                health_status = 'critical'
            
            analysis = {
                'health_score': round(health_score, 2),
                'health_status': health_status,
                'total_nodes': total_nodes,
                'ready_nodes': ready_nodes,
                'not_ready_nodes': not_ready_nodes,
                'scheduling_disabled_nodes': scheduling_disabled,
                'node_distribution': {},
                'issues': [],
                'recommendations': []
            }
            
            # Analyze node distribution by role
            nodes_by_role = node_data.get('nodes_by_role', {})
            for role, nodes in nodes_by_role.items():
                analysis['node_distribution'][role] = len(nodes)
            
            # Check for control plane nodes
            master_nodes = len(nodes_by_role.get('master', []))
            if master_nodes < 3:
                analysis['issues'].append({
                    'severity': 'critical',
                    'message': f"Only {master_nodes} control plane nodes detected. Recommended minimum is 3 for HA.",
                    'component': 'control_plane'
                })
            elif master_nodes % 2 == 0:
                analysis['issues'].append({
                    'severity': 'warning',
                    'message': f"Even number ({master_nodes}) of control plane nodes. Odd numbers are recommended for etcd quorum.",
                    'component': 'control_plane'
                })
            
            # Identify specific issues
            if not_ready_nodes > 0:
                analysis['issues'].append({
                    'severity': 'critical',
                    'message': f"{not_ready_nodes} nodes are not ready",
                    'component': 'nodes'
                })
            
            if scheduling_disabled > 0:
                analysis['issues'].append({
                    'severity': 'warning',
                    'message': f"{scheduling_disabled} nodes have scheduling disabled",
                    'component': 'nodes'
                })
            
            # Analyze detailed node conditions
            detailed_status = node_data.get('detailed_status', [])
            problematic_nodes = []
            
            for node in detailed_status:
                node_issues = []
                
                # Check for problematic conditions
                for condition in node.get('conditions', []):
                    if condition['type'] in ['MemoryPressure', 'DiskPressure', 'PIDPressure'] and condition['status'] == 'True':
                        node_issues.append(f"{condition['type']}: {condition.get('message', '')}")
                    elif condition['type'] == 'NetworkUnavailable' and condition['status'] == 'True':
                        node_issues.append(f"Network unavailable: {condition.get('message', '')}")
                
                if node_issues:
                    problematic_nodes.append({
                        'node': node['name'],
                        'issues': node_issues
                    })
            
            if problematic_nodes:
                analysis['problematic_nodes'] = problematic_nodes
                analysis['issues'].append({
                    'severity': 'warning',
                    'message': f"{len(problematic_nodes)} nodes have resource pressure or network issues",
                    'component': 'nodes'
                })
            
            # Generate recommendations
            if not_ready_nodes > 0:
                analysis['recommendations'].append(
                    "Investigate and resolve issues with NotReady nodes immediately"
                )
            
            if master_nodes < 3:
                analysis['recommendations'].append(
                    "Add additional control plane nodes to achieve HA configuration"
                )
            
            if scheduling_disabled > 0:
                analysis['recommendations'].append(
                    "Review nodes with scheduling disabled - uncordon if maintenance is complete"
                )
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing node status: {e}")
            return {'error': str(e)}
    
    def _analyze_operator_status(self, operator_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze cluster operator status information"""
        try:
            total_operators = operator_data.get('total_operators', 0)
            summary = operator_data.get('summary', {})
            available = summary.get('available', 0)
            degraded = summary.get('degraded', 0)
            progressing = summary.get('progressing', 0)
            not_available = summary.get('not_available', 0)
            
            # Calculate health score (0-100)
            if total_operators == 0:
                health_score = 0
            else:
                available_percentage = (available / total_operators) * 100
                degraded_penalty = (degraded / total_operators) * 30
                progressing_penalty = (progressing / total_operators) * 10
                health_score = max(0, min(100, available_percentage - degraded_penalty - progressing_penalty))
            
            # Determine health status
            if health_score >= 95:
                health_status = 'excellent'
            elif health_score >= 85:
                health_status = 'good'
            elif health_score >= 70:
                health_status = 'fair'
            elif health_score >= 50:
                health_status = 'poor'
            else:
                health_status = 'critical'
            
            analysis = {
                'health_score': round(health_score, 2),
                'health_status': health_status,
                'total_operators': total_operators,
                'available_operators': available,
                'degraded_operators': degraded,
                'progressing_operators': progressing,
                'not_available_operators': not_available,
                'critical_operators': [],
                'issues': [],
                'recommendations': []
            }
            
            # Identify critical operators
            critical_operator_names = [
                'kube-apiserver', 'etcd', 'kube-controller-manager', 
                'kube-scheduler', 'openshift-apiserver', 'network'
            ]
            
            operators = operator_data.get('operators', [])
            operators_by_status = operator_data.get('operators_by_status', {})
            
            for operator in operators:
                op_name = operator.get('name', '')
                is_critical = any(critical_name in op_name for critical_name in critical_operator_names)
                
                if is_critical:
                    op_status = {
                        'name': op_name,
                        'available': operator.get('available', False),
                        'degraded': operator.get('degraded', False),
                        'progressing': operator.get('progressing', False)
                    }
                    analysis['critical_operators'].append(op_status)
                    
                    if not operator.get('available', False):
                        analysis['issues'].append({
                            'severity': 'critical',
                            'message': f"Critical operator '{op_name}' is not available",
                            'component': 'operators'
                        })
                    elif operator.get('degraded', False):
                        analysis['issues'].append({
                            'severity': 'critical',
                            'message': f"Critical operator '{op_name}' is degraded",
                            'component': 'operators'
                        })
            
            # General operator issues
            if degraded > 0:
                degraded_ops = operators_by_status.get('degraded', [])
                analysis['issues'].append({
                    'severity': 'critical',
                    'message': f"{degraded} operators are degraded: {', '.join(degraded_ops[:5])}{'...' if len(degraded_ops) > 5 else ''}",
                    'component': 'operators'
                })
            
            if not_available > 0:
                unavailable_ops = operators_by_status.get('unavailable', [])
                analysis['issues'].append({
                    'severity': 'critical',
                    'message': f"{not_available} operators are not available: {', '.join(unavailable_ops[:5])}{'...' if len(unavailable_ops) > 5 else ''}",
                    'component': 'operators'
                })
            
            if progressing > 0:
                progressing_ops = operators_by_status.get('progressing', [])
                analysis['issues'].append({
                    'severity': 'warning',
                    'message': f"{progressing} operators are progressing: {', '.join(progressing_ops[:5])}{'...' if len(progressing_ops) > 5 else ''}",
                    'component': 'operators'
                })
            
            # Generate recommendations
            if degraded > 0 or not_available > 0:
                analysis['recommendations'].append(
                    "Investigate and resolve issues with degraded or unavailable cluster operators immediately"
                )
            
            if progressing > 0:
                analysis['recommendations'].append(
                    "Monitor progressing operators - if stuck, investigate upgrade or configuration issues"
                )
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing operator status: {e}")
            return {'error': str(e)}
    
    def _analyze_mcp_status(self, mcp_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze machine config pool status information"""
        try:
            total_pools = mcp_data.get('total_pools', 0)
            summary = mcp_data.get('summary', {})
            updated = summary.get('updated', 0)
            updating = summary.get('updating', 0)
            degraded = summary.get('degraded', 0)
            ready = summary.get('ready', 0)
            
            # Calculate health score (0-100)
            if total_pools == 0:
                health_score = 100  # No MCPs is technically healthy
            else:
                updated_percentage = (updated / total_pools) * 100
                degraded_penalty = (degraded / total_pools) * 40
                updating_penalty = (updating / total_pools) * 15
                health_score = max(0, min(100, updated_percentage - degraded_penalty - updating_penalty))
            
            # Determine health status
            if health_score >= 95:
                health_status = 'excellent'
            elif health_score >= 85:
                health_status = 'good'
            elif health_score >= 70:
                health_status = 'fair'
            elif health_score >= 50:
                health_status = 'poor'
            else:
                health_status = 'critical'
            
            analysis = {
                'health_score': round(health_score, 2),
                'health_status': health_status,
                'total_pools': total_pools,
                'updated_pools': updated,
                'updating_pools': updating,
                'degraded_pools': degraded,
                'ready_pools': ready,
                'pool_details': [],
                'issues': [],
                'recommendations': []
            }
            
            # Analyze individual pools
            pools = mcp_data.get('pools', [])
            pools_by_status = mcp_data.get('pools_by_status', {})
            
            for pool in pools:
                pool_name = pool.get('name', '')
                machine_count = pool.get('machine_count', 0)
                ready_machines = pool.get('ready_machine_count', 0)
                updated_machines = pool.get('updated_machine_count', 0)
                unavailable_machines = pool.get('unavailable_machine_count', 0)
                
                pool_detail = {
                    'name': pool_name,
                    'updated': pool.get('updated', False),
                    'updating': pool.get('updating', False),
                    'degraded': pool.get('degraded', False),
                    'machine_count': machine_count,
                    'ready_machines': ready_machines,
                    'updated_machines': updated_machines,
                    'unavailable_machines': unavailable_machines,
                    'health_percentage': round((ready_machines / machine_count * 100) if machine_count > 0 else 0, 1)
                }
                analysis['pool_details'].append(pool_detail)
                
                # Check for pool-specific issues
                if pool.get('degraded', False):
                    analysis['issues'].append({
                        'severity': 'critical',
                        'message': f"Machine config pool '{pool_name}' is degraded",
                        'component': 'machine_config_pools'
                    })
                
                if unavailable_machines > 0:
                    analysis['issues'].append({
                        'severity': 'warning',
                        'message': f"Pool '{pool_name}' has {unavailable_machines} unavailable machines",
                        'component': 'machine_config_pools'
                    })
                
                if machine_count > 0 and updated_machines < machine_count and not pool.get('updating', False):
                    analysis['issues'].append({
                        'severity': 'warning',
                        'message': f"Pool '{pool_name}' has machines with outdated configurations",
                        'component': 'machine_config_pools'
                    })
            
            # General MCP issues
            if degraded > 0:
                degraded_pools = pools_by_status.get('degraded', [])
                analysis['issues'].append({
                    'severity': 'critical',
                    'message': f"{degraded} machine config pools are degraded: {', '.join(degraded_pools)}",
                    'component': 'machine_config_pools'
                })
            
            if updating > 0:
                updating_pools = pools_by_status.get('updating', [])
                analysis['issues'].append({
                    'severity': 'info',
                    'message': f"{updating} machine config pools are updating: {', '.join(updating_pools)}",
                    'component': 'machine_config_pools'
                })
            
            # Generate recommendations
            if degraded > 0:
                analysis['recommendations'].append(
                    "Investigate degraded machine config pools and resolve configuration issues"
                )
            
            if updating > 0:
                analysis['recommendations'].append(
                    "Monitor updating machine config pools - ensure updates complete successfully"
                )
            
            # Check for stuck updates
            long_updating_pools = []
            for pool in pools:
                if pool.get('updating', False):
                    # Note: In real implementation, you'd check timing
                    # For now, we'll just flag all updating pools for monitoring
                    long_updating_pools.append(pool.get('name', ''))
            
            if long_updating_pools:
                analysis['recommendations'].append(
                    f"Monitor pools that may have stuck updates: {', '.join(long_updating_pools)}"
                )
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing MCP status: {e}")
            return {'error': str(e)}
    
    def _generate_overall_health_assessment(self, component_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate overall cluster health assessment"""
        try:
            health_scores = []
            component_statuses = {}
            
            # Collect health scores and statuses
            for component, analysis in component_analysis.items():
                if 'error' not in analysis:
                    score = analysis.get('health_score', 0)
                    status = analysis.get('health_status', 'unknown')
                    health_scores.append(score)
                    component_statuses[component] = {
                        'score': score,
                        'status': status
                    }
            
            if not health_scores:
                return {
                    'overall_score': 0,
                    'overall_status': 'unknown',
                    'component_scores': component_statuses
                }
            
            # Calculate weighted overall score
            # Give higher weight to critical components
            weights = {
                'nodes': 0.4,
                'operators': 0.4,
                'machine_config_pools': 0.2
            }
            
            weighted_score = 0
            total_weight = 0
            
            for component, analysis in component_analysis.items():
                if 'error' not in analysis:
                    weight = weights.get(component, 0.1)
                    score = analysis.get('health_score', 0)
                    weighted_score += score * weight
                    total_weight += weight
            
            overall_score = weighted_score / total_weight if total_weight > 0 else 0
            
            # Determine overall status
            if overall_score >= 95:
                overall_status = 'excellent'
            elif overall_score >= 85:
                overall_status = 'good'
            elif overall_score >= 70:
                overall_status = 'fair'
            elif overall_score >= 50:
                overall_status = 'poor'
            else:
                overall_status = 'critical'
            
            return {
                'overall_score': round(overall_score, 2),
                'overall_status': overall_status,
                'component_scores': component_statuses,
                'assessment_criteria': {
                    'excellent': '95-100: All components healthy',
                    'good': '85-94: Minor issues present',
                    'fair': '70-84: Some attention needed',
                    'poor': '50-69: Significant issues',
                    'critical': '0-49: Critical issues requiring immediate attention'
                }
            }
            
        except Exception as e:
            logger.error(f"Error generating overall health assessment: {e}")
            return {'error': str(e)}
    
    def _generate_recommendations(self, component_analysis: Dict[str, Any], overall_health: Dict[str, Any]) -> List[str]:
        """Generate comprehensive recommendations"""
        recommendations = []
        
        try:
            # Priority-based recommendations
            critical_recommendations = []
            important_recommendations = []
            general_recommendations = []
            
            # Collect recommendations from each component
            for component, analysis in component_analysis.items():
                if 'error' not in analysis:
                    component_recs = analysis.get('recommendations', [])
                    for rec in component_recs:
                        if 'immediate' in rec.lower() or 'critical' in rec.lower():
                            critical_recommendations.append(f"[{component.upper()}] {rec}")
                        elif 'investigate' in rec.lower() or 'resolve' in rec.lower():
                            important_recommendations.append(f"[{component.upper()}] {rec}")
                        else:
                            general_recommendations.append(f"[{component.upper()}] {rec}")
            
            # Add overall recommendations based on health status
            overall_status = overall_health.get('overall_status', 'unknown')
            
            if overall_status == 'critical':
                critical_recommendations.insert(0, 
                    "CRITICAL: Cluster is in critical state - immediate intervention required"
                )
            elif overall_status == 'poor':
                important_recommendations.insert(0,
                    "WARNING: Cluster has significant issues that need attention"
                )
            
            # Combine recommendations in priority order
            recommendations.extend(critical_recommendations)
            recommendations.extend(important_recommendations)
            recommendations.extend(general_recommendations)
            
            # Add general best practices if cluster is healthy
            if overall_status in ['excellent', 'good'] and len(recommendations) == 0:
                recommendations.extend([
                    "Continue regular monitoring of cluster health",
                    "Review cluster resource utilization and plan for capacity",
                    "Ensure backup and disaster recovery procedures are tested"
                ])
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            return [f"Error generating recommendations: {str(e)}"]
    
    def _extract_critical_issues(self, component_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract all critical issues from component analyses"""
        critical_issues = []
        
        try:
            for component, analysis in component_analysis.items():
                if 'error' not in analysis:
                    issues = analysis.get('issues', [])
                    for issue in issues:
                        if issue.get('severity') == 'critical':
                            issue['source_component'] = component
                            critical_issues.append(issue)
            
            return sorted(critical_issues, key=lambda x: x.get('component', ''))
            
        except Exception as e:
            logger.error(f"Error extracting critical issues: {e}")
            return [{'severity': 'error', 'message': f"Error extracting issues: {str(e)}", 'component': 'analysis'}]
    
    def _extract_warnings(self, component_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract all warnings from component analyses"""
        warnings = []
        
        try:
            for component, analysis in component_analysis.items():
                if 'error' not in analysis:
                    issues = analysis.get('issues', [])
                    for issue in issues:
                        if issue.get('severity') in ['warning', 'info']:
                            issue['source_component'] = component
                            warnings.append(issue)
            
            return sorted(warnings, key=lambda x: (x.get('severity', ''), x.get('component', '')))
            
        except Exception as e:
            logger.error(f"Error extracting warnings: {e}")
            return [{'severity': 'error', 'message': f"Error extracting warnings: {str(e)}", 'component': 'analysis'}]
    
    def _generate_executive_summary(self, analysis_result: Dict[str, Any]) -> Dict[str, Any]:
        """Generate executive summary of cluster status"""
        try:
            overall_health = analysis_result.get('overall_health', {})
            critical_issues = analysis_result.get('critical_issues', [])
            warnings = analysis_result.get('warnings', [])
            component_analysis = analysis_result.get('component_analysis', {})
            
            summary = {
                'cluster_status': overall_health.get('overall_status', 'unknown'),
                'health_score': overall_health.get('overall_score', 0),
                'critical_issues_count': len(critical_issues),
                'warnings_count': len(warnings),
                'components_analyzed': len(component_analysis),
                'immediate_action_required': len(critical_issues) > 0,
                'key_findings': []
            }
            
            # Generate key findings
            findings = []
            
            # Node findings
            if 'nodes' in component_analysis and 'error' not in component_analysis['nodes']:
                node_analysis = component_analysis['nodes']
                total_nodes = node_analysis.get('total_nodes', 0)
                ready_nodes = node_analysis.get('ready_nodes', 0)
                findings.append(f"Cluster has {total_nodes} nodes with {ready_nodes} ready")
            
            # Operator findings
            if 'operators' in component_analysis and 'error' not in component_analysis['operators']:
                op_analysis = component_analysis['operators']
                total_ops = op_analysis.get('total_operators', 0)
                available_ops = op_analysis.get('available_operators', 0)
                degraded_ops = op_analysis.get('degraded_operators', 0)
                findings.append(f"{available_ops}/{total_ops} operators available, {degraded_ops} degraded")
            
            # MCP findings
            if 'machine_config_pools' in component_analysis and 'error' not in component_analysis['machine_config_pools']:
                mcp_analysis = component_analysis['machine_config_pools']
                total_pools = mcp_analysis.get('total_pools', 0)
                updated_pools = mcp_analysis.get('updated_pools', 0)
                findings.append(f"{updated_pools}/{total_pools} machine config pools updated")
            
            summary['key_findings'] = findings
            
            # Add status explanation
            status_explanations = {
                'excellent': 'Cluster is operating optimally with no significant issues',
                'good': 'Cluster is healthy with minor issues that should be monitored',
                'fair': 'Cluster has some issues that need attention but is stable',
                'poor': 'Cluster has significant issues that require prompt attention',
                'critical': 'Cluster has critical issues requiring immediate intervention'
            }
            
            summary['status_explanation'] = status_explanations.get(
                summary['cluster_status'], 
                'Unknown cluster status'
            )
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating executive summary: {e}")
            return {
                'error': str(e),
                'cluster_status': 'unknown',
                'immediate_action_required': True
            }

    def generate_json_report(self, analysis_result: Dict[str, Any], pretty: bool = True) -> str:
        """Generate JSON format report of the analysis"""
        try:
            if pretty:
                return json.dumps(analysis_result, indent=2, default=str)
            else:
                return json.dumps(analysis_result, default=str)
        except Exception as e:
            logger.error(f"Error generating JSON report: {e}")
            return json.dumps({'error': str(e)}, indent=2)

    def generate_text_summary(self, analysis_result: Dict[str, Any]) -> str:
        """Generate human-readable text summary of the analysis"""
        try:
            lines = []
            lines.append("=" * 60)
            lines.append("OPENSHIFT CLUSTER STATUS ANALYSIS REPORT")
            lines.append("=" * 60)
            
            # Executive Summary
            summary = analysis_result.get('summary', {})
            lines.append(f"\nCLUSTER STATUS: {summary.get('cluster_status', 'UNKNOWN').upper()}")
            lines.append(f"Health Score: {summary.get('health_score', 0)}/100")
            lines.append(f"Analysis Time: {analysis_result.get('analysis_timestamp', 'Unknown')}")
            
            if summary.get('immediate_action_required', False):
                lines.append("\n⚠️  IMMEDIATE ACTION REQUIRED ⚠️")
            
            # Key Findings
            if summary.get('key_findings'):
                lines.append("\nKEY FINDINGS:")
                for finding in summary['key_findings']:
                    lines.append(f"• {finding}")
            
            # Critical Issues
            critical_issues = analysis_result.get('critical_issues', [])
            if critical_issues:
                lines.append(f"\n🚨 CRITICAL ISSUES ({len(critical_issues)}):")
                for issue in critical_issues:
                    lines.append(f"• [{issue.get('component', 'UNKNOWN').upper()}] {issue.get('message', '')}")
            
            # Warnings
            warnings = analysis_result.get('warnings', [])
            if warnings:
                lines.append(f"\n⚠️  WARNINGS ({len(warnings)}):")
                for warning in warnings[:10]:  # Limit to first 10
                    lines.append(f"• [{warning.get('component', 'UNKNOWN').upper()}] {warning.get('message', '')}")
                if len(warnings) > 10:
                    lines.append(f"• ... and {len(warnings) - 10} more warnings")
            
            # Component Analysis
            component_analysis = analysis_result.get('component_analysis', {})
            if component_analysis:
                lines.append("\nCOMPONENT ANALYSIS:")
                
                for component, analysis in component_analysis.items():
                    if 'error' not in analysis:
                        component_name = component.replace('_', ' ').title()
                        health_score = analysis.get('health_score', 0)
                        health_status = analysis.get('health_status', 'unknown').upper()
                        
                        lines.append(f"\n{component_name}:")
                        lines.append(f"  Status: {health_status} ({health_score}/100)")
                        
                        # Add component-specific details
                        if component == 'nodes':
                            total = analysis.get('total_nodes', 0)
                            ready = analysis.get('ready_nodes', 0)
                            lines.append(f"  Nodes: {ready}/{total} ready")
                            
                            dist = analysis.get('node_distribution', {})
                            if dist:
                                dist_str = ", ".join([f"{k}: {v}" for k, v in dist.items()])
                                lines.append(f"  Distribution: {dist_str}")
                                
                        elif component == 'operators':
                            total = analysis.get('total_operators', 0)
                            available = analysis.get('available_operators', 0)
                            degraded = analysis.get('degraded_operators', 0)
                            lines.append(f"  Operators: {available}/{total} available, {degraded} degraded")
                            
                        elif component == 'machine_config_pools':
                            total = analysis.get('total_pools', 0)
                            updated = analysis.get('updated_pools', 0)
                            lines.append(f"  Pools: {updated}/{total} updated")
            
            # Recommendations
            recommendations = analysis_result.get('recommendations', [])
            if recommendations:
                lines.append(f"\nRECOMMENDATIONS ({len(recommendations)}):")
                for i, rec in enumerate(recommendations, 1):
                    lines.append(f"{i}. {rec}")
            
            lines.append("\n" + "=" * 60)
            
            return "\n".join(lines)
            
        except Exception as e:
            logger.error(f"Error generating text summary: {e}")
            return f"Error generating text summary: {str(e)}"