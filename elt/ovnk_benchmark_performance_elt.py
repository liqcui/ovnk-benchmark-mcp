"""
Performance ELT (Extract, Load, Transform) Module
Handles extraction of metrics data, transformation to table format, and loading into DuckDB
"""

import asyncio
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import duckdb


class PerformanceELT:
    """Extract, Load, Transform operations for performance data"""
    
    def __init__(self, db_path: str = "storage/ovnk_benchmark.db"):
        self.db_path = db_path
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        self._ensure_storage_directory()
    
    def _ensure_storage_directory(self) -> None:
        """Ensure storage directory exists"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
    
    async def initialize(self) -> None:
        """Initialize database connection"""
        try:
            self.connection = duckdb.connect(self.db_path)
            print(f"✅ ELT initialized: {self.db_path}")
        except Exception as e:
            print(f"❌ Failed to initialize ELT: {e}")
            raise
    
    async def store_performance_data(self, metrics_data: Dict[str, Any], timestamp: Optional[str] = None) -> None:
        """
        Store performance data from MCP tools
        
        Args:
            metrics_data: Raw metrics data from MCP tools
            timestamp: Optional timestamp for the data
        """
        if not self.connection:
            await self.initialize()
        
        if not timestamp:
            timestamp = datetime.now(timezone.utc).isoformat()
        
        try:
            # Extract and transform general info
            if 'general_info' in metrics_data:
                await self._process_general_info(metrics_data['general_info'], timestamp)
            
            # Extract and transform different metric categories
            categories = ['api_server', 'multus', 'ovnk_pods', 'ovnk_containers', 'ovnk_sync']
            
            for category in categories:
                if category in metrics_data:
                    await self._process_category_metrics(category, metrics_data[category], timestamp)
            
            # Create performance summary
            await self._create_performance_summary(metrics_data, timestamp)
            
            print(f"✅ Performance data stored successfully")
            
        except Exception as e:
            print(f"❌ Failed to store performance data: {e}")
            raise
    
    async def _process_general_info(self, general_info: Dict[str, Any], timestamp: str) -> None:
        """Process and store general cluster information"""
        timestamp_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        # Create general info table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cluster_general_info (
            id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            total_networkpolicies INTEGER,
            total_adminnetworkpolicies INTEGER,
            total_egressfirewalls INTEGER,
            total_namespaces INTEGER,
            total_nodes INTEGER,
            master_nodes INTEGER,
            worker_nodes INTEGER,
            cluster_version VARCHAR,
            platform VARCHAR,
            raw_data JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        self.connection.execute(create_table_sql)
        
        # Extract summary data
        summary = general_info.get('summary', {})
        cluster_info = general_info.get('cluster_info', {})
        nodes_info = general_info.get('networking_resources', {}).get('nodes', {})
        node_roles = nodes_info.get('role_counts', {}) if isinstance(nodes_info, dict) else {}
        
        # Insert data
        self.connection.execute("""
            INSERT INTO cluster_general_info (
                timestamp, total_networkpolicies, total_adminnetworkpolicies, 
                total_egressfirewalls, total_namespaces, total_nodes,
                master_nodes, worker_nodes, cluster_version, platform, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            timestamp_dt,
            summary.get('total_networkpolicies', 0),
            summary.get('total_adminnetworkpolicies', 0),
            summary.get('total_egressfirewalls', 0),
            summary.get('total_namespaces', 0),
            summary.get('total_nodes', 0),
            node_roles.get('master', 0),
            node_roles.get('worker', 0),
            cluster_info.get('openshift_version', cluster_info.get('kubernetes_version', 'unknown')),
            cluster_info.get('platform', 'unknown'),
            json.dumps(general_info)
        ])
    
    async def _process_category_metrics(self, category: str, metrics_data: Dict[str, Any], timestamp: str) -> None:
        """Process and store metrics for a specific category"""
        timestamp_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        # Create category-specific table
        await self._create_category_table(category)
        
        # Process metrics
        metrics = metrics_data.get('metrics', {})
        summary = metrics_data.get('summary', {})
        
        # Store detailed metrics
        for metric_name, metric_data in metrics.items():
            if 'error' in metric_data:
                continue
            
            await self._store_metric_details(category, metric_name, metric_data, timestamp_dt)
        
        # Store category summary
        await self._store_category_summary(category, summary, metrics_data.get('duration', ''), timestamp_dt)
    
    async def _create_category_table(self, category: str) -> None:
        """Create table for storing category metrics"""
        table_name = f"{category}_metrics"
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            metric_name VARCHAR,
            metric_value DOUBLE,
            metric_unit VARCHAR,
            statistic_type VARCHAR,
            container VARCHAR,
            pod VARCHAR,
            labels JSON,
            duration VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        self.connection.execute(create_table_sql)
        
        # Create index
        index_sql = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp)"
        self.connection.execute(index_sql)
    
    async def _store_metric_details(self, category: str, metric_name: str, metric_data: Dict[str, Any], timestamp_dt: datetime) -> None:
        """Store detailed metric data"""
        table_name = f"{category}_metrics"
        
        # Store statistics
        statistics = metric_data.get('statistics', {})
        for stat_name, stat_value in statistics.items():
            if stat_value is not None and isinstance(stat_value, (int, float)):
                self.connection.execute(f"""
                    INSERT INTO {table_name} (timestamp, metric_name, metric_value, metric_unit, statistic_type, duration)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [
                    timestamp_dt,
                    metric_name,
                    stat_value,
                    metric_data.get('unit', ''),
                    stat_name,
                    metric_data.get('duration', '')
                ])
        
        # Store per-container data if available
        by_container = metric_data.get('by_container', {})
        for container, container_stats in by_container.items():
            for stat_name, stat_value in container_stats.items():
                if stat_value is not None and isinstance(stat_value, (int, float)):
                    self.connection.execute(f"""
                        INSERT INTO {table_name} (timestamp, metric_name, metric_value, metric_unit, statistic_type, container, duration)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [
                        timestamp_dt,
                        f"{metric_name}_{stat_name}",
                        stat_value,
                        metric_data.get('unit', ''),
                        stat_name,
                        container,
                        metric_data.get('duration', '')
                    ])
        
        # Store per-pod data if available
        by_pod = metric_data.get('by_pod', {})
        for pod, pod_stats in by_pod.items():
            for stat_name, stat_value in pod_stats.items():
                if stat_value is not None and isinstance(stat_value, (int, float)):
                    self.connection.execute(f"""
                        INSERT INTO {table_name} (timestamp, metric_name, metric_value, metric_unit, statistic_type, pod, duration)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [
                        timestamp_dt,
                        f"{metric_name}_{stat_name}",
                        stat_value,
                        metric_data.get('unit', ''),
                        stat_name,
                        pod,
                        metric_data.get('duration', '')
                    ])
    
    async def _store_category_summary(self, category: str, summary: Dict[str, Any], duration: str, timestamp_dt: datetime) -> None:
        """Store category summary data"""
        summary_table = f"{category}_summary"
        
        # Create summary table
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {summary_table} (
            id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            health_score INTEGER,
            overall_status VARCHAR,
            alerts JSON,
            performance_data JSON,
            total_metrics INTEGER,
            duration VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        self.connection.execute(create_table_sql)
        
        # Store summary
        self.connection.execute(f"""
            INSERT INTO {summary_table} (timestamp, health_score, overall_status, alerts, performance_data, total_metrics, duration)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            timestamp_dt,
            summary.get('health_score', 0),
            summary.get('overall_status', 'unknown'),
            json.dumps(summary.get('alerts', [])),
            json.dumps(summary),
            summary.get('total_metrics', 0),
            duration
        ])
    
    async def _create_performance_summary(self, metrics_data: Dict[str, Any], timestamp: str) -> None:
        """Create overall performance summary"""
        timestamp_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        # Create performance summary table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS performance_summary (
            id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            overall_health_score DOUBLE,
            api_server_health INTEGER,
            multus_health INTEGER,
            ovnk_pods_health INTEGER,
            ovnk_containers_health INTEGER,
            ovnk_sync_health INTEGER,
            total_alerts INTEGER,
            critical_alerts INTEGER,
            warning_alerts INTEGER,
            cluster_status VARCHAR,
            raw_data JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        self.connection.execute(create_table_sql)
        
        # Calculate overall metrics
        health_scores = []
        total_alerts = 0
        critical_alerts = 0
        warning_alerts = 0
        
        categories = ['api_server', 'multus', 'ovnk_pods', 'ovnk_containers', 'ovnk_sync']
        category_health = {}
        
        for category in categories:
            if category in metrics_data:
                summary = metrics_data[category].get('summary', {})
                health_score = summary.get('health_score', 100)
                health_scores.append(health_score)
                category_health[f"{category}_health"] = health_score
                
                alerts = summary.get('alerts', [])
                total_alerts += len(alerts)
                
                # Count alert severity (simple heuristic)
                for alert in alerts:
                    if 'high' in alert.lower() or 'critical' in alert.lower():
                        critical_alerts += 1
                    else:
                        warning_alerts += 1
        
        overall_health = sum(health_scores) / len(health_scores) if health_scores else 0
        
        # Determine cluster status
        if overall_health >= 90:
            cluster_status = 'excellent'
        elif overall_health >= 80:
            cluster_status = 'good'
        elif overall_health >= 70:
            cluster_status = 'warning'
        else:
            cluster_status = 'critical'
        
        # Store performance summary
        self.connection.execute("""
            INSERT INTO performance_summary (
                timestamp, overall_health_score, api_server_health, multus_health, 
                ovnk_pods_health, ovnk_containers_health, ovnk_sync_health,
                total_alerts, critical_alerts, warning_alerts, cluster_status, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            timestamp_dt,
            round(overall_health, 2),
            category_health.get('api_server_health', 0),
            category_health.get('multus_health', 0),
            category_health.get('ovnk_pods_health', 0),
            category_health.get('ovnk_containers_health', 0),
            category_health.get('ovnk_sync_health', 0),
            total_alerts,
            critical_alerts,
            warning_alerts,
            cluster_status,
            json.dumps(metrics_data)
        ])
    
    async def get_performance_history(self, days: int = 7, metric_type: Optional[str] = None) -> Dict[str, Any]:
        """Get historical performance data"""
        if not self.connection:
            await self.initialize()
        
        since = datetime.now(timezone.utc) - timedelta(days=days)
        
        result = {
            'period': f"last_{days}_days",
            'start_time': since.isoformat(),
            'end_time': datetime.now(timezone.utc).isoformat(),
            'data': {}
        }
        
        try:
            # Get overall performance trend
            overall_trend = self.connection.execute("""
                SELECT 
                    DATE(timestamp) as date,
                    AVG(overall_health_score) as avg_health,
                    MIN(overall_health_score) as min_health,
                    MAX(overall_health_score) as max_health,
                    SUM(total_alerts) as total_alerts,
                    AVG(api_server_health) as api_health,
                    AVG(multus_health) as multus_health,
                    AVG(ovnk_pods_health) as pods_health
                FROM performance_summary
                WHERE timestamp >= ?
                GROUP BY DATE(timestamp)
                ORDER BY date DESC
            """, [since]).fetchall()
            
            result['data']['overall_trend'] = [
                {
                    'date': row[0],
                    'avg_health': round(row[1], 2) if row[1] else 0,
                    'min_health': round(row[2], 2) if row[2] else 0,
                    'max_health': round(row[3], 2) if row[3] else 0,
                    'total_alerts': int(row[4]) if row[4] else 0,
                    'api_health': round(row[5], 2) if row[5] else 0,
                    'multus_health': round(row[6], 2) if row[6] else 0,
                    'pods_health': round(row[7], 2) if row[7] else 0
                }
                for row in overall_trend
            ]
            
            # Get cluster info trend
            cluster_trend = self.connection.execute("""
                SELECT 
                    DATE(timestamp) as date,
                    AVG(total_networkpolicies) as avg_networkpolicies,
                    AVG(total_adminnetworkpolicies) as avg_adminnetworkpolicies,
                    AVG(total_egressfirewalls) as avg_egressfirewalls,
                    MAX(total_namespaces) as max_namespaces,
                    MAX(total_nodes) as max_nodes
                FROM cluster_general_info
                WHERE timestamp >= ?
                GROUP BY DATE(timestamp)
                ORDER BY date DESC
            """, [since]).fetchall()
            
            result['data']['cluster_trend'] = [
                {
                    'date': row[0],
                    'avg_networkpolicies': int(row[1]) if row[1] else 0,
                    'avg_adminnetworkpolicies': int(row[2]) if row[2] else 0,
                    'avg_egressfirewalls': int(row[3]) if row[3] else 0,
                    'max_namespaces': int(row[4]) if row[4] else 0,
                    'max_nodes': int(row[5]) if row[5] else 0
                }
                for row in cluster_trend
            ]
            
            # Get specific metric type data if requested
            if metric_type and metric_type in ['api_server', 'multus', 'ovnk_pods', 'ovnk_containers', 'ovnk_sync']:
                specific_data = await self._get_specific_metric_history(metric_type, since)
                result['data'][f'{metric_type}_specific'] = specific_data
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    async def _get_specific_metric_history(self, metric_type: str, since: datetime) -> List[Dict[str, Any]]:
        """Get specific metric type history"""
        table_name = f"{metric_type}_summary"
        
        try:
            data = self.connection.execute(f"""
                SELECT 
                    timestamp,
                    health_score,
                    overall_status,
                    alerts,
                    total_metrics
                FROM {table_name}
                WHERE timestamp >= ?
                ORDER BY timestamp DESC
                LIMIT 100
            """, [since]).fetchall()
            
            return [
                {
                    'timestamp': row[0].isoformat() if row[0] else None,
                    'health_score': row[1],
                    'overall_status': row[2],
                    'alerts': json.loads(row[3]) if row[3] else [],
                    'total_metrics': row[4]
                }
                for row in data
            ]
        except Exception:
            return []
    
    async def export_performance_data_to_dataframe(self, 
                                                  category: Optional[str] = None,
                                                  start_time: Optional[str] = None,
                                                  end_time: Optional[str] = None) -> pd.DataFrame:
        """Export performance data to pandas DataFrame for analysis"""
        if not self.connection:
            await self.initialize()
        
        query_parts = []
        params = []
        
        if category:
            # Export specific category data
            table_name = f"{category}_metrics"
            query = f"""
                SELECT 
                    timestamp,
                    metric_name,
                    metric_value,
                    metric_unit,
                    statistic_type,
                    container,
                    pod,
                    duration
                FROM {table_name}
                WHERE 1=1
            """
            
            if start_time:
                query += " AND timestamp >= ?"
                params.append(datetime.fromisoformat(start_time.replace('Z', '+00:00')))
            
            if end_time:
                query += " AND timestamp <= ?"
                params.append(datetime.fromisoformat(end_time.replace('Z', '+00:00')))
            
            query += " ORDER BY timestamp DESC"
            
        else:
            # Export overall performance summary
            query = """
                SELECT 
                    timestamp,
                    overall_health_score,
                    api_server_health,
                    multus_health,
                    ovnk_pods_health,
                    ovnk_containers_health,
                    ovnk_sync_health,
                    total_alerts,
                    critical_alerts,
                    warning_alerts,
                    cluster_status
                FROM performance_summary
                WHERE 1=1
            """
            
            if start_time:
                query += " AND timestamp >= ?"
                params.append(datetime.fromisoformat(start_time.replace('Z', '+00:00')))
            
            if end_time:
                query += " AND timestamp <= ?"
                params.append(datetime.fromisoformat(end_time.replace('Z', '+00:00')))
            
            query += " ORDER BY timestamp DESC"
        
        try:
            return pd.read_sql_query(query, self.connection, params=params)
        except Exception as e:
            print(f"Error exporting to DataFrame: {e}")
            return pd.DataFrame()
    
    async def generate_performance_report_data(self, days: int = 7) -> Dict[str, Any]:
        """Generate data for performance reports"""
        if not self.connection:
            await self.initialize()
        
        since = datetime.now(timezone.utc) - timedelta(days=days)
        
        report_data = {
            'report_period': {
                'days': days,
                'start_time': since.isoformat(),
                'end_time': datetime.now(timezone.utc).isoformat()
            },
            'executive_summary': {},
            'detailed_metrics': {},
            'trends': {},
            'alerts_analysis': {},
            'recommendations': []
        }
        
        try:
            # Executive Summary
            exec_summary = self.connection.execute("""
                SELECT 
                    AVG(overall_health_score) as avg_health,
                    MIN(overall_health_score) as min_health,
                    MAX(overall_health_score) as max_health,
                    SUM(total_alerts) as total_alerts,
                    SUM(critical_alerts) as total_critical,
                    COUNT(*) as total_measurements
                FROM performance_summary
                WHERE timestamp >= ?
            """, [since]).fetchone()
            
            if exec_summary:
                report_data['executive_summary'] = {
                    'average_health_score': round(exec_summary[0], 2) if exec_summary[0] else 0,
                    'minimum_health_score': round(exec_summary[1], 2) if exec_summary[1] else 0,
                    'maximum_health_score': round(exec_summary[2], 2) if exec_summary[2] else 0,
                    'total_alerts': int(exec_summary[3]) if exec_summary[3] else 0,
                    'critical_alerts': int(exec_summary[4]) if exec_summary[4] else 0,
                    'total_measurements': int(exec_summary[5]) if exec_summary[5] else 0
                }
            
            # Category Performance
            categories = ['api_server', 'multus', 'ovnk_pods', 'ovnk_containers', 'ovnk_sync']
            for category in categories:
                try:
                    category_data = self.connection.execute(f"""
                        SELECT 
                            AVG(health_score) as avg_health,
                            MIN(health_score) as min_health,
                            MAX(health_score) as max_health,
                            COUNT(*) as measurements
                        FROM {category}_summary
                        WHERE timestamp >= ?
                    """, [since]).fetchone()
                    
                    if category_data:
                        report_data['detailed_metrics'][category] = {
                            'average_health': round(category_data[0], 2) if category_data[0] else 0,
                            'minimum_health': round(category_data[1], 2) if category_data[1] else 0,
                            'maximum_health': round(category_data[2], 2) if category_data[2] else 0,
                            'measurements': int(category_data[3]) if category_data[3] else 0
                        }
                except Exception:
                    report_data['detailed_metrics'][category] = {'error': 'No data available'}
            
            # Cluster Growth Trend
            cluster_growth = self.connection.execute("""
                SELECT 
                    MIN(total_networkpolicies) as min_np,
                    MAX(total_networkpolicies) as max_np,
                    MIN(total_namespaces) as min_ns,
                    MAX(total_namespaces) as max_ns,
                    MIN(total_nodes) as min_nodes,
                    MAX(total_nodes) as max_nodes
                FROM cluster_general_info
                WHERE timestamp >= ?
            """, [since]).fetchone()
            
            if cluster_growth:
                report_data['trends']['cluster_growth'] = {
                    'networkpolicies_change': (cluster_growth[1] or 0) - (cluster_growth[0] or 0),
                    'namespaces_change': (cluster_growth[3] or 0) - (cluster_growth[2] or 0),
                    'nodes_change': (cluster_growth[5] or 0) - (cluster_growth[4] or 0)
                }
            
            # Generate recommendations based on data
            avg_health = report_data['executive_summary'].get('average_health_score', 0)
            total_alerts = report_data['executive_summary'].get('total_alerts', 0)
            
            if avg_health < 70:
                report_data['recommendations'].append({
                    'priority': 'high',
                    'category': 'performance',
                    'message': f'Average health score is {avg_health}%, indicating performance issues that need immediate attention.'
                })
            
            if total_alerts > 50:
                report_data['recommendations'].append({
                    'priority': 'medium',
                    'category': 'monitoring',
                    'message': f'High number of alerts ({total_alerts}) detected in the reporting period. Consider investigating root causes.'
                })
            
            # Add category-specific recommendations
            for category, metrics in report_data['detailed_metrics'].items():
                if isinstance(metrics, dict) and 'average_health' in metrics:
                    if metrics['average_health'] < 80:
                        report_data['recommendations'].append({
                            'priority': 'medium',
                            'category': category,
                            'message': f'{category.replace("_", " ").title()} performance is below optimal (${metrics["average_health"]}%). Review configuration and resource allocation.'
                        })
            
        except Exception as e:
            report_data['error'] = str(e)
        
        return report_data
    
    async def close(self) -> None:
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None