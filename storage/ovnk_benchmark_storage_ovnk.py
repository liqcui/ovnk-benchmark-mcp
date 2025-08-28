"""
Prometheus Storage Module for OVN-Kubernetes Benchmarks
Handles DuckDB storage operations for performance data
"""

import asyncio
import duckdb
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import os


class PrometheusStorage:
    """Storage handler for Prometheus metrics using DuckDB"""
    
    def __init__(self, db_path: str = "storage/ovnk_benchmark.db"):
        self.db_path = db_path
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        self._ensure_storage_directory()
    
    def _ensure_storage_directory(self) -> None:
        """Ensure storage directory exists"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
    
    async def initialize(self) -> None:
        """Initialize database and create tables"""
        try:
            self.connection = duckdb.connect(self.db_path)
            await self._create_tables()
            print(f"✅ Storage initialized: {self.db_path}")
        except Exception as e:
            print(f"❌ Failed to initialize storage: {e}")
            raise
    
    async def _create_tables(self) -> None:
        """Create necessary tables for storing metrics"""
        
        # Main metrics table
        metrics_table = """
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            metric_category VARCHAR,
            metric_name VARCHAR,
            metric_value DOUBLE,
            metric_unit VARCHAR,
            labels JSON,
            duration VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Summary table for aggregated data
        summary_table = """
        CREATE TABLE IF NOT EXISTS metric_summaries (
            id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            category VARCHAR,
            summary_data JSON,
            health_score INTEGER,
            overall_status VARCHAR,
            alerts JSON,
            duration VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Performance snapshots table
        snapshots_table = """
        CREATE TABLE IF NOT EXISTS performance_snapshots (
            id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            snapshot_data JSON,
            cluster_info JSON,
            total_metrics INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Benchmark runs table
        benchmark_runs_table = """
        CREATE TABLE IF NOT EXISTS benchmark_runs (
            id INTEGER PRIMARY KEY,
            run_id VARCHAR UNIQUE,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration VARCHAR,
            run_type VARCHAR,
            configuration JSON,
            status VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Alerts history table
        alerts_table = """
        CREATE TABLE IF NOT EXISTS alerts_history (
            id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            alert_type VARCHAR,
            alert_message VARCHAR,
            severity VARCHAR,
            category VARCHAR,
            metric_name VARCHAR,
            value DOUBLE,
            threshold DOUBLE,
            resolved_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        tables = [
            metrics_table,
            summary_table,
            snapshots_table,
            benchmark_runs_table,
            alerts_table
        ]
        
        for table_sql in tables:
            self.connection.execute(table_sql)
        
        # Create indexes for better performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_metrics_category ON metrics(metric_category)",
            "CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(metric_name)",
            "CREATE INDEX IF NOT EXISTS idx_summaries_timestamp ON metric_summaries(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_summaries_category ON metric_summaries(category)",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON performance_snapshots(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_benchmark_runs_start_time ON benchmark_runs(start_time)",
            "CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts_history(timestamp)",
        ]
        
        for index_sql in indexes:
            self.connection.execute(index_sql)
    
    async def store_metrics(self, category: str, metrics_data: Dict[str, Any], timestamp: Optional[str] = None) -> None:
        """Store metrics data"""
        if not timestamp:
            timestamp = datetime.now(timezone.utc).isoformat()
        
        timestamp_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        self.connection.execute("""
            INSERT INTO performance_snapshots (timestamp, snapshot_data, cluster_info, total_metrics)
            VALUES (?, ?, ?, ?)
        """, [
            timestamp_dt,
            json.dumps(snapshot_data),
            json.dumps(snapshot_data.get('cluster_info', {})),
            len(snapshot_data.get('metrics', {}))
        ])
    
    async def start_benchmark_run(self, run_id: str, run_type: str, configuration: Dict[str, Any]) -> None:
        """Start a new benchmark run"""
        self.connection.execute("""
            INSERT INTO benchmark_runs (run_id, start_time, run_type, configuration, status)
            VALUES (?, ?, ?, ?, ?)
        """, [
            run_id,
            datetime.now(timezone.utc),
            run_type,
            json.dumps(configuration),
            'running'
        ])
    
    async def end_benchmark_run(self, run_id: str, status: str = 'completed') -> None:
        """End a benchmark run"""
        self.connection.execute("""
            UPDATE benchmark_runs 
            SET end_time = ?, status = ?
            WHERE run_id = ?
        """, [
            datetime.now(timezone.utc),
            status,
            run_id
        ])
    
    async def store_alert(self, alert_data: Dict[str, Any], timestamp: Optional[str] = None) -> None:
        """Store an alert"""
        if not timestamp:
            timestamp = datetime.now(timezone.utc).isoformat()
        
        timestamp_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        self.connection.execute("""
            INSERT INTO alerts_history (timestamp, alert_type, alert_message, severity, category, metric_name, value, threshold)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            timestamp_dt,
            alert_data.get('type', 'unknown'),
            alert_data.get('message', ''),
            alert_data.get('severity', 'info'),
            alert_data.get('category', ''),
            alert_data.get('metric_name', ''),
            alert_data.get('value'),
            alert_data.get('threshold')
        ])
    
    async def get_metrics_history(self, 
                                 category: Optional[str] = None,
                                 metric_name: Optional[str] = None,
                                 start_time: Optional[str] = None,
                                 end_time: Optional[str] = None,
                                 limit: int = 1000) -> List[Dict[str, Any]]:
        """Get metrics history"""
        query = "SELECT * FROM metrics WHERE 1=1"
        params = []
        
        if category:
            query += " AND metric_category = ?"
            params.append(category)
        
        if metric_name:
            query += " AND metric_name = ?"
            params.append(metric_name)
        
        if start_time:
            query += " AND timestamp >= ?"
            params.append(datetime.fromisoformat(start_time.replace('Z', '+00:00')))
        
        if end_time:
            query += " AND timestamp <= ?"
            params.append(datetime.fromisoformat(end_time.replace('Z', '+00:00')))
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        result = self.connection.execute(query, params).fetchall()
        
        columns = ['id', 'timestamp', 'metric_category', 'metric_name', 'metric_value', 
                  'metric_unit', 'labels', 'duration', 'created_at']
        
        return [dict(zip(columns, row)) for row in result]
    
    async def get_summary_history(self, 
                                 category: Optional[str] = None,
                                 start_time: Optional[str] = None,
                                 end_time: Optional[str] = None,
                                 limit: int = 100) -> List[Dict[str, Any]]:
        """Get summary history"""
        query = "SELECT * FROM metric_summaries WHERE 1=1"
        params = []
        
        if category:
            query += " AND category = ?"
            params.append(category)
        
        if start_time:
            query += " AND timestamp >= ?"
            params.append(datetime.fromisoformat(start_time.replace('Z', '+00:00')))
        
        if end_time:
            query += " AND timestamp <= ?"
            params.append(datetime.fromisoformat(end_time.replace('Z', '+00:00')))
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        result = self.connection.execute(query, params).fetchall()
        
        columns = ['id', 'timestamp', 'category', 'summary_data', 'health_score', 
                  'overall_status', 'alerts', 'duration', 'created_at']
        
        history = []
        for row in result:
            row_dict = dict(zip(columns, row))
            # Parse JSON fields
            if row_dict['summary_data']:
                row_dict['summary_data'] = json.loads(row_dict['summary_data'])
            if row_dict['alerts']:
                row_dict['alerts'] = json.loads(row_dict['alerts'])
            history.append(row_dict)
        
        return history
    
    async def get_performance_snapshots(self, 
                                       start_time: Optional[str] = None,
                                       end_time: Optional[str] = None,
                                       limit: int = 50) -> List[Dict[str, Any]]:
        """Get performance snapshots"""
        query = "SELECT * FROM performance_snapshots WHERE 1=1"
        params = []
        
        if start_time:
            query += " AND timestamp >= ?"
            params.append(datetime.fromisoformat(start_time.replace('Z', '+00:00')))
        
        if end_time:
            query += " AND timestamp <= ?"
            params.append(datetime.fromisoformat(end_time.replace('Z', '+00:00')))
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        result = self.connection.execute(query, params).fetchall()
        
        columns = ['id', 'timestamp', 'snapshot_data', 'cluster_info', 'total_metrics', 'created_at']
        
        snapshots = []
        for row in result:
            row_dict = dict(zip(columns, row))
            # Parse JSON fields
            if row_dict['snapshot_data']:
                row_dict['snapshot_data'] = json.loads(row_dict['snapshot_data'])
            if row_dict['cluster_info']:
                row_dict['cluster_info'] = json.loads(row_dict['cluster_info'])
            snapshots.append(row_dict)
        
        return snapshots
    
    async def get_benchmark_runs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get benchmark runs"""
        result = self.connection.execute("""
            SELECT * FROM benchmark_runs 
            ORDER BY start_time DESC 
            LIMIT ?
        """, [limit]).fetchall()
        
        columns = ['id', 'run_id', 'start_time', 'end_time', 'duration', 'run_type', 
                  'configuration', 'status', 'created_at']
        
        runs = []
        for row in result:
            row_dict = dict(zip(columns, row))
            if row_dict['configuration']:
                row_dict['configuration'] = json.loads(row_dict['configuration'])
            runs.append(row_dict)
        
        return runs
    
    async def get_recent_alerts(self, hours: int = 24, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent alerts"""
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        result = self.connection.execute("""
            SELECT * FROM alerts_history 
            WHERE timestamp >= ? 
            ORDER BY timestamp DESC 
            LIMIT ?
        """, [since, limit]).fetchall()
        
        columns = ['id', 'timestamp', 'alert_type', 'alert_message', 'severity', 
                  'category', 'metric_name', 'value', 'threshold', 'resolved_at', 'created_at']
        
        return [dict(zip(columns, row)) for row in result]
    
    async def get_health_trend(self, category: str, days: int = 7) -> List[Dict[str, Any]]:
        """Get health score trend for a category"""
        since = datetime.now(timezone.utc) - timedelta(days=days)
        
        result = self.connection.execute("""
            SELECT DATE(timestamp) as date, 
                   AVG(health_score) as avg_health_score,
                   MIN(health_score) as min_health_score,
                   MAX(health_score) as max_health_score,
                   COUNT(*) as measurements
            FROM metric_summaries 
            WHERE category = ? AND timestamp >= ?
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
        """, [category, since]).fetchall()
        
        columns = ['date', 'avg_health_score', 'min_health_score', 'max_health_score', 'measurements']
        return [dict(zip(columns, row)) for row in result]
    
    async def get_performance_comparison(self, 
                                       category: str,
                                       current_start: str,
                                       current_end: str,
                                       comparison_start: str,
                                       comparison_end: str) -> Dict[str, Any]:
        """Compare performance between two time periods"""
        
        # Current period stats
        current_result = self.connection.execute("""
            SELECT 
                AVG(health_score) as avg_health_score,
                MIN(health_score) as min_health_score,
                MAX(health_score) as max_health_score,
                COUNT(*) as measurements
            FROM metric_summaries 
            WHERE category = ? AND timestamp BETWEEN ? AND ?
        """, [
            category,
            datetime.fromisoformat(current_start.replace('Z', '+00:00')),
            datetime.fromisoformat(current_end.replace('Z', '+00:00'))
        ]).fetchone()
        
        # Comparison period stats
        comparison_result = self.connection.execute("""
            SELECT 
                AVG(health_score) as avg_health_score,
                MIN(health_score) as min_health_score,
                MAX(health_score) as max_health_score,
                COUNT(*) as measurements
            FROM metric_summaries 
            WHERE category = ? AND timestamp BETWEEN ? AND ?
        """, [
            category,
            datetime.fromisoformat(comparison_start.replace('Z', '+00:00')),
            datetime.fromisoformat(comparison_end.replace('Z', '+00:00'))
        ]).fetchone()
        
        current_stats = {
            'avg_health_score': current_result[0] if current_result[0] else 0,
            'min_health_score': current_result[1] if current_result[1] else 0,
            'max_health_score': current_result[2] if current_result[2] else 0,
            'measurements': current_result[3] if current_result[3] else 0
        }
        
        comparison_stats = {
            'avg_health_score': comparison_result[0] if comparison_result[0] else 0,
            'min_health_score': comparison_result[1] if comparison_result[1] else 0,
            'max_health_score': comparison_result[2] if comparison_result[2] else 0,
            'measurements': comparison_result[3] if comparison_result[3] else 0
        }
        
        # Calculate differences
        health_diff = current_stats['avg_health_score'] - comparison_stats['avg_health_score']
        
        return {
            'category': category,
            'current_period': current_stats,
            'comparison_period': comparison_stats,
            'health_score_difference': round(health_diff, 2),
            'trend': 'improving' if health_diff > 0 else 'declining' if health_diff < 0 else 'stable'
        }
    
    async def cleanup_old_data(self, days: int = 30) -> Dict[str, int]:
        """Clean up old data beyond retention period"""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Count records before deletion
        metrics_count = self.connection.execute("SELECT COUNT(*) FROM metrics WHERE timestamp < ?", [cutoff]).fetchone()[0]
        summaries_count = self.connection.execute("SELECT COUNT(*) FROM metric_summaries WHERE timestamp < ?", [cutoff]).fetchone()[0]
        snapshots_count = self.connection.execute("SELECT COUNT(*) FROM performance_snapshots WHERE timestamp < ?", [cutoff]).fetchone()[0]
        alerts_count = self.connection.execute("SELECT COUNT(*) FROM alerts_history WHERE timestamp < ?", [cutoff]).fetchone()[0]
        
        # Delete old records
        self.connection.execute("DELETE FROM metrics WHERE timestamp < ?", [cutoff])
        self.connection.execute("DELETE FROM metric_summaries WHERE timestamp < ?", [cutoff])
        self.connection.execute("DELETE FROM performance_snapshots WHERE timestamp < ?", [cutoff])
        self.connection.execute("DELETE FROM alerts_history WHERE timestamp < ?", [cutoff])
        
        # Vacuum to reclaim space
        self.connection.execute("VACUUM")
        
        return {
            'metrics_deleted': metrics_count,
            'summaries_deleted': summaries_count,
            'snapshots_deleted': snapshots_count,
            'alerts_deleted': alerts_count,
            'cutoff_date': cutoff.isoformat()
        }
    
    async def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        stats = {}
        
        # Table counts
        stats['metrics_count'] = self.connection.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        stats['summaries_count'] = self.connection.execute("SELECT COUNT(*) FROM metric_summaries").fetchone()[0]
        stats['snapshots_count'] = self.connection.execute("SELECT COUNT(*) FROM performance_snapshots").fetchone()[0]
        stats['benchmark_runs_count'] = self.connection.execute("SELECT COUNT(*) FROM benchmark_runs").fetchone()[0]
        stats['alerts_count'] = self.connection.execute("SELECT COUNT(*) FROM alerts_history").fetchone()[0]
        
        # Date ranges
        try:
            oldest_metric = self.connection.execute("SELECT MIN(timestamp) FROM metrics").fetchone()[0]
            newest_metric = self.connection.execute("SELECT MAX(timestamp) FROM metrics").fetchone()[0]
            stats['data_range'] = {
                'oldest': oldest_metric.isoformat() if oldest_metric else None,
                'newest': newest_metric.isoformat() if newest_metric else None
            }
        except:
            stats['data_range'] = {'oldest': None, 'newest': None}
        
        # Database file size
        try:
            file_size = os.path.getsize(self.db_path)
            stats['database_size_mb'] = round(file_size / (1024 * 1024), 2)
        except:
            stats['database_size_mb'] = 0
        
        return stats
    
    async def close(self) -> None:
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = Nonedt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        metrics_to_insert = []
        
        for metric_name, metric_data in metrics_data.get('metrics', {}).items():
            if 'error' in metric_data:
                continue
            
            # Extract values from the metric data
            if 'values' in metric_data:
                for value_item in metric_data['values']:
                    labels = value_item.get('labels', {})
                    
                    if 'values' in value_item:
                        # Range query data
                        for ts_value in value_item['values']:
                            metrics_to_insert.append({
                                'timestamp': datetime.fromtimestamp(ts_value['timestamp'], timezone.utc),
                                'metric_category': category,
                                'metric_name': metric_name,
                                'metric_value': ts_value['value'],
                                'metric_unit': metric_data.get('unit', ''),
                                'labels': json.dumps(labels),
                                'duration': metrics_data.get('duration', '')
                            })
                    else:
                        # Instant query data
                        for ts_value in value_item.get('values', []):
                            metrics_to_insert.append({
                                'timestamp': datetime.fromtimestamp(ts_value['timestamp'], timezone.utc),
                                'metric_category': category,
                                'metric_name': metric_name,
                                'metric_value': ts_value['value'],
                                'metric_unit': metric_data.get('unit', ''),
                                'labels': json.dumps(labels),
                                'duration': metrics_data.get('duration', '')
                            })
            
            # Store statistics as separate entries
            if 'statistics' in metric_data and metric_data['statistics']:
                stats = metric_data['statistics']
                for stat_name, stat_value in stats.items():
                    if stat_value is not None and isinstance(stat_value, (int, float)):
                        metrics_to_insert.append({
                            'timestamp': timestamp_dt,
                            'metric_category': category,
                            'metric_name': f"{metric_name}_{stat_name}",
                            'metric_value': stat_value,
                            'metric_unit': metric_data.get('unit', ''),
                            'labels': json.dumps({'statistic': stat_name}),
                            'duration': metrics_data.get('duration', '')
                        })
        
        # Bulk insert
        if metrics_to_insert:
            df = pd.DataFrame(metrics_to_insert)
            self.connection.register('metrics_df', df)
            self.connection.execute("""
                INSERT INTO metrics (timestamp, metric_category, metric_name, metric_value, metric_unit, labels, duration)
                SELECT timestamp, metric_category, metric_name, metric_value, metric_unit, labels, duration
                FROM metrics_df
            """)
    
    async def store_summary(self, category: str, summary_data: Dict[str, Any], timestamp: Optional[str] = None) -> None:
        """Store summary data"""
        if not timestamp:
            timestamp = datetime.now(timezone.utc).isoformat()
        
        timestamp_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        self.connection.execute("""
            INSERT INTO metric_summaries (timestamp, category, summary_data, health_score, overall_status, alerts, duration)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            timestamp_dt,
            category,
            json.dumps(summary_data),
            summary_data.get('health_score', 0),
            summary_data.get('overall_status', 'unknown'),
            json.dumps(summary_data.get('alerts', [])),
            summary_data.get('duration', '')
        ])
    
    async def store_performance_snapshot(self, snapshot_data: Dict[str, Any], timestamp: Optional[str] = None) -> None:
        """Store a complete performance snapshot"""
        if not timestamp:
            timestamp = datetime.now(timezone.utc).isoformat()
        
        timestamp_