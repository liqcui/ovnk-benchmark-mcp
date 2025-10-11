"""
OVN-Kubernetes Metrics Storage Module
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


class ovnkMetricStor:
    """Storage handler for OVN-Kubernetes metrics using DuckDB"""
    
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
            start_time TIMESTAMP,
            end_time TIMESTAMP,
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
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Performance snapshots table
        snapshots_table = """
        CREATE TABLE IF NOT EXISTS performance_snapshots (
            id INTEGER PRIMARY KEY,
            run_id VARCHAR,
            timestamp TIMESTAMP,
            snapshot_data JSON,
            cluster_info JSON,
            total_metrics INTEGER,
            duration VARCHAR,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            success_count INTEGER,
            total_steps INTEGER,
            success_rate DOUBLE,
            errors JSON,
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
            total_collections INTEGER,
            successful_collections INTEGER,
            success_rate DOUBLE,
            errors JSON,
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
        
        # Collection categories table
        collection_categories_table = """
        CREATE TABLE IF NOT EXISTS collection_categories (
            id INTEGER PRIMARY KEY,
            run_id VARCHAR,
            timestamp TIMESTAMP,
            category VARCHAR,
            category_data JSON,
            status VARCHAR,
            error_message VARCHAR,
            duration VARCHAR,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        tables = [
            metrics_table,
            summary_table,
            snapshots_table,
            benchmark_runs_table,
            alerts_table,
            collection_categories_table
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
            "CREATE INDEX IF NOT EXISTS idx_snapshots_run_id ON performance_snapshots(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_benchmark_runs_start_time ON benchmark_runs(start_time)",
            "CREATE INDEX IF NOT EXISTS idx_benchmark_runs_run_id ON benchmark_runs(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts_history(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_collection_categories_run_id ON collection_categories(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_collection_categories_category ON collection_categories(category)",
        ]
        
        for index_sql in indexes:
            self.connection.execute(index_sql)
    
    # ========== Common Save Functions ==========
    
    async def save_collection_result(self, collection_result: Dict[str, Any]) -> str:
        """
        Common function to save complete collection result to DuckDB
        Handles all aspects: run metadata, categories, metrics, and snapshots
        
        Args:
            collection_result: Complete result from agent collection
            
        Returns:
            run_id of the saved collection
        """
        run_id = collection_result.get('run_id')
        timestamp = collection_result.get('timestamp')
        
        if not run_id or not timestamp:
            raise ValueError("Collection result must contain run_id and timestamp")
        
        try:
            # 1. Save benchmark run metadata
            await self._save_benchmark_run(collection_result)
            
            # 2. Save individual category data
            await self._save_collection_categories(collection_result)
            
            # 3. Save performance snapshot
            await self._save_performance_snapshot(collection_result)
            
            # 4. Extract and save metrics from categories
            await self._save_metrics_from_categories(collection_result)
            
            # 5. Extract and save alerts if present
            await self._save_alerts_from_collection(collection_result)
            
            print(f"✅ Collection saved successfully - Run ID: {run_id}")
            return run_id
            
        except Exception as e:
            print(f"❌ Error saving collection result: {e}")
            raise
    
    async def _save_benchmark_run(self, collection_result: Dict[str, Any]) -> None:
        """Save benchmark run metadata"""
        run_id = collection_result.get('run_id')
        timestamp_str = collection_result.get('timestamp')
        duration = collection_result.get('duration')
        start_time_str = collection_result.get('start_time')
        end_time_str = collection_result.get('end_time')
        
        timestamp_dt = self._parse_timestamp(timestamp_str)
        start_time_dt = self._parse_timestamp(start_time_str) if start_time_str else None
        end_time_dt = self._parse_timestamp(end_time_str) if end_time_str else None
        
        self.connection.execute("""
            INSERT INTO benchmark_runs 
            (run_id, start_time, end_time, duration, run_type, configuration, status, 
             total_collections, successful_collections, success_rate, errors)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            run_id,
            start_time_dt or timestamp_dt,
            end_time_dt or timestamp_dt,
            duration,
            collection_result.get('mode', 'unknown'),
            json.dumps(collection_result.get('config', {})),
            'completed' if collection_result.get('success') else 'failed',
            collection_result.get('total_steps', 0),
            collection_result.get('success_count', 0),
            collection_result.get('success_rate', 0.0),
            json.dumps(collection_result.get('errors', []))
        ])
    
    async def _save_collection_categories(self, collection_result: Dict[str, Any]) -> None:
        """Save individual category data"""
        run_id = collection_result.get('run_id')
        timestamp_str = collection_result.get('timestamp')
        timestamp_dt = self._parse_timestamp(timestamp_str)
        duration = collection_result.get('duration')
        start_time_str = collection_result.get('start_time')
        end_time_str = collection_result.get('end_time')
        
        start_time_dt = self._parse_timestamp(start_time_str) if start_time_str else None
        end_time_dt = self._parse_timestamp(end_time_str) if end_time_str else None
        
        collected_data = collection_result.get('collected_data', {})
        
        for category, category_data in collected_data.items():
            status = 'success' if not category_data.get('error') else 'failed'
            error_message = category_data.get('error', '')
            
            self.connection.execute("""
                INSERT INTO collection_categories 
                (run_id, timestamp, category, category_data, status, error_message, 
                 duration, start_time, end_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                run_id,
                timestamp_dt,
                category,
                json.dumps(category_data),
                status,
                error_message,
                duration,
                start_time_dt,
                end_time_dt
            ])
    
    async def _save_performance_snapshot(self, collection_result: Dict[str, Any]) -> None:
        """Save complete performance snapshot"""
        run_id = collection_result.get('run_id')
        timestamp_str = collection_result.get('timestamp')
        timestamp_dt = self._parse_timestamp(timestamp_str)
        duration = collection_result.get('duration')
        start_time_str = collection_result.get('start_time')
        end_time_str = collection_result.get('end_time')
        
        start_time_dt = self._parse_timestamp(start_time_str) if start_time_str else None
        end_time_dt = self._parse_timestamp(end_time_str) if end_time_str else None
        
        collected_data = collection_result.get('collected_data', {})
        cluster_info = collected_data.get('cluster_info', {})
        
        self.connection.execute("""
            INSERT INTO performance_snapshots 
            (run_id, timestamp, snapshot_data, cluster_info, total_metrics, duration, 
             start_time, end_time, success_count, total_steps, success_rate, errors)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            run_id,
            timestamp_dt,
            json.dumps(collected_data),
            json.dumps(cluster_info),
            len(collected_data),
            duration,
            start_time_dt,
            end_time_dt,
            collection_result.get('success_count', 0),
            collection_result.get('total_steps', 0),
            collection_result.get('success_rate', 0.0),
            json.dumps(collection_result.get('errors', []))
        ])
    
    async def _save_metrics_from_categories(self, collection_result: Dict[str, Any]) -> None:
        """Extract and save individual metrics from category data"""
        run_id = collection_result.get('run_id')
        timestamp_str = collection_result.get('timestamp')
        timestamp_dt = self._parse_timestamp(timestamp_str)
        duration = collection_result.get('duration')
        start_time_str = collection_result.get('start_time')
        end_time_str = collection_result.get('end_time')
        
        start_time_dt = self._parse_timestamp(start_time_str) if start_time_str else None
        end_time_dt = self._parse_timestamp(end_time_str) if end_time_str else None
        
        collected_data = collection_result.get('collected_data', {})
        
        metrics_to_insert = []
        
        for category, category_data in collected_data.items():
            if 'error' in category_data:
                continue
            
            # Extract metrics based on category structure
            metrics_to_insert.extend(
                self._extract_metrics_from_category(
                    category, category_data, timestamp_dt, duration, 
                    start_time_dt, end_time_dt
                )
            )
        
        # Bulk insert metrics
        if metrics_to_insert:
            df = pd.DataFrame(metrics_to_insert)
            self.connection.register('metrics_df', df)
            self.connection.execute("""
                INSERT INTO metrics 
                (timestamp, metric_category, metric_name, metric_value, metric_unit, 
                 labels, duration, start_time, end_time)
                SELECT timestamp, metric_category, metric_name, metric_value, metric_unit, 
                       labels, duration, start_time, end_time
                FROM metrics_df
            """)
            self.connection.unregister('metrics_df')
    
    def _extract_metrics_from_category(self, category: str, category_data: Dict[str, Any],
                                       timestamp_dt: datetime, duration: Optional[str],
                                       start_time_dt: Optional[datetime],
                                       end_time_dt: Optional[datetime]) -> List[Dict[str, Any]]:
        """Extract metrics from category data structure"""
        metrics = []
        
        # Handle different category structures
        if category == 'cluster_info':
            metrics.extend(self._extract_cluster_info_metrics(
                category_data, timestamp_dt, duration, start_time_dt, end_time_dt
            ))
        elif category == 'node_usage':
            metrics.extend(self._extract_node_usage_metrics(
                category_data, timestamp_dt, duration, start_time_dt, end_time_dt
            ))
        elif category == 'api_server':
            metrics.extend(self._extract_api_metrics(
                category_data, timestamp_dt, duration, start_time_dt, end_time_dt
            ))
        elif category in ['ovnk_pods', 'ovnk_containers', 'multus']:
            metrics.extend(self._extract_pod_metrics(
                category, category_data, timestamp_dt, duration, start_time_dt, end_time_dt
            ))
        elif category == 'latency_metrics':
            metrics.extend(self._extract_latency_metrics(
                category_data, timestamp_dt, duration, start_time_dt, end_time_dt
            ))
        elif category == 'ovs_metrics':
            metrics.extend(self._extract_ovs_metrics(
                category_data, timestamp_dt, duration, start_time_dt, end_time_dt
            ))
        
        return metrics
    
    def _extract_cluster_info_metrics(self, data: Dict[str, Any], timestamp_dt: datetime,
                                      duration: Optional[str], start_time_dt: Optional[datetime],
                                      end_time_dt: Optional[datetime]) -> List[Dict[str, Any]]:
        """Extract metrics from cluster info"""
        metrics = []
        
        # Total nodes
        if 'total_nodes' in data:
            metrics.append({
                'timestamp': timestamp_dt,
                'metric_category': 'cluster_info',
                'metric_name': 'total_nodes',
                'metric_value': float(data['total_nodes']),
                'metric_unit': 'count',
                'labels': json.dumps({'cluster_version': data.get('cluster_version', 'unknown')}),
                'duration': duration,
                'start_time': start_time_dt,
                'end_time': end_time_dt
            })
        
        # Network policies
        if 'networkpolicies_count' in data:
            metrics.append({
                'timestamp': timestamp_dt,
                'metric_category': 'cluster_info',
                'metric_name': 'network_policies_count',
                'metric_value': float(data['networkpolicies_count']),
                'metric_unit': 'count',
                'labels': json.dumps({}),
                'duration': duration,
                'start_time': start_time_dt,
                'end_time': end_time_dt
            })
        
        # Resource counts
        for resource_type in ['pods', 'services', 'secrets', 'configmaps']:
            key = f'{resource_type}_count'
            if key in data:
                metrics.append({
                    'timestamp': timestamp_dt,
                    'metric_category': 'cluster_info',
                    'metric_name': key,
                    'metric_value': float(data[key]),
                    'metric_unit': 'count',
                    'labels': json.dumps({'resource_type': resource_type}),
                    'duration': duration,
                    'start_time': start_time_dt,
                    'end_time': end_time_dt
                })
        
        return metrics
    
    def _extract_node_usage_metrics(self, data: Dict[str, Any], timestamp_dt: datetime,
                                    duration: Optional[str], start_time_dt: Optional[datetime],
                                    end_time_dt: Optional[datetime]) -> List[Dict[str, Any]]:
        """Extract metrics from node usage data"""
        metrics = []
        
        groups = data.get('groups', {})
        for role, group_data in groups.items():
            nodes = group_data.get('nodes', [])
            
            for node in nodes:
                node_name = node.get('node', 'unknown')
                
                # CPU metrics
                cpu = node.get('cpu', {})
                for stat in ['min', 'avg', 'max']:
                    if stat in cpu:
                        metrics.append({
                            'timestamp': timestamp_dt,
                            'metric_category': 'node_usage',
                            'metric_name': f'cpu_{stat}',
                            'metric_value': float(cpu[stat]),
                            'metric_unit': 'percent',
                            'labels': json.dumps({'node': node_name, 'role': role}),
                            'duration': duration,
                            'start_time': start_time_dt,
                            'end_time': end_time_dt
                        })
                
                # Memory metrics
                memory = node.get('memory', {})
                for stat in ['min', 'avg', 'max']:
                    if stat in memory:
                        metrics.append({
                            'timestamp': timestamp_dt,
                            'metric_category': 'node_usage',
                            'metric_name': f'memory_{stat}',
                            'metric_value': float(memory[stat]),
                            'metric_unit': 'bytes',
                            'labels': json.dumps({'node': node_name, 'role': role}),
                            'duration': duration,
                            'start_time': start_time_dt,
                            'end_time': end_time_dt
                        })
        
        return metrics
    
    def _extract_api_metrics(self, data: Dict[str, Any], timestamp_dt: datetime,
                            duration: Optional[str], start_time_dt: Optional[datetime],
                            end_time_dt: Optional[datetime]) -> List[Dict[str, Any]]:
        """Extract metrics from API server data"""
        metrics = []
        
        summary = data.get('summary', {})
        
        # Health score
        if 'health_score' in summary:
            metrics.append({
                'timestamp': timestamp_dt,
                'metric_category': 'api_server',
                'metric_name': 'health_score',
                'metric_value': float(summary['health_score']),
                'metric_unit': 'score',
                'labels': json.dumps({'status': summary.get('overall_status', 'unknown')}),
                'duration': duration,
                'start_time': start_time_dt,
                'end_time': end_time_dt
            })
        
        return metrics
    
    def _extract_pod_metrics(self, category: str, data: Dict[str, Any], timestamp_dt: datetime,
                            duration: Optional[str], start_time_dt: Optional[datetime],
                            end_time_dt: Optional[datetime]) -> List[Dict[str, Any]]:
        """Extract metrics from pod/container data"""
        metrics = []
        
        top_pods = data.get('top_10_pods', [])
        
        for pod_data in top_pods:
            pod_name = pod_data.get('pod', 'unknown')
            node = pod_data.get('node', 'unknown')
            
            # CPU metrics
            cpu = pod_data.get('cpu', {})
            for stat in ['min', 'avg', 'max']:
                if stat in cpu:
                    metrics.append({
                        'timestamp': timestamp_dt,
                        'metric_category': category,
                        'metric_name': f'pod_cpu_{stat}',
                        'metric_value': float(cpu[stat]),
                        'metric_unit': 'percent',
                        'labels': json.dumps({'pod': pod_name, 'node': node}),
                        'duration': duration,
                        'start_time': start_time_dt,
                        'end_time': end_time_dt
                    })
            
            # Memory metrics
            memory = pod_data.get('memory', {})
            for stat in ['min', 'avg', 'max']:
                if stat in memory:
                    metrics.append({
                        'timestamp': timestamp_dt,
                        'metric_category': category,
                        'metric_name': f'pod_memory_{stat}',
                        'metric_value': float(memory[stat]),
                        'metric_unit': 'bytes',
                        'labels': json.dumps({'pod': pod_name, 'node': node}),
                        'duration': duration,
                        'start_time': start_time_dt,
                        'end_time': end_time_dt
                    })
        
        return metrics
    
    def _extract_latency_metrics(self, data: Dict[str, Any], timestamp_dt: datetime,
                                 duration: Optional[str], start_time_dt: Optional[datetime],
                                 end_time_dt: Optional[datetime]) -> List[Dict[str, Any]]:
        """Extract metrics from latency data"""
        metrics = []
        
        latency_categories = data.get('latency_metrics', {})
        
        for metric_name, metric_data in latency_categories.items():
            if 'error' in metric_data:
                continue
            
            # Extract max value
            max_val = metric_data.get('max')
            if max_val is not None:
                metrics.append({
                    'timestamp': timestamp_dt,
                    'metric_category': 'latency',
                    'metric_name': metric_name,
                    'metric_value': float(max_val),
                    'metric_unit': 'seconds',
                    'labels': json.dumps({}),
                    'duration': duration,
                    'start_time': start_time_dt,
                    'end_time': end_time_dt
                })
        
        return metrics
    
    def _extract_ovs_metrics(self, data: Dict[str, Any], timestamp_dt: datetime,
                            duration: Optional[str], start_time_dt: Optional[datetime],
                            end_time_dt: Optional[datetime]) -> List[Dict[str, Any]]:
        """Extract metrics from OVS data"""
        metrics = []
        
        # Extract from various OVS metric categories
        for category_key in ['ovs_vswitchd_cpu', 'ovs_vswitchd_memory', 'dataplane_flows']:
            category_data = data.get(category_key, {})
            if 'top_6' in category_data:
                for item in category_data['top_6']:
                    node = item.get('node', 'unknown')
                    value_key = 'max' if 'max' in item else 'avg'
                    value = item.get(value_key)
                    
                    if value is not None:
                        metrics.append({
                            'timestamp': timestamp_dt,
                            'metric_category': 'ovs',
                            'metric_name': f'{category_key}_{value_key}',
                            'metric_value': float(value),
                            'metric_unit': self._get_ovs_unit(category_key),
                            'labels': json.dumps({'node': node}),
                            'duration': duration,
                            'start_time': start_time_dt,
                            'end_time': end_time_dt
                        })
        
        return metrics
    
    def _get_ovs_unit(self, metric_name: str) -> str:
        """Get unit for OVS metric"""
        if 'cpu' in metric_name:
            return 'percent'
        elif 'memory' in metric_name:
            return 'bytes'
        elif 'flows' in metric_name:
            return 'count'
        return 'unknown'
    
    async def _save_alerts_from_collection(self, collection_result: Dict[str, Any]) -> None:
        """Extract and save alerts from collection data"""
        timestamp_str = collection_result.get('timestamp')
        timestamp_dt = self._parse_timestamp(timestamp_str)
        
        collected_data = collection_result.get('collected_data', {})
        
        for category, category_data in collected_data.items():
            if 'error' in category_data:
                continue
            
            # Check for alerts in summary
            summary = category_data.get('summary', {})
            alerts = summary.get('alerts', [])
            
            for alert in alerts:
                self.connection.execute("""
                    INSERT INTO alerts_history 
                    (timestamp, alert_type, alert_message, severity, category, 
                     metric_name, value, threshold)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    timestamp_dt,
                    alert.get('type', 'unknown'),
                    alert.get('message', ''),
                    alert.get('severity', 'info'),
                    category,
                    alert.get('metric_name', ''),
                    alert.get('value'),
                    alert.get('threshold')
                ])
    
    def _parse_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """Parse timestamp string to datetime object"""
        if not timestamp_str:
            return None
        
        try:
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except Exception:
            return None
    
    # ========== Query Functions ==========
    
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
            params.append(self._parse_timestamp(start_time))
        
        if end_time:
            query += " AND timestamp <= ?"
            params.append(self._parse_timestamp(end_time))
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        result = self.connection.execute(query, params).fetchall()
        
        columns = ['id', 'timestamp', 'metric_category', 'metric_name', 'metric_value', 
                  'metric_unit', 'labels', 'duration', 'start_time', 'end_time', 'created_at']
        
        return [dict(zip(columns, row)) for row in result]
    
    async def get_collection_by_run_id(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get complete collection data by run_id"""
        # Get benchmark run
        run_result = self.connection.execute("""
            SELECT * FROM benchmark_runs WHERE run_id = ?
        """, [run_id]).fetchone()
        
        if not run_result:
            return None
        
        run_columns = ['id', 'run_id', 'start_time', 'end_time', 'duration', 'run_type', 
                      'configuration', 'status', 'total_collections', 'successful_collections', 
                      'success_rate', 'errors', 'created_at']
        run_data = dict(zip(run_columns, run_result))
        
        # Parse JSON fields
        if run_data['configuration']:
            run_data['configuration'] = json.loads(run_data['configuration'])
        if run_data['errors']:
            run_data['errors'] = json.loads(run_data['errors'])
        
        # Get collection categories
        categories_result = self.connection.execute("""
            SELECT * FROM collection_categories WHERE run_id = ? ORDER BY timestamp
        """, [run_id]).fetchall()
        
        category_columns = ['id', 'run_id', 'timestamp', 'category', 'category_data', 
                           'status', 'error_message', 'duration', 'start_time', 'end_time', 
                           'created_at']
        
        categories = []
        for row in categories_result:
            cat_data = dict(zip(category_columns, row))
            if cat_data['category_data']:
                cat_data['category_data'] = json.loads(cat_data['category_data'])
            categories.append(cat_data)
        
        run_data['categories'] = categories
        
        return run_data
    
    async def get_performance_snapshots(self, 
                                       start_time: Optional[str] = None,
                                       end_time: Optional[str] = None,
                                       limit: int = 50) -> List[Dict[str, Any]]:
        """Get performance snapshots"""
        query = "SELECT * FROM performance_snapshots WHERE 1=1"
        params = []
        
        if start_time:
            query += " AND timestamp >= ?"
            params.append(self._parse_timestamp(start_time))
        
        if end_time:
            query += " AND timestamp <= ?"
            params.append(self._parse_timestamp(end_time))
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        result = self.connection.execute(query, params).fetchall()
        
        columns = ['id', 'run_id', 'timestamp', 'snapshot_data', 'cluster_info', 
                  'total_metrics', 'duration', 'start_time', 'end_time', 'success_count', 
                  'total_steps', 'success_rate', 'errors', 'created_at']
        
        snapshots = []
        for row in result:
            row_dict = dict(zip(columns, row))
            # Parse JSON fields
            if row_dict['snapshot_data']:
                row_dict['snapshot_data'] = json.loads(row_dict['snapshot_data'])
            if row_dict['cluster_info']:
                row_dict['cluster_info'] = json.loads(row_dict['cluster_info'])
            if row_dict['errors']:
                row_dict['errors'] = json.loads(row_dict['errors'])
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
                  'configuration', 'status', 'total_collections', 'successful_collections', 
                  'success_rate', 'errors', 'created_at']
        
        runs = []
        for row in result:
            row_dict = dict(zip(columns, row))
            if row_dict['configuration']:
                row_dict['configuration'] = json.loads(row_dict['configuration'])
            if row_dict['errors']:
                row_dict['errors'] = json.loads(row_dict['errors'])
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
    
    async def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        stats = {}
        
        # Table counts
        stats['metrics_count'] = self.connection.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        stats['summaries_count'] = self.connection.execute("SELECT COUNT(*) FROM metric_summaries").fetchone()[0]
        stats['snapshots_count'] = self.connection.execute("SELECT COUNT(*) FROM performance_snapshots").fetchone()[0]
        stats['benchmark_runs_count'] = self.connection.execute("SELECT COUNT(*) FROM benchmark_runs").fetchone()[0]
        stats['alerts_count'] = self.connection.execute("SELECT COUNT(*) FROM alerts_history").fetchone()[0]
        stats['categories_count'] = self.connection.execute("SELECT COUNT(*) FROM collection_categories").fetchone()[0]
        
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
    
    async def cleanup_old_data(self, days: int = 30) -> Dict[str, int]:
        """Clean up old data beyond retention period"""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Count records before deletion
        metrics_count = self.connection.execute("SELECT COUNT(*) FROM metrics WHERE timestamp < ?", [cutoff]).fetchone()[0]
        summaries_count = self.connection.execute("SELECT COUNT(*) FROM metric_summaries WHERE timestamp < ?", [cutoff]).fetchone()[0]
        snapshots_count = self.connection.execute("SELECT COUNT(*) FROM performance_snapshots WHERE timestamp < ?", [cutoff]).fetchone()[0]
        alerts_count = self.connection.execute("SELECT COUNT(*) FROM alerts_history WHERE timestamp < ?", [cutoff]).fetchone()[0]
        categories_count = self.connection.execute("SELECT COUNT(*) FROM collection_categories WHERE timestamp < ?", [cutoff]).fetchone()[0]
        
        # Delete old records
        self.connection.execute("DELETE FROM metrics WHERE timestamp < ?", [cutoff])
        self.connection.execute("DELETE FROM metric_summaries WHERE timestamp < ?", [cutoff])
        self.connection.execute("DELETE FROM performance_snapshots WHERE timestamp < ?", [cutoff])
        self.connection.execute("DELETE FROM alerts_history WHERE timestamp < ?", [cutoff])
        self.connection.execute("DELETE FROM collection_categories WHERE timestamp < ?", [cutoff])
        
        # Vacuum to reclaim space
        self.connection.execute("VACUUM")
        
        return {
            'metrics_deleted': metrics_count,
            'summaries_deleted': summaries_count,
            'snapshots_deleted': snapshots_count,
            'alerts_deleted': alerts_count,
            'categories_deleted': categories_count,
            'cutoff_date': cutoff.isoformat()
        }
    
    async def close(self) -> None:
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            print("✅ Database connection closed")


# Alias for backward compatibility
PrometheusStorage = ovnkMetricStor