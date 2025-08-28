"""
Prometheus Base Query Module
Provides base functionality for querying Prometheus
"""

import aiohttp
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Union
from urllib.parse import urlencode
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import json


class PrometheusQueryError(Exception):
    """Custom exception for Prometheus query errors"""
    pass


class PrometheusBaseQuery:
    """Base class for Prometheus queries"""
    
    def __init__(self, prometheus_url: str, token: Optional[str] = None):
        self.prometheus_url = prometheus_url.rstrip('/')
        self.token = token
        self.session: Optional[aiohttp.ClientSession] = None
        self.timeout = aiohttp.ClientTimeout(total=30)
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def _ensure_session(self) -> None:
        """Ensure aiohttp session is available"""
        if not self.session or self.session.closed:
            headers = {}
            if self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            
            self.session = aiohttp.ClientSession(
                headers=headers,
                timeout=self.timeout,
                connector=aiohttp.TCPConnector(verify_ssl=False)
            )
    
    async def close(self) -> None:
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def query_instant(self, query: str, time: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute an instant query against Prometheus
        
        Args:
            query: PromQL query string
            time: Optional timestamp (RFC3339 or Unix timestamp)
            
        Returns:
            Query result as dictionary
        """
        await self._ensure_session()
        
        params = {'query': query}
        if time:
            params['time'] = time
        
        url = f"{self.prometheus_url}/api/v1/query"
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise PrometheusQueryError(f"Query failed with status {response.status}: {error_text}")
                
                data = await response.json()
                
                if data.get('status') != 'success':
                    error_msg = data.get('error', 'Unknown error')
                    raise PrometheusQueryError(f"Prometheus query error: {error_msg}")
                
                return data['data']
                
        except aiohttp.ClientError as e:
            raise PrometheusQueryError(f"HTTP client error: {str(e)}")
        except json.JSONDecodeError as e:
            raise PrometheusQueryError(f"Failed to decode JSON response: {str(e)}")
    
    async def query_range(self, query: str, start: str, end: str, step: str = '15s') -> Dict[str, Any]:
        """
        Execute a range query against Prometheus
        
        Args:
            query: PromQL query string
            start: Start time (RFC3339 or Unix timestamp)
            end: End time (RFC3339 or Unix timestamp)
            step: Query resolution step width
            
        Returns:
            Query result as dictionary
        """
        await self._ensure_session()
        
        params = {
            'query': query,
            'start': start,
            'end': end,
            'step': step
        }
        
        url = f"{self.prometheus_url}/api/v1/query_range"
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise PrometheusQueryError(f"Range query failed with status {response.status}: {error_text}")
                
                data = await response.json()
                
                if data.get('status') != 'success':
                    error_msg = data.get('error', 'Unknown error')
                    raise PrometheusQueryError(f"Prometheus range query error: {error_msg}")
                
                return data['data']
                
        except aiohttp.ClientError as e:
            raise PrometheusQueryError(f"HTTP client error: {str(e)}")
        except json.JSONDecodeError as e:
            raise PrometheusQueryError(f"Failed to decode JSON response: {str(e)}")
    
    async def query_multiple_instant(self, queries: Dict[str, str], time: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Execute multiple instant queries concurrently
        
        Args:
            queries: Dictionary of query_name -> query_string
            time: Optional timestamp
            
        Returns:
            Dictionary of query_name -> result
        """
        tasks = []
        for name, query in queries.items():
            task = asyncio.create_task(self.query_instant(query, time))
            task.query_name = name  # Store query name for result mapping
            tasks.append(task)
        
        results = {}
        completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
        
        for task, result in zip(tasks, completed_tasks):
            query_name = getattr(task, 'query_name', 'unknown')
            if isinstance(result, Exception):
                results[query_name] = {'error': str(result)}
            else:
                results[query_name] = result
        
        return results
    
    async def query_multiple_range(self, queries: Dict[str, str], start: str, end: str, step: str = '15s') -> Dict[str, Dict[str, Any]]:
        """
        Execute multiple range queries concurrently
        
        Args:
            queries: Dictionary of query_name -> query_string
            start: Start time
            end: End time
            step: Query resolution step
            
        Returns:
            Dictionary of query_name -> result
        """
        tasks = []
        for name, query in queries.items():
            task = asyncio.create_task(self.query_range(query, start, end, step))
            task.query_name = name  # Store query name for result mapping
            tasks.append(task)
        
        results = {}
        completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
        
        for task, result in zip(tasks, completed_tasks):
            query_name = getattr(task, 'query_name', 'unknown')
            if isinstance(result, Exception):
                results[query_name] = {'error': str(result)}
            else:
                results[query_name] = result
        
        return results
    
    def parse_duration(self, duration: str) -> timedelta:
        """
        Parse duration string (e.g., '5m', '1h', '1d') to timedelta
        
        Args:
            duration: Duration string
            
        Returns:
            timedelta object
        """
        duration = duration.strip().lower()
        
        if duration.endswith('s'):
            return timedelta(seconds=int(duration[:-1]))
        elif duration.endswith('m'):
            return timedelta(minutes=int(duration[:-1]))
        elif duration.endswith('h'):
            return timedelta(hours=int(duration[:-1]))
        elif duration.endswith('d'):
            return timedelta(days=int(duration[:-1]))
        elif duration.endswith('w'):
            return timedelta(weeks=int(duration[:-1]))
        else:
            # Assume seconds if no unit specified
            return timedelta(seconds=int(duration))
    
    def get_time_range_from_duration(self, duration: str, end_time: Optional[str] = None) -> tuple[str, str]:
        """
        Get start and end time from duration
        
        Args:
            duration: Duration string (e.g., '5m', '1h')
            end_time: Optional end time, defaults to now
            
        Returns:
            Tuple of (start_time, end_time) as ISO strings
        """
        if end_time:
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        else:
            end_dt = datetime.now(timezone.utc)
        
        duration_td = self.parse_duration(duration)
        start_dt = end_dt - duration_td
        
        return (
            start_dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            end_dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        )
    
    async def test_connection(self) -> bool:
        """
        Test connection to Prometheus
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            result = await self.query_instant('up')
            return isinstance(result, dict) and 'result' in result
        except Exception:
            return False
    
    async def get_metrics_metadata(self, metric_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get metrics metadata from Prometheus
        
        Args:
            metric_name: Optional specific metric name
            
        Returns:
            Metadata dictionary
        """
        await self._ensure_session()
        
        url = f"{self.prometheus_url}/api/v1/metadata"
        params = {}
        if metric_name:
            params['metric'] = metric_name
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    return {}
                
                data = await response.json()
                return data.get('data', {})
                
        except Exception:
            return {}
    
    def format_query_result(self, result: Dict[str, Any], include_labels: bool = True) -> List[Dict[str, Any]]:
        """
        Format Prometheus query result for easier consumption
        
        Args:
            result: Raw Prometheus query result
            include_labels: Whether to include metric labels
            
        Returns:
            List of formatted result dictionaries
        """
        formatted_results = []
        
        if 'result' not in result:
            return formatted_results
        
        for item in result['result']:
            formatted_item = {}
            
            # Add metric name if available
            if '__name__' in item.get('metric', {}):
                formatted_item['metric_name'] = item['metric']['__name__']
            
            # Add labels if requested
            if include_labels and 'metric' in item:
                formatted_item['labels'] = {
                    k: v for k, v in item['metric'].items() 
                    if k != '__name__'
                }
            
            # Add value(s)
            if 'value' in item:
                # Instant query result
                timestamp, value = item['value']
                formatted_item['timestamp'] = float(timestamp)
                formatted_item['value'] = float(value) if value != 'NaN' else None
                
            elif 'values' in item:
                # Range query result
                formatted_item['values'] = []
                for timestamp, value in item['values']:
                    formatted_item['values'].append({
                        'timestamp': float(timestamp),
                        'value': float(value) if value != 'NaN' else None
                    })
            
            formatted_results.append(formatted_item)
        
        return formatted_results