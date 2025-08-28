"""
OVN Basic Info Collector Module
Collects basic OVN database information and pod status from Prometheus
"""

import asyncio
import json
import logging
from typing import Dict, Any, Optional
from .ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError

logger = logging.getLogger(__name__)


class ovnBasicInfoCollector:
    """Collector for basic OVN database information"""
    
    def __init__(self, prometheus_url: str, token: Optional[str] = None):
        """
        Initialize the OVN Basic Info Collector
        
        Args:
            prometheus_url: Prometheus server URL
            token: Optional authentication token
        """
        self.prometheus_url = prometheus_url
        self.token = token
        self.default_metrics = {
            "ovn_northbound_db_size": 'ovn_db_db_size_bytes{db_name=~"OVN_Northbound"}',
            "ovn_southbound_db_size": 'ovn_db_db_size_bytes{db_name=~"OVN_Southbound"}'
        }
        logger.debug(f"Initialized ovnBasicInfoCollector with URL={prometheus_url}")
    
    async def collect_max_values(self, metrics: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Collect maximum values for OVN database metrics
        
        Args:
            metrics: Optional dictionary of metric_name -> query_string
                    If None, uses default metrics
                    
        Returns:
            Dictionary with metric names and their maximum values in JSON format
        """
        if metrics is None:
            metrics = self.default_metrics
        
        logger.info(f"Collecting max values for metrics: {list(metrics.keys())}")
        
        async with PrometheusBaseQuery(self.prometheus_url, self.token) as prom:
            results = {}
            
            for metric_name, query in metrics.items():
                try:
                    logger.debug(f"Querying metric {metric_name}: {query}")
                    
                    # Query the metric
                    raw_result = await prom.query_instant(query)
                    formatted_result = prom.format_query_result(raw_result, include_labels=True)
                    
                    # Find maximum value
                    max_value = None
                    max_labels = None
                    
                    for item in formatted_result:
                        if item.get('value') is not None:
                            if max_value is None or item['value'] > max_value:
                                max_value = item['value']
                                max_labels = item.get('labels', {})
                    
                    # Store result
                    results[metric_name] = {
                        "max_value": max_value,
                        "labels": max_labels,
                        "unit": "bytes"  # Default unit for db_size_bytes metrics
                    }
                    
                    logger.debug(f"Metric {metric_name} max_value={max_value}")
                    
                except Exception as e:
                    logger.error(f"Error querying metric {metric_name}: {e}")
                    results[metric_name] = {
                        "max_value": None,
                        "error": str(e)
                    }
            
            logger.info(f"Collected max values for {len(results)} metrics")
            return results
    
    def to_json(self, results: Dict[str, Any]) -> str:
        """
        Convert results to JSON format
        
        Args:
            results: Results dictionary from collect_max_values()
            
        Returns:
            JSON formatted string
        """
        return json.dumps(results, indent=2)


async def get_pod_phase_counts(prometheus_url: str, token: Optional[str] = None) -> Dict[str, Any]:
    """
    Get pod counts by phase from Prometheus
    
    Args:
        prometheus_url: Prometheus server URL
        token: Optional authentication token
        
    Returns:
        Dictionary with pod counts by phase in JSON format
    """
    query = "sum(kube_pod_status_phase{}) by (phase) > 0"
    
    logger.info("Collecting pod phase counts")
    logger.debug(f"Pod phase query: {query}")
    
    async with PrometheusBaseQuery(prometheus_url, token) as prom:
        try:
            # Execute the instant query
            raw_result = await prom.query_instant(query)
            formatted_result = prom.format_query_result(raw_result, include_labels=True)
            
            # Process results into phase counts
            phase_counts = {}
            total_pods = 0
            
            for item in formatted_result:
                if item.get('value') is not None and item.get('labels'):
                    phase = item['labels'].get('phase', 'unknown')
                    count = int(item['value'])
                    phase_counts[phase] = count
                    total_pods += count
                    logger.debug(f"Phase {phase}: {count} pods")
            
            # Prepare result
            result = {
                "metric_name": "pod-status",
                "total_pods": total_pods,
                "phases": phase_counts,
                "query_type": "instant",
                "timestamp": None  # Will be filled with actual timestamp if needed
            }
            
            # Add timestamp from first result if available
            if formatted_result and formatted_result[0].get('timestamp'):
                result["timestamp"] = formatted_result[0]['timestamp']
            
            logger.info(f"Collected pod phase counts: {len(phase_counts)} phases, {total_pods} total pods")
            return result
            
        except Exception as e:
            logger.error(f"Error querying pod phases: {e}")
            return {
                "metric_name": "pod-status",
                "error": str(e),
                "query_type": "instant"
            }


def get_pod_phase_counts_json(prometheus_url: str, token: Optional[str] = None) -> str:
    """
    Get pod counts by phase as JSON string
    
    Args:
        prometheus_url: Prometheus server URL
        token: Optional authentication token
        
    Returns:
        JSON formatted string with pod phase counts
    """
    loop = asyncio.get_event_loop()
    if loop.is_running():
        # If we're already in an async context, create a task
        task = asyncio.create_task(get_pod_phase_counts(prometheus_url, token))
        result = asyncio.run_coroutine_threadsafe(task, loop).result()
    else:
        # If not in async context, run normally
        result = asyncio.run(get_pod_phase_counts(prometheus_url, token))
    
    return json.dumps(result, indent=2)


# Example usage
async def main():
    """Example usage of the ovnBasicInfoCollector"""
    prometheus_url = "https://your-prometheus-server"
    token = "your-token-here"  # Optional
    
    # Collect OVN database max values
    collector = ovnBasicInfoCollector(prometheus_url, token)
    
    # Use default metrics
    max_values = await collector.collect_max_values()
    print("OVN Database Max Values:")
    print(collector.to_json(max_values))
    
    # Use custom metrics
    custom_metrics = {
        "custom_metric": 'your_custom_query_here'
    }
    custom_max_values = await collector.collect_max_values(custom_metrics)
    print("\nCustom Metrics Max Values:")
    print(collector.to_json(custom_max_values))
    
    # Get pod phase counts
    pod_phases = await get_pod_phase_counts(prometheus_url, token)
    print("\nPod Phase Counts:")
    print(json.dumps(pod_phases, indent=2))


if __name__ == "__main__":
    asyncio.run(main())