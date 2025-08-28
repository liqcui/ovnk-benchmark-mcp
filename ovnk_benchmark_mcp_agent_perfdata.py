#!/usr/bin/env python3
"""
OVN-Kubernetes Benchmark MCP Agent - Performance Data Collection
AI agent using LangGraph to collect performance data via MCP server
Fixed for LangGraph >=0.3 and proper MCP HTTP integration
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, TypedDict, Annotated
import aiohttp
import traceback

from langgraph.graph import StateGraph, END, START
from langgraph.graph.message import add_messages
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, BaseMessage
from langchain_openai import ChatOpenAI


class AgentState(TypedDict):
    """State for the performance data collection agent"""
    messages: Annotated[List[BaseMessage], add_messages]
    current_step: str
    collected_data: Dict[str, Any]
    errors: List[str]
    duration: str
    run_id: str
    timestamp: str
    config: Dict[str, Any]
    success_count: int
    total_steps: int


class PerformanceDataAgent:
    """AI agent for collecting performance data via MCP server"""
    
    def __init__(self, 
                 mcp_server_url: str = "http://localhost:8000",
                 openai_api_key: Optional[str] = None,
                 duration: str = "5m"):
        self.mcp_server_url = mcp_server_url.rstrip('/')
        self.duration = duration
        self.run_id = str(uuid.uuid4())
        
        # Initialize LLM
        self.llm = ChatOpenAI(
            model="gpt-4o-mini",
            temperature=0.1,
            api_key=openai_api_key
        )
        
        # Create workflow graph
        self.workflow = self._create_workflow()
        self.app = self.workflow.compile(checkpointer=MemorySaver())
    
    def _create_workflow(self) -> StateGraph:
        """Create the LangGraph workflow"""
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("initialize", self._initialize_collection)
        workflow.add_node("collect_general_info", self._collect_general_info)
        workflow.add_node("collect_api_metrics", self._collect_api_metrics)
        workflow.add_node("collect_multus_metrics", self._collect_multus_metrics)
        workflow.add_node("collect_ovnk_pods", self._collect_ovnk_pods)
        workflow.add_node("collect_ovnk_containers", self._collect_ovnk_containers)
        workflow.add_node("collect_ovnk_sync", self._collect_ovnk_sync)
        workflow.add_node("store_performance_data", self._store_performance_data)
        workflow.add_node("finalize", self._finalize_collection)
        workflow.add_node("handle_error", self._handle_error)
        
        # Set entry point
        workflow.add_edge(START, "initialize")
        
        # Add conditional edges based on success/failure
        workflow.add_conditional_edges(
            "initialize",
            lambda state: "collect_general_info" if not state.get("errors") else "handle_error"
        )
        
        workflow.add_conditional_edges(
            "collect_general_info",
            lambda state: "collect_api_metrics" if len(state.get("errors", [])) == 0 else "collect_api_metrics"
        )
        
        workflow.add_conditional_edges(
            "collect_api_metrics",
            lambda state: "collect_multus_metrics" if len(state.get("errors", [])) <= 1 else "collect_multus_metrics"
        )
        
        workflow.add_conditional_edges(
            "collect_multus_metrics",
            lambda state: "collect_ovnk_pods" if len(state.get("errors", [])) <= 2 else "collect_ovnk_pods"
        )
        
        workflow.add_conditional_edges(
            "collect_ovnk_pods", 
            lambda state: "collect_ovnk_containers" if len(state.get("errors", [])) <= 3 else "collect_ovnk_containers"
        )
        
        workflow.add_conditional_edges(
            "collect_ovnk_containers",
            lambda state: "collect_ovnk_sync" if len(state.get("errors", [])) <= 4 else "collect_ovnk_sync"
        )
        
        workflow.add_conditional_edges(
            "collect_ovnk_sync",
            lambda state: "store_performance_data" if len(state.get("errors", [])) <= 5 else "store_performance_data"
        )
        
        workflow.add_edge("store_performance_data", "finalize")
        workflow.add_edge("handle_error", "finalize")
        workflow.add_edge("finalize", END)
        
        return workflow
    
    async def _initialize_collection(self, state: AgentState) -> AgentState:
        """Initialize the data collection process"""
        print(f"Starting performance data collection - Run ID: {self.run_id}")
        
        state["current_step"] = "initialize"
        state["run_id"] = self.run_id
        state["timestamp"] = datetime.now(timezone.utc).isoformat()
        state["duration"] = self.duration
        state["collected_data"] = {}
        state["errors"] = []
        state["success_count"] = 0
        state["total_steps"] = 6  # Number of collection steps
        state["config"] = {
            "mcp_server_url": self.mcp_server_url,
            "collection_duration": self.duration,
            "collection_categories": [
                "general_info", "api_server", "multus", 
                "ovnk_pods", "ovnk_containers", "ovnk_sync"
            ]
        }
        
        # Add initialization messages
        initialization_message = SystemMessage(content="""You are a performance data collection agent for OpenShift OVN-Kubernetes clusters.
Your task is to collect comprehensive performance metrics from various components and store them for analysis.

Collection steps:
1. Collect general cluster information
2. Collect API server metrics  
3. Collect Multus CNI metrics
4. Collect OVN-K pods metrics
5. Collect OVN-K containers metrics
6. Collect OVN-K sync metrics
7. Store all collected data

Be thorough and handle errors gracefully.""")
        
        start_message = HumanMessage(content=f"Start performance data collection for duration: {self.duration}")
        
        state["messages"] = [initialization_message, start_message]
        
        return state
    
    async def _collect_general_info(self, state: AgentState) -> AgentState:
        """Collect general cluster information"""
        print("Collecting general cluster information...")
        state["current_step"] = "collect_general_info"
        
        try:
            data = await self._call_mcp_tool("get_openshift_general_info", {})
            state["collected_data"]["general_info"] = data
            state["success_count"] += 1
            
            # Extract summary for logging
            summary = data.get("summary", {})
            total_policies = summary.get("total_networkpolicies", 0)
            total_nodes = summary.get("total_nodes", 0)
            
            success_msg = AIMessage(content=f"Successfully collected general cluster information. Found {total_policies} NetworkPolicies, {total_nodes} nodes.")
            state["messages"].append(success_msg)
            
        except Exception as e:
            error_msg = f"Failed to collect general info: {str(e)}"
            state["errors"].append(error_msg)
            error_message = AIMessage(content=f"Error: {error_msg}")
            state["messages"].append(error_message)
        
        return state
    
    async def _collect_api_metrics(self, state: AgentState) -> AgentState:
        """Collect API server metrics"""
        print("Collecting API server metrics...")
        state["current_step"] = "collect_api_metrics"
        
        try:
            data = await self._call_mcp_tool("query_kube_api_metrics", {
                "duration": state["duration"]
            })
            state["collected_data"]["api_server"] = data
            state["success_count"] += 1
            
            summary = data.get("summary", {})
            health_score = summary.get("health_score", 0)
            status = summary.get("overall_status", "unknown")
            
            success_msg = AIMessage(content=f"API server metrics collected. Health score: {health_score}, Status: {status}")
            state["messages"].append(success_msg)
            
        except Exception as e:
            error_msg = f"Failed to collect API server metrics: {str(e)}"
            state["errors"].append(error_msg)
            error_message = AIMessage(content=f"Error: {error_msg}")
            state["messages"].append(error_message)
        
        return state
    
    async def _collect_multus_metrics(self, state: AgentState) -> AgentState:
        """Collect Multus CNI metrics"""
        print("Collecting Multus CNI metrics...")
        state["current_step"] = "collect_multus_metrics"
        
        try:
            data = await self._call_mcp_tool("query_multus_metrics", {
                "duration": state["duration"],
                "pod_pattern": "multus.*",
                "namespace_pattern": "openshift-multus"
            })
            state["collected_data"]["multus"] = data
            state["success_count"] += 1
            
            # Extract summary info
            collection_timestamp = data.get("collection_timestamp", "unknown")
            total_pods = len(data.get("top_10_pods", []))
            
            success_msg = AIMessage(content=f"Multus metrics collected. Total pods analyzed: {total_pods}, Timestamp: {collection_timestamp}")
            state["messages"].append(success_msg)
            
        except Exception as e:
            error_msg = f"Failed to collect Multus metrics: {str(e)}"
            state["errors"].append(error_msg)
            error_message = AIMessage(content=f"Error: {error_msg}")
            state["messages"].append(error_message)
        
        return state
    
    async def _collect_ovnk_pods(self, state: AgentState) -> AgentState:
        """Collect OVN-K pods metrics"""
        print("Collecting OVN-K pods metrics...")
        state["current_step"] = "collect_ovnk_pods"
        
        try:
            data = await self._call_mcp_tool("query_ovnk_pods_metrics", {
                "duration": state["duration"],
                "pod_pattern": "ovnkube.*"
            })
            state["collected_data"]["ovnk_pods"] = data
            state["success_count"] += 1
            
            # Extract summary info
            total_pods = len(data.get("top_10_pods", []))
            collection_timestamp = data.get("collection_timestamp", "unknown")
            
            success_msg = AIMessage(content=f"OVN-K pods metrics collected. Total pods: {total_pods}, Timestamp: {collection_timestamp}")
            state["messages"].append(success_msg)
            
        except Exception as e:
            error_msg = f"Failed to collect OVN-K pods metrics: {str(e)}"
            state["errors"].append(error_msg)
            error_message = AIMessage(content=f"Error: {error_msg}")
            state["messages"].append(error_message)
        
        return state
    
    async def _collect_ovnk_containers(self, state: AgentState) -> AgentState:
        """Collect OVN-K containers metrics"""
        print("Collecting OVN-K containers metrics...")
        state["current_step"] = "collect_ovnk_containers"
        
        try:
            data = await self._call_mcp_tool("query_ovnk_containers_metrics", {
                "duration": state["duration"],
                "pod_pattern": "ovnkube.*",
                "container_pattern": ".*"
            })
            state["collected_data"]["ovnk_containers"] = data
            state["success_count"] += 1
            
            # Extract summary info
            total_containers = len(data.get("top_10_pods", []))  # Container data is in top_10_pods structure
            collection_timestamp = data.get("collection_timestamp", "unknown")
            
            success_msg = AIMessage(content=f"OVN-K containers metrics collected. Total containers analyzed: {total_containers}, Timestamp: {collection_timestamp}")
            state["messages"].append(success_msg)
            
        except Exception as e:
            error_msg = f"Failed to collect OVN-K containers metrics: {str(e)}"
            state["errors"].append(error_msg)
            error_message = AIMessage(content=f"Error: {error_msg}")
            state["messages"].append(error_message)
        
        return state
    
    async def _collect_ovnk_sync(self, state: AgentState) -> AgentState:
        """Collect OVN-K sync metrics"""
        print("Collecting OVN-K sync metrics...")
        state["current_step"] = "collect_ovnk_sync"
        
        try:
            data = await self._call_mcp_tool("query_ovnk_sync_duration_seconds_metrics", {
                "duration": state["duration"]
            })
            state["collected_data"]["ovnk_sync"] = data
            state["success_count"] += 1
            
            # Extract summary info
            duration = data.get("duration", state["duration"])
            timestamp = data.get("timestamp", "unknown")
            
            success_msg = AIMessage(content=f"OVN-K sync metrics collected. Duration: {duration}, Timestamp: {timestamp}")
            state["messages"].append(success_msg)
            
        except Exception as e:
            error_msg = f"Failed to collect OVN-K sync metrics: {str(e)}"
            state["errors"].append(error_msg)
            error_message = AIMessage(content=f"Error: {error_msg}")
            state["messages"].append(error_message)
        
        return state
    
    async def _store_performance_data(self, state: AgentState) -> AgentState:
        """Store all collected performance data"""
        print("Storing performance data...")
        state["current_step"] = "store_performance_data"
        
        try:
            # Store the complete dataset
            result = await self._call_mcp_tool("store_performance_data", {
                "metrics_data": state["collected_data"],
                "timestamp": state["timestamp"]
            })
            
            status = result.get("status", "unknown")
            timestamp = result.get("timestamp", state["timestamp"])
            
            success_msg = AIMessage(content=f"Performance data successfully stored in database. Status: {status}, Timestamp: {timestamp}")
            state["messages"].append(success_msg)
            
        except Exception as e:
            error_msg = f"Failed to store performance data: {str(e)}"
            state["errors"].append(error_msg)
            error_message = AIMessage(content=f"Error: {error_msg}")
            state["messages"].append(error_message)
        
        return state
    
    async def _finalize_collection(self, state: AgentState) -> AgentState:
        """Finalize the data collection process"""
        print("Finalizing performance data collection...")
        state["current_step"] = "finalize"
        
        # Generate summary
        successful_collections = state["success_count"]
        total_errors = len(state["errors"])
        success_rate = (successful_collections / state["total_steps"]) * 100 if state["total_steps"] > 0 else 0
        
        # Calculate overall health score if we have the data
        overall_health = 0
        health_scores = []
        
        for category in ["api_server", "multus", "ovnk_pods", "ovnk_containers", "ovnk_sync"]:
            if category in state["collected_data"]:
                data = state["collected_data"][category]
                if isinstance(data, dict):
                    summary = data.get("summary", {})
                    health_score = summary.get("health_score", 0)
                    if health_score > 0:
                        health_scores.append(health_score)
        
        overall_health = sum(health_scores) / len(health_scores) if health_scores else 0
        
        # Generate final summary message
        summary_msg = f"""
Performance Data Collection Complete - Run ID: {state['run_id']}

Collection Summary:
• Successful Collections: {successful_collections}/{state['total_steps']} ({success_rate:.1f}%)
• Overall Health Score: {overall_health:.1f}/100
• Total Errors: {total_errors}
• Duration: {state['duration']}
• Timestamp: {state['timestamp']}

Category Collection Status:
"""
        
        categories = ["general_info", "api_server", "multus", "ovnk_pods", "ovnk_containers", "ovnk_sync"]
        for category in categories:
            if category in state["collected_data"]:
                summary_msg += f"• {category.replace('_', ' ').title()}: SUCCESS\n"
            else:
                summary_msg += f"• {category.replace('_', ' ').title()}: FAILED\n"
        
        if state["errors"]:
            summary_msg += f"\nErrors encountered:\n"
            for error in state["errors"]:
                summary_msg += f"• {error}\n"
        
        final_message = AIMessage(content=summary_msg)
        state["messages"].append(final_message)
        
        print(f"Performance data collection completed!")
        print(f"Success Rate: {success_rate:.1f}%")
        print(f"Overall Health Score: {overall_health:.1f}/100")
        
        return state
    
    async def _handle_error(self, state: AgentState) -> AgentState:
        """Handle critical errors during collection"""
        state["current_step"] = "handle_error"
        error_msg = f"Critical error in step {state.get('current_step', 'unknown')}"
        state["errors"].append(error_msg)
        error_message = AIMessage(content=f"Critical error: {error_msg}")
        state["messages"].append(error_message)
        return state
    
    async def _call_mcp_tool(self, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Call MCP server tool via HTTP"""
        try:
            async with aiohttp.ClientSession() as session:
                # Prepare the request payload for FastMCP
                payload = {
                    "method": "tools/call",
                    "params": {
                        "name": tool_name,
                        "arguments": params
                    }
                }
                
                headers = {
                    "Content-Type": "application/json"
                }
                
                async with session.post(
                    f"{self.mcp_server_url}/message",
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=120)
                ) as response:
                    
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"MCP server returned {response.status}: {error_text}")
                    
                    result = await response.json()
                    
                    # Handle FastMCP response format
                    if "error" in result:
                        raise Exception(f"MCP tool error: {result['error']}")
                    
                    # Extract content from FastMCP response
                    if "result" in result and "content" in result["result"]:
                        content = result["result"]["content"]
                        if isinstance(content, list) and len(content) > 0:
                            # Handle text content
                            if content[0].get("type") == "text":
                                text_content = content[0].get("text", "{}")
                                try:
                                    return json.loads(text_content)
                                except json.JSONDecodeError:
                                    return {"raw_content": text_content}
                        return content
                    
                    # Fallback for direct result
                    if "result" in result:
                        return result["result"]
                    
                    return result
                    
        except aiohttp.ClientError as e:
            raise Exception(f"HTTP client error: {str(e)}")
        except Exception as e:
            print(f"Error calling MCP tool {tool_name}: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            raise
    
    async def collect_performance_data(self) -> Dict[str, Any]:
        """Main method to collect performance data"""
        try:
            print(f"Starting AI-driven performance data collection...")
            
            # Initialize state
            initial_state: AgentState = {
                "messages": [],
                "current_step": "",
                "collected_data": {},
                "errors": [],
                "duration": self.duration,
                "run_id": self.run_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "config": {},
                "success_count": 0,
                "total_steps": 6
            }
            
            # Run the workflow
            config = {"configurable": {"thread_id": self.run_id}}
            final_state = await self.app.ainvoke(initial_state, config)
            
            return {
                "run_id": final_state["run_id"],
                "timestamp": final_state["timestamp"],
                "duration": final_state["duration"],
                "collected_data": final_state["collected_data"],
                "errors": final_state["errors"],
                "success": len(final_state["errors"]) == 0,
                "success_count": final_state["success_count"],
                "total_steps": final_state["total_steps"],
                "success_rate": (final_state["success_count"] / final_state["total_steps"]) * 100 if final_state["total_steps"] > 0 else 0,
                "summary": {
                    "categories_collected": len(final_state["collected_data"]),
                    "total_errors": len(final_state["errors"]),
                    "successful_collections": final_state["success_count"]
                }
            }
            
        except Exception as e:
            print(f"Critical error in performance data collection: {e}")
            print(f"Traceback: {traceback.format_exc()}")
            return {
                "run_id": self.run_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "success": False,
                "success_rate": 0
            }


async def main():
    """Main function for standalone execution"""
    import os
    
    # Configuration
    mcp_server_url = os.getenv("MCP_SERVER_URL", "http://localhost:8000")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    duration = os.getenv("COLLECTION_DURATION", "5m")
    
    if not openai_api_key:
        print("OPENAI_API_KEY environment variable is required")
        return
    
    # Create and run agent
    agent = PerformanceDataAgent(
        mcp_server_url=mcp_server_url,
        openai_api_key=openai_api_key,
        duration=duration
    )
    
    # Run data collection
    result = await agent.collect_performance_data()
    
    # Print results
    print("\n" + "="*60)
    print("PERFORMANCE DATA COLLECTION RESULTS")
    print("="*60)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())