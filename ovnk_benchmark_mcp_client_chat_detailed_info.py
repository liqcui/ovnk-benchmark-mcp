#!/usr/bin/env python3
"""
OpenShift OVN-K Benchmark MCP Client with LangGraph Integration
Connects to MCP server via streamable HTTP and provides LLM interface with memory
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, AsyncGenerator
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# LangGraph and LangChain imports
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.tools import tool
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.language_models.llms import BaseLLM
from langchain_openai import ChatOpenAI

        # MCP imports
try:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client
    MCP_AVAILABLE = True
except ImportError:
    logger.warning("MCP not available, using mock implementation")
    MCP_AVAILABLE = False
    ClientSession = None
    streamable_http_client = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables
mcp_client = None
agent_executor = None
memory = None
llm = None

# MCP Server Configuration
MCP_SERVER_URL = "http://localhost:8000"

class ChatRequest(BaseModel):
    message: str
    conversation_id: str

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    mcp_connected: bool = False

class MCPToolExecutor:
    """Handles MCP tool execution and response formatting"""
    
    def __init__(self, mcp_session: ClientSession):
        self.session = mcp_session
        self.available_tools = {}
        
    async def initialize(self):
        """Initialize and discover available MCP tools"""
        try:
            # List available tools from MCP server
            tools_response = await self.session.call_tool("list_tools", {})
            logger.info(f"Available MCP tools discovered: {len(tools_response.content)}")
            
            # Get tool schemas
            for tool_info in tools_response.content:
                tool_name = tool_info.get("name")
                if tool_name:
                    self.available_tools[tool_name] = tool_info
                    
        except Exception as e:
            logger.error(f"Failed to initialize MCP tools: {e}")
            
    async def execute_tool(self, tool_name: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an MCP tool and return formatted response"""
        try:
            logger.info(f"Executing MCP tool: {tool_name} with params: {parameters}")
            
            # Call the MCP tool
            result = await self.session.call_tool(tool_name, parameters)
            
            # Format the response
            formatted_result = {
                "tool_name": tool_name,
                "parameters": parameters,
                "result": result.content,
                "timestamp": datetime.utcnow().isoformat(),
                "success": True
            }
            
            logger.info(f"MCP tool {tool_name} executed successfully")
            return formatted_result
            
        except Exception as e:
            error_result = {
                "tool_name": tool_name,
                "parameters": parameters,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
                "success": False
            }
            logger.error(f"MCP tool execution failed: {e}")
            return error_result

# Create LangChain tools that wrap MCP functionality
@tool
async def get_cluster_health() -> str:
    """Get comprehensive OpenShift cluster health information and status."""
    global mcp_client
    if not mcp_client:
        return "MCP client not available"
    
    try:
        result = await mcp_client.execute_tool("get_performance_health_check", {})
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting cluster health: {str(e)}"

@tool 
async def get_cluster_info(namespace: Optional[str] = None) -> str:
    """Get comprehensive OpenShift cluster information including nodes, specs, and configuration."""
    global mcp_client
    if not mcp_client:
        return "MCP client not available"
    
    try:
        params = {}
        if namespace:
            params["namespace"] = namespace
        result = await mcp_client.execute_tool("get_openshift_general_info", params)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting cluster info: {str(e)}"

@tool
async def get_node_usage(duration: str = "1h") -> str:
    """Get cluster node resource usage and performance metrics."""
    global mcp_client
    if not mcp_client:
        return "MCP client not available"
    
    try:
        params = {"duration": duration}
        result = await mcp_client.execute_tool("get_cluster_node_usage", params)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting node usage: {str(e)}"

@tool
async def get_api_metrics(duration: str = "15m") -> str:
    """Get Kubernetes API server performance metrics and health indicators."""
    global mcp_client
    if not mcp_client:
        return "MCP client not available"
    
    try:
        params = {"duration": duration}
        result = await mcp_client.execute_tool("query_kube_api_metrics", params)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting API metrics: {str(e)}"

@tool
async def get_ovnk_pods_metrics(duration: str = "1h", pod_pattern: str = "ovnkube.*") -> str:
    """Get detailed OVN-Kubernetes pod resource usage and performance metrics."""
    global mcp_client
    if not mcp_client:
        return "MCP client not available"
    
    try:
        params = {
            "duration": duration,
            "pod_pattern": pod_pattern,
            "namespace_pattern": "openshift-ovn-kubernetes"
        }
        result = await mcp_client.execute_tool("query_ovnk_pods_metrics", params)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting OVN-K pod metrics: {str(e)}"

@tool
async def get_ovs_metrics(duration: str = "1h") -> str:
    """Get comprehensive Open vSwitch (OVS) performance metrics and dataplane health."""
    global mcp_client
    if not mcp_client:
        return "MCP client not available"
    
    try:
        params = {"duration": duration}
        result = await mcp_client.execute_tool("query_ovnk_ovs_metrics", params)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting OVS metrics: {str(e)}"

@tool
async def get_sync_duration_metrics(duration: str = "30m") -> str:
    """Get OVN-Kubernetes synchronization duration metrics and control plane performance."""
    global mcp_client
    if not mcp_client:
        return "MCP client not available"
    
    try:
        params = {"duration": duration}
        result = await mcp_client.execute_tool("query_ovnk_sync_duration_seconds_metrics", params)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting sync duration metrics: {str(e)}"

@tool
async def analyze_performance(component: str = "all", duration: str = "1h") -> str:
    """Perform comprehensive performance analysis for specified component (pods, ovs, sync_duration, multus, or all)."""
    global mcp_client
    if not mcp_client:
        return "MCP client not available"
    
    try:
        if component == "all":
            params = {"component": "all", "duration": duration, "save_reports": False}
            result = await mcp_client.execute_tool("analyze_unified_performance", params)
        else:
            params = {"component": component, "duration": duration, "save_reports": False}
            if component == "pods":
                result = await mcp_client.execute_tool("analyze_pods_performance", params)
            elif component == "ovs":
                result = await mcp_client.execute_tool("analyze_ovs_performance", params)
            elif component == "sync_duration":
                result = await mcp_client.execute_tool("analyze_sync_duration_performance", params)
            elif component == "multus":
                result = await mcp_client.execute_tool("analyze_multus_performance", params)
            else:
                return f"Unknown component: {component}. Use: pods, ovs, sync_duration, multus, or all"
        
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error analyzing {component} performance: {str(e)}"

async def create_mcp_connection():
    """Create MCP connection to the server"""
    global mcp_client
    
    try:
        # Create streamable HTTP client session
        async with streamable_http_client(MCP_SERVER_URL) as (read, write):
            async with ClientSession(read, write) as session:
                # Initialize MCP tool executor
                mcp_client = MCPToolExecutor(session)
                await mcp_client.initialize()
                
                logger.info("MCP client connected successfully")
                return session
                
    except Exception as e:
        logger.error(f"Failed to connect to MCP server: {e}")
        raise

async def initialize_agent():
    """Initialize the LangGraph agent with memory"""
    global agent_executor, memory, llm
    
    try:
        # Initialize LLM (using OpenAI, but can be swapped)
        llm = ChatOpenAI(
            model="gpt-3.5-turbo", 
            temperature=0.1,
            streaming=True
        )
        
        # Create memory saver for persistent conversations
        memory = MemorySaver()
        
        # Define available tools
        tools = [
            get_cluster_health,
            get_cluster_info,
            get_node_usage,
            get_api_metrics,
            get_ovnk_pods_metrics,
            get_ovs_metrics,
            get_sync_duration_metrics,
            analyze_performance
        ]
        
        # Create the React agent with memory
        agent_executor = create_react_agent(
            llm,
            tools,
            checkpointer=memory,
            state_modifier="""You are an expert OpenShift and OVN-Kubernetes performance analyst. 
            
You help users analyze cluster performance, identify bottlenecks, and provide optimization recommendations.

Key capabilities:
- Cluster health assessment and monitoring
- Node resource utilization analysis  
- API server performance evaluation
- OVN-K pod and container metrics analysis
- OVS dataplane performance monitoring
- Synchronization duration analysis
- Comprehensive performance analysis with actionable insights

When users ask questions:
1. Analyze their request and determine which tools to call
2. Call appropriate tools with relevant parameters
3. Interpret the results and provide clear, actionable insights
4. Format data in tables when appropriate
5. Provide specific recommendations for performance optimization

Always explain what you're doing and why you're calling specific tools.
Focus on practical, actionable insights rather than just raw data.
"""
        )
        
        logger.info("LangGraph agent initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize agent: {e}")
        raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan manager"""
    logger.info("Starting MCP client application...")
    
    try:
        # Initialize MCP connection
        await create_mcp_connection()
        
        # Initialize agent
        await initialize_agent()
        
        logger.info("Application initialized successfully")
        yield
        
    except Exception as e:
        logger.error(f"Failed to initialize application: {e}")
        yield
    finally:
        logger.info("Shutting down application...")

# Create FastAPI app
app = FastAPI(
    title="OVNK Benchmark MCP Client",
    description="MCP Client with LangGraph integration for OpenShift OVN-K performance analysis",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy" if (mcp_client and agent_executor) else "unhealthy",
        timestamp=datetime.utcnow().isoformat(),
        mcp_connected=mcp_client is not None
    )

async def format_response_as_stream(response_text: str, conversation_id: str) -> AsyncGenerator[str, None]:
    """Format LLM response as server-sent events"""
    
    # Check if response contains table data
    try:
        # Try to parse as JSON to detect structured data
        if response_text.strip().startswith('{') and 'result' in response_text:
            data = json.loads(response_text)
            
            # Send the main response
            yield f"data: {json.dumps({'type': 'message', 'content': 'Here are the results:'})}\n\n"
            
            # If there's tabular data, format it as a table
            if 'result' in data and isinstance(data['result'], dict):
                result_data = data['result']
                
                # Format different types of data as tables
                if 'top_10_pods' in result_data:
                    table_html = format_pods_table(result_data['top_10_pods'])
                    yield f"data: {json.dumps({'type': 'table', 'content': table_html, 'metadata': {'title': 'Top 10 Pods by Resource Usage'}})}\n\n"
                
                elif 'cluster_info' in result_data:
                    table_html = format_cluster_info_table(result_data['cluster_info'])
                    yield f"data: {json.dumps({'type': 'table', 'content': table_html, 'metadata': {'title': 'Cluster Information'}})}\n\n"
                
                elif 'node_usage' in result_data:
                    table_html = format_node_usage_table(result_data['node_usage'])
                    yield f"data: {json.dumps({'type': 'table', 'content': table_html, 'metadata': {'title': 'Node Resource Usage'}})}\n\n"
            
    except json.JSONDecodeError:
        pass
    
    # Stream the response text
    words = response_text.split()
    for i, word in enumerate(words):
        yield f"data: {json.dumps({'type': 'message', 'content': word + ' '})}\n\n"
        await asyncio.sleep(0.01)  # Small delay for streaming effect

def format_pods_table(pods_data: List[Dict]) -> str:
    """Format pods data as HTML table"""
    if not pods_data:
        return "<p>No pod data available</p>"
    
    html = "<table><thead><tr>"
    html += "<th>Pod Name</th><th>Namespace</th><th>CPU (cores)</th><th>Memory (MB)</th><th>Status</th>"
    html += "</tr></thead><tbody>"
    
    for pod in pods_data:
        html += f"<tr>"
        html += f"<td>{pod.get('pod_name', 'N/A')}</td>"
        html += f"<td>{pod.get('namespace', 'N/A')}</td>"
        html += f"<td>{pod.get('cpu_usage_cores', 'N/A')}</td>"
        html += f"<td>{pod.get('memory_usage_mb', 'N/A')}</td>"
        html += f"<td>{pod.get('status', 'N/A')}</td>"
        html += f"</tr>"
    
    html += "</tbody></table>"
    return html

def format_cluster_info_table(cluster_data: Dict) -> str:
    """Format cluster info as HTML table"""
    html = "<table><thead><tr><th>Property</th><th>Value</th></tr></thead><tbody>"
    
    for key, value in cluster_data.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value, indent=2)
        html += f"<tr><td>{key}</td><td>{value}</td></tr>"
    
    html += "</tbody></table>"
    return html

def format_node_usage_table(node_data: List[Dict]) -> str:
    """Format node usage data as HTML table"""
    if not node_data:
        return "<p>No node data available</p>"
    
    html = "<table><thead><tr>"
    html += "<th>Node Name</th><th>Role</th><th>CPU Usage %</th><th>Memory Usage %</th><th>Status</th>"
    html += "</tr></thead><tbody>"
    
    for node in node_data:
        html += f"<tr>"
        html += f"<td>{node.get('node_name', 'N/A')}</td>"
        html += f"<td>{node.get('role', 'N/A')}</td>"
        html += f"<td>{node.get('cpu_usage_percent', 'N/A')}</td>"
        html += f"<td>{node.get('memory_usage_percent', 'N/A')}</td>"
        html += f"<td>{node.get('status', 'N/A')}</td>"
        html += f"</tr>"
    
    html += "</tbody></table>"
    return html

@app.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    """Stream chat response with LangGraph agent"""
    if not agent_executor:
        raise HTTPException(status_code=503, detail="Agent not initialized")
    
    try:
        # Create thread config for memory
        config = {
            "configurable": {
                "thread_id": request.conversation_id
            }
        }
        
        # Process the message with the agent
        response = await agent_executor.ainvoke(
            {"messages": [HumanMessage(content=request.message)]},
            config=config
        )
        
        # Extract the AI response
        ai_response = ""
        if response and "messages" in response:
            for message in response["messages"]:
                if isinstance(message, AIMessage):
                    ai_response += message.content + " "
        
        # Return streaming response
        return StreamingResponse(
            format_response_as_stream(ai_response.strip(), request.conversation_id),
            media_type="text/plain",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
        )
        
    except Exception as e:
        logger.error(f"Chat error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/tools/{tool_name}")
async def call_tool_direct(tool_name: str, parameters: Dict[str, Any]):
    """Direct tool invocation endpoint"""
    if not mcp_client:
        raise HTTPException(status_code=503, detail="MCP client not available")
    
    try:
        result = await mcp_client.execute_tool(tool_name, parameters)
        return result
    except Exception as e:
        logger.error(f"Tool execution error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(
        "ovnk_benchmark_mcp_client_chat:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )