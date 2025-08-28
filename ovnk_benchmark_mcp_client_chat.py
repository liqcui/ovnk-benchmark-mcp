#!/usr/bin/env python3
"""
OpenShift OVN-Kubernetes Benchmark MCP Client Chat
Integrates MCP tools with LangGraph agent and exposes via FastAPI
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, AsyncGenerator

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# Reduce noisy httpx/httpcore info logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Global variables
mcp_session = None
agent_executor = None
memory = None
mcp_tools_cache = []

class MCPSessionManager:
    """Manages the MCP client session lifecycle"""
    
    def __init__(self, server_url: str):
        self.server_url = server_url
        self.session = None
        self.client_context = None
    
    async def initialize(self):
        """Initialize the MCP session"""
        try:
            logger.info(f"Connecting to MCP server at: {self.server_url}")
            
            # Use the streamablehttp_client context manager properly
            self.client_context = streamablehttp_client(self.server_url)
            read, write = await self.client_context.__aenter__()
            
            # Create and initialize session
            self.session = ClientSession(read, write)
            await self.session.initialize()
            
            logger.info("MCP session initialized successfully")
            return self.session
            
        except Exception as e:
            logger.error(f"Error initializing MCP session: {e}")
            await self.cleanup()
            raise
    
    async def cleanup(self):
        """Clean up resources"""
        try:
            if self.session:
                if hasattr(self.session, 'close'):
                    await self.session.close()
                self.session = None
            
            if self.client_context:
                await self.client_context.__aexit__(None, None, None)
                self.client_context = None
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

class ChatRequest(BaseModel):
    message: str
    conversation_id: str

class MCPToolWrapper:
    """Enhanced MCP Tool wrapper with automatic parameter handling"""
    
    def __init__(self, tool_info):
        self.name = tool_info.name
        self.description = tool_info.description
        self.input_schema = tool_info.inputSchema or {}
        self.properties = self.input_schema.get("properties", {})
        self.required = self.input_schema.get("required", [])
    
    def get_parameter_info(self):
        """Extract parameter information for LangChain tool creation"""
        return {
            "name": self.name,
            "description": self.description,
            "properties": self.properties,
            "required": self.required
        }
    
    async def invoke(self, **kwargs) -> Dict[str, Any]:
        """Invoke the MCP tool with proper parameter handling"""
        try:
            if not mcp_session:
                raise Exception("MCP session not initialized")
            
            # Filter kwargs to match expected parameters
            filtered_kwargs = {}
            for key, value in kwargs.items():
                if key in self.properties or key in self.required:
                    filtered_kwargs[key] = value
            
            logger.info(f"Calling MCP tool {self.name} with parameters: {list(filtered_kwargs.keys())}")
            
            # Call the MCP tool
            result = await mcp_session.call_tool(self.name, filtered_kwargs)
            
            # Handle different result types
            if hasattr(result, 'content'):
                content = result.content
                if isinstance(content, list):
                    # Handle multiple content items
                    content_texts = []
                    for item in content:
                        if hasattr(item, 'text'):
                            content_texts.append(item.text)
                        else:
                            content_texts.append(str(item))
                    return {"result": "\n".join(content_texts)}
                elif hasattr(content, 'text'):
                    return {"result": content.text}
                else:
                    return {"result": str(content)}
            else:
                return {"result": str(result)}
                
        except Exception as e:
            error_msg = f"Error calling tool {self.name}: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}

class MCPClientManager:
    """Manages MCP client connection lifecycle"""
    
    def __init__(self):
        self.session = None
        self.client_context = None
        self.server_url = None
    
    async def connect(self, server_url: str):
        """Connect to MCP server"""
        self.server_url = server_url
        
        try:
            logger.info(f"Connecting to MCP server at: {server_url}")
            
            # Create client context
            self.client_context = streamablehttp_client(server_url)
            
            # Get read/write streams
            read, write = await self.client_context.__aenter__()
            
            # Create and initialize session
            self.session = ClientSession(read, write)
            await self.session.initialize()
            
            logger.info("MCP session initialized successfully")
            return self.session
            
        except Exception as e:
            logger.error(f"Failed to connect to MCP server: {e}")
            await self.cleanup()
            raise
    
    async def cleanup(self):
        """Clean up connection resources"""
        try:
            if self.session:
                # Don't close session here as it's managed by the context
                self.session = None
            
            if self.client_context:
                await self.client_context.__aexit__(None, None, None)
                self.client_context = None
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Global MCP client manager
mcp_client_manager = None

async def initialize_mcp_session():
    """Initialize MCP session with the server"""
    global mcp_session, mcp_client_manager
    
    # Get MCP server URL - try without /mcp suffix first
    base_url = os.getenv("MCP_SERVER_URL", "http://localhost:8000")
    
    # Try different URL patterns
    possible_urls = [
        base_url,  # Try base URL first
        f"{base_url}/mcp",
        "http://localhost:8000",
        "http://127.0.0.1:8000"
    ]
    
    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in possible_urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)
    
    last_error = None
    
    for server_url in unique_urls:
        try:
            logger.info(f"Attempting to connect to: {server_url}")
            
            # Create new client manager for this attempt
            mcp_client_manager = MCPClientManager()
            session = await mcp_client_manager.connect(server_url)
            
            # Test the connection by listing tools
            await session.list_tools()
            
            # If we get here, connection is successful
            mcp_session = session
            logger.info(f"Successfully connected to MCP server at: {server_url}")
            return session
            
        except Exception as e:
            last_error = e
            logger.warning(f"Failed to connect to {server_url}: {e}")
            if mcp_client_manager:
                await mcp_client_manager.cleanup()
                mcp_client_manager = None
            continue
    
    # If we get here, none of the URLs worked
    error_msg = f"Could not connect to MCP server at any URL. Last error: {last_error}"
    logger.error(error_msg)
    logger.error("Please ensure your MCP server is running and accessible")
    logger.error("Check the server logs for more details")
    raise Exception(error_msg)

async def get_mcp_tools() -> List[MCPToolWrapper]:
    """Retrieve available tools from MCP server"""
    global mcp_tools_cache
    
    try:
        if not mcp_session:
            raise Exception("MCP session not initialized")
        
        # List available tools
        tools_response = await mcp_session.list_tools()
        tools = []
        
        for tool_info in tools_response.tools:
            tool = MCPToolWrapper(tool_info)
            tools.append(tool)
                
        mcp_tools_cache = tools
        logger.info(f"Retrieved {len(tools)} tools from MCP server")
        return tools
        
    except Exception as e:
        logger.error(f"Error getting MCP tools: {e}")
        return []

def create_langchain_tools(mcp_tools: List[MCPToolWrapper]):
    """Convert MCP tools to LangChain tools with proper parameter handling"""
    langchain_tools = []
    
    for mcp_tool in mcp_tools:
        # Create a closure to capture the current mcp_tool
        def make_tool_function(tool_wrapper):
            @tool(name=tool_wrapper.name, description=tool_wrapper.description)
            async def tool_func(**kwargs):
                return await tool_wrapper.invoke(**kwargs)
            return tool_func
        
        langchain_tool = make_tool_function(mcp_tool)
        langchain_tools.append(langchain_tool)
    
    return langchain_tools

async def initialize_agent():
    """Initialize the LangGraph agent with MCP tools"""
    global agent_executor, memory
    
    try:
        # Initialize OpenAI LLM
        load_dotenv()
        api_key = os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("BASE_URL")    

        if base_url:
            llm = ChatOpenAI(
                model="gemini-1.5-flash",
                base_url=base_url,
                api_key=api_key,
                temperature=0.1,
                streaming=True         
            )
        else:
            llm = ChatOpenAI(
                model="gpt-4o-mini",
                temperature=0.1,
                streaming=True
            )
        
        # Get MCP tools and convert to LangChain format
        mcp_tools = await get_mcp_tools()
        if not mcp_tools:
            raise Exception("No MCP tools available")
        
        langchain_tools = create_langchain_tools(mcp_tools)
        
        # Initialize memory
        memory = MemorySaver()
        
        # Create enhanced system message
        system_message = """You are an expert OpenShift OVN-Kubernetes performance analyst with access to comprehensive monitoring tools. 

Your available tools include:
- get_openshift_general_info: Get cluster configuration and status
- get_cluster_node_usage: Analyze node resource utilization  
- query_kube_api_metrics: Monitor API server performance
- query_ovnk_pods_metrics: Analyze OVN-K pod resource usage
- query_ovnk_containers_metrics: Deep-dive container analysis
- query_ovnk_ovs_metrics: Monitor Open vSwitch performance
- query_multus_metrics: Analyze Multus CNI performance
- query_ovnk_sync_duration_seconds_metrics: Monitor sync performance
- analyze_pods_performance: Comprehensive pod analysis
- analyze_ovs_performance: OVS performance analysis
- analyze_sync_duration_performance: Sync duration analysis
- analyze_multus_performance: Multus performance analysis
- analyze_unified_performance: Cross-component analysis
- get_performance_health_check: Quick health assessment
- store_performance_data: Store metrics for historical analysis
- get_performance_history: Retrieve historical data

Guidelines:
1. For quick health checks, use shorter durations (5m-15m)
2. For detailed analysis, use longer durations (1h-1d)
3. Always explain the significance of metrics and provide context
4. Use appropriate tools based on the specific analysis needed
5. Provide actionable recommendations for performance issues
6. Format responses clearly with proper structure
7. When calling tools, the parameters will be automatically matched to the tool's schema

When users ask about performance, start with general cluster info and health checks, then dive deeper into specific components as needed."""
        
        # Create the agent
        agent_executor = create_react_agent(
            llm, 
            langchain_tools, 
            checkpointer=memory,
            system_message=system_message
        )
        
        logger.info("Agent initialized successfully with MCP tools")
        
    except Exception as e:
        logger.error(f"Error initializing agent: {e}")
        raise

# Updated lifespan management to handle session properly
@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan management"""
    global mcp_session, mcp_client_manager
    
    # Startup
    try:
        logger.info("Starting MCP Client initialization...")
        
        # Wait for MCP server and establish connection
        max_retries = 5  # Reduced retries since we try multiple URLs
        retry_delay = 3
        
        for i in range(max_retries):
            try:
                await initialize_mcp_session()
                logger.info("MCP server connection established")
                break
            except Exception as e:
                if i < max_retries - 1:
                    logger.info(f"Retrying MCP connection... attempt {i+1}/{max_retries}")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 1.5, 10)  # Exponential backoff
                else:
                    logger.error(f"Failed to connect to MCP server after {max_retries} attempts")
                    logger.error("Please check that your MCP server is running")
                    logger.error("The server should be accessible at one of these URLs:")
                    logger.error("  - http://localhost:8000")
                    logger.error("  - http://localhost:8000/mcp")
                    logger.error("You can also set MCP_SERVER_URL environment variable")
                    raise Exception("MCP server not available")
        
        # Initialize agent
        try:
            await initialize_agent()
            logger.info("MCP Client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize agent: {e}")
            # Don't raise here, as we might still want the server to run for diagnostics
            logger.warning("Server will start but chat functionality may be limited")
        
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down MCP Client...")
    if mcp_client_manager:
        try:
            await mcp_client_manager.cleanup()
        except Exception as e:
            logger.error(f"Error during MCP cleanup: {e}")

# Create FastAPI app
app = FastAPI(
    title="OVNK Benchmark MCP Client",
    description="Chat interface for OpenShift OVN-Kubernetes performance analysis",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def format_response_as_table(data: Dict[str, Any]) -> Optional[str]:
    """Convert response data to HTML table format when appropriate"""
    try:
        # Look for tabular data patterns
        if isinstance(data, dict):
            # Check for common table patterns
            table_candidates = []
            
            # Pattern 1: List of dictionaries (most common)
            for key, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    if all(isinstance(item, dict) for item in value):
                        table_candidates.append((key, value))
            
            # Pattern 2: Dictionary with consistent structure
            if not table_candidates and len(data) > 3:
                first_values = list(data.values())[:3]
                if all(isinstance(v, (int, float, str)) for v in first_values):
                    # Convert dict to list of dicts for table format
                    table_data = [{"key": k, "value": v} for k, v in data.items()]
                    table_candidates.append(("data", table_data))
            
            # Generate HTML tables
            tables_html = ""
            for table_name, table_data in table_candidates:
                if len(table_data) > 0:
                    headers = list(table_data[0].keys())
                    
                    table_html = f"""
                    <div class="table-wrapper">
                        <h4>{table_name.replace('_', ' ').title()}</h4>
                        <table>
                            <thead>
                                <tr>
                    """
                    
                    for header in headers:
                        table_html += f"<th>{header.replace('_', ' ').title()}</th>"
                    
                    table_html += """
                                </tr>
                            </thead>
                            <tbody>
                    """
                    
                    for row in table_data[:20]:  # Limit to 20 rows
                        table_html += "<tr>"
                        for header in headers:
                            value = row.get(header, "")
                            if isinstance(value, (int, float)):
                                if isinstance(value, float):
                                    value = f"{value:.2f}"
                            table_html += f"<td>{value}</td>"
                        table_html += "</tr>"
                    
                    table_html += """
                            </tbody>
                        </table>
                    </div>
                    """
                    
                    tables_html += table_html
            
            return tables_html if tables_html else None
            
    except Exception as e:
        logger.error(f"Error formatting table: {e}")
    
    return None

async def stream_agent_response(message: str, conversation_id: str) -> AsyncGenerator[str, None]:
    """Stream response from the agent"""
    try:
        config = {"configurable": {"thread_id": conversation_id}}
        
        # Stream the agent response
        async for event in agent_executor.astream(
            {"messages": [HumanMessage(content=message)]}, 
            config=config
        ):
            # Handle different event types
            if "messages" in event:
                for msg in event["messages"]:
                    if hasattr(msg, 'content') and msg.content:
                        # Check if this is a tool result that could be a table
                        try:
                            if hasattr(msg, 'additional_kwargs') or 'tool' in str(type(msg)):
                                # Try to extract structured data for tables
                                content = msg.content
                                if isinstance(content, str):
                                    try:
                                        data = json.loads(content)
                                        table_html = format_response_as_table(data)
                                        if table_html:
                                            yield f"data: {json.dumps({'type': 'table', 'content': table_html, 'metadata': {'title': 'Analysis Results'}})}\n\n"
                                            continue
                                    except:
                                        pass
                            
                            # Stream regular message content
                            if isinstance(msg.content, str):
                                # Split content into chunks for streaming
                                content = msg.content
                                chunk_size = 50
                                for i in range(0, len(content), chunk_size):
                                    chunk = content[i:i+chunk_size]
                                    yield f"data: {json.dumps({'type': 'message', 'content': chunk})}\n\n"
                                    await asyncio.sleep(0.05)  # Small delay for streaming effect
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            yield f"data: {json.dumps({'type': 'message', 'content': str(msg.content)})}\n\n"
            
            # Handle tool calls and other events
            elif "tools" in event:
                yield "data: " + json.dumps({'type': 'message', 'content': '🔧 Executing analysis tools...\n\n'}) + "\n\n"
            
    except Exception as e:
        error_msg = f"Error: {str(e)}"
        logger.error(error_msg)
        yield f"data: {json.dumps({'type': 'error', 'content': error_msg})}\n\n"

@app.get("/")
async def root():
    """Root endpoint with server status"""
    status = {
        "message": "OVNK Benchmark MCP Client is running",
        "mcp_server_status": "disconnected"
    }
    
    if mcp_session:
        try:
            # Quick health check
            await mcp_session.list_tools()
            status["mcp_server_status"] = "connected"
        except:
            status["mcp_server_status"] = "disconnected"
    
    return status

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    try:
        if mcp_session:
            # Check if session is still active
            try:
                # Try to list tools as a health check
                tools = await mcp_session.list_tools()
                return {
                    "status": "healthy", 
                    "mcp_server": "connected",
                    "tools_count": len(tools.tools) if tools else 0
                }
            except Exception as e:
                logger.error(f"MCP session health check failed: {e}")
                return {"status": "unhealthy", "mcp_server": "disconnected", "error": str(e)}
        
        return {"status": "unhealthy", "mcp_server": "not_initialized"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    """Stream chat responses"""
    try:
        if not agent_executor:
            raise HTTPException(status_code=503, detail="Agent not initialized")
        
        return StreamingResponse(
            stream_agent_response(request.message, request.conversation_id),
            media_type="text/plain",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Content-Type": "text/event-stream",
            }
        )
    except Exception as e:
        logger.error(f"Chat stream error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tools/health_check")
@app.post("/api/tools/health_check")
async def tools_health_check():
    """Quick health check using MCP tools"""
    try:
        if not mcp_session:
            return {"overall_status": "error", "error": "MCP session not initialized"}
        
        # Find the health check tool
        health_tool = None
        for tool in mcp_tools_cache:
            if tool.name == "get_performance_health_check":
                health_tool = tool
                break
        
        if not health_tool:
            return {"overall_status": "error", "error": "Health check tool not available"}
        
        # Call the health check tool
        result = await health_tool.invoke(duration="5m")
        
        if "error" in result:
            return {"overall_status": "error", "error": result["error"]}
        
        # Parse the result
        tool_result = result.get("result", {})
        if isinstance(tool_result, str):
            try:
                tool_result = json.loads(tool_result)
            except:
                pass
        
        return {
            "overall_status": tool_result.get("overall_cluster_health", "unknown").lower(),
            "components": tool_result.get("component_health_status", {}),
            "critical_issues": tool_result.get("critical_issues", 0),
            "timestamp": tool_result.get("health_check_timestamp")
        }
            
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {"overall_status": "error", "error": str(e)}

@app.get("/api/tools")
async def list_tools():
    """List available MCP tools"""
    try:
        if not mcp_tools_cache:
            return {"tools": [], "error": "No tools loaded"}
        
        tools_info = []
        for tool in mcp_tools_cache:
            tools_info.append({
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.properties,
                "required": tool.required
            })
        
        return {"tools": tools_info}
    except Exception as e:
        return {"tools": [], "error": str(e)}

@app.get("/api/debug/connection")
async def debug_connection():
    """Debug MCP connection status"""
    debug_info = {
        "mcp_session_status": "not_initialized",
        "tools_count": 0,
        "server_url": os.getenv("MCP_SERVER_URL", "http://localhost:8000"),
        "client_manager_status": "not_initialized"
    }
    
    if mcp_client_manager:
        debug_info["client_manager_status"] = "initialized"
        debug_info["server_url_used"] = mcp_client_manager.server_url
        
        if mcp_session:
            try:
                tools = await mcp_session.list_tools()
                debug_info["mcp_session_status"] = "connected"
                debug_info["tools_count"] = len(tools.tools) if tools else 0
                debug_info["tools_list"] = [tool.name for tool in tools.tools] if tools else []
            except Exception as e:
                debug_info["mcp_session_status"] = "error"
                debug_info["error"] = str(e)
        else:
            debug_info["mcp_session_status"] = "session_none"
    
    return debug_info

@app.get("/api/debug/test_connection")
async def test_connection():
    """Test MCP server connectivity"""
    test_urls = [
        "http://localhost:8000",
        "http://localhost:8000/mcp", 
        "http://127.0.0.1:8000",
        "http://127.0.0.1:8000/mcp"
    ]
    
    results = {}
    
    for url in test_urls:
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                # Try a simple GET request first
                try:
                    response = await client.get(url)
                    results[url] = {
                        "status": "reachable",
                        "http_status": response.status_code,
                        "headers": dict(response.headers)
                    }
                except httpx.HTTPStatusError as e:
                    results[url] = {
                        "status": "http_error", 
                        "http_status": e.response.status_code,
                        "error": str(e)
                    }
        except Exception as e:
            results[url] = {
                "status": "unreachable",
                "error": str(e)
            }
    
    return results

@app.post("/api/debug/reconnect")
async def force_reconnect():
    """Force MCP reconnection"""
    global mcp_session, mcp_client_manager
    
    try:
        # Clean up existing connection
        if mcp_client_manager:
            await mcp_client_manager.cleanup()
            mcp_client_manager = None
        mcp_session = None
        
        # Attempt reconnection
        await initialize_mcp_session()
        
        # Re-initialize agent if needed
        if not agent_executor:
            await initialize_agent()
        
        return {"status": "success", "message": "Reconnected successfully"}
        
    except Exception as e:
        return {"status": "error", "error": str(e)}

# Mount static files (if you want to serve the HTML directly)
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "ovnk_benchmark_mcp_client_chat:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
        log_level="info"
    )