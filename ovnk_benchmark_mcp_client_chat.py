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
# Reduce noisy logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("mcp").setLevel(logging.INFO)

# Global variables
mcp_session = None
agent_executor = None
memory = None
mcp_tools_cache = []

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
    """Simplified MCP client connection manager"""
    
    def __init__(self):
        self.session = None
        self.server_url = None
        self.client_context = None
    
    async def connect(self, server_url: str):
        """Connect to MCP server with improved timeout handling"""
        self.server_url = server_url
        
        try:
            logger.info(f"Connecting to MCP server at: {server_url}")
            
            # Use the streamablehttp_client with shorter timeout
            self.client_context = streamablehttp_client(server_url)
            
            # Enter the context with timeout
            try:
                streams = await asyncio.wait_for(
                    self.client_context.__aenter__(), 
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                raise Exception("Connection to MCP server timed out")
            
            # Handle different return formats from streamablehttp_client
            if isinstance(streams, tuple) and len(streams) >= 2:
                read_stream, write_stream = streams[0], streams[1]
            else:
                raise ValueError(f"Unexpected return from streamablehttp_client: {type(streams)}")
            
            # Create session
            self.session = ClientSession(read_stream, write_stream)
            
            # Initialize with shorter timeout and retry logic
            max_init_attempts = 3
            init_timeout = 3.0
            
            for attempt in range(max_init_attempts):
                try:
                    logger.debug(f"Initializing MCP session (attempt {attempt + 1}/{max_init_attempts})")
                    await asyncio.wait_for(self.session.initialize(), timeout=init_timeout)
                    logger.info("MCP session initialized successfully")
                    return self.session
                    
                except asyncio.TimeoutError:
                    if attempt < max_init_attempts - 1:
                        logger.debug(f"Session init timeout, retrying... ({attempt + 1}/{max_init_attempts})")
                        await asyncio.sleep(0.5)  # Brief pause before retry
                        init_timeout += 1.0  # Increase timeout for next attempt
                    else:
                        raise Exception(f"MCP session initialization timed out after {max_init_attempts} attempts")
                        
                except Exception as e:
                    logger.error(f"MCP session initialization failed on attempt {attempt + 1}: {e}")
                    if attempt < max_init_attempts - 1:
                        await asyncio.sleep(0.5)
                        # Try to recreate the session
                        try:
                            self.session = ClientSession(read_stream, write_stream)
                        except Exception as recreate_error:
                            logger.debug(f"Failed to recreate session: {recreate_error}")
                    else:
                        raise Exception(f"MCP session initialization failed: {e}")
                
        except Exception as e:
            logger.error(f"Failed to connect to MCP server: {e}")
            await self.cleanup()
            raise
    
    async def cleanup(self):
        """Clean up connection resources"""
        try:
            if self.session:
                # Session cleanup is handled by the context manager
                self.session = None
            
            if self.client_context:
                await self.client_context.__aexit__(None, None, None)
                self.client_context = None
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Global MCP client manager
mcp_client_manager = None

async def test_server_connectivity(url: str) -> Dict[str, Any]:
    """Test if the server is reachable and check for MCP support"""
    try:
        import httpx
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.get(url)
            
            # Check for MCP-related headers or content
            headers = response.headers
            content = response.text.lower() if hasattr(response, 'text') else ""
            
            result = {
                "reachable": True,
                "status_code": response.status_code,
                "likely_mcp": False,
                "server_type": "unknown"
            }
            
            # Check for signs this might be an MCP server
            if any(keyword in content for keyword in ["mcp", "model context protocol", "tools", "resources"]):
                result["likely_mcp"] = True
                result["server_type"] = "possible_mcp"
            elif "application/json" in headers.get("content-type", ""):
                result["likely_mcp"] = True
                result["server_type"] = "json_api"
            elif response.status_code in [200, 404, 405]:
                result["likely_mcp"] = True  # Server is up, might support MCP
                result["server_type"] = "http_server"
            
            return result
            
    except Exception as e:
        logger.debug(f"Server {url} not reachable: {e}")
        return {
            "reachable": False,
            "error": str(e),
            "likely_mcp": False,
            "server_type": "unreachable"
        }

async def initialize_mcp_session():
    """Initialize MCP session with improved server detection and timeout handling"""
    global mcp_session, mcp_client_manager
    
    # Get MCP server URL
    base_url = os.getenv("MCP_SERVER_URL", "http://localhost:8000")
    
    # Try different URL patterns
    possible_urls = [
        f"{base_url}/mcp",  # Most likely endpoint for MCP
        base_url,
        "http://localhost:8000/mcp",
        "http://localhost:8000",
        "http://127.0.0.1:8000/mcp", 
        "http://127.0.0.1:8000"
    ]
    
    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in possible_urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)
    
    # Test connectivity and assess MCP likelihood
    logger.info("Testing server connectivity and MCP compatibility...")
    candidate_servers = []
    
    for url in unique_urls:
        base_server = url.replace('/mcp', '')  # Test base server first
        connectivity_info = await test_server_connectivity(base_server)
        
        if connectivity_info["reachable"]:
            logger.info(f"✅ Server reachable: {base_server} (status: {connectivity_info['status_code']}, type: {connectivity_info['server_type']})")
            candidate_servers.append((url, connectivity_info))
        else:
            logger.debug(f"❌ Server unreachable: {base_server}")
    
    if not candidate_servers:
        error_msg = "No servers are reachable. Please ensure your MCP server is running."
        logger.error(error_msg)
        logger.error("Checked URLs: " + ", ".join(set(url.replace('/mcp', '') for url in unique_urls)))
        logger.error("Start your MCP server with: python your_mcp_server.py")
        raise Exception(error_msg)
    
    # Sort by likelihood of being MCP server
    candidate_servers.sort(key=lambda x: (x[1]["likely_mcp"], x[1]["status_code"] == 200), reverse=True)
    
    last_error = None
    
    for server_url, connectivity_info in candidate_servers:
        try:
            logger.info(f"🔄 Attempting MCP connection to: {server_url}")
            logger.debug(f"Server info: {connectivity_info}")
            
            # Clean up any existing connection
            if mcp_client_manager:
                await mcp_client_manager.cleanup()
            
            mcp_client_manager = MCPClientManager()
            
            # Try connection with overall timeout
            try:
                session = await asyncio.wait_for(
                    mcp_client_manager.connect(server_url),
                    timeout=15.0
                )
            except asyncio.TimeoutError:
                raise Exception("Overall connection process timed out")
            
            # Test the session by listing tools with timeout
            logger.debug("Testing MCP session by listing tools...")
            try:
                tools_response = await asyncio.wait_for(
                    session.list_tools(),
                    timeout=5.0
                )
                tools_count = len(tools_response.tools) if tools_response else 0
                logger.info(f"✅ MCP connection successful! Found {tools_count} tools")
                
                # Log available tools for debugging
                if tools_response and tools_response.tools:
                    tool_names = [tool.name for tool in tools_response.tools]
                    logger.info(f"Available tools: {', '.join(tool_names)}")
                
                mcp_session = session
                return session
                
            except asyncio.TimeoutError:
                raise Exception("Tool listing timed out - server may not be fully MCP compatible")
                
        except Exception as e:
            last_error = e
            error_type = "timeout" if "timeout" in str(e).lower() else "protocol"
            logger.warning(f"❌ Failed to connect to {server_url} ({error_type}): {e}")
            
            if mcp_client_manager:
                await mcp_client_manager.cleanup()
                mcp_client_manager = None
            
            # Add delay before trying next server
            await asyncio.sleep(0.5)
            continue
    
    # If we get here, none of the URLs worked
    error_msg = f"Could not establish MCP connection to any server. Last error: {last_error}"
    logger.error(error_msg)
    logger.error("")
    logger.error("🔧 Troubleshooting suggestions:")
    logger.error("1. Verify your MCP server is running and listening on the expected port")
    logger.error("2. Check if the server implements the MCP protocol correctly")
    logger.error("3. Try connecting with a simple MCP client to test the server")
    logger.error("4. Check server logs for any error messages")
    logger.error("5. Verify no firewall is blocking the connection")
    logger.error("")
    logger.error(f"Reachable servers found: {len(candidate_servers)}")
    for url, info in candidate_servers:
        logger.error(f"  - {url}: {info['server_type']} (HTTP {info.get('status_code', 'unknown')})")
    
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
        for tool in tools:
            logger.info(f"  - {tool.name}: {tool.description}")
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

# Updated lifespan management - always allow server to start
@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan management - graceful startup even if MCP fails"""
    global mcp_session, mcp_client_manager, agent_executor
    
    # Startup - never fail, always let the server start
    logger.info("Starting OVNK Benchmark MCP Client on port 8080...")
    
    try:
        logger.info("Attempting MCP server connection...")
        
        # Try to connect to MCP server with limited retries
        max_retries = 2
        retry_delay = 1
        
        mcp_connected = False
        for i in range(max_retries):
            try:
                await initialize_mcp_session()
                logger.info("✅ MCP server connection established successfully")
                mcp_connected = True
                break
            except Exception as e:
                logger.warning(f"❌ MCP connection attempt {i+1}/{max_retries} failed: {e}")
                if i < max_retries - 1:
                    await asyncio.sleep(retry_delay)
        
        if not mcp_connected:
            logger.warning("⚠️  MCP server connection failed - starting in standalone mode")
            logger.info("Server will be available for debugging and manual connection attempts")
        
        # Try to initialize agent if MCP connected
        if mcp_connected:
            try:
                await initialize_agent()
                logger.info("✅ Agent initialized successfully with MCP tools")
            except Exception as e:
                logger.error(f"❌ Failed to initialize agent: {e}")
                logger.warning("Chat functionality will be limited")
                agent_executor = None
        else:
            logger.info("Skipping agent initialization (no MCP connection)")
            agent_executor = None
        
    except Exception as e:
        logger.error(f"Startup error: {e}")
        logger.warning("⚠️  Starting server in debug mode")
    
    # Server is ready
    logger.info("🚀 Server starting on http://0.0.0.0:8080")
    logger.info("Available endpoints:")
    logger.info("  - GET  /                     - Server status")
    logger.info("  - GET  /api/health          - Health check")  
    logger.info("  - GET  /api/debug/connection - Debug MCP connection")
    logger.info("  - POST /api/debug/reconnect  - Retry MCP connection")
    logger.info("  - GET  /api/tools           - List available tools")
    
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
    """Root endpoint with comprehensive server status"""
    try:
        # Check MCP connection status
        mcp_status = "disconnected"
        tools_count = 0
        
        if mcp_session:
            try:
                tools = await mcp_session.list_tools()
                mcp_status = "connected"
                tools_count = len(tools.tools) if tools else 0
            except:
                mcp_status = "error"
        
        # Check agent status
        agent_status = "initialized" if agent_executor else "not_initialized"
        
        status = {
            "message": "🚀 OVNK Benchmark MCP Client is running",
            "server": {
                "status": "running",
                "port": 8080,
                "host": "0.0.0.0"
            },
            "mcp_server": {
                "status": mcp_status,
                "tools_count": tools_count,
                "server_url": os.getenv("MCP_SERVER_URL", "http://localhost:8000")
            },
            "agent": {
                "status": agent_status,
                "chat_available": agent_status == "initialized" and mcp_status == "connected"
            },
            "endpoints": {
                "health": "/api/health",
                "debug_connection": "/api/debug/connection", 
                "test_connection": "/api/debug/test_connection",
                "reconnect": "/api/debug/reconnect",
                "tools": "/api/tools",
                "chat_stream": "/chat/stream"
            },
            "instructions": {
                "if_mcp_disconnected": "Use /api/debug/reconnect to retry connection",
                "test_connectivity": "Use /api/debug/test_connection to test server reachability",
                "view_tools": "Use /api/tools to see available MCP tools"
            }
        }
        
        return status
        
    except Exception as e:
        return {
            "message": "🚀 OVNK Benchmark MCP Client is running",
            "server": {"status": "running", "port": 8080},
            "error": str(e)
        }

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
        # Check if agent is available
        if not agent_executor:
            # Return helpful error message
            error_response = {
                "type": "error",
                "content": "❌ Chat functionality is not available. This can happen when:\n\n" +
                         "1. MCP server is not connected\n" +
                         "2. Agent initialization failed\n" +
                         "3. No MCP tools are available\n\n" +
                         "📋 Troubleshooting steps:\n" +
                         "• Check server status at /api/health\n" +
                         "• Test MCP connection at /api/debug/connection\n" +
                         "• Try reconnecting at /api/debug/reconnect\n" +
                         "• Ensure your MCP server is running and accessible\n\n" +
                         f"🔧 MCP Server URL: {os.getenv('MCP_SERVER_URL', 'http://localhost:8000')}"
            }
            
            async def error_stream():
                yield f"data: {json.dumps(error_response)}\n\n"
            
            return StreamingResponse(
                error_stream(),
                media_type="text/plain",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive", 
                    "Content-Type": "text/event-stream",
                }
            )
        
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
    """Test MCP server connectivity with detailed analysis"""
    test_urls = [
        "http://localhost:8000",
        "http://localhost:8000/mcp", 
        "http://127.0.0.1:8000",
        "http://127.0.0.1:8000/mcp"
    ]
    
    results = {}
    
    for url in test_urls:
        try:
            # Use our enhanced connectivity test
            connectivity_info = await test_server_connectivity(url)
            results[url] = connectivity_info
            
            # Add MCP-specific testing
            if connectivity_info["reachable"]:
                try:
                    # Try to make a simple MCP connection test
                    test_manager = MCPClientManager()
                    start_time = asyncio.get_event_loop().time()
                    
                    try:
                        session = await asyncio.wait_for(
                            test_manager.connect(url + ("/mcp" if not url.endswith("/mcp") else "")),
                            timeout=3.0
                        )
                        connection_time = asyncio.get_event_loop().time() - start_time
                        
                        # Try to list tools
                        tools = await asyncio.wait_for(session.list_tools(), timeout=2.0)
                        total_time = asyncio.get_event_loop().time() - start_time
                        
                        results[url].update({
                            "mcp_compatible": True,
                            "connection_time_seconds": round(connection_time, 2),
                            "total_time_seconds": round(total_time, 2),
                            "tools_count": len(tools.tools) if tools else 0
                        })
                        
                        await test_manager.cleanup()
                        
                    except asyncio.TimeoutError:
                        results[url].update({
                            "mcp_compatible": False,
                            "mcp_error": "MCP connection or initialization timed out"
                        })
                        await test_manager.cleanup()
                        
                except Exception as mcp_error:
                    results[url].update({
                        "mcp_compatible": False,
                        "mcp_error": str(mcp_error)
                    })
                    
        except Exception as e:
            results[url] = {
                "reachable": False,
                "error": str(e),
                "likely_mcp": False
            }
    
    # Add summary
    summary = {
        "reachable_servers": sum(1 for r in results.values() if r.get("reachable", False)),
        "mcp_compatible_servers": sum(1 for r in results.values() if r.get("mcp_compatible", False)),
        "recommended_action": "none"
    }
    
    if summary["mcp_compatible_servers"] > 0:
        summary["recommended_action"] = "try_reconnect"
    elif summary["reachable_servers"] > 0:
        summary["recommended_action"] = "check_mcp_server_implementation"
    else:
        summary["recommended_action"] = "start_mcp_server"
    
    return {
        "test_results": results,
        "summary": summary
    }

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
    
    # Ensure the server always starts
    logger.info("🚀 Starting OVNK Benchmark MCP Client...")
    logger.info("Server will be available at: http://localhost:8080")
    
    try:
        uvicorn.run(
            "ovnk_benchmark_mcp_client_chat:app",
            host="0.0.0.0",
            port=8080,
            reload=True,
            log_level="info"
        )
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        logger.info("Trying to start without reload...")
        try:
            uvicorn.run(
                app,
                host="0.0.0.0", 
                port=8080,
                log_level="info"
            )
        except Exception as e2:
            logger.error(f"Failed to start server even without reload: {e2}")
            logger.error("Please check if port 8080 is already in use:")
            logger.error("  netstat -tlnp | grep :8080")
            logger.error("  lsof -i :8080")
            raise