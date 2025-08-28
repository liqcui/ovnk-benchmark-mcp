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

from mcp import ClientSession
from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client
from mcp.client.sse import sse_client

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables
mcp_session: Optional[ClientSession] = None
agent_executor = None
memory = None

class ChatRequest(BaseModel):
    message: str
    conversation_id: str

class MCPTool:
    """MCP Tool wrapper for LangGraph"""
    
    def __init__(self, name: str, description: str, parameters: Dict[str, Any]):
        self.name = name
        self.description = description
        self.parameters = parameters
    
    async def invoke(self, **kwargs) -> Dict[str, Any]:
        """Invoke the MCP tool"""
        try:
            if not mcp_session:
                raise Exception("MCP session not initialized")
            
            # Call the MCP tool using the session
            result = await mcp_session.call_tool(self.name, kwargs)
            logger.info(f"Tool {self.name} executed successfully")
            
            # Handle different result types
            if hasattr(result, 'content'):
                # If result has content attribute, extract it
                content = result.content
                if isinstance(content, list):
                    # Handle multiple content items
                    return {"result": [item.text if hasattr(item, 'text') else str(item) for item in content]}
                elif hasattr(content, 'text'):
                    return {"result": content.text}
                else:
                    return {"result": str(content)}
            else:
                # Direct result
                return {"result": result}
                
        except Exception as e:
            error_msg = f"Error calling tool {self.name}: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}

async def initialize_mcp_session() -> ClientSession:
    """Initialize MCP session with the server"""
    try:
        # For HTTP/SSE connection
        session = await sse_client("http://localhost:8000/sse")
        
        # Initialize the session
        await session.initialize()
        
        logger.info("MCP session initialized successfully")
        return session
        
    except Exception as e:
        logger.error(f"Error initializing MCP session: {e}")
        raise

async def get_mcp_tools() -> List[MCPTool]:
    """Retrieve available tools from MCP server"""
    try:
        if not mcp_session:
            raise Exception("MCP session not initialized")
        
        # List available tools
        tools_response = await mcp_session.list_tools()
        tools = []
        
        for tool_info in tools_response.tools:
            tool = MCPTool(
                name=tool_info.name,
                description=tool_info.description,
                parameters=tool_info.inputSchema.get("properties", {}) if tool_info.inputSchema else {}
            )
            tools.append(tool)
                
        logger.info(f"Retrieved {len(tools)} tools from MCP server")
        return tools
        
    except Exception as e:
        logger.error(f"Error getting MCP tools: {e}")
        return []

def create_langchain_tools(mcp_tools: List[MCPTool]):
    """Convert MCP tools to LangChain tools"""
    from langchain_core.tools import tool
    
    langchain_tools = []
    
    for mcp_tool in mcp_tools:
        
        @tool(name=mcp_tool.name, description=mcp_tool.description)
        async def tool_func(*args, **kwargs):
            # Find the corresponding MCP tool
            tool_name = tool_func.name
            mcp_t = next((t for t in mcp_tools if t.name == tool_name), None)
            if mcp_t:
                return await mcp_t.invoke(**kwargs)
            return {"error": f"Tool {tool_name} not found"}
        
        langchain_tools.append(tool_func)
    
    return langchain_tools

async def initialize_agent():
    """Initialize the LangGraph agent with MCP tools"""
    global agent_executor, memory
    
    try:
        # Initialize OpenAI LLM
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
        
        # Create system message
        system_message = """You are an expert OpenShift OVN-Kubernetes performance analyst. You have access to comprehensive monitoring tools for analyzing cluster performance, network policies, and resource utilization.

Your capabilities include:
- Cluster health assessment and general information
- Node resource usage analysis
- API server performance monitoring  
- OVN-Kubernetes pod metrics analysis
- Open vSwitch (OVS) performance monitoring
- Multus CNI metrics analysis
- Synchronization duration analysis
- Comprehensive performance analysis with recommendations

Guidelines:
1. Always provide context about what metrics you're analyzing
2. Interpret results and explain their significance
3. Provide actionable recommendations when performance issues are found
4. Use appropriate time durations based on the analysis type
5. For health checks, use shorter durations (5m-15m)
6. For trend analysis, use longer durations (1h-1d)
7. When presenting data, explain what normal vs concerning values look like
8. Always format responses clearly with proper structure

When calling tools, automatically determine appropriate parameters based on the user's request and the tool descriptions."""
        
        # Create the agent
        agent_executor = create_react_agent(
            llm, 
            langchain_tools, 
            checkpointer=memory,
            system_message=system_message
        )
        
        logger.info("Agent initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing agent: {e}")
        raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan management"""
    global mcp_session
    
    # Startup
    try:
        # Wait for MCP server to be ready and initialize session
        max_retries = 30
        for i in range(max_retries):
            try:
                mcp_session = await initialize_mcp_session()
                logger.info("MCP server is ready")
                break
            except Exception as e:
                if i < max_retries - 1:
                    logger.info(f"Waiting for MCP server... attempt {i+1}/{max_retries}")
                    await asyncio.sleep(2)
                else:
                    logger.error(f"MCP server not available after {max_retries * 2} seconds: {e}")
                    raise Exception("MCP server not available")
        
        # Initialize agent
        await initialize_agent()
        logger.info("MCP Client initialized successfully")
        
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        raise
    
    yield
    
    # Shutdown
    if mcp_session:
        try:
            await mcp_session.close()
        except Exception as e:
            logger.error(f"Error closing MCP session: {e}")

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
                yield f"data: {json.dumps({'type': 'message', 'content': '🔧 Executing analysis tools...\n\n'})}\n\n"
            
    except Exception as e:
        error_msg = f"Error: {str(e)}"
        logger.error(error_msg)
        yield f"data: {json.dumps({'type': 'error', 'content': error_msg})}\n\n"

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "OVNK Benchmark MCP Client is running"}

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    try:
        if mcp_session:
            # Check if session is still active
            try:
                # Try to list tools as a health check
                await mcp_session.list_tools()
                return {"status": "healthy", "mcp_server": "connected"}
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
        
        # Create a temporary MCPTool instance for health check
        health_tool = MCPTool(
            name="get_performance_health_check",
            description="Performance health check tool",
            parameters={"duration": "string"}
        )
        
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

# Mount static files (if you want to serve the HTML directly)
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "ovnk_benchmark_mcp_client_chat:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )