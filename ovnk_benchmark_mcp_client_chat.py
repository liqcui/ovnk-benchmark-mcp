#!/usr/bin/env python3
"""
OpenShift OVN-K Benchmark MCP Client with FastAPI and LangGraph Integration
Provides AI-powered chat interface for interacting with MCP tools
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, AsyncGenerator
import aiohttp
import traceback
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel, Field

# MCP and LangGraph imports
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

# LangChain and LangGraph imports
from langchain.tools import BaseTool
from langchain.schema import BaseMessage
from langchain_openai import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from langgraph.prebuilt import create_react_agent
from langgraph.graph.state import CompiledStateGraph
from langgraph.checkpoint.memory import MemorySaver
from dotenv import load_dotenv
from elt.ovnk_benchmark_elt_json2table import extract_and_transform_mcp_results,format_results_as_table

import warnings
# Suppress urllib3 deprecation warning triggered by kubernetes client using HTTPResponse.getheaders()
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message=r"HTTPResponse\.getheaders\(\) is deprecated"
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChatRequest(BaseModel):
    """Chat request model"""
    message: str = Field(..., description="User message to process")
    conversation_id: str = Field(default="default", description="Conversation identifier")

class HealthResponse(BaseModel):
    """Health check response model"""
    status: str = Field(..., description="Health status")
    timestamp: str = Field(..., description="Check timestamp")
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional details")

class MCPTool(BaseTool):
    """LangChain tool wrapper for MCP tools"""
    
    name: str
    description: str
    mcp_client: 'MCPClient'
    
    def __init__(self, name: str, description: str, mcp_client: 'MCPClient'):
        # Initialize BaseTool (pydantic model) with fields
        super().__init__(name=name, description=description, mcp_client=mcp_client)
    
    def _run(self, **kwargs) -> str:
        """Synchronous run - not used"""
        raise NotImplementedError("Use async version")
    
    async def _arun(self, **kwargs) -> str:
        """Asynchronous tool execution"""
        try:
            result = await self.mcp_client.call_tool(self.name, kwargs)
            return json.dumps(result, indent=2)
        except Exception as e:
            logger.error(f"Error calling MCP tool {self.name}: {e}")
            return f"Error: {str(e)}"

class MCPClient:
    """MCP Client for interacting with the benchmark server"""
    
    def __init__(self, mcp_server_url: str = "http://localhost:8000"):
        self.mcp_server_url = mcp_server_url
        self.session = None
        self.available_tools: List[Dict[str, Any]] = []
        self.langchain_tools: List[MCPTool] = []
        
    async def connect(self):
        """Connect to MCP server and initialize tools"""
        try:
            url = f"{self.mcp_server_url}/mcp"

            # Connect to the server using Streamable HTTP
            async with streamablehttp_client(
                url,
                # headers={"accept": "application/json"}
                ) as (
                    read_stream,
                    write_stream,
                    get_session_id,
            ):
                async with ClientSession(read_stream, write_stream) as session:
                    # Initialize the connection
                    await session.initialize()
 
                    # Get session id once connection established
                    session_id = get_session_id()
                    print("Session ID: in call_tool", session_id)
                    tools_result = await session.list_tools()
                   
                    #print("Available tools:")
                    for tool in tools_result.tools:
 
                        # Handle schema that may be a dict or a Pydantic model
                        input_schema = {}
                        try:
                            if tool.inputSchema:
                                if isinstance(tool.inputSchema, dict):
                                    input_schema = tool.inputSchema
                                elif hasattr(tool.inputSchema, "model_dump"):
                                    input_schema = tool.inputSchema.model_dump()
                                elif hasattr(tool.inputSchema, "dict"):
                                    input_schema = tool.inputSchema.dict()
                        except Exception:
                            input_schema = {}
                        self.available_tools.append({
                            "name": tool.name,
                            "description": tool.description,
                            "input_schema": input_schema
                        })
                    self._create_langchain_tools()
                    logger.info(f"Loaded {len(self.available_tools)} MCP tools")
                    return True
                    
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Failed to connect to MCP server: {e}")
            return False

    def _create_langchain_tools(self):
        """Create LangChain tool wrappers for MCP tools"""
        self.langchain_tools = []
        for tool_info in self.available_tools:
            langchain_tool = MCPTool(
                name=tool_info["name"],
                description=tool_info["description"],
                mcp_client=self
            )
            self.langchain_tools.append(langchain_tool)
    
    async def call_tool(self, tool_name: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Call an MCP tool with parameters"""
        try:
            primary_url = f"{self.mcp_server_url}/mcp"

            async def _open_and_call(url: str) -> Dict[str, Any]:
                async with streamablehttp_client(url) as connection:
                    if isinstance(connection, tuple) and len(connection) == 3:
                        read_stream, write_stream, get_session_id = connection
                    else:
                        read_stream, write_stream = connection
                        get_session_id = None

                    async with ClientSession(read_stream, write_stream) as session:
                        await session.initialize()
                        if get_session_id:
                            logger.info(f"Session ID: in call_tool {get_session_id()}")
                        logger.info(f"Calling tool {tool_name} with params {params}")

                        # Pass parameters directly according to the tool's input schema
                        request_data = params or {}

                        result = await session.call_tool(tool_name, request_data)
                        print("#*"*50)
                        print(type(result))
                        print("result in call_tool of mcp client:\n",result)
                        print("#*"*50)
                        
                        # Enhanced error handling for JSON parsing
                        if not result.content:
                            logger.warning(f"Tool {tool_name} returned empty content")
                            return {"error": "Empty response from tool", "tool": tool_name}
                        
                        if len(result.content) == 0:
                            logger.warning(f"Tool {tool_name} returned no content items")
                            return {"error": "No content items in response", "tool": tool_name}
                        
                        content_text = result.content[0].text
                        if not content_text or content_text.strip() == "":
                            logger.warning(f"Tool {tool_name} returned empty text content")
                            return {"error": "Empty text content from tool", "tool": tool_name}
                        
                        # Handle different response formats
                        content_text = content_text.strip()
                        
                        # If content is empty after stripping
                        if not content_text:
                            return {"error": "Empty response after trimming whitespace", "tool": tool_name}
                        
                        # Try to parse as JSON first
                        try:
                            json_data = json.loads(content_text)
                            # print("#*"*50)
                            # print("json_data type is:\n",type(json_data))
                            if tool_name in ["get_mcp_health_status",]:
                                formated_result=json_data           
                            else:
                                formated_result=format_results_as_table(json_data)
                            # print("formated_result in call_tool of mcp client:\n",formated_result)
                            # print("#*"*50)
                            return json_data
                            # return formated_result
                        except json.JSONDecodeError as json_err:
                            logger.error(f"Failed to parse JSON from tool {tool_name}. Content: '{content_text[:200]}...'")
                            logger.error(f"JSON decode error: {json_err}")
                            
                            # Check if it's a simple string response that should be JSON
                            if content_text.startswith('{') or content_text.startswith('['):
                                # Looks like malformed JSON
                                return {
                                    "error": f"Malformed JSON response: {str(json_err)}",
                                    "tool": tool_name,
                                    "raw_content": content_text[:500],
                                    "content_type": "malformed_json"
                                }
                            else:
                                # Return as plain text response
                                return {
                                    "result": content_text,
                                    "tool": tool_name,
                                    "content_type": "text",
                                    "message": "Tool returned plain text instead of JSON"
                                }

            last_error = None
            for attempt in range(1, 3):
                try:
                    logger.info(f"Calling tool via {primary_url} (attempt {attempt})")
                    return await _open_and_call(primary_url)
                except Exception as e:
                    traceback.print_exc()
                    last_error = e
                    logger.warning(f"call_tool attempt {attempt} failed for {primary_url}: {repr(e)}")
                    await asyncio.sleep(0.2 * attempt)
            if last_error:
                raise last_error
                    
        except Exception as e:
            logger.error(f"Error calling tool {tool_name}: {e}")
            # Return a structured error response instead of raising
            return {
                "error": str(e),
                "tool": tool_name,
                "error_type": type(e).__name__
            }

    async def check_mcp_connectivity_health(self):
        """Connect to MCP server and initialize tools"""
        try:
            url = f"{self.mcp_server_url}/mcp"

            # Connect to the server using Streamable HTTP
            async with streamablehttp_client(
                url
                # headers={"accept": "application/json"}
                ) as (
                    read_stream,
                    write_stream,
                    get_session_id,
            ):
                async with ClientSession(read_stream, write_stream) as session:
                    # Initialize the connection
                    await session.initialize()
 
                    # Get session id once connection established
                    session_id = get_session_id()
                    print("Session ID: in call_tool", session_id)
                    if session_id:
                       return {
                        "status": "healthy",
                        "mcp_connection": "ok",
                        "last_check": datetime.now(timezone.utc).isoformat()
                      }
                    else:
                       return {
                        "status": "unhealthy",
                        "mcp_connection": "disconnected",
                        "last_check": datetime.now(timezone.utc).isoformat()
                      }
                    
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Failed to connect to MCP server: {e}")
            return {
                "status": "unhealthy",
                "mcp_connection": "disconnected", 
                "error": str(e),
                "last_check": datetime.now(timezone.utc).isoformat()
            }

    async def check_cluster_connectivity_health(self) -> Dict[str, Any]:
        """Check cluster health via MCP tools"""
        try:
            # Test basic connectivity first
            await self.connect()
            
            # Try different health check tools in order of preference
            health_tools = [
                "get_mcp_health_status"
            ]
            
            health_result = None
            tool_used = None
            
            for tool_name in health_tools:
                # Check if tool exists in available tools
                if any(tool["name"] == tool_name for tool in self.available_tools):
                    try:
                        logger.info(f"Trying health check with tool: {tool_name}")
                        health_result = await self.call_tool(tool_name, {})
                        
                        # Check if we got a valid result
                        if (isinstance(health_result, dict) and 
                            "error" not in health_result and 
                            health_result):
                            tool_used = tool_name
                            break
                        elif isinstance(health_result, dict) and "error" in health_result:
                            logger.warning(f"Tool {tool_name} returned error: {health_result.get('error')}")
                            continue
                            
                    except Exception as tool_error:
                        logger.warning(f"Tool {tool_name} failed: {str(tool_error)}")
                        continue
            
            # Analyze the health result
            if health_result and tool_used:
                # Extract health information based on tool type
                overall_health = "unknown"
                if tool_used == "get_mcp_health_status":
                    overall_health = health_result.get("status", "unknown")
                elif tool_used == "get_openshift_general_info":
                    # Infer health from cluster info
                    if "nodes" in health_result or "cluster_version" in health_result:
                        overall_health = "good"
                elif tool_used in ["get_node_info", "get_api_server_metrics"]:
                    # If we can get node or API metrics, cluster is responsive
                    overall_health = "moderate"
                
                return {
                    "status": "healthy",
                    "prometheus_connection": "ok",
                    "tools_available": len(self.available_tools),
                    "overall_cluster_health": overall_health,
                    "tool_used": tool_used,
                    "last_check": datetime.now(timezone.utc).isoformat(),
                    "health_details": health_result
                }
            else:
                # No tools worked, but MCP connection is established
                return {
                    "status": "partial",
                    "prometheus_connection": "error",
                    "tools_available": len(self.available_tools),
                    "tool_error": "All health check tools failed or returned empty responses",
                    "overall_cluster_health": "unknown",
                    "last_check": datetime.now(timezone.utc).isoformat()
                }
                
        except Exception as e:
            logger.error(f"Cluster connectivity check failed: {e}")
            return {
                "status": "unhealthy",
                "prometheus_connection": "unknown", 
                "error": str(e),
                "overall_cluster_health": "unknown",
                "last_check": datetime.now(timezone.utc).isoformat()
            }

class ChatBot:
    """LangGraph-powered chatbot for MCP interaction"""
    
    def __init__(self, mcp_client: MCPClient):
        self.mcp_client = mcp_client
        self.memory = MemorySaver()
        self.conversations: Dict[str, CompiledStateGraph] = {}

        load_dotenv()
        api_key = os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("BASE_URL")    

        self.llm = ChatOpenAI(
                model="gemini-1.5-flash",
                base_url=base_url,
                api_key=api_key,
                temperature=0.1,
                streaming=True         
            )        
        # Initialize LLM (you may need to set OPENAI_API_KEY environment variable)
        # self.llm = ChatOpenAI(
        #     model="gpt-4o-mini",  # or gpt-3.5-turbo for cost efficiency
        #     temperature=0,
        #     streaming=True
        # )
    
    def get_or_create_agent(self, conversation_id: str) -> CompiledStateGraph:
        """Get or create a conversation agent with memory"""
        if conversation_id not in self.conversations:
            # Create agent with memory
            agent = create_react_agent(
                self.llm,
                self.mcp_client.langchain_tools,
                checkpointer=self.memory
            )
            self.conversations[conversation_id] = agent
            
        return self.conversations[conversation_id]
    
    async def chat_stream(self, message: str, conversation_id: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream chat response"""
        try:
            agent = self.get_or_create_agent(conversation_id)
            
            # Create thread config for this conversation
            config = {"configurable": {"thread_id": conversation_id}}
            
            # Stream the response
            async for chunk in agent.astream(
                {"messages": [("user", message)]},
                config=config
            ):
                # Process different types of chunks
                if "agent" in chunk:
                    agent_data = chunk["agent"]
                    if "messages" in agent_data:
                        for msg in agent_data["messages"]:
                            if hasattr(msg, 'content') and msg.content:
                                yield {
                                    "type": "message",
                                    "content": str(msg.content),
                                    "timestamp": datetime.now(timezone.utc).isoformat()
                                }
                
                elif "tools" in chunk:
                    tool_data = chunk["tools"]
                    if "messages" in tool_data:
                        for msg in tool_data["messages"]:
                            if hasattr(msg, 'content') and msg.content:
                                # Format tool result for display
                                try:
                                    result = json.loads(msg.content)
                                    yield {
                                        "type": "message",
                                        "content": self._format_json_response(result),
                                        "timestamp": datetime.now(timezone.utc).isoformat()
                                    }
                                except (json.JSONDecodeError, TypeError):
                                    yield {
                                        "type": "message",
                                        "content": str(msg.content),
                                        "timestamp": datetime.now(timezone.utc).isoformat()
                                    }
                        
        except Exception as e:
            logger.error(f"Error in chat stream: {e}")
            yield {
                "type": "error",
                "content": f"An error occurred: {str(e)}",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def _format_json_response(self, data: Dict[str, Any]) -> str:
        """Format JSON response as readable text"""
        # If not a dict, return as string safely
        if not isinstance(data, dict):
            return data if isinstance(data, str) else json.dumps(data, indent=2)

        if "error" in data:
            return f"⚠️ Error: {data.get('error')}"

        # Format different types of responses
        if "health_check_timestamp" in data:
            return self._format_health_check(data)
        elif "analysis_result" in data:
            return self._format_analysis_result(data)
        elif "collection_type" in data:
            return self._format_metrics_collection(data)

        # Generic formatting
        return json.dumps(data, indent=2)
    
    def _format_health_check(self, data: Dict[str, Any]) -> str:
        """Format health check response"""
        status = data.get("overall_cluster_health", "unknown")
        critical = data.get("critical_issues", 0)
        warnings = data.get("warning_issues", 0)
        
        status_emoji = {
            "excellent": "🟢",
            "good": "🟢", 
            "moderate": "🟡",
            "poor": "🟠",
            "critical": "🔴"
        }.get(status.lower(), "❓")
        
        response = f"{status_emoji} **Cluster Health: {status.upper()}**\n\n"
        
        if critical > 0:
            response += f"🚨 **Critical Issues:** {critical}\n"
        if warnings > 0:
            response += f"⚠️ **Warnings:** {warnings}\n"
            
        if data.get("immediate_action_required"):
            response += "\n🔥 **Immediate action required!**\n"
        
        return response
    
    def _format_analysis_result(self, data: Dict[str, Any]) -> str:
        """Format analysis result response"""
        analysis = data.get("analysis_result", {})
        
        response = "📊 **Performance Analysis Complete**\n\n"
        
        if "health_score" in analysis:
            score = analysis["health_score"]
            response += f"**Health Score:** {score}/100\n"
        
        if "summary" in analysis:
            response += f"**Summary:** {analysis['summary']}\n"
        
        if "critical_issues" in analysis:
            issues = analysis["critical_issues"]
            if issues:
                response += f"\n🚨 **Critical Issues ({len(issues)}):**\n"
                for issue in issues[:3]:  # Show first 3
                    response += f"• {issue}\n"
        
        return response
    
    def _format_metrics_collection(self, data: Dict[str, Any]) -> str:
        """Format metrics collection response"""
        collection_type = data.get("collection_type", "unknown")
        timestamp = data.get("collection_timestamp", "")
        
        response = f"📈 **Metrics Collection: {collection_type.title()}**\n"
        if timestamp:
            response += f"**Collected at:** {timestamp}\n"
        
        # Add summary statistics
        if "top_10_pods" in data:
            response += f"**Pods analyzed:** {len(data['top_10_pods'])}\n"
        
        return response

# Global instances
mcp_client = MCPClient()
chatbot = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan manager"""
    global chatbot
    
    logger.info("Starting MCP Client...")
    
    # Initialize MCP client
    connected = await mcp_client.connect()
    if connected:
        logger.info("MCP Client connected successfully")
        # Initialize chatbot
        chatbot = ChatBot(mcp_client)
    else:
        logger.warning("Failed to connect to MCP server, continuing with limited functionality")
        chatbot = ChatBot(mcp_client)  # Create anyway for graceful degradation
    
    yield
    
    logger.info("Shutting down MCP Client...")

# Create FastAPI app
app = FastAPI(
    title="OVN-K Benchmark MCP Client",
    description="AI-powered chat interface for OpenShift OVN-K performance analysis",
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

# Serve the web UI HTML
HTML_FILE_PATH = os.path.join(os.path.dirname(__file__), "html", "ovnk_benchmark_mcp_llm.html")

@app.get("/", include_in_schema=False)
async def serve_root_html():
    if os.path.exists(HTML_FILE_PATH):
        return FileResponse(HTML_FILE_PATH, media_type="text/html")
    raise HTTPException(status_code=404, detail="ovnk_benchmark_mcp_llm.html not found")

@app.get("/ui", include_in_schema=False)
async def serve_ui_html():
    if os.path.exists(HTML_FILE_PATH):
        return FileResponse(HTML_FILE_PATH, media_type="text/html")
    raise HTTPException(status_code=404, detail="ovnk_benchmark_mcp_llm.html not found")

@app.get("/api/mcp/health", response_model=HealthResponse)
async def mcp_health_check():
    """MCP connectivity health check endpoint"""
    health_data = await mcp_client.check_mcp_connectivity_health()
    return HealthResponse(
        status=health_data["status"],
        timestamp=health_data["last_check"],
        details=health_data
    )

@app.get("/api/cluster/health", response_model=HealthResponse)
async def cluster_health_check():
    """Cluster health check endpoint"""
    health_data = await mcp_client.check_cluster_connectivity_health()
    return HealthResponse(
        status=health_data["status"],
        timestamp=health_data["last_check"],
        details=health_data
    )

@app.get("/api/tools")
async def list_tools():
    """List available MCP tools"""
    return {
        "tools": mcp_client.available_tools,
        "count": len(mcp_client.available_tools)
    }

@app.post("/api/tools/{tool_name}")
async def call_tool_direct(tool_name: str, params: Dict[str, Any] = None):
    """Direct tool call endpoint"""
    try:
        params={
            "request": params or {}
        }
        result = await mcp_client.call_tool(tool_name, params or {})
        print("#*"*35)
        print("result of /api/tools is:\n",type(result),result)
        # If the tool already returned a formatted HTML string, return as is
        if isinstance(result, str):
            return result
        # If the result is a dict with an error from the tool call, pass through
        if isinstance(result, dict) and result.get("error") and result.get("tool"):
            return result
        # Otherwise, attempt to format dict results into HTML table
        formated_result = format_results_as_table(result) if isinstance(result, dict) else result
        print("#*"*35)
        print("result of /api/tools is:\n",type(formated_result),formated_result)        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    """Streaming chat endpoint"""
    if not chatbot:
        raise HTTPException(status_code=503, detail="Chatbot not initialized")
    
    async def generate():
        try:
            async for chunk in chatbot.chat_stream(request.message, request.conversation_id):
                # Format as Server-Sent Events
                yield f"data: {json.dumps(chunk)}\n\n"
        except Exception as e:
            error_chunk = {
                "type": "error",
                "content": f"Stream error: {str(e)}",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            yield f"data: {json.dumps(error_chunk)}\n\n"
    
    return StreamingResponse(
        generate(),
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
        }
    )

@app.post("/chat")
async def chat_simple(request: ChatRequest):
    """Simple chat endpoint (non-streaming)"""
    if not chatbot:
        raise HTTPException(status_code=503, detail="Chatbot not initialized")
    
    responses = []
    async for chunk in chatbot.chat_stream(request.message, request.conversation_id):
        responses.append(chunk)
    
    return {"responses": responses}

if __name__ == "__main__":
    import uvicorn
    
    # Run the server
    uvicorn.run(
        "ovnk_benchmark_mcp_client_chat:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
        log_level="info"
    )