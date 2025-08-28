#!/usr/bin/env python3
"""
OpenShift OVN-K Benchmark MCP Client with FastAPI and LangGraph Integration
Provides AI-powered chat interface for interacting with MCP tools
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, AsyncGenerator
import aiohttp

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
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
        super().__init__()
        self.name = name
        self.description = description
        self.mcp_client = mcp_client
    
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
            
            # Test connection using streamable HTTP
            async with streamablehttp_client(
                url,
                headers={"accept": "application/json"}
            ) as (
                read_stream,
                write_stream,
                get_session_id,
            ):
                async with ClientSession(read_stream, write_stream) as session:
                    # Initialize the connection
                    self.session = session
                    await self.session.initialize()
                    session_id = get_session_id()
                    logger.info(f"Successfully connected to MCP server. Session ID: {session_id}")
                    
                    # Get available tools
                    tools_result = await session.list_tools()
                    self.available_tools = [
                        {
                            "name": tool.name,
                            "description": tool.description,
                            "input_schema": tool.inputSchema.model_dump() if tool.inputSchema else {}
                        }
                        for tool in tools_result.tools
                    ]
                    
                    # Create LangChain tool wrappers
                    self._create_langchain_tools()
                    
                    logger.info(f"Loaded {len(self.available_tools)} MCP tools")
                    return True
                    
        except Exception as e:
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
                    logger.info(f"Session ID: in call_tool {session_id}")
                    
                    logger.info(f"Calling tool {tool_name} with params {params}")
                    
                    # Make a request to the server using HTTP
                    request_data = {
                        "params": params or {}
                    }
                    logger.info(f"Calling tool {tool_name} with params {request_data}")
                    result = await session.call_tool(tool_name, request_data)
                    
                    json_data = json.loads(result.content[0].text)
                    return json_data
                    
        except Exception as e:
            logger.error(f"Error calling tool {tool_name}: {e}")
            raise
    
    async def check_health(self) -> Dict[str, Any]:
        """Check MCP server health"""
        try:
            # Test basic connectivity
            await self.connect()
            
            # Try a simple tool call to verify functionality
            try:
                health_result = await self.call_tool("get_mcp_health_status", {})
                return {
                    "status": "healthy",
                    "mcp_connection": "ok",
                    "prometheus_connection": "ok" if not health_result.get("error") else "error",
                    "tools_available": len(self.available_tools),
                    "last_check": datetime.now(timezone.utc).isoformat()
                }
            except Exception as tool_error:
                return {
                    "status": "partial",
                    "mcp_connection": "ok",
                    "prometheus_connection": "error",
                    "tools_available": len(self.available_tools),
                    "tool_error": str(tool_error),
                    "last_check": datetime.now(timezone.utc).isoformat()
                }
                
        except Exception as e:
            return {
                "status": "unhealthy",
                "mcp_connection": "error",
                "prometheus_connection": "unknown",
                "error": str(e),
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

        llm = ChatOpenAI(
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
                                # Try to parse tool result for table formatting
                                try:
                                    result = json.loads(msg.content)
                                    if self._is_tabular_data(result):
                                        table_html = self._format_as_table(result)
                                        yield {
                                            "type": "table",
                                            "content": table_html,
                                            "metadata": {"title": "Query Results"}
                                        }
                                    else:
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
    
    def _is_tabular_data(self, data: Dict[str, Any]) -> bool:
        """Check if data contains tabular information"""
        if not isinstance(data, dict):
            return False
        
        # Look for common patterns indicating tabular data
        tabular_keys = ['top_10_pods', 'nodes', 'api_metrics', 'ovs_metrics', 
                       'sync_duration_metrics', 'cluster_info', 'containers']
        
        return any(key in data for key in tabular_keys)
    
    def _format_as_table(self, data: Dict[str, Any]) -> str:
        """Format data as HTML table"""
        html_tables = []
        
        for key, value in data.items():
            if isinstance(value, list) and len(value) > 0:
                if isinstance(value[0], dict):
                    html_tables.append(self._dict_list_to_table(value, key))
                    
        return "\n".join(html_tables) if html_tables else self._dict_to_simple_table(data)
    
    def _dict_list_to_table(self, dict_list: List[Dict], title: str) -> str:
        """Convert list of dictionaries to HTML table"""
        if not dict_list:
            return ""
        
        headers = list(dict_list[0].keys())
        
        html = f'<table><thead><tr>'
        for header in headers:
            html += f'<th>{header.replace("_", " ").title()}</th>'
        html += '</tr></thead><tbody>'
        
        for item in dict_list:
            html += '<tr>'
            for header in headers:
                value = item.get(header, "")
                # Format numeric values
                if isinstance(value, float):
                    value = f"{value:.2f}"
                elif isinstance(value, dict):
                    value = json.dumps(value, indent=1)
                html += f'<td>{value}</td>'
            html += '</tr>'
        
        html += '</tbody></table>'
        return html
    
    def _dict_to_simple_table(self, data: Dict[str, Any]) -> str:
        """Convert dictionary to simple HTML table"""
        html = '<table><thead><tr><th>Property</th><th>Value</th></tr></thead><tbody>'
        
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                value = json.dumps(value, indent=1)
            elif isinstance(value, float):
                value = f"{value:.2f}"
            
            html += f'<tr><td>{key.replace("_", " ").title()}</td><td>{value}</td></tr>'
        
        html += '</tbody></table>'
        return html
    
    def _format_json_response(self, data: Dict[str, Any]) -> str:
        """Format JSON response as readable text"""
        if "error" in data:
            return f"❌ Error: {data['error']}"
        
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

@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    health_data = await mcp_client.check_health()
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
        result = await mcp_client.call_tool(tool_name, params or {})
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