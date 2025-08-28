# OpenShift OVN-K Benchmark MCP Client with LangGraph

This project provides an AI-powered chat interface for analyzing OpenShift OVN-Kubernetes performance using MCP (Model Context Protocol) and LangGraph for intelligent agent orchestration.

## Architecture

- **MCP Server**: `ovnk_benchmark_mcp_server_detailed_info.py` - Provides tools for cluster analysis
- **MCP Client**: `ovnk_benchmark_mcp_client_chat.py` - LangGraph-powered chat interface with memory
- **Web Interface**: `ovnk_benchmark_mcp_llm.html` - Interactive web UI for chat and data visualization

## Features

- **Intelligent Agent**: LangGraph-powered React agent with persistent memory
- **MCP Integration**: Streamable HTTP connection to MCP server tools
- **Real-time Chat**: Streaming responses with SSE (Server-Sent Events)
- **Data Visualization**: Automatic table formatting for structured data
- **Memory Persistence**: Conversation memory across sessions
- **Tool Auto-discovery**: Agent automatically learns available tools and parameters

## Prerequisites

1. **OpenShift Cluster**: Running OVN-Kubernetes networking
2. **Prometheus**: Accessible for metrics collection
3. **Python 3.8+**: Required for both server and client
4. **OpenAI API Key**: For LLM functionality (or configure alternative LLM)

## Installation

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set Environment Variables**:
   ```bash
   export OPENAI_API_KEY="your-openai-api-key"
   # Or configure alternative LLM provider
   ```

3. **Configure OpenShift Access**:
   Ensure you have proper kubeconfig and Prometheus access configured.

## Usage

### 1. Start the MCP Server

First, start the MCP server that provides the analysis tools:

```bash
python ovnk_benchmark_mcp_server_detailed_info.py
```

The server will start on `http://localhost:8000` and provide the following tools:
- Cluster health checks and general information
- Node resource usage analysis
- API server performance metrics
- OVN-K pod and container metrics
- OVS performance analysis
- Synchronization duration metrics
- Comprehensive performance analysis

### 2. Start the MCP Client

In a new terminal, start the LangGraph-powered client:

```bash
python ovnk_benchmark_mcp_client_chat.py
```

The client will start on `http://localhost:8001` and provide:
- `/api/health` - Health check endpoint
- `/chat/stream` - Streaming chat interface
- `/api/tools/{tool_name}` - Direct tool invocation

### 3. Open the Web Interface

Open `ovnk_benchmark_mcp_llm.html` in your browser. The interface will connect to the client on port 8001.

## Available Commands

The AI agent can understand natural language requests and automatically call appropriate tools:

### Quick Actions
- **"Perform a comprehensive health check"** - Overall system health
- **"Show me node information with resource utilization"** - Node usage analysis
- **"Check API server performance metrics"** - API server health
- **"Show me OVN-K pod resource usage"** - Pod-level analysis
- **"Analyze OVS performance metrics"** - OVS dataplane analysis
- **"Analyze sync duration performance"** - Control plane sync metrics
- **"Run unified performance analysis"** - Comprehensive system analysis

### Natural Language Examples
- "What's the current status of my cluster?"
- "Show me the top 5 pods using the most CPU"
- "Are there any performance issues with the API server?"
- "Generate a performance report for the last 2 hours"
- "Which nodes are under the most stress?"
- "How is the OVN synchronization performing?"

## Agent Capabilities

The LangGraph agent provides:

1. **Intelligent Tool Selection**: Automatically chooses appropriate tools based on user queries
2. **Parameter Generation**: Creates correct parameters for tool calls based on context
3. **Data Interpretation**: Analyzes results and provides actionable insights
4. **Memory Persistence**: Remembers conversation context across interactions
5. **Error Handling**: Gracefully handles failures and provides helpful guidance

## Configuration

### LLM Configuration
By default, the client uses OpenAI's GPT-3.5-turbo. To use a different LLM, modify the `initialize_agent()` function in `ovnk_benchmark_mcp_client_chat.py`:

```python
# For Anthropic Claude
from langchain_anthropic import ChatAnthropic
llm = ChatAnthropic(model="claude-3-sonnet-20240229")

# For Google Gemini
from langchain_google_genai import ChatGoogleGenerativeAI
llm = ChatGoogleGenerativeAI(model="gemini-pro")

# For local models via Ollama
from langchain_ollama import ChatOllama
llm = ChatOllama(model="llama2")
```

### Memory Configuration
The agent uses LangGraph's MemorySaver for conversation persistence. Memory is stored in-process and will be lost when the client restarts. For persistent memory, implement a database-backed checkpointer.

## Troubleshooting

### Connection Issues
1. Verify MCP server is running on port 8000
2. Check that MCP client is running on port 8001
3. Ensure no firewall blocking local connections

### Tool Execution Errors
1. Verify OpenShift cluster accessibility
2. Check Prometheus connectivity
3. Validate authentication credentials

### Memory Issues
1. Conversation memory is per-session
2. Use unique conversation_id for different contexts
3. Clear memory by restarting the client

## Development

### Adding New Tools
1. Add tool functions to the MCP server
2. Create corresponding LangChain @tool wrappers in the client
3. Add tools to the agent's tool list in `initialize_agent()`

### Custom Analysis
The agent can be extended with custom analysis capabilities by adding new tool functions that call the MCP server's analysis endpoints.

## API Endpoints

### MCP Client Endpoints
- `GET /api/health` - Health status
- `POST /chat/stream` - Streaming chat
- `POST /api/tools/{tool_name}` - Direct tool calls

### MCP Server Endpoints (via tools)
- Health and cluster information tools
- Performance analysis tools  
- Historical data storage and retrieval
- Comprehensive analysis and reporting

## License

This project is part of the OpenShift OVN-K Benchmark suite and follows the same licensing terms.