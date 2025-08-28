# OpenShift OVN-Kubernetes Benchmark MCP System Setup

This guide will help you set up the complete MCP system with LangGraph agent, FastAPI client, and web interface.

## Architecture Overview

1. **MCP Server** (port 8000): Your existing benchmark server with tools
2. **MCP Client with LangGraph Agent** (port 8001): FastAPI app with AI agent
3. **Web Interface** (port 8080): HTML interface for user interaction

## Prerequisites

1. **Python 3.9+** installed
2. **OpenAI API Key** for the LLM
3. **OpenShift cluster access** (for MCP server tools)
4. **All your existing MCP server dependencies** (Kubernetes client, Prometheus, etc.)

## Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

## Step 2: Set Environment Variables

```bash
export OPENAI_API_KEY="your-openai-api-key-here"

# Optional: Set model preference (defaults to gpt-4o-mini)
export OPENAI_MODEL_NAME="gpt-4o-mini"
```

## Step 3: File Structure

Ensure you have these files in your project directory:

```
project/
├── ovnk_benchmark_mcp_server.py          # Your existing MCP server
├── ovnk_benchmark_mcp_client_chat.py     # New MCP client with LangGraph
├── ovnk_benchmark_mcp_llm.html           # Updated web interface
├── requirements.txt                       # Dependencies
├── run_system.py                          # Startup script
├── tools/                                 # Your existing tools directory
├── config/                                # Your existing config directory
├── ocauth/                                # Your existing auth directory
└── ... (other existing directories)
```

## Step 4: Start the System

### Option A: Automated Startup (Recommended)

```bash
python3 run_system.py
```

This will start all three components in the correct order and monitor them.

### Option B: Manual Startup

In separate terminals:

1. **Start MCP Server:**
```bash
python3 ovnk_benchmark_mcp_server.py
```

2. **Start MCP Client:**
```bash
python3 ovnk_benchmark_mcp_client_chat.py
```

3. **Start Web Server:**
```bash
python3 -m http.server 8080
```

## Step 5: Access the Interface

Open your browser and go to:
```
http://localhost:8080/ovnk_benchmark_mcp_llm.html
```

## System Features

### AI Agent Capabilities

The LangGraph agent can:
- Automatically select appropriate MCP tools based on user queries
- Generate proper parameters for tool calls
- Maintain conversation memory across sessions
- Stream responses in real-time
- Convert structured data to HTML tables

### Example Queries

Try these queries in the web interface:

1. **Health Checks:**
   - "Perform a comprehensive health check"
   - "What's the current cluster status?"

2. **Performance Analysis:**
   - "Show me the top resource-consuming OVN pods"
   - "Analyze OVS performance metrics"
   - "Check API server latency"

3. **Detailed Analysis:**
   - "Run a complete performance analysis of all components"
   - "Show me sync duration bottlenecks"
   - "Analyze Multus CNI performance"

## Troubleshooting

### Common Issues

1. **"Agent not initialized" error:**
   - Make sure OPENAI_API_KEY is set
   - Check that MCP server is running and accessible

2. **"MCP tools not available" error:**
   - Verify MCP server is running on port 8000
   - Check server logs for initialization errors

3. **Connection errors in web interface:**
   - Ensure MCP client is running on port 8001
   - Check browser console for specific errors

4. **Table formatting issues:**
   - The system automatically detects tabular data
   - Check that your tool responses include structured data

### Debugging

1. **Check service status:**
```bash
# Check if ports are in use
netstat -tlnp | grep -E "(8000|8001|8080)"

# Test MCP server health
curl http://localhost:8000/health

# Test MCP client health  
curl http://localhost:8001/api/health
```

2. **View logs:**
   - MCP Server logs: Check terminal where server is running
   - MCP Client logs: Check terminal where client is running
   - Browser logs: Open browser developer tools

## Architecture Details

### MCP Client Components

1. **Streamable HTTP Client**: Uses httpx to communicate with MCP server
2. **LangGraph Agent**: React agent with memory for conversation handling
3. **FastAPI Server**: Provides streaming chat API and health endpoints
4. **Tool Conversion**: Automatically converts MCP tools to LangChain tools

### Data Flow

1. User enters query in web interface
2. JavaScript sends request to MCP client (port 8001)
3. LangGraph agent processes query and selects tools
4. MCP client calls appropriate tools on MCP server (port 8000)
5. Results are streamed back through the chain
6. Web interface displays formatted results with tables

### Memory Management

- Each conversation has a unique ID
- LangGraph MemorySaver maintains conversation context
- Memory persists across tool calls within a session
- New browser sessions start fresh conversations

## Performance Considerations

- The system handles concurrent conversations
- Streaming responses improve user experience
- Table formatting is applied automatically for structured data
- Connection pooling is used for MCP server communication

## Security Notes

- The system runs locally by default
- CORS is enabled for development (restrict in production)
- No authentication is implemented (add as needed)
- API keys should be managed securely

## Next Steps

Once the system is running:
1. Try the suggested queries to test functionality
2. Explore the available MCP tools through conversation
3. Monitor performance and adjust timeouts as needed
4. Consider adding authentication for production use