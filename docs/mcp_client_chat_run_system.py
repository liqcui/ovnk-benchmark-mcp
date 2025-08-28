#!/usr/bin/env python3
"""
Startup script for OpenShift OVN-Kubernetes Benchmark MCP System
Starts both MCP server and client with proper sequencing
"""

import asyncio
import subprocess
import time
import sys
import os
import signal
from pathlib import Path

def run_command(cmd, name, wait_for=None):
    """Run a command in a subprocess"""
    print(f"Starting {name}...")
    try:
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        if wait_for:
            print(f"Waiting for {name} to be ready...")
            # Read output until we see the wait_for pattern
            while True:
                line = process.stdout.readline()
                if line:
                    print(f"[{name}] {line.strip()}")
                    if wait_for in line:
                        break
                else:
                    if process.poll() is not None:
                        raise Exception(f"{name} failed to start")
                time.sleep(0.1)
        
        return process
        
    except Exception as e:
        print(f"Error starting {name}: {e}")
        return None

def main():
    """Main startup function"""
    print("OpenShift OVN-Kubernetes Benchmark MCP System Startup")
    print("=" * 60)
    
    # Check if required files exist
    required_files = [
        "ovnk_benchmark_mcp_server.py",
        "ovnk_benchmark_mcp_client_chat.py",
        "ovnk_benchmark_mcp_llm.html"
    ]
    
    for file in required_files:
        if not Path(file).exists():
            print(f"Error: Required file {file} not found!")
            sys.exit(1)
    
    # Check environment variables
    if not os.getenv("OPENAI_API_KEY"):
        print("Warning: OPENAI_API_KEY environment variable not set!")
        print("Please set it before starting: export OPENAI_API_KEY='your-api-key'")
        sys.exit(1)
    
    processes = []
    
    try:
        # Start MCP Client (port 8001)
        client_process = run_command(
            "python3 ovnk_benchmark_mcp_client_chat.py",
            "MCP Client",
            wait_for="Application startup complete"
        )
        
        if client_process:
            processes.append(("MCP Client", client_process))
            time.sleep(2)
        else:
            print("Failed to start MCP Client")
            # Continue anyway, server might still work
        
        # Start simple HTTP server for HTML file (port 8080)
        html_server = run_command(
            "python3 -m http.server 8080",
            "HTML Server"
        )
        
        if html_server:
            processes.append(("HTML Server", html_server))
        
        print("\n" + "=" * 60)
        print("System Status:")
        print(f"✓ MCP Server: http://localhost:8000")
        print(f"✓ MCP Client: http://localhost:8001") 
        print(f"✓ Web Interface: http://localhost:8080/ovnk_benchmark_mcp_llm.html")
        print("=" * 60)
        print("\nSystem is ready! Open the web interface to start using the benchmark tool.")
        print("Press Ctrl+C to stop all services.")
        
        # Keep running and monitor processes
        while True:
            time.sleep(5)
            # Check if processes are still running
            for name, process in processes:
                if process.poll() is not None:
                    print(f"Warning: {name} has stopped unexpectedly")
            
    except KeyboardInterrupt:
        print("\n\nShutting down system...")
        
        # Terminate all processes
        for name, process in processes:
            try:
                print(f"Stopping {name}...")
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print(f"Force killing {name}...")
                process.kill()
            except Exception as e:
                print(f"Error stopping {name}: {e}")
        
        print("System shutdown complete.")
        
    except Exception as e:
        print(f"System error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() MCP Server (port 8000)
        server_process = run_command(
            "python3 ovnk_benchmark_mcp_server.py",
            "MCP Server",
            wait_for="started server on"
        )
        
        if server_process:
            processes.append(("MCP Server", server_process))
            time.sleep(3)  # Additional wait for server to be fully ready
        else:
            print("Failed to start MCP Server")
            sys.exit(1)
        
        # Start