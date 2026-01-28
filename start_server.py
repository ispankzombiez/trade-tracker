#!/usr/bin/env python3
"""
Local Development Server
Serves the web dashboard for local testing before GitHub deployment.
"""

import http.server
import socketserver
import webbrowser
import os
import sys
from pathlib import Path

def start_server():
    """Start a local HTTP server for testing the dashboard."""
    
    # Change to web directory
    script_dir = Path(__file__).parent
    web_dir = script_dir / "web"
    
    if not web_dir.exists():
        print("❌ Web directory not found. Run master.py first to generate dashboard data.")
        return
    
    # Check if data directory exists
    data_dir = web_dir / "data"
    if not data_dir.exists() or not (data_dir / "summary.json").exists():
        print("❌ Dashboard data not found. Run master.py first to generate dashboard data.")
        print(f"📁 Expected data in: {data_dir}")
        return
    
    os.chdir(web_dir)
    
    # Count available JSON files
    json_files = list(data_dir.glob("*.json"))
    
    print(f"📊 Found {len(json_files)} data files:")
    for file in json_files:
        print(f"   • {file.name}")
    print()
    
    # Find available port
    port = 8000
    while port < 8100:
        try:
            with socketserver.TCPServer(("", port), http.server.SimpleHTTPRequestHandler) as httpd:
                print(f"🌻 trade-tracker Dashboard")
                print(f"🚀 Server starting at http://localhost:{port}")
                print(f"📁 Serving from: {web_dir}")
                print(f"🔗 Direct links:")
                print(f"   • Dashboard: http://localhost:{port}/index.html")
                print(f"   • Dev Info:  http://localhost:{port}/local-server.html")
                print()
                print("Press Ctrl+C to stop the server")
                
                # Open browser automatically
                webbrowser.open(f"http://localhost:{port}/local-server.html")
                
                # Start server
                httpd.serve_forever()
        except OSError:
            port += 1
            continue
        break
    else:
        print("❌ Could not find available port between 8000-8099")

if __name__ == "__main__":
    try:
        start_server()
    except KeyboardInterrupt:
        print("\n\n🛑 Server stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)