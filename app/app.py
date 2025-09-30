#!/usr/bin/env python3
"""FastAPI launcher for ARIA application.

This is the main entry point for the ARIA application which now uses:
- FastAPI for the Python backend API
- Node.js/Express for the frontend
"""

import os
import sys
from pathlib import Path

# Add src directory to Python path if not already set via PYTHONPATH
if "PYTHONPATH" not in os.environ:
    src_path = Path(__file__).parent / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
        os.environ["PYTHONPATH"] = str(src_path)

import uvicorn
from aria.api.app import app


def main() -> None:
    """Main application entry point."""
    # Configuration for Databricks Apps deployment
    host = os.getenv("HOST", "0.0.0.0")  # Bind to all interfaces for container access
    port = int(os.getenv("BACKEND_PORT", "8080"))  # Use different env var to avoid conflict
    debug = os.getenv("DEBUG", "false").lower() == "true"
    # Environment-based log level: debug locally, warning in production
    uvicorn_log_level = os.getenv("LOG_LEVEL", "debug" if os.getenv("NODE_ENV", "development") == "development" else "warning").lower()
    
    print(f"Starting ARIA FastAPI server on {host}:{port}")
    print(f"Debug mode: {debug}")
    print(f"Log level: {uvicorn_log_level}")
    print(f"Python path: {sys.path}")
    print(f"Current working directory: {os.getcwd()}")
    
    try:
        uvicorn.run(
            app,
            host=host,
            port=port,
            reload=debug,
            access_log=True,
            log_level=uvicorn_log_level
        )
    except Exception as e:
        print(f"Failed to start FastAPI server: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main() 
