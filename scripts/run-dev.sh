#!/bin/bash
cd "$(dirname "$0")/.."
source venv/bin/activate

# Run backend in development mode
# Using --reload for auto-reloading on code changes
# Using log-level=debug for more detailed logging
# Changed port from 8000 to 8001 for development
uvicorn src.api.app:app --host 0.0.0.0 --port 8001 --reload --log-level debug