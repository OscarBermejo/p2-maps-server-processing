#!/bin/bash
cd "$(dirname "$0")/.."
source venv/bin/activate
nohup uvicorn src.api.app:app --host 0.0.0.0 --port 8000 --reload > nohup.out 2>&1 &