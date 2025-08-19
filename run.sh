#!/usr/bin/env bash
set -euo pipefail

# cd into the folder where this script lives
cd "$(dirname "$0")"

# Create venv if it doesn't exist
if [ ! -d ".venv" ]; then
  echo "Creating venv..."
  python3 -m venv .venv
fi

# Activate venv
source .venv/bin/activate

# Upgrade pip once (safe to rerun)
python3 -m pip install --upgrade pip

# Install requirements if needed
if [ -f "requirements.txt" ]; then
  python3 -m pip install -r requirements.txt
fi

# Load environment variables from .env.local (if it exists)
if [ -f ".env.local" ]; then
  echo "Loading .env.local..."
  set -a
  source .env.local
  set +a
fi

# Run uvicorn
exec python3 -m uvicorn main:app --reload --host 127.0.0.1 --port 8000