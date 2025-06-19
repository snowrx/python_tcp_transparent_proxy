#!/bin/bash

cd "$(dirname "$0")"
if [ -d ".venv" ]; then
  source .venv/bin/activate
  exec python3 proxy.py
  deactivate
else
  echo "Please run install.sh first"
  exit
fi
