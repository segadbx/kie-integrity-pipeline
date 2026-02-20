#!/bin/bash
# Load environment variables for Databricks Asset Bundle deployment.
# Usage: source set_env.sh [env_file]
#
# Reads from .env by default. Falls back to .env.local if .env is missing.
# Pass an explicit path to override both: source set_env.sh .env.prod

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${1:-$SCRIPT_DIR/.env}"

if [ ! -f "$ENV_FILE" ]; then
  echo "Warning: $ENV_FILE not found, falling back to .env.local"
  ENV_FILE="$SCRIPT_DIR/.env.local"
fi

if [ ! -f "$ENV_FILE" ]; then
  echo "Error: No env file found. Copy .env.local to .env and fill in your values."
  return 1 2>/dev/null || exit 1
fi

set -a
source "$ENV_FILE"
set +a

echo "Loaded environment from $ENV_FILE"
