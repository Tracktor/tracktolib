#!/bin/sh
set -e

# Wait for Garage to be ready
until curl -sf http://localhost:3903/health; do
    echo "Waiting for Garage..."
    sleep 1
done

# Get node ID using awk (works in minimal containers without grep)
# garage status outputs the node ID as a 16-char hex string
NODE_ID=$(garage status 2>&1 | awk '/^[a-f0-9]{16}/ {print $1; exit}')
if [ -z "$NODE_ID" ]; then
    # Fallback: extract from any line containing a 16-char hex string
    NODE_ID=$(garage status 2>&1 | awk '{for(i=1;i<=NF;i++) if($i ~ /^[a-f0-9]{16}$/) {print $i; exit}}')
fi
if [ -n "$NODE_ID" ]; then
    # Check if layout is already applied
    if ! garage layout show 2>&1 | awk -v id="$NODE_ID" '$0 ~ id {found=1} END {exit !found}'; then
        garage layout assign -z dc1 -c 1G "$NODE_ID"
        garage layout apply --version 1
    fi
fi

# Import test key (idempotent - will fail silently if exists)
garage key import GKtest0123456789abcdef test0123456789abcdef0123456789abcdef01234567 2>/dev/null || true
garage key allow --create-bucket GKtest0123456789abcdef 2>/dev/null || true

echo "Garage initialized successfully"
