#!/bin/sh
set -e

# Get node ID and configure layout
NODE_ID=$(garage status 2>&1 | grep -oE '^[a-f0-9]{16}' | head -1)
garage layout assign -z dc1 -c 1G "$NODE_ID"
garage layout apply --version 1

# Create test key
garage key import --yes GK0123456789abcdef01234567 \
    0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
garage key allow --create-bucket GK0123456789abcdef01234567

echo "Garage initialized"