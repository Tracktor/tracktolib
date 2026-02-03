#!/bin/sh
set -e

NODE_ID=$(garage status 2>&1 | grep -oE '^[a-f0-9]{16}' | head -1)

# Apply layout only if not already configured
if ! garage layout show 2>&1 | grep -q "$NODE_ID"; then
    garage layout assign -z dc1 -c 1G "$NODE_ID"
    garage layout apply --version 1
fi

# Create test key (ignore if exists)
garage key import --yes GK0123456789abcdef01234567 \
    0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef 2>/dev/null || true
garage key allow --create-bucket GK0123456789abcdef01234567

echo "Garage initialized"
sleep 2  # let healthcheck pass before exit