#!/usr/bin/env bash

set -e

cd "$(dirname "$0")/.." || exit

docker compose -f "tests/docker-compose.yml" up "${@}"
