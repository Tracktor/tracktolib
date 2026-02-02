#!/usr/bin/env bash
#
# DESCRIPTION
#   Generate TypedDicts from GitHub's OpenAPI spec for the tracktolib.gh module.
#   Downloads the full GitHub REST API spec, filters to required schemas, and
#   generates Python TypedDict classes using datamodel-codegen.
#
# USAGE
#   bin/gen-gh-types.sh
#
#   No arguments required. The script outputs to tracktolib/gh/types.py.
#   To add new schemas, edit the SCHEMAS array in this script.

set -euo pipefail


cd "$(dirname "$0")/.." || exit

OUTPUT_FILE="tracktolib/gh/types.py"
TEMP_FULL=$(mktemp)
TEMP_SPEC=$(mktemp)
trap 'rm -f "$TEMP_FULL" "$TEMP_SPEC"' EXIT

GITHUB_OPENAPI_URL="https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.json"

# Schemas we need (and their dependencies will be resolved)
SCHEMAS=(
  "issue-comment"
  "label"
  "deployment"
  "deployment-status"
  "nullable-simple-user"
  "simple-user"
  "nullable-integration"

  "reaction-rollup"
)

echo "Downloading GitHub OpenAPI spec..."
curl -sL "$GITHUB_OPENAPI_URL" -o "$TEMP_FULL"

echo "Filtering to required schemas..."
# Build jq filter for selected schemas
SCHEMA_FILTER=$(printf '"%s",' "${SCHEMAS[@]}" | sed 's/,$//')

jq --argjson schemas "[$SCHEMA_FILTER]" '
{
  openapi: .openapi,
  info: .info,
  servers: .servers,
  components: {
    schemas: (.components.schemas | with_entries(select(.key as $k | $schemas | index($k))))
  }
}' "$TEMP_FULL" > "$TEMP_SPEC"

echo "Generating TypedDicts..."
uv run datamodel-codegen \
  --input "$TEMP_SPEC" \
  --input-file-type openapi \
  --output "$OUTPUT_FILE" \
  --output-model-type typing.TypedDict \
  --target-python-version 3.14 \
  --disable-future-imports \
  --use-standard-collections \
  --use-union-operator \
  --use-double-quotes \
  --collapse-root-models \
  --strip-default-none \
  --use-schema-description \
  --openapi-scopes schemas

# Clean up generated file
sed -i '/from typing_extensions import/d' "$OUTPUT_FILE"
sed -i '/^#   filename:/d' "$OUTPUT_FILE"

echo "Done! Generated $(wc -l < "$OUTPUT_FILE") lines to $OUTPUT_FILE"
