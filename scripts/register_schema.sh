#!/usr/bin/env bash
set -euo pipefail

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
TOPIC="${TOPIC:-warehouse-events}"
SUBJECT="${TOPIC}-value"
SCHEMA_FILE="${SCHEMA_FILE:-/schemas/warehouse_event.avsc}"

if [[ ! -f "$SCHEMA_FILE" ]]; then
  echo "Schema file not found: $SCHEMA_FILE" >&2
  exit 1
fi

payload="$(python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    print(json.dumps({'schema': f.read()}))
" "$SCHEMA_FILE")"

response="$(curl -sf -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$payload" \
  "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions")"

echo "Registered schema for subject $SUBJECT: $response"
