#!/usr/bin/env bash
set -euo pipefail

# Publish reckon-db to hex.pm
# Usage: ./scripts/publish-to-hex.sh

cd "$(dirname "$0")/.."

echo "==> Building reckon-db..."
rebar3 compile

echo "==> Running tests..."
rebar3 eunit

echo "==> Building docs..."
rebar3 ex_doc

echo "==> Publishing to hex.pm..."
rebar3 hex publish

echo "==> Done! reckon-db published to hex.pm"
