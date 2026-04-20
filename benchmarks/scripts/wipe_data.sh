#!/usr/bin/env bash
#
# Wipe the bench-local reckon-db data directory so a new run starts
# from a clean slate. The path mirrors config/sys.config.

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BENCH_DIR"

if [[ -d data/bench_store ]]; then
    printf 'wiping data/bench_store\n'
    rm -rf data/bench_store
fi
mkdir -p data/bench_store
