#!/usr/bin/env bash
#
# Model C re-keys events from the flat [streams, StreamId, Version] layout to
# the structural [streams, Type, Id, Version] subtree. The integrity suites
# tamper with events by writing directly to Khepri (bypassing the public API,
# which is the whole point of a tamper test), and built the OLD flat path
# inline. Route those direct accesses through reckon_db_stream_path:event_path/2
# so the tests mutate the real event node under the new layout.
#
# Converts every `[streams, StreamId, <Var>]` occurrence (a 3-element event
# path with an identifier stream id and a padded-version variable) into
# `reckon_db_stream_path:event_path(StreamId, <Var>)`. Leaves base paths like
# `[streams]` / `[metadata]` untouched. Idempotent.
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "$0")/.." && pwd)/test/integration"

for s in reads snapshots subscriptions; do
    f="$SUITE_DIR/reckon_db_integrity_${s}_SUITE.erl"
    sed -i -E \
        "s/\\[streams, StreamId, ([A-Za-z0-9_]+)\\]/reckon_db_stream_path:event_path(StreamId, \\1)/g" \
        "$f"
done

echo "Routed integrity-suite tamper paths through reckon_db_stream_path:event_path/2."
