#!/usr/bin/env bash
#
# One-time repair: the integrity integration suites predate the strict
# reckon_gater_stream_id 2.2.0 regex (^[a-z]{1,32}-[a-f0-9]{32}$) and use
# human-readable stream ids ("clean-chain", "stream-disabled", ...) that the
# append validation gate now rejects. Wrap each CONFIRMED stream-id literal in
# reckon_db_test_helpers:sid/1, which deterministically maps a label to a
# conforming id. Event-type and tag literals ("e1", "forged", ...) are left
# untouched — only the exact stream-id literals listed below are rewritten.
#
# Idempotent: re-running is a no-op because wrapped literals no longer match
# the bare-literal patterns.
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "$0")/.." && pwd)/test/integration"

# Replace an exact bare literal <<"LABEL">> with the sid/1 wrapper.
# Idempotent: first unwrap any prior wrapping, then wrap — so re-running
# never double-wraps.
wrap() {
    local file="$1" label="$2"
    sed -i -E "s/reckon_db_test_helpers:sid\\(<<\"${label}\">>\\)/<<\"${label}\">>/g" "$file"
    sed -i -E "s/<<\"${label}\">>/reckon_db_test_helpers:sid(<<\"${label}\">>)/g" "$file"
}

WRITES="$SUITE_DIR/reckon_db_integrity_writes_SUITE.erl"
for L in stream-disabled stream-enabled-0 stream-chain-test stream-A stream-B \
         stream-mac-vs stream-mac-vs-2; do
    wrap "$WRITES" "$L"
done

READS="$SUITE_DIR/reckon_db_integrity_reads_SUITE.erl"
for L in clean-chain single middle t-prev t-cleared t-deleted t-inserted \
         t-swapped mixed-skip-legacy strict-test skip-all mixed \
         backward-tamper backward-clean disabled \
         t-data t-meta t-type t-tags t-ts t-mac; do
    wrap "$READS" "$L"
done

SNAPS="$SUITE_DIR/reckon_db_integrity_snapshots_SUITE.erl"
for L in snap-save-1 snap-disabled snap-no-event snap-legacy-event \
         snap-load-ok snap-t-stream snap-legacy snap-disabled-load \
         snap-t-state snap-t-meta snap-t-anchor snap-t-mac; do
    wrap "$SNAPS" "$L"
done

echo "Wrapped integrity-suite stream-id literals in reckon_db_test_helpers:sid/1."
