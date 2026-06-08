#!/usr/bin/env bash
#
# Same pre-existing breakage as fix_integrity_stream_ids.sh, in the
# subscription / delivery / pg-scope suites: stream ids of the shape
# "<name>-0NN" (e.g. "teststreamsub-001") predate the strict
# reckon_gater_stream_id 2.2.0 regex and are rejected by the append gate.
#
# Every "<lowercase/Upper letters>-0NN" literal in these suites is a stream id
# (verified: no subscription name, event type, or data value uses that shape).
# Wrap each in reckon_db_test_helpers:sid/1, which maps the label to a
# conforming id deterministically — so a stream referenced by the same label in
# multiple places (append + assertion) still resolves to one id.
#
# Idempotent: unwrap any prior wrapping first, then wrap.
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "$0")/.." && pwd)/test/integration"

ID_RE='[a-zA-Z]+-0[0-9]+'

for s in reckon_db_subscriptions_SUITE \
         reckon_db_subscription_delivery_SUITE \
         reckon_db_integrity_subscriptions_SUITE \
         reckon_db_pg_scope_SUITE \
         reckon_db_snapshots_SUITE \
         reckon_db_emitter_autostart_SUITE; do
    f="$SUITE_DIR/$s.erl"
    sed -i -E \
        "s/reckon_db_test_helpers:sid\\(<<\"(${ID_RE})\">>\\)/<<\"\\1\">>/g" "$f"
    sed -i -E \
        "s/<<\"(${ID_RE})\">>/reckon_db_test_helpers:sid(<<\"\\1\">>)/g" "$f"
done

echo "Wrapped subscription-suite stream-id literals in reckon_db_test_helpers:sid/1."
