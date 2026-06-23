# CCC Payload Indexes

*Available in reckon-db 5.3.0+.*

CCC (Command Context Consistency) payload indexes extend the DCB conditional-append
primitive with query predicates over event data fields. This document covers the
reckon-db implementation: store configuration, what gets indexed, and the Horus
constraint that shapes the API.

For the conceptual framing — what CCC is, how it relates to DCB, and worked
end-to-end examples — see the
[reckon-gater CCC guide](https://codeberg.org/reckon-db-org/reckon-gater/src/branch/main/guides/ccc.md).

---

## Declaring Indexes

Payload indexes are opt-in per store. Declare them in the `store_config` at
store creation time (or add them before writing events that need to be indexed):

```erlang
Indexes = [
    tags,                                           %% always-on tag index
    event_type,                                     %% always-on event-type index
    {payload, <<"account_id">>},                    %% single-field payload index
    {payload_hash, [<<"flight_id">>, <<"seat_no">>]} %% composite hash index
],
reckon_gater_api:create_store(StoreId, #{indexes => Indexes}).
```

### `{payload, Key}`

Builds a `[by_payload, Key, Value, SeqKey]` index entry for every event whose
JSON data contains `Key` with a binary (string) value. Enables
`{payload_match, Key, Value}` filters in `append_if_no_tag_matches` and
`dcb_read_context`.

One Khepri subtree lookup per distinct key-value pair checked. Correct when the
command narrows its window by one field value.

### `{payload_hash, [K1, K2, ...]}`

Builds a `[by_payload_hash, Hash, SeqKey]` index entry for every event whose
JSON data contains all of K1, K2, … with binary values. The hash is
`SHA-256(term_to_binary(lists:sort(zip(Keys, Values))))` — field-order
independent. Enables `{payload_hash_match, Keys, Values}` filters.

One Khepri lookup regardless of how many fields are in the combination. Correct
for multi-field uniqueness checks (e.g., "no other reservation for this flight
and seat").

---

## Filter Shapes

Two new shapes added to `tag_filter()` in 5.3.0:

```erlang
{payload_match, Key :: binary(), Value :: binary()}
%% Matches events where data[Key] == Value (string comparison).
%% Requires {payload, Key} declared in the store.

{payload_hash_match, Keys :: [binary()], Values :: [binary()]}
%% Matches events where the combination (Keys, Values) hashes identically
%% to the indexed combination. All supplied keys must be present in the
%% event data as binary values.
%% Requires {payload_hash, Keys} declared in the store.
```

Both compose freely with `and_`, `or_`, `any_of`, `all_of`, and `event_type`:

```erlang
%% event_type=seat_reserved_v1 AND (flight_id, seat_no) == (FL001, 12A)
Filter = {and_, [
    {event_type, <<"seat_reserved_v1">>},
    {payload_hash_match,
     [<<"flight_id">>, <<"seat_no">>],
     [<<"FL001">>,     <<"12A">>]}
]}.
```

---

## The Horus Constraint

The consistency check runs inside a `khepri:transaction/2` body that Horus
extracts into a portable Raft log entry. Horus rejects any code that calls
`crypto:*` — including SHA-256 — even on branches that can never execute.

`{payload_hash_match, Keys, Values}` requires hashing. The solution: call
`reckon_db_ccc_filter:preprocess_filter/1` **before** entering the transaction.
It rewrites `{payload_hash_match, Keys, Values}` into
`{payload_hash_match_pre, Hash}`, where `Hash` is the pre-computed SHA-256.
The transaction body sees only the pre-computed value.

`append_if_no_tag_matches/4` does this automatically. If you write your own
transaction that calls `reckon_db_ccc_filter:match_any_above_cutoff/3`, call
`preprocess_filter/1` first:

```erlang
ProcessedFilter = reckon_db_ccc_filter:preprocess_filter(TagFilter),
%% Now safe to use ProcessedFilter inside khepri:transaction/2
```

`{payload_match, Key, Value}` does **not** involve hashing and requires no
preprocessing.

---

## What Is Not Indexed

- Non-binary JSON values (numbers, booleans, objects, arrays, null). Cast to
  binary at write time if you need them queryable.
- Absent fields. Events missing the declared key produce no index entry and
  are invisible to that filter.
- Nested JSON fields. Only top-level keys are indexed.
- Events written before the index was declared. Retroactive re-indexing is not
  automatic; re-write events through `append_if_no_tag_matches/4` if you need
  historical coverage.

---

## Implementation Modules

| Module | Purpose |
|--------|---------|
| `reckon_db_index_config` | Validates and stores index declarations; `declared_dcb_payload/1` extracts payload declarations |
| `reckon_db_ccc_paths` | Khepri path builders for payload indexes (`by_payload_path/3`, `by_payload_hash_path/2`, `payload_combo_hash/2`) |
| `reckon_db_ccc_filter` | Filter evaluation: `match_seqs/2`, `match_any_above_cutoff/2,3`, `preprocess_filter/1` |
| `reckon_db_dcb` | `extract_payload_entries/2` (outside tx), `write_one_record/3` (writes index entries inside tx) |

---

## See also

- [DCB guide](dcb.md) — the full filter algebra and the conditional-append API
- [DCB Raft design](dcb_raft_design.md) — the Horus constraint and the pre-stamp pattern
- [reckon-gater CCC guide](https://codeberg.org/reckon-db-org/reckon-gater/src/branch/main/guides/ccc.md) — conceptual framing, worked examples, literature
