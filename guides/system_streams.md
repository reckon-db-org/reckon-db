# System Streams and the `$` Namespace

Reckon-db distinguishes **user streams** from **system streams** at the stream-id level. This guide explains the convention, when each is used, and how subscriptions interact with both.

The convention is borrowed from Greg Young's EventStoreDB, which uses the same `$`-prefix scheme for projections and built-in operational streams (`$all`, `$ce-account`, `$et-UserCreated`, `$by_correlation_id`, `$stats-127.0.0.1`, ...).

---

## Two stream-id formats

### User streams: `<prefix>-<hex>`

User streams hold application data — `OrderPlaced`, `UserRegistered`, `PaymentReceived`, etc.

```
account-018f6a7b8c9d4abc8901234567890abc
order-018f6b1234567abc8901234567890def0
invoice-018f6cabcdef4abc8901234567890123
```

| Part | Rule |
|---|---|
| `<prefix>` | `[A-Za-z]+` — the bounded-context / aggregate type name. No digits, no hyphens, no `$`. |
| `-` | Mandatory separator. |
| `<hex>` | `[A-Fa-f0-9]+` — the aggregate id, typically a UUIDv7 with dashes stripped. |

The two-part shape makes prefix-based queries trivial (`order-*` matches every order stream) and the hex tail is stable, sortable (UUIDv7 is time-ordered), and machine-friendly.

### System streams: `$<namespace>:<name>`

System streams are reckon-db-managed: projections, derived views, operational metadata. They use a `$`-prefixed namespace so operators can immediately tell "this isn't user data."

```
$link:high-value-orders
$link:webhook-feed
$link-sub:high-value-orders
```

| Part | Rule |
|---|---|
| `$` | Mandatory prefix. Marks the stream as system / reckon-db-managed. |
| `<namespace>` | Short identifier for the kind of system stream (`link`, `link-sub`, future `ce`, `et`, etc.). |
| `:` | Separator between namespace and name. |
| `<name>` | Human-readable. The whole point is operational legibility: `$link:high-value-orders` reads as English; `$link:018f6a...` would defeat the purpose. |

The `<name>` is **intentionally not** hex. System streams are stable named artifacts, not aggregate instances. The name is the API.

---

## Subscribing across both namespaces

The `by_stream` subscription filter accepts both forms identically — it just uses the selector as a literal path component. The only special case is the global wildcard:

```erlang
%% Wildcard: every event in every stream
reckon_db_subscriptions:subscribe(my_store, stream, <<"$all">>, <<"audit">>).

%% User stream: one specific account
reckon_db_subscriptions:subscribe(my_store, stream,
    <<"account-018f6a7b8c9d4abc8901234567890abc">>, <<"acct-projector">>).

%% System stream: a link's derived events
reckon_db_subscriptions:subscribe(my_store, stream,
    <<"$link:high-value-orders">>, <<"alerter">>).
```

See [`reckon_db_filters:by_stream/1`](../src/reckon_db_filters.erl) for the formal grammar.

---

## Existing system namespaces

As of this writing:

| Namespace | Owner | Purpose |
|---|---|---|
| `$all` | reckon-db | Global wildcard sentinel for `by_stream`. Not a real stream — a recognised selector value. |
| `$link:<name>` | [`reckon_db_links`](../src/reckon_db_links.erl) | Derived / projected streams. See the [Stream Links guide](stream_links.md). |
| `$link-sub:<name>` | [`reckon_db_links`](../src/reckon_db_links.erl) | The subscription names the link engine uses internally to drive its derivations. Visible in `ListSubscriptions` so operators can see what links are running. |

---

## Reserved for future use

These namespaces aren't implemented yet but are reserved to match EventStoreDB's convention and to leave room for reckon-db-native equivalents:

| Namespace | Equivalent in EventStoreDB | Use |
|---|---|---|
| `$ce-<category>` | Category Events projection | All events from streams matching `<category>-*`. Likely a thin wrapper on `$link` with a default `stream_pattern` source. |
| `$et-<eventType>` | Event Type projection | All events of one type across all streams. Today covered by `by_event_type` subscription; a materialised link would give the replayability + stable target. |
| `$by_correlation_id-<cid>` | Correlation projection | All events sharing one correlation id. Useful for tracing a single business transaction across many streams. |
| `$stats-<host>` | Stats stream | Per-host operational metrics. Probably belongs in reckon-gateway, not reckon-db. |

Don't introduce a new `$<namespace>:` without coordinating — they're a public surface.

---

## What about appended events with malformed ids?

Reckon-db is currently **permissive at append time**: any non-empty binary works as a stream id. The convention above is documented but not enforced. A strict validator (rejecting ids that don't match either format) is a tracked follow-up; it would catch test-fixture mistakes (`partition$XYZ`, `test$basic-stream`) at the source instead of letting them accumulate in stores.

When the validator lands:

- `account-018f6a...` ✅ accepted (user format)
- `$link:my-derived` ✅ accepted (system format)
- `partition$XYZ` ❌ rejected (neither format; the `$` mid-string is the giveaway)
- `test$basic-stream` ❌ rejected (same — looks like a system stream that isn't)
- `myStream` ❌ rejected (no separator, no hex tail)

Until then, treat the rules above as the **canonical convention**; tooling (lazyreckon, etc.) renders both forms correctly already.

---

## Why two formats, not one?

A single combined format would either:

1. **Force operational streams into hex too** — `$link-018f6a7b...` instead of `$link:high-value-orders`. You lose the operational legibility that's the whole point of system streams. Operators reading a stream list shouldn't need a lookup table to know what each derived view is for.
2. **Allow human-readable names for user data** — `account-alice@example.com` or `account-Acme Corp`. Now the id is unstable (PII / renames), unsortable, painful to log-grep across, and you've lost the UUIDv7 time-ordering property.

Splitting along the user/system axis lets each side optimise for its consumer: machines deal with user ids, humans deal with system ids.

---

## See Also

- [Stream Links](stream_links.md) — the mechanics of `$link:` streams (create, filter, transform, lifecycle)
- [`reckon_db_filters`](../src/reckon_db_filters.erl) — `by_stream/1` grammar
- [`reckon_db_links`](../src/reckon_db_links.erl) — module docstring expands on the rationale for the link primitive vs typed subscriptions
- EventStoreDB's [System streams reference](https://developers.eventstore.com/server/v23.10/projections.html#system-projections) — origin of the `$`-prefix convention
