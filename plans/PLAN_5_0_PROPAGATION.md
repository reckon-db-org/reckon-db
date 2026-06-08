# Plan: Propagating reckon-db 5.0.0 to the polyglot stack

**Status:** ✅ Propagated (2026-06-08) — code landed in proto/gateway/go; lazy needs nothing. See Outcome below.
**Created:** 2026-06-08
**Spans repos:** reckon-proto, reckon-gateway, reckon-go, reckon-lazy
**Origin:** reckon-db 5.0.0 (Model C structural layout + opt-in secondary index)
  — see [PLAN_STREAM_NAMESPACE_MODEL_C.md](PLAN_STREAM_NAMESPACE_MODEL_C.md),
  [PLAN_SECONDARY_INDEX_IMPL.md](PLAN_SECONDARY_INDEX_IMPL.md)

## TL;DR

- **Model C (on-disk layout) is transparent downstream.** Nothing in
  proto/gateway/go/lazy reads Khepri directly or splits stream-id structure —
  all treat `stream_id` as an opaque string and go through the gRPC contract /
  `reckon_gater_api`. **No code changes for Model C anywhere.** One caveat: a
  dep-constraint bump (gateway pins `reckon_db ~> 4.0`).
- **`read_by_metadata` needs the full additive chain** (proto RPC → gateway
  handler → go client), in dependency order. Each is a straight mirror of the
  existing `read_by_tags` path. lazy is optional.
- **Index declaration (`store_config.indexes`) does NOT flow through the client
  stack.** Stores are deployment-managed (no `CreateStore` RPC by design);
  declaration is server-side config. Only reckon-gateway (which embeds a store)
  needs a small config-plumbing change to let its embedded store declare indexes.

## Current versions / dep constraints (as found)

| Repo | Version | Pins |
|------|---------|------|
| reckon-proto | 0.5.0 | — |
| reckon-gateway | 0.8.0 | `reckon_gater ~> 3.0`, **`reckon_db ~> 4.0`**, `reckon_proto v0.5.0` |
| reckon-go | ~0.5 (proto stubs committed, target proto 0.5.x) | grpc/protobuf only in go.mod; stubs vendored |
| reckon-lazy | tracks `reckon-go v0.4.0` | reckon-go only (proto/db transitive) |

## Propagation matrix

| Repo | Model C | `read_by_metadata` | Index declaration |
|------|---------|--------------------|--------------------|
| **reckon-proto** | none (opaque `stream_id`) | **NEW** `ReadByMetadata` RPC on `StreamService` + `ReadByMetadataRequest{store_id, key, value, batch_size}` → `ReadStreamResponse` (mirror `ReadByTags`). Minor bump. | none — no store-mgmt proto (stores ephemeral by design) |
| **reckon-gateway** | none (all via `reckon_gateway_dispatch` → `reckon_gater_api`; zero `khepri:*`/`[streams,…]`) | **NEW** handler `reckon_gateway_stream_service:read_by_metadata/2` (mirror `read_by_tags/2`, L173-193) + register in service map | env-var plumbing only: parse `RECKON_GATEWAY_STORE_INDEXES` in `reckon_gateway_config`, add `indexes` to `embedded_store_spec`, set `#store_config{indexes=…}` in `reckon_gateway_store_starter` (L35-39, currently 3 fields) |
| **reckon-go** | none (opaque; zero stream-id parsing) | regen stubs from proto 5.x; **NEW** `(*streams.Client).ReadByMetadata(ctx, key, value, batch, opts)` (mirror `ReadByTags`, streams.go:322). Minor bump. | none — no `CreateStore` (discovery-only `stores` pkg) |
| **reckon-lazy** | none (via reckon-go gRPC; treats ids opaque; only special-cases `_dcb` by exact string) | **optional** — could add a metadata/tag/type filter view; not required to keep working | none |

## Required dependency bump (do this first / regardless)

**reckon-gateway** pins `reckon_db ~> 4.0` — it cannot even pull 5.0.0. To run
against 5.0.0 stores it must widen to **`reckon_db ~> 5.0`** (`reckon_gater ~> 3.0`
already admits 3.2.0). And because Model C is an on-disk layout break, the
gateway's **embedded store must be recreated** on that upgrade (not just
restarted) — see the containerized-reckon_db / stable-node antipattern. This is
the most time-sensitive item: the gateway is the live ingress.

## Order of work (when we choose to ship `read_by_metadata` to clients)

1. **reckon-proto** — add `ReadByMetadata` RPC + request msg. Tag a minor
   (0.6.0). Nothing else compiles against the new RPC until this lands.
2. **reckon-gateway** — bump `reckon_db ~> 5.0` + `reckon_proto` to the new tag;
   add the `read_by_metadata` handler; (optionally) the index env-var plumbing.
   Recreate the embedded store. Release a minor (0.9.0).
3. **reckon-go** — regen stubs from the new proto; add `ReadByMetadata`. Minor
   (0.6.0). reckon-gateway (2) and reckon-go (3) are independent once (1) lands.
4. **reckon-lazy** (optional) — bump reckon-go; surface metadata/tag/type
   filtering or Model-C type grouping if/when desired. Not required.

## Notes / open choices

- **DCB precedent everywhere.** Each repo already has the cross-cutting-read
  shape: proto `DcbService`/`ReadByTags`, gateway `dcb_service` + `read_by_tags`
  handler, go `dcb` pkg + `ReadByTags`. `read_by_metadata` is a strictly simpler
  mirror (no recursive filter — just Key+Value).
- **Index declaration is intentionally NOT a client concern.** Per the
  ephemeral-store design (no CreateStore), an app declares `store_config.indexes`
  in its own sys.config/boot. Only the gateway's *embedded* store needs the
  env-var path. Don't add a CreateStore RPC just for indexes.
- **Model-C upside for lazy:** stream *type* is now structural; a future
  gateway/proto field could expose it so lazy groups streams by type. Opt-in,
  not now.
- ~~**Nothing here is started**~~ — done; see Outcome.

## Outcome (2026-06-08)

`read_by_metadata` shipped through the whole chain; Model C confirmed
transparent. Commits on `main` in each repo (TBD); tagging/release per repo is
the remaining manual step.

| Repo | Change | Version | Tag/release status |
|------|--------|---------|--------------------|
| reckon-proto | `StreamService.ReadByMetadata` RPC + `ReadByMetadataRequest` | 0.6.0 | **tagged `v0.6.0`** (git-only) ✅ |
| reckon-gateway | `read_by_metadata/2` handler (regen stubs) + `RECKON_GATEWAY_STORE_INDEXES` env → `store_config.indexes`; deps `reckon_db ~> 5.0`, `reckon_proto v0.6.0`. 72 eunit, dialyzer-clean | 0.9.0 | committed; deploy via hecate-gitops (git-tag) — **not yet tagged** |
| reckon-go | `(*streams.Client).ReadByMetadata` + regen stubs + tests | 0.6.0 | committed; **release via `scripts/release-local.sh`** (tag `v0.6.0`) — not yet run |
| reckon-lazy | none required (insulated via reckon-go; ids opaque) | — | optional: bump reckon-go `v0.4.0 → v0.6.0` once tagged; optional metadata/tag/type filter UI |

**Still manual (per release policy):** tag reckon-gateway + run reckon-go's
`release-local.sh` for v0.6.0. **Operational reminder:** any reckon-gateway with
an embedded store must recreate that store on the `reckon_db ~> 5.0` upgrade
(Model C layout break) — catalogue-mode gateways are unaffected.
