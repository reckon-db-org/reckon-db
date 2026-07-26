# ReckonDB Plans — Index

This directory tracks design plans, research, and roadmap documents for `reckon-db` and the surrounding ecosystem packages.

## Active plans

| Document | Status | Target | Description |
|----------|--------|--------|-------------|
| [PLAN_FIX_STORE_CLUSTER_SPLIT_BRAIN.md](PLAN_FIX_STORE_CLUSTER_SPLIT_BRAIN.md) | Open / Not Started | `reckon_db_store_coordinator` | **High.** Simultaneous cold boot self-forms N standalone 1-member clusters instead of one N-member cluster (recurring on the parksim beam fleet). Root cause: racy coordinator election on partial connected-node views, and a self-elected `coordinator` never reconciles/merges. Fix: stable election + standalone-coordinator split-detect-and-merge (mirrors ex-esdb's persistent `:check_members`). Interim external mitigation shipped (`converge-parksim.sh`). |
| [PLAN_TAMPER_RESISTANCE.md](PLAN_TAMPER_RESISTANCE.md) | Design / Not Started | `reckon-db` 2.1.0 | Add HMAC + hash-chain integrity to events and snapshots; verify on read; new `integrity_violation` error class; migration via `chain_start_version` watermark. Spans `reckon-gater`, `reckon-db`, `evoq`, `reckon-gateway`. |
| [PLAN_PLUGGABLE_BACKENDS.md](PLAN_PLUGGABLE_BACKENDS.md) | See document | TBD | Alternative storage backends behind the `reckon_store` behaviour. |
| [PLAN_DCB_IMPLEMENTATION.md](PLAN_DCB_IMPLEMENTATION.md) | Design / Not Started | `reckon-db` 2.4.0, `reckon-gater` 2.3.0, `reckon-evoq` 2.3.0, `evoq` 1.18.0 | Query-based concurrency (DCB) — full stack: storage primitive `append_if_no_tag_matches`, wire verb, adapter passthrough, `evoq_decision` behaviour, reference example. Supersedes PLAN_FUTURE_RESEARCH § DCB Phase 3. |
| [PLAN_STREAM_NAMESPACE_MODEL_C.md](PLAN_STREAM_NAMESPACE_MODEL_C.md) | ✅ Implemented (2026-06-08) | `reckon-db` (next major), `reckon-gater` (minor) | **Implementation plan for Model C.** New `reckon_gater_stream_id:parts/1` + a single `reckon_db_stream_path` module that owns the 4-level layout; exhaustive grepped call-site list; 10-step order; full test plan (round-trip property, namespace isolation, `_dcb` coexistence, integrity-chain canary). DCB **per-store** (locked). Implements DESIGN_STREAM_NAMESPACE. |
| [DESIGN_STREAM_NAMESPACE.md](DESIGN_STREAM_NAMESPACE.md) | ✅ Accepted + Implemented (2026-06-08) | `reckon-db` (next major) | Make aggregate **type** a structural Khepri path level — `[streams, Type, Id, Version]` instead of flat `[streams, Id, Version]` with a prefix convention. One store per Division/tenant (no boot storm), per-type subtrees (un-mixed namespace, O(type) type-scoped ops, deliberate DCB/scavenge scope). Breaking layout change with **no migration** (not in production — recreate stores fresh). **Implement first**, then the secondary index. |
| [PLAN_SECONDARY_INDEX_IMPL.md](PLAN_SECONDARY_INDEX_IMPL.md) | ✅ Implemented (2026-06-08) | `reckon-db` (after Model C) | **Implementation of the secondary index.** New `reckon_db_index` (`[idx]` layout, `entries/2`, `lookup_*`) + `reckon_db_index_config` (per-store opt-in via persistent_term); transactional write path (`build_event_writes` + `write_batch`); `read_by_tags`/`read_by_event_types` index-or-scan dispatch + new `read_by_metadata/3`. 579 eunit + 143 ct green. |
| [DESIGN_SECONDARY_INDEX.md](DESIGN_SECONDARY_INDEX.md) | ✅ Accepted + Implemented (2026-06-08) | `reckon-db` (after Model C) | Generic **opt-in, write-maintained** secondary index (`by_tag` for all events / `by_event_type` / `{meta, Key}`) — turns cross-cutting lookups (`read_by_tags`, `read_by_event_types`, `read_by_metadata`) from O(total) scans into O(matches) subtree reads, mirroring the DCB `by_tag` mechanism. The **index primitive** apps build causation/correlation read models on (`read_by_metadata`); the store never interprets lineage. Transactional with append; per-store opt-in (don't index "just in case"). Complementary to Model C. |

| [PLAN_CCC.md](PLAN_CCC.md) | Design | `reckon-db` 5.3.0, `reckon-gater` 3.5.0, `reckon-gateway` 0.13.0 | Command Context Consistency — extends DCB with payload predicates. New `{payload, Key}` index declaration, `[by_payload, ...]` DCB path, `{payload_match, K, V}` filter variant, gateway + proto exposure. DCB-scoped (not global). |

## Cross-repo propagation

| Document | Status | Description |
|----------|--------|-------------|
| [PLAN_5_0_PROPAGATION.md](PLAN_5_0_PROPAGATION.md) | Inventory complete (2026-06-08) | How reckon-db 5.0.0 (Model C + secondary index) propagates to reckon-{proto,gateway,go,lazy}. Model C is transparent everywhere; `read_by_metadata` needs proto→gateway→go (additive, in order); index declaration is server-side only. Urgent: reckon-gateway pins `reckon_db ~> 4.0` and must widen to `~> 5.0` (+ recreate its embedded store). |

## Research

| Document | Status | Description |
|----------|--------|-------------|
| [PLAN_FUTURE_RESEARCH.md](PLAN_FUTURE_RESEARCH.md) | Active research log | Dynamic Consistency Boundaries, event-sourcing paradigm explorations, and other forward-looking research topics. |

## Ecosystem applications

Plans owned by other repos in the ReckonDB ecosystem, referenced here for visibility.

| Document | Repo | Status | Description |
|----------|------|--------|-------------|
| [PLAN_STRIP_TO_PRODUCT_SITE.md](https://codeberg.org/reckon-internal/reckon-portal/src/branch/main/plans/PLAN_STRIP_TO_PRODUCT_SITE.md) | `reckon-internal/reckon-portal` | ✅ Shipped (2026-05-22, PRs #4–#7) | Strip reckon-portal to a product-only site: remove auth/membership/dashboard/admin/payments/sponsorship and **drop Postgres entirely**. Now marketing + public docs + the event-sourced blog (ReckonDB only); boots with no DB. Authenticated console belongs in macula-realm. Includes daisyUI token migration and a Support/Sponsor/Enterprise pricing reframe. |

## Conventions

- Filename pattern: `PLAN_{DESCRIPTIVE_NAME}.md`
- Each plan opens with: `# Plan: {Title}` then `**Status:**`, `**Created:**`, `**Last Updated:**`, then optionally `**Target release:**` and `**Spans repos:**` for multi-repo work
- Cross-repo plans live here (in `reckon-db/plans/`) when `reckon-db` is the primary target repo; cross-references to changes in other repos are documented inline
- Multi-part plans split as `PLAN_{NAME}_PART{N}.md` when length exceeds ~1500 lines (per workspace CLAUDE.md guidance)

## Master cross-ecosystem index

Plans relevant to downstream Macula and Hecate consumers should also be referenced from `/home/rl/work/github.com/macula-io/macula-architecture/plans/PLAN_MACULA_ROOT.md` so cross-repo work has a single point of visibility.
