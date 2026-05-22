# ReckonDB Plans — Index

This directory tracks design plans, research, and roadmap documents for `reckon-db` and the surrounding ecosystem packages.

## Active plans

| Document | Status | Target | Description |
|----------|--------|--------|-------------|
| [PLAN_TAMPER_RESISTANCE.md](PLAN_TAMPER_RESISTANCE.md) | Design / Not Started | `reckon-db` 2.1.0 | Add HMAC + hash-chain integrity to events and snapshots; verify on read; new `integrity_violation` error class; migration via `chain_start_version` watermark. Spans `reckon-gater`, `reckon-db`, `evoq`, `reckon-gateway`. |
| [PLAN_PLUGGABLE_BACKENDS.md](PLAN_PLUGGABLE_BACKENDS.md) | See document | TBD | Alternative storage backends behind the `reckon_store` behaviour. |

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

Plans relevant to downstream Macula and Hecate consumers should also be referenced from `/home/rl/work/codeberg.org/macula-internal/macula-architecture/plans/PLAN_MACULA_ROOT.md` so cross-repo work has a single point of visibility.
