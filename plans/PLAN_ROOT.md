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

## Conventions

- Filename pattern: `PLAN_{DESCRIPTIVE_NAME}.md`
- Each plan opens with: `# Plan: {Title}` then `**Status:**`, `**Created:**`, `**Last Updated:**`, then optionally `**Target release:**` and `**Spans repos:**` for multi-repo work
- Cross-repo plans live here (in `reckon-db/plans/`) when `reckon-db` is the primary target repo; cross-references to changes in other repos are documented inline
- Multi-part plans split as `PLAN_{NAME}_PART{N}.md` when length exceeds ~1500 lines (per workspace CLAUDE.md guidance)

## Master cross-ecosystem index

Plans relevant to downstream Macula and Hecate consumers should also be referenced from `/home/rl/work/codeberg.org/macula-internal/macula-architecture/plans/PLAN_MACULA_ROOT.md` so cross-repo work has a single point of visibility.
