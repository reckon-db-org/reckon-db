# Contributing to reckon-db

Thanks for your interest in contributing. reckon-db is a BEAM-native event store and the contribution surface is intentionally small — most production change comes from internal use, and external contributions are most valuable as bug reports, test cases, and small targeted fixes.

## How to report a bug

1. Open an issue on the canonical Codeberg repository: <https://codeberg.org/reckon-db-org/reckon-db/issues>.
2. Include:
   - reckon-db version (`{vsn, ...}` from `src/reckon_db.app.src`, or the hex package version)
   - Erlang/OTP version (`erl -version`)
   - Minimal reproduction — preferably an EUnit test that demonstrates the failure
   - Stack trace if the failure is a crash

If you're reporting a security issue, please follow [SECURITY.md](SECURITY.md) (when present) or contact the maintainer privately rather than opening a public issue.

## How to propose a change

For non-trivial changes, open an issue first to discuss the approach. This avoids you investing time in a direction the project won't accept.

For small fixes, a pull request is welcome directly.

### Pull request expectations

- **Tests.** New behaviour MUST come with EUnit tests. Bug fixes MUST come with a regression test that fails before the fix and passes after.
- **No new dialyzer warnings.** `rebar3 dialyzer` will flag anything new. See [docs/dialyzer-backlog.md](docs/dialyzer-backlog.md) for the existing backlog — you do not need to fix pre-existing warnings as part of an unrelated change, but you also should not add to them.
- **No new ex_doc warnings.** `rebar3 ex_doc` must complete clean.
- **Vertical slicing.** New features go in directories named by intent (`add_stream/`, `verify_chain/`), not by technical layer. No `services/`, `repositories/`, `utils/`. See the project root [CLAUDE.md](https://codeberg.org/reckon-db-org/reckon-db) ecosystem-wide for the full rule set.
- **CRUD events are taboo.** Domain events use business verbs (`stream_appended`, `subscription_registered`, `snapshot_recorded`), never `created` / `updated` / `deleted`.

### Commit message style

Follow conventional-commit-ish prefixes — `feat(scope):`, `fix(scope):`, `chore:`, `docs:`. The body should explain *why* the change exists, not what the diff shows. Recent commit history is a good reference.

### Code style

- Erlang style: spaces over tabs, 4-space indent, line-length pragmatic (not enforced).
- Comments only when the *why* is non-obvious. Don't restate what well-named identifiers already say.
- Use `:module.function/arity` (not Elixir wrappers) for Erlang library access.

## Building and testing locally

```sh
rebar3 deps         # fetch deps
rebar3 compile      # build
rebar3 eunit        # run unit tests (518 currently)
rebar3 dialyzer     # type-check (see backlog note above)
rebar3 ex_doc       # generate docs (must be warning-free)
```

The full integration-test matrix lives in the sibling [reckon-e2e](https://codeberg.org/reckon-db-org/reckon-e2e) repo. Run that suite when touching cluster, subscription, or snapshot machinery.

## Releasing

Maintainer-only. See the release checklist in the workspace `~/.claude/CLAUDE.md` "Releasing a Package" section. The short form is: pre-release verification (tests + dialyzer-delta + ex_doc + required files + visual asset check) → version bump → commit → tag → push → user publishes to hex.pm manually.

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating you agree to uphold its terms.

## License

By submitting a pull request you agree your contribution is licensed under the same Apache-2.0 license that covers the rest of the project (see [LICENSE](LICENSE)).
