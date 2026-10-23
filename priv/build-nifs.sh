#!/bin/bash
# Build all Rust NIFs for the reckon-db package.
#
# Modeled on macula's priv/build-nifs.sh — embeds NIFs inside the
# package that uses them rather than fragmenting them across a
# separate reckon-nifs sidecar. Each reckon-db NIF wrapper module
# (reckon_db_*_nif.erl) loads its .so from priv/ via its own
# `-on_load(init/0)` hook.
#
# Usage: priv/build-nifs.sh [BASEDIR]
# Called by rebar.config pre_hooks during compilation.
#
# Behaviour:
#   - Idempotent: if priv/<crate>.so already exists, skip.
#   - Honours CARGO_TARGET_DIR (via `cargo metadata`), so a shared build cache
#     still yields the NIF; see target_dir_for below.
#   - Tolerant: missing Rust toolchain → log a warning and skip
#     (reckon-db's wrapper modules will use the Erlang fallbacks).
#   - Loud: cargo ran and produced no artefact → ERROR and a non-zero exit.
#   - Per-crate: a failure in one crate does not abort the others; the script
#     exits non-zero after attempting every crate.
#   - Checked by scripts/does_build_nifs_honour_cargo_target_dir.sh.
set -eu

BASEDIR="${1:-.}"
PRIV_DIR="${BASEDIR}/priv"
NATIVE_DIR="${BASEDIR}/native"

# When compiling inside _build/, native/ isn't symlinked but src/ is.
# Follow the src symlink to find the source root and its native/ dir.
if [ ! -d "${NATIVE_DIR}" ] && [ -L "${BASEDIR}/src" ]; then
    SRC_TARGET=$(readlink -f "${BASEDIR}/src")
    SOURCE_ROOT=$(dirname "${SRC_TARGET}")
    if [ -d "${SOURCE_ROOT}/native" ]; then
        NATIVE_DIR="${SOURCE_ROOT}/native"
    fi
fi

mkdir -p "${PRIV_DIR}"

# Where cargo ACTUALLY wrote the library, rather than where it usually does.
#
# `CARGO_TARGET_DIR' set (a shared CI build cache, sccache, a monorepo build)
# sends the artefact to `${CARGO_TARGET_DIR}/release' and both hardcoded
# `${CRATE_DIR}/target/release' copies miss: the script printed a warning,
# exited 0, and the package shipped with the Erlang fallbacks and nothing else
# saying so (reckon-db-org/reckon-db#10). `cargo metadata' reports the resolved
# directory, so it answers for that case and for a workspace member, whose
# artefact lands under the WORKSPACE root, not the crate's own dir. A relative
# `CARGO_TARGET_DIR' is resolved by cargo against the directory cargo runs in,
# which is this script's own working directory, so metadata's answer and the
# `cargo build' above agree.
target_dir_for() {
    local CRATE_DIR="$1"
    local RESOLVED
    if RESOLVED=$(cargo metadata --format-version 1 --no-deps \
                    --manifest-path "${CRATE_DIR}/Cargo.toml" 2>/dev/null \
                  | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p' | head -n 1) \
       && [ -n "${RESOLVED}" ]; then
        printf '%s\n' "${RESOLVED}"
        return 0
    fi
    # cargo too old for metadata: the documented default plus the env var.
    printf '%s\n' "${CARGO_TARGET_DIR:-${CRATE_DIR}/target}"
}

build_nif() {
    local CRATE_NAME="$1"
    local NIF_FILE="${PRIV_DIR}/${CRATE_NAME}.so"
    local CRATE_DIR="${NATIVE_DIR}/${CRATE_NAME}"

    # Skip only if already built AND no source file is newer than the
    # built artifact -- "already built" alone goes stale silently on any
    # edit to the crate. Same defect, same fix as macula-io/macula's
    # priv/build-nifs.sh (this script's own header already says
    # "Modeled on macula's"), found 2026-09-05 when a stale .so there
    # silently masked a security fix through a full day of "clean
    # rebar3 eunit" runs. `find -newer` is POSIX and portable.
    if [ -f "${NIF_FILE}" ] && [ -z "$(find "${CRATE_DIR}" \( -name '*.rs' -o -name 'Cargo.toml' -o -name 'Cargo.lock' \) -newer "${NIF_FILE}" 2>/dev/null)" ]; then
        return 0  # already built and up to date
    fi

    if [ ! -d "${CRATE_DIR}" ]; then
        echo "[${CRATE_NAME}] WARNING: No source at ${CRATE_DIR}, skipping."
        return 0
    fi

    if ! command -v cargo >/dev/null 2>&1; then
        echo "[${CRATE_NAME}] WARNING: Rust toolchain not found, skipping NIF build."
        echo "[${CRATE_NAME}] Install Rust: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
        return 0
    fi

    echo "[${CRATE_NAME}] Building NIF from source..."
    # Stated, not left to `set -e': this function is called as
    # `build_nif ... || NIF_FAILURES=...', which suspends -e inside it. A
    # failed compile must not fall through to the artefact search, where a
    # stale .so from an earlier build would be copied and reported as success.
    if ! cargo build --release --manifest-path "${CRATE_DIR}/Cargo.toml"; then
        echo "[${CRATE_NAME}] ERROR: cargo build failed; no NIF for this crate." >&2
        return 1
    fi

    # Cargo ran, so one of the two platform artefacts MUST be there. A
    # missing one is a broken build, not something to warn about: a package
    # without the NIF runs slower with nothing in the build output to say why.
    local TARGET_DIR ARTEFACT=""
    TARGET_DIR="$(target_dir_for "${CRATE_DIR}")"
    local CANDIDATE
    for CANDIDATE in "${TARGET_DIR}/release/lib${CRATE_NAME}.so" \
                     "${TARGET_DIR}/release/lib${CRATE_NAME}.dylib"; do
        if [ -f "${CANDIDATE}" ]; then
            ARTEFACT="${CANDIDATE}"
            break
        fi
    done

    if [ -z "${ARTEFACT}" ]; then
        echo "[${CRATE_NAME}] ERROR: cargo succeeded but no lib${CRATE_NAME}.so or .dylib is under ${TARGET_DIR}/release." >&2
        echo "[${CRATE_NAME}] Refusing to continue NIF-less: reckon_db would silently run the Erlang fallbacks." >&2
        return 1
    fi

    cp "${ARTEFACT}" "${NIF_FILE}"
}

# No toolchain is tolerated (the fallbacks are the documented path); a build
# that ran and produced nothing is not. Each crate is still attempted so one
# broken crate does not hide another, and the script exits non-zero at the end.
NIF_FAILURES=0
build_nif reckon_db_crypto_nif || NIF_FAILURES=$((NIF_FAILURES + 1))
build_nif reckon_db_archive_nif || NIF_FAILURES=$((NIF_FAILURES + 1))
build_nif reckon_db_hash_nif || NIF_FAILURES=$((NIF_FAILURES + 1))
build_nif reckon_db_aggregate_nif || NIF_FAILURES=$((NIF_FAILURES + 1))
build_nif reckon_db_filter_nif || NIF_FAILURES=$((NIF_FAILURES + 1))

if [ "${NIF_FAILURES}" -ne 0 ]; then
    echo "[reckon-db] ${NIF_FAILURES} NIF(s) were built by cargo but not found. See the ERROR lines above." >&2
    exit 1
fi

echo "[reckon-db] All NIFs ready."
