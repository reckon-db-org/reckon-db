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
#   - Tolerant: missing Rust toolchain → log a warning and skip
#     (reckon-db's wrapper modules will use the Erlang fallbacks).
#   - Per-crate: a failure in one crate does not abort the others.
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

build_nif() {
    local CRATE_NAME="$1"
    local NIF_FILE="${PRIV_DIR}/${CRATE_NAME}.so"
    local CRATE_DIR="${NATIVE_DIR}/${CRATE_NAME}"

    if [ -f "${NIF_FILE}" ]; then
        return 0  # already built
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
    cargo build --release --manifest-path "${CRATE_DIR}/Cargo.toml"

    cp "${CRATE_DIR}/target/release/lib${CRATE_NAME}.so" "${NIF_FILE}" 2>/dev/null || \
    cp "${CRATE_DIR}/target/release/lib${CRATE_NAME}.dylib" "${NIF_FILE}" 2>/dev/null || \
    echo "[${CRATE_NAME}] WARNING: Could not find compiled NIF."
}

build_nif reckon_db_crypto_nif
build_nif reckon_db_archive_nif
build_nif reckon_db_hash_nif
build_nif reckon_db_aggregate_nif
build_nif reckon_db_filter_nif
build_nif reckon_db_graph_nif

echo "[reckon-db] All NIFs ready."
