#!/usr/bin/env bash
# Does priv/build-nifs.sh find the artefact cargo actually wrote?
#
# THIS EXISTS BECAUSE IT WENT WRONG. `CARGO_TARGET_DIR' set (a shared CI build
# cache, sccache, a monorepo build) makes cargo write the library to
# `$CARGO_TARGET_DIR/release/', and the script's two `cp' calls -- both hardcoded
# to `${CRATE_DIR}/target/release' -- missed, printed a warning, and exited 0.
# reckon_db then ran on the Erlang fallbacks with no failure anywhere
# (reckon-db-org/reckon-db#10).
#
# Three cases, run against a fake `cargo' so the outcome is deterministic and
# takes no compiler time:
#
#   A. CARGO_TARGET_DIR set, cargo writes there      -> priv/<crate>.so exists, exit 0
#   B. cargo succeeds but writes no artefact         -> non-zero exit, names the crate
#   C. no Rust toolchain on PATH                     -> exit 0, the tolerated fallbacks
#   D. cargo build fails, a stale .so in target/     -> non-zero exit, stale one not copied
#
# D guards the shape this fix introduced: the per-crate call is
# `build_nif ... || NIF_FAILURES=...', which suspends `set -e' inside the
# function, so a failed compile has to be checked explicitly or a stale
# artefact from an earlier build gets installed and reported as success.
#
# `--real' adds the end-to-end case against the real toolchain: a scratch copy
# of native/ plus the script, built once with CARGO_TARGET_DIR pointing
# elsewhere, asserting every crate's .so lands in priv/. CI runs it; it is the
# case the issue was filed about, with nothing faked.
set -uo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT_REL="priv/build-nifs.sh"
CRATES="reckon_db_crypto_nif reckon_db_archive_nif reckon_db_hash_nif reckon_db_aggregate_nif reckon_db_filter_nif"

FAIL=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1"; FAIL=1; }

# A cargo that does what the case needs and nothing else. `--manifest-path'
# names the crate; the artefact goes where the real cargo would put it.
make_fake_cargo() {
    local BIN_DIR="$1" MODE="$2"
    mkdir -p "$BIN_DIR"
    cat > "$BIN_DIR/cargo" <<FAKE
#!/usr/bin/env bash
# mode=${MODE}
set -eu
prev=""
case "\$1" in
    build)
        manifest=""
        for arg in "\$@"; do
            case "\$prev" in --manifest-path) manifest="\$arg" ;; esac
            prev="\$arg"
        done
        [ -n "\${manifest}" ] || { echo "fake cargo: no --manifest-path" >&2; exit 9; }
        crate_dir="\$(cd "\$(dirname "\${manifest}")" && pwd)"
        crate="\$(basename "\${crate_dir}")"
        target="\${CARGO_TARGET_DIR:-\${crate_dir}/target}"
        case "\${target}" in /*) ;; *) target="\$PWD/\${target}" ;; esac
        [ "${MODE}" = "no-artefact" ] && exit 0
        [ "${MODE}" = "failing" ] && { echo "fake cargo: the crate does not compile" >&2; exit 101; }
        mkdir -p "\${target}/release"
        printf 'fake nif for %s\n' "\${crate}" > "\${target}/release/lib\${crate}.so"
        exit 0
        ;;
    metadata)
        manifest=""
        for arg in "\$@"; do
            case "\$prev" in --manifest-path) manifest="\$arg" ;; esac
            prev="\$arg"
        done
        crate_dir="\$(cd "\$(dirname "\${manifest}")" && pwd)"
        target="\${CARGO_TARGET_DIR:-\${crate_dir}/target}"
        case "\${target}" in /*) ;; *) target="\$PWD/\${target}" ;; esac
        printf '{"target_directory":"%s","packages":[]}\n' "\${target}"
        exit 0
        ;;
esac
exit 1
FAKE
    chmod +x "$BIN_DIR/cargo"
}

# A throwaway "repository": the script plus an empty crate dir per crate, the
# five names the script builds.
make_fixture() {
    local DIR="$1"
    mkdir -p "$DIR/priv" "$DIR/native"
    cp "${HERE}/${SCRIPT_REL}" "$DIR/${SCRIPT_REL}"
    local crate
    for crate in ${CRATES}; do
        mkdir -p "$DIR/native/${crate}/src"
        : > "$DIR/native/${crate}/src/lib.rs"
        printf '[package]\nname = "%s"\n' "${crate}" > "$DIR/native/${crate}/Cargo.toml"
    done
}

run_script() { # fixture-dir env-assignments...
    local DIR="$1"; shift
    env "$@" sh "${DIR}/${SCRIPT_REL}" "${DIR}"
}

count_so() { ls "$1"/priv/*.so 2>/dev/null | wc -l | tr -d ' '; }

all_artefacts_present() {
    local DIR="$1" crate
    for crate in ${CRATES}; do
        [ -s "${DIR}/priv/${crate}.so" ] || return 1
    done
    return 0
}

echo "== A: CARGO_TARGET_DIR set, cargo writes there -> the NIF reaches priv/"
A_DIR=$(mktemp -d) && trap 'rm -rf "${A_DIR}" "${A_TARGET:-}" "${B_DIR:-}" "${C_DIR:-}" "${D_DIR:-}" "${E_DIR:-}" "${E_TARGET:-}"' EXIT
A_TARGET=$(mktemp -d)
make_fixture "$A_DIR"
make_fake_cargo "$A_DIR/bin" normal
A_OUT=$(PATH="$A_DIR/bin:$PATH" CARGO_TARGET_DIR="$A_TARGET" run_script "$A_DIR" 2>&1)
A_RC=$?
if [ "$A_RC" -ne 0 ]; then
    fail "exit code $A_RC with CARGO_TARGET_DIR set"
    printf '%s\n' "$A_OUT" | sed 's/^/       /'
elif ! all_artefacts_present "$A_DIR"; then
    fail "cargo wrote to CARGO_TARGET_DIR but priv/<crate>.so is missing (the #10 defect)"
    printf '%s\n' "$A_OUT" | sed 's/^/       /'
else
    pass "all $(printf '%s\n' ${CRATES} | wc -l | tr -d ' ') crates' .so copied out of CARGO_TARGET_DIR"
fi

echo "== B: cargo succeeds, no artefact appears -> the build fails"
B_DIR=$(mktemp -d)
make_fixture "$B_DIR"
make_fake_cargo "$B_DIR/bin" no-artefact
B_OUT=$(PATH="$B_DIR/bin:$PATH" run_script "$B_DIR" 2>&1)
B_RC=$?
if [ "$B_RC" -eq 0 ]; then
    fail "a cargo run that produced no artefact exited 0 (the #10 defect)"
    printf '%s\n' "$B_OUT" | sed 's/^/       /'
elif ! printf '%s\n' "$B_OUT" | grep -q "reckon_db_crypto_nif"; then
    fail "non-zero exit, but the message does not name the crate"
    printf '%s\n' "$B_OUT" | sed 's/^/       /'
else
    pass "missing artefact is an error that names the crate"
fi

echo "== C: no Rust toolchain -> tolerated, fallbacks, exit 0"
C_DIR=$(mktemp -d)
make_fixture "$C_DIR"
# System dirs only: coreutils for the script, no cargo (it lives in a
# toolchain dir such as ~/.cargo/bin or ~/.asdf/installs/rust/*/bin).
NO_CARGO_PATH="/usr/bin:/bin"
if PATH="$NO_CARGO_PATH" env sh -c 'command -v cargo' >/dev/null 2>&1; then
    fail "this machine has cargo in /usr/bin or /bin; case C cannot isolate the fallback"
else
C_OUT=$(PATH="$NO_CARGO_PATH" run_script "$C_DIR" 2>&1)
C_RC=$?
if [ "$C_RC" -ne 0 ]; then
    fail "no toolchain exited $C_RC; the fallback path must stay tolerated"
    printf '%s\n' "$C_OUT" | sed 's/^/       /'
elif all_artefacts_present "$C_DIR"; then
    fail "no toolchain, yet priv/ carries .so files"
elif ! printf '%s\n' "$C_OUT" | grep -q "Rust toolchain not found"; then
    fail "no toolchain: exited 0 but said nothing about it"
    printf '%s\n' "$C_OUT" | sed 's/^/       /'
else
    pass "no toolchain: warns, skips, exits 0"
fi
fi

echo "== D: cargo build FAILED, a stale artefact sits in target/ -> not shipped"
D_DIR=$(mktemp -d)
make_fixture "$D_DIR"
make_fake_cargo "$D_DIR/bin" failing
# The artefact an earlier build left behind. A failed compile must not copy it.
for crate in ${CRATES}; do
    mkdir -p "$D_DIR/native/${crate}/target/release"
    printf 'stale nif for %s\n' "$crate" > "$D_DIR/native/${crate}/target/release/lib${crate}.so"
done
D_OUT=$(PATH="$D_DIR/bin:$PATH" run_script "$D_DIR" 2>&1)
D_RC=$?
if [ "$D_RC" -eq 0 ]; then
    fail "a failed cargo build exited 0"
    printf '%s\n' "$D_OUT" | sed 's/^/       /'
elif all_artefacts_present "$D_DIR"; then
    fail "a failed cargo build copied the STALE artefact into priv/"
    printf '%s\n' "$D_OUT" | sed 's/^/       /'
elif ! printf '%s\n' "$D_OUT" | grep -q "cargo build failed"; then
    fail "non-zero exit, but the per-crate line does not say the build failed"
    printf '%s\n' "$D_OUT" | sed 's/^/       /'
elif ! printf '%s\n' "$D_OUT" | grep -q "failed to build or were not found"; then
    fail "the summary line does not say a build may have FAILED (it counts both causes)"
    printf '%s\n' "$D_OUT" | sed 's/^/       /'
elif printf '%s\n' "$D_OUT" | grep -q "were built by cargo but not found"; then
    fail "the summary line claims cargo built them, which a failed compile contradicts"
    printf '%s\n' "$D_OUT" | sed 's/^/       /'
else
    pass "failed compile is an error, stale artefact left alone"
fi

if [ "${1:-}" = "--real" ]; then
    echo "== E: real cargo, CARGO_TARGET_DIR set -> every crate's .so lands in priv/"
    if ! command -v cargo >/dev/null 2>&1; then
        fail "--real asked for, but no cargo on PATH"
    else
        E_DIR=$(mktemp -d)
        E_TARGET=$(mktemp -d)
        make_fixture "$E_DIR"
        rm -rf "$E_DIR/native"
        cp -a "${HERE}/native" "$E_DIR/native"
        E_OUT=$(CARGO_TARGET_DIR="$E_TARGET" run_script "$E_DIR" 2>&1)
        E_RC=$?
        if [ "$E_RC" -ne 0 ]; then
            fail "real build with CARGO_TARGET_DIR exited $E_RC"
            printf '%s\n' "$E_OUT" | tail -n 20 | sed 's/^/       /'
        elif ! all_artefacts_present "$E_DIR"; then
            fail "real build: an artefact is missing from priv/"
        else
            pass "real cargo, shared cache: $(count_so "$E_DIR") NIFs in priv/"
        fi
    fi
fi

if [ "$FAIL" -ne 0 ]; then
    echo "REFUSED: priv/build-nifs.sh does not honour CARGO_TARGET_DIR. This is reckon-db#10."
    exit 1
fi
echo "OK: build-nifs.sh honours CARGO_TARGET_DIR, fails loudly on a missing or failed build, and still tolerates no toolchain."
