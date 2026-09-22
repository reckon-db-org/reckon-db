#!/usr/bin/env bash
# Does the hex package actually carry the Rust NIF sources?
#
# THIS EXISTS BECAUSE IT ALREADY WENT WRONG. v2.3.0 was published with the
# default file glob and dropped the embedded NIFs entirely: no native/, no
# priv/build-nifs.sh. v2.3.1 fixed it by listing them in `files' in
# src/reckon_db.app.src, which is where rebar3_hex reads package metadata from,
# NOT rebar.config's `{hex, ...}' block.
#
# That list names each crate by hand, so a crate added under native/ later is
# silently absent from the package, and consumers get the pure-Erlang fallbacks
# with only a build warning to say so. This script compares the crates in git
# against the crates in the built tarball, so the gap fails the build instead.
#
# rebar3_hex strips compiled `.so' from the tarball, so a package with no `.so'
# is correct and expected; what must be there is the SOURCE plus the build
# script. Measured on 5.11.9: 0 `.so', 5 Cargo.toml, 1 build-nifs.sh.
# Takes the repository to check, so CI can run the script from its own
# checkout of this workflow against a separate checkout of the release tag,
# the way is_checkout_publishable.sh is used.
set -uo pipefail
REPO_DIR="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$REPO_DIR" || { echo "REFUSED: no such directory: $REPO_DIR"; exit 1; }

VSN="$(sed -n 's/.*{vsn, *"\([^"]*\)"}.*/\1/p' src/reckon_db.app.src | head -n 1)"
[ -n "$VSN" ] || { echo "REFUSED: no {vsn, ...} in src/reckon_db.app.src"; exit 1; }

rebar3 hex build > /dev/null 2>&1 || { echo "REFUSED: rebar3 hex build failed"; exit 1; }
TAR="_build/default/lib/reckon_db/hex/reckon_db-${VSN}.tar"
[ -f "$TAR" ] || { echo "REFUSED: no package at $TAR"; exit 1; }

LIST="$(tar -xOf "$TAR" contents.tar.gz | tar -tzf -)"
MISSING=()
while IFS= read -r crate; do
    printf '%s\n' "$LIST" | grep -qx "$crate" || MISSING+=("$crate")
done < <(git ls-files 'native/*/Cargo.toml')
printf '%s\n' "$LIST" | grep -qx "priv/build-nifs.sh" || MISSING+=("priv/build-nifs.sh")

if [ "${#MISSING[@]}" -gt 0 ]; then
    echo "REFUSED: reckon_db ${VSN}'s package does not carry:"
    printf '  - %s\n' "${MISSING[@]}"
    echo "Add them to {files, [...]} in src/reckon_db.app.src. This is the v2.3.0 defect."
    exit 1
fi
echo "OK: the package carries $(git ls-files 'native/*/Cargo.toml' | wc -l) NIF crate(s) and priv/build-nifs.sh."
