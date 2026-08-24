#!/usr/bin/env bash
# Fails when a `[[test]]` target declared in a workspace member's Cargo.toml
# has no matching stub in the Dockerfile's dependency layer.
#
# Cargo validates a `[[test]]` path at MANIFEST-PARSE time for every workspace
# member EXCEPT the one being built. The Dockerfile's dependency layer compiles
# with stubbed sources, so a `[[test]]` in another member whose file is absent
# makes `cargo build --bin rtdb-server` fail with "can't find `<name>` test at
# ..." — during `make deploy`, long after `make checkall` went green. This has
# fired for real: a new rust-client `[[test]]` broke the image build and
# nothing in the local gate noticed.
#
# The built member is exempt, and that is not an assumption: measured
# 2026-08-23, hiding `server/tests` and running the Dockerfile's exact
# `cargo build --manifest-path server/Cargo.toml --bin rtdb-server` exits 0,
# while hiding `rust-client/tests` exits 101 with six parse errors. The
# Dockerfile deliberately never copies `server/tests` (a test-only edit would
# invalidate the release layer's cache), so demanding a stub for it would be a
# false alarm.
#
# Direction is one-way: declared-but-not-stubbed is a bug. A leftover stub for
# a target that no longer exists is harmless — an empty file cargo never
# compiles — so it is not reported.
set -euo pipefail

cd "$(dirname "$0")/.."

# The member the Dockerfile actually builds, read from its build line rather
# than hardcoded, so retargeting the image moves the exemption with it.
built_member=$(grep -oE 'cargo build [^\\]*--manifest-path +[A-Za-z0-9_-]+/Cargo\.toml' Dockerfile \
    | head -1 | sed -E 's|.*--manifest-path +([A-Za-z0-9_-]+)/Cargo\.toml|\1|')
if [[ -z "$built_member" ]]; then
    echo "dockerfile-stub-check: could not read the built member from Dockerfile" >&2
    exit 1
fi

# Every `[[test]]` in the workspace, as "<member>/tests/<name>.rs". Cargo's
# default path for `[[test]] name = "x"` is `tests/x.rs`; an explicit `path`
# overrides it.
declared=""
for manifest in */Cargo.toml; do
    member=$(dirname "$manifest")
    [[ "$member" == "$built_member" ]] && continue
    in_test=0
    name=""
    path=""
    flush() {
        if [[ -n "$name" || -n "$path" ]]; then
            [[ -n "$path" ]] || path="tests/$name.rs"
            declared="${declared}${member}/${path}"$'\n'
        fi
        name=""
        path=""
    }
    while IFS= read -r line; do
        case "$line" in
            '[[test]]'*) flush; in_test=1; continue ;;
            '['*)        [[ $in_test -eq 1 ]] && flush; in_test=0; continue ;;
        esac
        [[ $in_test -eq 1 ]] || continue
        case "$line" in
            *name*=*) name=$(sed -E 's/.*"([^"]*)".*/\1/' <<<"$line") ;;
            *path*=*) path=$(sed -E 's/.*"([^"]*)".*/\1/' <<<"$line") ;;
        esac
    done < "$manifest"
    [[ $in_test -eq 1 ]] && flush
done
declared=$(grep -v '^$' <<<"$declared" | sort -u)

# Every source path the Dockerfile names. The stub list is a `touch` whose
# arguments continue across backslash-escaped lines, so match every path-shaped
# `.rs` token rather than only the first argument of each command.
stubbed=$(grep -oE '[A-Za-z0-9_-]+(/[A-Za-z0-9_.-]+)+\.rs' Dockerfile | sort -u)

missing=""
while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    grep -qxF "$path" <<<"$stubbed" || missing="${missing}  ${path}"$'\n'
done <<<"$declared"

if [[ -n "$missing" ]]; then
    echo "dockerfile-stub-check: [[test]] targets with no Dockerfile stub:" >&2
    printf '%s' "$missing" >&2
    echo "Add each to the dependency layer's stub list in Dockerfile, or the" >&2
    echo "image build fails at manifest parse during 'make deploy'." >&2
    exit 1
fi

echo "dockerfile-stub-check: ok ($(grep -c . <<<"$declared") declared [[test]] targets, all stubbed)"
