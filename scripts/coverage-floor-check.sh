#!/usr/bin/env bash
# ENH-054: fails when a client-mirror in-memory engine directory's measured
# coverage drops below its floor (docs/coverage-floors.md). A semantics change
# that adds engine code without the matching engine tests therefore fails the
# leg instead of landing silently — the same drift-check contract as
# env-drift-check.sh: read the report, compare, exit nonzero with a clear
# message.
#
# Usage: coverage-floor-check.sh <format> <profile> <pattern> <floor>
#   lcov   — lcov .info trace file;    pattern = substring of the SF path
#   go     — go coverprofile;          pattern = substring of the file path
#   kover  — Kover XML report;         pattern = fully-qualified class prefix
#   xccov  — xccov --export-format json; pattern = regex on file name/path
#
# Percentages are line-based (statements for go), aggregated across every
# matching file: sum(hit)/sum(found). Matching files report 0% when the
# pattern matches nothing, so a renamed engine directory fails loudly instead
# of silently passing an empty match.
set -euo pipefail

cd "$(dirname "$0")/.."

format=$1
profile=$2
pattern=$3
floor=$4

if [ ! -f "$profile" ]; then
  echo "coverage-floor-check: report '$profile' not found — run the coverage leg first" >&2
  exit 1
fi

case "$format" in
  lcov)
    # Sections are SF:<path> ... LF:<found> LH:<hit> ... end_of_record. The
    # colon-joined records carry no whitespace, so $1 would be the whole line —
    # anchor on the prefix and substr off the tag instead.
    read -r found hit <<EOF
$(awk -v pat="$pattern" '
  /^SF:/ { match_me = index($0, pat) > 0 }
  /^LF:/ { if (match_me) found += substr($0, 4) }
  /^LH:/ { if (match_me) hit   += substr($0, 4) }
  /^end_of_record/ { match_me = 0 }
  END { printf "%d %d", found, hit }
' "$profile")
EOF
    kind="lines"
    ;;
  go)
    # Statement rows: <file>:<range> <numStmt> <count>; count > 0 = covered.
    read -r found hit <<EOF
$(awk -v pat="$pattern" '
  /^(mode:|total:)/ { next }
  {
    stmts = $(NF-1); count = $NF
    if (index($1, pat)) { ft += stmts; if (count + 0 > 0) fc += stmts }
  }
  END { printf "%d %d", ft, fc }
' "$profile")
EOF
    kind="statements"
    ;;
  kover)
    # JaCoCo/Kover XML: <class name="pkg/Class">...<counter type="LINE"
    # missed= covered=>. Class names use slashes, so the dot-separated pattern
    # is normalized to slashes before matching.
    read -r found hit <<EOF
$(python3 - "$profile" "$pattern" <<'PYEOF'
import re, sys
xml = open(sys.argv[1]).read()
pat = sys.argv[2].replace(".", "/")
found = hit = 0
for m in re.finditer(r'<class name="([^"]+)"[^>]*>(.*?)</class>', xml, re.S):
    name, body = m.group(1), m.group(2)
    if not name.startswith(pat):
        continue
    c = re.findall(r'<counter type="LINE" missed="(\d+)" covered="(\d+)"', body)
    if c:
        # The LAST LINE counter in a class element is the class total; earlier
        # ones belong to individual methods and would undercount badly.
        missed, cov = map(int, c[-1])
        found += missed + cov
        hit += cov
print(found, hit)
PYEOF
)
EOF
    kind="lines"
    ;;
  llvmcov)
    # llvm-cov export --summary-only JSON (swift-client leg): data[].files[]
    # with filename + summary.lines{covered,count}.
    read -r found hit <<EOF
$(python3 - "$profile" "$pattern" <<'PYEOF'
import json, sys
data = json.load(open(sys.argv[1]))
pat = sys.argv[2]
found = hit = 0
for f in data["data"][0]["files"]:
    if pat in f.get("filename", ""):
        s = f["summary"]["lines"]
        found += s["count"]
        hit += s["covered"]
print(found, hit)
PYEOF
)
EOF
    kind="lines"
    ;;
  *)
    echo "coverage-floor-check: unknown format '$format' (lcov|go|kover|xccov)" >&2
    exit 1
    ;;
esac

if [ "$found" -eq 0 ]; then
  echo "coverage-floor-check: pattern '$pattern' matched NO files in '$profile' — engine renamed or report malformed? Failing rather than passing an empty match." >&2
  exit 1
fi

pct=$(awk "BEGIN { printf \"%.1f\", 100 * $hit / $found }")

if awk "BEGIN { exit !($pct < $floor) }"; then
  echo "coverage-floor-check: '$pattern' coverage ${pct}% is BELOW its floor of ${floor}% ($hit/$found $kind covered)." >&2
  echo "  Engine semantics changed without the matching engine tests — see docs/coverage-floors.md: add the tests, or ratchet the floor upward only." >&2
  exit 1
fi

echo "coverage-floor-check: '$pattern' ${pct}% >= floor ${floor}% ($hit/$found $kind covered) — pass"
