#!/usr/bin/env bash
# check-file-loc.sh -- per-file LOC budget for Go services.
#
# Why this exists: golangci-lint has no file-length linter (funlen measures
# functions). File size is a different failure mode -- a 1900-line service file
# hides an authorization gate in the middle of unrelated CRUD, which is exactly
# how the PUT /stores/:id and PUT /products/:id bugs survived review.
#
# Budgets are per file KIND, because "300 lines" means different things for a
# handler (one HTTP surface, holds auth gates) and for a model file (dense
# declarations, no logic, splitting it just scatters the type).
#
#   controller/ handler/    300
#   service/ usecase/       400
#   model/ dto/ router/ db/ 600
#   everything else         400
#
# Usage:
#   scripts/check-file-loc.sh                 # scan whole repo, WARN only (exit 0)
#   scripts/check-file-loc.sh --changed       # only files changed vs origin/main
#   scripts/check-file-loc.sh --changed --strict   # exit 1 on violation (use in CI)
#   scripts/check-file-loc.sh --strict        # exit 1 on any violation (brutal on legacy)
#   BASE_REF=main scripts/check-file-loc.sh --changed
#
# Recommended CI wiring: `--changed --strict`. Scanning the whole repo with
# --strict on a legacy codebase makes CI permanently red, and a permanently red
# check is a disabled check.

set -uo pipefail

STRICT=0
CHANGED_ONLY=0
BASE_REF="${BASE_REF:-}"

for arg in "$@"; do
  case "$arg" in
    --strict)  STRICT=1 ;;
    --changed) CHANGED_ONLY=1 ;;
    -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

# Resolve the base ref for --changed. Falls back through the usual suspects so
# this works both in CI (where origin/main exists) and locally.
resolve_base() {
  if [ -n "$BASE_REF" ]; then echo "$BASE_REF"; return; fi
  for candidate in origin/main origin/master main master; do
    if git rev-parse --verify --quiet "$candidate" >/dev/null; then
      echo "$candidate"; return
    fi
  done
  echo ""
}

# budget_for <path> -> max allowed LOC
budget_for() {
  case "$1" in
    */controller/*|*/handler/*|*/controllers/*|*/handlers/*) echo 300 ;;
    */service/*|*/services/*|*/usecase/*|*/usecases/*)       echo 400 ;;
    */model/*|*/models/*|*/dto/*|*/router/*|*/db/*|*/config/*) echo 600 ;;
    *) echo 400 ;;
  esac
}

is_exempt() {
  case "$1" in
    *_test.go)      return 0 ;;  # tests are never penalised for length
    *.pb.go|*_gen.go|*_generated.go|*.gen.go) return 0 ;;
    */vendor/*|*/mocks/*|*/mock/*)            return 0 ;;
    */migrations/*|*/migration/*)             return 0 ;;
    *) return 1 ;;
  esac
}

collect_files() {
  if [ "$CHANGED_ONLY" -eq 1 ]; then
    local base; base="$(resolve_base)"
    if [ -z "$base" ]; then
      echo "check-file-loc: no base ref found for --changed, scanning everything instead" >&2
      git ls-files '*.go'
      return
    fi
    # ACMR: added/copied/modified/renamed. Deleted files have no LOC to check.
    git diff --name-only --diff-filter=ACMR "$base"...HEAD -- '*.go' 2>/dev/null \
      || git ls-files '*.go'
  else
    git ls-files '*.go'
  fi
}

violations=0
checked=0
# ./ prefix keeps the case patterns above (which all start with */) matching
# top-level directories like controller/foo.go too.
while IFS= read -r f; do
  [ -z "$f" ] && continue
  [ -f "$f" ] || continue
  path="./$f"
  is_exempt "$path" && continue
  checked=$((checked + 1))
  loc=$(wc -l < "$f" | tr -d ' ')
  max=$(budget_for "$path")
  if [ "$loc" -gt "$max" ]; then
    over=$((loc - max))
    printf '%s:1: LOC %s exceeds budget %s (over by %s)\n' "$f" "$loc" "$max" "$over"
    violations=$((violations + 1))
  fi
done < <(collect_files)

echo "----"
echo "check-file-loc: $checked file(s) checked, $violations over budget"

if [ "$violations" -gt 0 ] && [ "$STRICT" -eq 1 ]; then
  echo "check-file-loc: FAIL (strict mode)"
  exit 1
fi
exit 0
