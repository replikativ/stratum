#!/usr/bin/env bash
# psql_smoke_test.sh — Integration test for Stratum's PostgreSQL wire protocol.
#
# Starts a Stratum server with --demo data, runs SQL queries via psql,
# and validates output. Requires psql on PATH.
#
# Usage: bash test/bin/psql_smoke_test.sh [port]

set -euo pipefail

PORT="${1:-15432}"
PASS=0
FAIL=0
PIDS=""

cleanup() {
  if [ -n "$PIDS" ]; then
    kill $PIDS 2>/dev/null || true
    wait $PIDS 2>/dev/null || true
  fi
}
trap cleanup EXIT

# Check psql availability
if ! command -v psql &>/dev/null; then
  echo "SKIP: psql not found on PATH"
  exit 0
fi

echo "=== Starting Stratum server on port $PORT ==="
clj -M:server --demo --port "$PORT" &>/dev/null &
PIDS=$!

# Wait for TCP readiness
echo "Waiting for server..."
for i in $(seq 1 30); do
  if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
    echo "Server ready after ${i}s"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "FAIL: Server did not start within 30s"
    exit 1
  fi
  sleep 1
done

run_query() {
  local desc="$1"
  local sql="$2"
  local expect_pattern="$3"
  local result
  result=$(psql -h 127.0.0.1 -p "$PORT" -U stratum -d stratum -t -A -c "$sql" 2>&1) || true
  if echo "$result" | grep -qE "$expect_pattern"; then
    echo "  PASS: $desc"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $desc"
    echo "    SQL:      $sql"
    echo "    Expected: $expect_pattern"
    echo "    Got:      $result"
    FAIL=$((FAIL + 1))
  fi
}

echo ""
echo "=== Running queries ==="

# Basic aggregate
run_query "COUNT(*)" \
  "SELECT COUNT(*) FROM demo" \
  "^[0-9]+"

# Literal + aggregate (the core bug fix)
run_query "SELECT 1, COUNT(*)" \
  "SELECT 1, COUNT(*) FROM demo" \
  "1\|"

# Aliased literal + aggregate
run_query "SELECT 1 AS one, COUNT(*) AS n" \
  "SELECT 1 AS one, COUNT(*) AS n FROM demo" \
  "1\|"

# Pure projection
run_query "SELECT 1 FROM demo LIMIT 1" \
  "SELECT 1 FROM demo LIMIT 1" \
  "^1$"

# SUM aggregate
run_query "SUM aggregate" \
  "SELECT SUM(a) FROM demo" \
  "^[0-9]"

# GROUP BY + COUNT
run_query "GROUP BY + COUNT" \
  "SELECT category, COUNT(*) FROM demo GROUP BY category" \
  "[0-9]"

# ORDER BY + LIMIT
run_query "ORDER BY + LIMIT" \
  "SELECT a FROM demo ORDER BY a LIMIT 3" \
  "^[0-9]"

# Empty result
run_query "Empty WHERE" \
  "SELECT COUNT(*) FROM demo WHERE a > 999999999" \
  "^0$"

# SELECT VERSION()
run_query "VERSION()" \
  "SELECT VERSION()" \
  "Stratum"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
