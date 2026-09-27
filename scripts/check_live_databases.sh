#!/usr/bin/env bash
set -euo pipefail

: "${KYMO_LIVE_TEST_DATABASE_URL:?Set a throwaway PostgreSQL test URL}"
: "${KYMO_LIVE_TEST_CLICKHOUSE_URL:?Set a throwaway ClickHouse test URL}"

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd -- "$script_dir/.."
test_groups=(
  pg::live_pg_tests::
  clickhouse::registry_outbox_tests::
  cdn_gc::tests::
  deletion::tests::
  ingest::live_tests::
)
test_list=$(cargo test --locked --package kymo-server --lib -- --ignored --list "${test_groups[@]}")
for group in "${test_groups[@]}"; do
  if ! grep -q "^${group}.*: test$" <<< "$test_list"; then
    echo "No ignored live tests matched $group; update the local check." >&2
    exit 1
  fi
done
if ! cargo test --locked --package kymo-server --lib -- --ignored --nocapture "${test_groups[@]}"; then
  echo 'Live database checks failed. Recreate both throwaway databases before retrying.' >&2
  exit 1
fi
