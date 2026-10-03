#!/bin/sh
# Initialize, start, query and stop an extracted PostgreSQL tree the way local mode uses it, including the PL/pgSQL and Snowball modules it loads. Given a previous tree, that build creates and fills the cluster first, as the build an installation replaces would have. Needs only POSIX tools, so it runs in bare distribution images; PostgreSQL refuses to run as root.
# Usage: smoke.sh <tree> [<previous tree>]
set -eu
tree=$(cd "$1" && pwd)
previous=$(cd "${2:-$1}" && pwd)
work=$(mktemp -d)
trap '"$tree/bin/pg_ctl" -D "$work/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$work"' EXIT
# A loopback port, not a socket: macOS temporary paths can exceed the socket path limit.
port=$((20000 + $$ % 10000))
start() {
    "$1/bin/pg_ctl" -D "$work/data" -l "$work/log" -w \
        -o "-c listen_addresses=127.0.0.1 -c port=$port -c unix_socket_directories=" start >/dev/null || {
        cat "$work/log" >&2
        exit 1
    }
}
query() {
    "$1/bin/psql" -h 127.0.0.1 -p "$port" -U kymo -d smoke -XqAtc "$2"
}

"$tree/bin/postgres" --version
"$previous/bin/initdb" -D "$work/data" -U kymo --auth-local=trust --auth-host=trust --encoding=UTF8 --no-locale >/dev/null
start "$previous"
"$previous/bin/createdb" -h 127.0.0.1 -p "$port" -U kymo smoke
query "$previous" "CREATE TABLE t (v text); INSERT INTO t VALUES ('runs')"
"$previous/bin/pg_ctl" -D "$work/data" -m fast -w stop >/dev/null
# The launcher's check before it records a build for an existing cluster.
"$tree/bin/postgres" -C data_checksums -D "$work/data" >/dev/null
start "$tree"
result=$(query "$tree" "DO \$\$ BEGIN INSERT INTO t VALUES ('jumps'); END \$\$; SELECT string_agg(to_tsvector('english', v)::text, ' ' ORDER BY v) FROM t")
[ "$result" = "'jump':1 'run':1" ] || {
    echo "unexpected query result: $result" >&2
    exit 1
}
"$tree/bin/pg_ctl" -D "$work/data" -m fast -w stop >/dev/null
echo "smoke test passed"
