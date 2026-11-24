#!/bin/sh
# Container entry point for scripts/route-store/launch. Runs INSIDE the writer
# environment (runner image, --network none, read-only rootfs, store bind-mounted at
# /sbx/store). Do not run it on the host.
#
#   entry.sh provision|start [main.exs options...]
#
# 1. Compile the five store modules from the read-only checkout sources into the
#    container's private /tmp (before the lock, so no compiler child inherits it).
# 2. exec the lock wrapper on /sbx/store, which execs `elixir`, which execs `erl`,
#    which execs beam.smp: one PID (the container's PID 1) from the wrapper onwards,
#    and that process holds the locked descriptor until it exits.
set -eu

case ${1-} in provision | start) mode=$1 ;; *) echo "route-store: entry.sh: bad mode" >&2; exit 2 ;; esac
shift

mkdir /tmp/ebin
cd /tmp/ebin
elixirc --warnings-as-errors \
  /opt/telelem/src/boundary.ex \
  /opt/telelem/src/plan_digest.ex \
  /opt/telelem/src/file_store.ex \
  /opt/telelem/src/revision.ex \
  /opt/telelem/src/path_policy.ex >&2
cd /

exec /opt/lock/with-route-store-lock /sbx/store -- \
  elixir -pa /tmp/ebin /opt/route-store/main.exs "$mode" --dir /sbx/store "$@"
