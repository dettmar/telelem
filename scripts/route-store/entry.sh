#!/bin/sh
# Container entry point for scripts/route-store/launch. Runs INSIDE the writer
# environment (runner image, --network none, read-only rootfs, store bind-mounted at
# /sbx/store). Do not run it on the host.
#
#   entry.sh provision|start|boot-validated [main.exs options...]
#
# 1. Compile the five store modules from the read-only checkout sources into the
#    container's private /tmp (before the lock, so no compiler child inherits it).
# 2. exec the lock wrapper on /sbx/store, which execs `elixir`, which execs `erl`,
#    which execs beam.smp: one PID (the container's PID 1) from the wrapper onwards,
#    and that process holds the locked descriptor until it exits.
#
# boot-validated adds, before step 1 (that is, before any BEAM, the compiler included):
#   - the environment must be exactly the fixed `env -i` set the launcher passes;
#   - the resolver file must be exactly the pinned bytes (size and SHA-256 below), at
#     the path ERL_INETRC names.
# It also compiles the four sandbox modules and boot_check.ex, discards compiler
# output and prints only fixed "route-store:" lines. Everything after the exec is the
# same path as `start`; main.exs then re-checks env, resolver and mounts in the VM.
set -eu

case ${1-} in
  provision | start | boot-validated) mode=$1 ;;
  *) echo "route-store: entry.sh: bad mode" >&2; exit 2 ;;
esac
shift

if [ "$mode" = boot-validated ]; then
  boot_refuse() { echo "route-store: boot refused: $1" >&2; exit 5; }
  # Exact environment (names and values) before anything else runs. A value with a
  # newline adds a line and so also fails.
  want_env='ERL_CRASH_DUMP_SECONDS=0
ERL_INETRC=/opt/route-store/inetrc
HOME=/tmp
LANG=C.UTF-8
PATH=/usr/local/bin:/usr/bin:/bin
PWD=/'
  [ "$(/usr/bin/env | LC_ALL=C /usr/bin/sort)" = "$want_env" ] || boot_refuse entry_env_unexpected
  # Pinned resolver bytes: hosts-file lookup only (scripts/route-store/inetrc).
  inetrc_size=181
  inetrc_sha256=37ff36d2a9872d2d3c535e46ee638f98796f8da6281893826537b32f9f8704a8
  [ -f "$ERL_INETRC" ] && [ ! -L "$ERL_INETRC" ] || boot_refuse entry_resolver_unpinned
  [ "$(LC_ALL=C /usr/bin/stat -c %s -- "$ERL_INETRC")" = "$inetrc_size" ] || boot_refuse entry_resolver_unpinned
  got=$(/usr/bin/sha256sum <"$ERL_INETRC") || boot_refuse entry_resolver_unpinned
  [ "${got%% *}" = "$inetrc_sha256" ] || boot_refuse entry_resolver_unpinned
fi

mkdir /tmp/ebin
cd /tmp/ebin
if [ "$mode" = boot-validated ]; then
  elixirc --warnings-as-errors \
    /opt/telelem/src/boundary.ex \
    /opt/telelem/src/plan_digest.ex \
    /opt/telelem/src/file_store.ex \
    /opt/telelem/src/revision.ex \
    /opt/telelem/src/path_policy.ex \
    /opt/telelem/src/id.ex \
    /opt/telelem/src/bindings.ex \
    /opt/telelem/src/verified_target.ex \
    /opt/telelem/src/bootstrap.ex \
    /opt/route-store/boot_check.ex >/dev/null 2>&1 || boot_refuse compile_failed
else
  elixirc --warnings-as-errors \
    /opt/telelem/src/boundary.ex \
    /opt/telelem/src/plan_digest.ex \
    /opt/telelem/src/file_store.ex \
    /opt/telelem/src/revision.ex \
    /opt/telelem/src/path_policy.ex >&2
fi
cd /

exec /opt/lock/with-route-store-lock /sbx/store -- \
  elixir -pa /tmp/ebin /opt/route-store/main.exs "$mode" --dir /sbx/store "$@"
