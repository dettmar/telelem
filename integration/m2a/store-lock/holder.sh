#!/bin/bash
# Stand-in foreground writer for the store-lock tests. Run it only through
# scripts/with-route-store-lock, which execs it.
#
#   holder.sh CTL_DIR EXIT_CODE MODE
#
# CTL_DIR holds two FIFOs that the test opens read-write: `ready` (holder -> test) and
# `release` (test -> holder). Both sides open them O_RDWR, so neither open blocks.
#
# MODE
#   plain  report, then wait for one release line, then exit EXIT_CODE
#   term   as plain; on SIGTERM report "stopping", wait for another release line, exit
#   child  start a background child that inherits the lock fd and waits on
#          CTL_DIR/child-release; report "<own pid> <child pid>", then wait like plain
#   close  close the inherited lock fd (contract violation), report, wait like plain
# Writes nothing outside CTL_DIR.
set -uo pipefail
ctl=$1 code=$2 mode=$3
exec {out}<>"$ctl/ready" {in}<>"$ctl/release"

fd=${TELELEM_ROUTE_STORE_LOCK_FD:-none}
dir=${TELELEM_ROUTE_STORE_LOCK_DIR:-none}
flags=none
[[ -r /proc/$$/fdinfo/$fd ]] && read -r _ flags < <(grep '^flags:' "/proc/$$/fdinfo/$fd")

case $mode in
  plain) ;;
  term) trap 'printf "stopping %s\n" "$$" >&"$out"; read -r _ <&"$in"; exit "$code"' TERM ;;
  child)
    exec {cin}<>"$ctl/child-release"
    bash -c 'read -r _ <&"$1"' child "$cin" &
    printf 'child %s\n' "$!" >&"$out"
    ;;
  close)
    exec {fd}<&-
    ;;
  *) exit 99 ;;
esac

printf 'ready %s %s %s %s\n' "$$" "$fd" "$flags" "$dir" >&"$out"
read -r _ <&"$in"
exit "$code"
