#!/bin/bash
# Run inside the pinned runner image by ./run. It checks that with-route-store-lock ->
# erl -> erlexec -> beam.smp is one PID holding the store lock, and that the lock lasts
# through init:stop/0 shutdown and is released when the BEAM is SIGKILLed.
# Output lines: "ok ...", "FAIL ...", "info ...". It touches only /work (tmpfs).
set -uo pipefail
W=/opt/lock/with-route-store-lock
S=/work/store
T=60

ok() { echo "ok $*"; }
bad() { echo "FAIL $*"; }
info() { echo "info $*"; }
expect() { if [[ $2 == "$3" ]]; then ok "$1"; else bad "$1 (got '$2', want '$3')"; fi; }
lock_key() {
  local ma mi ino
  read -r ma mi ino < <(stat -c '%Hd %Ld %i' -- "$1")
  printf '%02x:%02x:%d' "$ma" "$mi" "$ino"
}
# /proc/locks hides locks whose locker PID is not visible in this PID namespace (the
# short-lived flock(1) helper took it), so read the lock attached to the fd instead.
fd_flock() { grep -cE "^lock:.*FLOCK +ADVISORY +WRITE .* $2 " "/proc/$1/fdinfo/$3" 2>/dev/null; }
fd_holders() { find /proc/[0-9]*/fd -maxdepth 1 -lname "$1" 2>/dev/null | sed -E 's#^/proc/([0-9]+)/fd/.*#\1#' | sort -un | tr '\n' ' ' | sed 's/ $//'; }
holders_named() { local p; for p in $(fd_holders "$S"); do printf '%s(%s) ' "$p" "$(cat "/proc/$p/comm" 2>/dev/null)"; done; }
contend() { "$W" "$S" -- /bin/sh -c 'printf x >"$1/M"' sh "$S" 2>/dev/null; }

mkdir -m 700 "$S" /work/ctl
mkfifo /work/ctl/in /work/ctl/out
exec {I}<>/work/ctl/in {O}<>/work/ctl/out
KEY=$(lock_key "$S")
info "erl $(command -v erl), store $S on $(stat -f -c %T "$S"), key $KEY"

# The BEAM reports its OS pid and what its inherited lock fd points at, then waits for
# one stdin line: "stop" -> init:stop() (orderly shutdown), anything else -> halt.
EVAL='Fd = os:getenv("TELELEM_ROUTE_STORE_LOCK_FD"),
{ok, L} = file:read_link("/proc/self/fd/" ++ Fd),
io:format("ready ~s ~s ~s~n", [os:getpid(), Fd, L]),
case io:get_line("") of "stop\n" -> init:stop(); _ -> erlang:halt(0) end.'

start_beam() {
  "$W" "$S" -- erl -noshell -eval "$EVAL" <&"$I" >&"$O" 2>/work/ctl/err &
  BP=$!
  RPT=
  read -r -t "$T" RPT <&"$O" || RPT=timeout
  read -r tag rpid rfd rlink <<<"$RPT"
  expect "BEAM reported ready" "$tag" ready
  expect "beam.smp OS pid == wrapper PID (exec chain, no fork)" "$rpid" "$BP"
  expect "BEAM's inherited lock fd is the store" "$rlink" "$S"
  expect "process is beam.smp" "$(cat "/proc/$BP/comm")" beam.smp
  info "fd holders while running: $(holders_named)"
  expect "only the BEAM holds the store fd (erl_child_setup did not inherit it)" "$(fd_holders "$S")" "$BP"
  expect "kernel shows the exclusive flock on the BEAM's own fd ($KEY)" "$(fd_flock "$BP" "$KEY" "$rfd")" 1
  contend; expect "second writer rejected while BEAM runs" "$?" 75
}

# Run 1: orderly shutdown with init:stop/0.
start_beam
printf 'stop\n' >&"$I"
wait "$BP"; expect "BEAM exited 0 after init:stop" "$?" 0
expect "no process holds the store fd after BEAM exit" "$(fd_holders "$S")" ""
contend; expect "next writer acquires after orderly stop" "$?" 0
rm -f "$S/M"

# Run 2: SIGKILL.
start_beam
kill -KILL "$BP"; wait "$BP"; expect "BEAM died by SIGKILL" "$?" 137
expect "no process holds the store fd after SIGKILL" "$(fd_holders "$S")" ""
contend; expect "next writer acquires after BEAM crash" "$?" 0
exit 0
