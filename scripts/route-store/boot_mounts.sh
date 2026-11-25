# shellcheck shell=sh
# Mount boundary check for `entry.sh boot-validated`, run BEFORE any BEAM (the
# compiler included). Sourced by entry.sh; test/route_store/boot_check_test.exs sources
# it too and runs it on the same mountinfo inputs as RouteStore.BootCheck.mounts/1.
#
# It is the same policy, with the same codes, as BootCheck.mounts/1 (boot_check.ex),
# which main.exs still runs inside the VM as the writer's runtime recheck. The test
# checks that the two agree: the tables below must equal BootCheck's (rs_mounts_table),
# and both must give the same verdict on every mountinfo case in the test.
#
# Only shell builtins run here (read, case, set, [, printf), so no program from the
# container's filesystem runs before the mount boundary is known. Mounts cannot change
# after the check: the container has no capabilities, and every mount must show no
# propagation tag (no shared:/master:/propagate_from:/unbindable optional field), so
# no later host mount event reaches it.
#
#   rs_mounts FILE   0 if FILE (a /proc/PID/mountinfo) passes; otherwise 1 with
#                    RS_MOUNTS_CODE set to one code from BootCheck's closed set.
#
# Policy (in this order; the first failure decides):
#   1. each non-empty line: no empty field (single spaces only), at least 6 fields,
#      field 7 is the "-" separator (else mount_propagation if " - " comes later,
#      mount_unexpected if there is none), and no backslash escape in the mount point;
#      any other shape is mountinfo_unreadable. No lines at all: mountinfo_unreadable.
#   2. mount points are unique                                       (mount_unexpected)
#   3. "/" is mounted read-only                                  (mount_rootfs_writable)
#   4. every mount is known: "/", an application mount (RS_APP_RO/RS_APP_RW), a standard
#      mount (RS_STD_ANY any mode, RS_STD_RO read-only), /sys or below read-only, or
#      below /proc read-only or bound from "/null"                   (mount_unexpected)
#   5. every application mount is present                      (mount_app_set_mismatch)
#   6. RS_APP_RO mounts are read-only, RS_APP_RW mounts writable         (mount_app_mode)

RS_APP_RO='/opt/telelem/src/boundary.ex /opt/telelem/src/plan_digest.ex
/opt/telelem/src/file_store.ex /opt/telelem/src/revision.ex
/opt/telelem/src/path_policy.ex /opt/telelem/src/id.ex /opt/telelem/src/bindings.ex
/opt/telelem/src/verified_target.ex /opt/telelem/src/bootstrap.ex
/opt/route-store /opt/lock/with-route-store-lock /sbx/boot'
RS_APP_RW='/sbx/store'
RS_STD_ANY='/proc /dev /dev/pts /dev/mqueue /dev/shm /tmp'
RS_STD_RO='/etc/hostname /etc/hosts /etc/resolv.conf'
# The default field separators (space, tab, newline), restored after each split.
RS_IFS=$(printf ' \t\nx')
RS_IFS=${RS_IFS%x}

# rs_mounts_table: the tables, one "KIND POINT" line each (for the parity test).
rs_mounts_table() {
  for _p in $RS_APP_RO; do printf 'app_ro %s\n' "$_p"; done
  for _p in $RS_APP_RW; do printf 'app_rw %s\n' "$_p"; done
  for _p in $RS_STD_ANY; do printf 'std_any %s\n' "$_p"; done
  for _p in $RS_STD_RO; do printf 'std_ro %s\n' "$_p"; done
}

# rs_in WORD LIST: WORD is one of the whitespace-separated words of LIST (literal match).
rs_in() {
  for _w in $2; do [ "$_w" = "$1" ] && return 0; done
  return 1
}

# rs_known POINT RW(0|1) ROOT
rs_known() {
  [ "$1" = / ] && return 0
  rs_in "$1" "$RS_APP_RO $RS_APP_RW" && return 0
  rs_in "$1" "$RS_STD_ANY" && return 0
  rs_in "$1" "$RS_STD_RO" && { [ "$2" = 0 ]; return; }
  case $1 in
    /sys | /sys/*) [ "$2" = 0 ]; return ;;
    /proc/*) [ "$2" = 0 ] || [ "$3" = /null ]; return ;;
  esac
  return 1
}

rs_mounts() {
  RS_MOUNTS_CODE=
  IFS=$RS_IFS
  _parse= _n=0 _seen=' ' _dup=0 _root=missing _unknown=0 _app=' '
  if ! { [ -f "$1" ] && [ -r "$1" ]; }; then
    RS_MOUNTS_CODE=mountinfo_unreadable
    return 1
  fi
  set -f
  while IFS= read -r _l || [ -n "$_l" ]; do
    [ -n "$_l" ] || continue
    case $_l in
      ' '* | *' ' | *'  '*) _parse=mountinfo_unreadable; break ;;
    esac
    IFS=' '
    # shellcheck disable=SC2086 # split on single spaces, globbing is off (set -f)
    set -- $_l
    IFS=$RS_IFS
    if [ $# -lt 6 ]; then _parse=mountinfo_unreadable; break; fi
    if [ "${7-}" != - ]; then
      case $_l in *' - '*) _parse=mount_propagation ;; *) _parse=mount_unexpected ;; esac
      break
    fi
    case $5 in *\\*) _parse=mount_unexpected; break ;; esac
    _pt=$5
    case ",$6," in *,rw,*) _rw=1 ;; *) _rw=0 ;; esac
    _n=$((_n + 1))
    case $_seen in *" $_pt "*) _dup=1 ;; esac
    _seen="$_seen$_pt "
    [ "$_pt" = / ] && _root=$_rw
    rs_known "$_pt" "$_rw" "$4" || _unknown=1
    case $_pt in /opt | /opt/* | /sbx | /sbx/*) _app="$_app$_pt:$_rw " ;; esac
  done 2>/dev/null <"$1"
  IFS=$RS_IFS
  set +f

  if [ -n "$_parse" ]; then RS_MOUNTS_CODE=$_parse
  elif [ "$_n" = 0 ]; then RS_MOUNTS_CODE=mountinfo_unreadable
  elif [ "$_dup" = 1 ]; then RS_MOUNTS_CODE=mount_unexpected
  elif [ "$_root" != 0 ]; then RS_MOUNTS_CODE=mount_rootfs_writable
  elif [ "$_unknown" = 1 ]; then RS_MOUNTS_CODE=mount_unexpected
  else
    for _p in $RS_APP_RO $RS_APP_RW; do
      case $_app in *" $_p:"[01]" "*) ;; *) RS_MOUNTS_CODE=mount_app_set_mismatch; return 1 ;; esac
    done
    for _p in $RS_APP_RO; do
      case $_app in *" $_p:0 "*) ;; *) RS_MOUNTS_CODE=mount_app_mode; return 1 ;; esac
    done
    for _p in $RS_APP_RW; do
      case $_app in *" $_p:1 "*) ;; *) RS_MOUNTS_CODE=mount_app_mode; return 1 ;; esac
    done
    return 0
  fi
  return 1
}
