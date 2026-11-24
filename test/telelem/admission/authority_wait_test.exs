defmodule Telelem.Admission.AuthorityWaitTest do
  @moduledoc """
  TEL-012 slice A: `Authority.reserve/5` and `authorize/3` take a caller
  wait. A timeout ends the caller's wait only; it does not cancel or roll
  back a request already queued at the authority.

  Listener-free: one `Telelem.Instance` with no workers (the transport is
  never started), owners are plain spawned processes, no network.

  Ordering is deterministic. The authority is held with `:sys.suspend/1`,
  so a call to it cannot be answered and its caller wait must expire. The
  owner then inspects the authority's mailbox and makes its next call
  itself, so both are ordered after its timed-out request by the per-pair
  signal ordering guarantee. Owner departure is observed through a
  monitor and the authority's `:owner_down` observation. Receive timeouts
  only detect failure; there are no sleeps.

      scripts/mix test test/telelem/admission/authority_wait_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.Admission.{Authority, Ledger}
  alias Telelem.Instance
  alias Telelem.Instance.Directory

  @moduletag :tmp_dir
  @wait 1_000
  # Caller wait for the timed-out calls. The authority is suspended, so this
  # expires whatever its value.
  @short 10

  setup %{tmp_dir: dir} do
    sup =
      start_supervised!({
        Instance,
        # No worker is started, so no transport is ever opened.
        id: :wait_gw,
        limits: %{slots: 2, units: 10},
        spool_dir: Path.join(dir, "spool"),
        transport: {:unused_no_workers, nil},
        reserve_ttl: nil
      })

    inst = Instance.handle(sup)
    :ok = Instance.subscribe(inst)
    auth = Instance.authority(inst)

    on_exit(fn ->
      # Never leave a suspended authority behind if an assertion failed.
      if Process.alive?(auth), do: :sys.resume(auth)
    end)

    %{inst: inst, auth: auth}
  end

  test "default-wait arities stay; explicit-wait arities exist" do
    for {f, a} <- [reserve: 4, reserve: 5, authorize: 2, authorize: 3],
        do: assert(function_exported?(Authority, f, a), "#{f}/#{a}")
  end

  # Passed through the context so the type checker does not reject the call.
  @tag bad_waits: [-1, 1.5, :soon]
  test "an invalid wait is rejected before anything is sent", %{auth: auth, bad_waits: bad} do
    for w <- bad do
      assert_raise FunctionClauseError, fn -> Authority.reserve(auth, "op", self(), 1, w) end
      assert_raise FunctionClauseError, fn -> Authority.authorize(auth, :gid, w) end
    end

    assert Authority.snapshot(auth).grants == %{}
  end

  test "authorize/3 timeout: queued dispatch completes after resume; owner departure leaves it unresolved, units encumbered",
       %{inst: inst, auth: auth} do
    test_pid = self()
    tag = inst.tag

    owner =
      spawn_link(fn ->
        {:ok, gid} = Authority.reserve(auth, "op-1", self(), 4, @wait)
        send(test_pid, {:reserved, self(), gid})
        receive do: (:authorize -> :ok)

        exit_reason =
          try do
            {:returned, Authority.authorize(auth, gid, @short)}
          catch
            :exit, reason -> reason
          end

        # Ordered after the authorize request (same sender, same receiver).
        {:messages, queued} = Process.info(auth, :messages)
        send(test_pid, {:timed_out, self(), exit_reason, queued})

        # Also ordered after it: answered only once the authority resumes and
        # has applied the queued authorize.
        snap = Authority.snapshot(auth)
        {:messages, own} = Process.info(self(), :messages)
        send(test_pid, {:after_resume, self(), snap, own})

        receive do: (:leave -> :ok)
      end)

    ref = Process.monitor(owner)
    assert_receive {:reserved, ^owner, gid}, @wait
    assert Authority.snapshot(auth).grants[gid].state == :reserved

    :ok = :sys.suspend(auth)
    send(owner, :authorize)

    assert_receive {:timed_out, ^owner, exit_reason, queued}, @wait
    assert {:timeout, {GenServer, :call, [^auth, {:authorize, ^gid}, @short]}} = exit_reason

    # The request is queued at the authority, and the timeout changed nothing:
    # the committed ledger still holds the grant as reserved.
    assert [{:"$gen_call", {^owner, _}, {:authorize, ^gid}}] = queued
    l = Directory.ledger(inst.table)
    assert Ledger.grant(l, gid).state == :reserved
    assert %{active: 1, reserved_units: 4, encumbered_units: 0} = Ledger.snapshot(l)

    # The owner is still alive after its wait expired.
    assert Process.alive?(owner)
    :ok = :sys.resume(auth)

    assert_receive {:after_resume, ^owner, snap, own}, @wait
    assert snap.grants[gid].state == :dispatched
    assert snap.grants[gid].owner == owner

    assert %{active: 1, reserved_units: 0, encumbered_units: 4, available_units: 6} =
             snap

    # The late authorize reply was discarded: it is not in the owner's mailbox
    # although the authority sent it before the snapshot reply.
    assert own == []

    # Owner departure: dispatched -> unresolved. The slot is freed, but the
    # units stay encumbered instead of being released.
    send(owner, :leave)
    assert_receive {:DOWN, ^ref, :process, ^owner, :normal}, @wait
    assert_receive {:telelem_admission, ^tag, {:owner_down, ^owner, [{^gid, {:ok, ^gid}}]}}, @wait

    snap = Authority.snapshot(auth)
    assert snap.grants[gid].state == :unresolved

    assert %{
             active: 0,
             available_slots: 2,
             reserved_units: 0,
             encumbered_units: 4,
             spent_units: 0,
             available_units: 6
           } = snap

    # A second authorize does not revive it.
    assert {:error, {:not_reserved, :unresolved}} = Authority.authorize(auth, gid)
  end

  test "reserve/5 timeout: the queued reservation is applied after resume and found again by op id",
       %{auth: auth} do
    test_pid = self()
    # Suspend first, so the owner's reserve cannot be answered.
    :ok = :sys.suspend(auth)

    owner =
      spawn_link(fn ->
        exit_reason =
          try do
            {:returned, Authority.reserve(auth, "op-2", self(), 3, @short)}
          catch
            :exit, reason -> reason
          end

        {:messages, queued} = Process.info(auth, :messages)
        send(test_pid, {:timed_out, self(), exit_reason, queued})

        # Ordered after the queued reserve: idempotent retry by op id.
        retry = Authority.reserve(auth, "op-2", self(), 3)
        {:messages, own} = Process.info(self(), :messages)
        send(test_pid, {:after_resume, self(), retry, own})

        receive do: (:leave -> :ok)
      end)

    assert_receive {:timed_out, ^owner, exit_reason, queued}, @wait

    assert {:timeout, {GenServer, :call, [^auth, {:reserve, "op-2", ^owner, 3}, @short]}} =
             exit_reason

    assert [{:"$gen_call", {^owner, _}, {:reserve, "op-2", ^owner, 3}}] = queued
    assert Process.alive?(owner)
    :ok = :sys.resume(auth)

    assert_receive {:after_resume, ^owner, {:duplicate, gid}, []}, @wait
    snap = Authority.snapshot(auth)
    assert snap.grants[gid].state == :reserved
    assert %{active: 1, reserved_units: 3, available_units: 7} = snap

    send(owner, :leave)
  end
end
