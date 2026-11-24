defmodule Telelem.Receipts.StoreTest do
  @moduledoc """
  M2a §2 evidence store: one atomic fixed-key publication per accepted
  record, hard pending bound, sequenced sole-writer evidence, producer and
  store death, retention, invalid data, and authenticated tenant reads.

  Determinism: a suspended store (`:sys.suspend/1`) cannot sweep, so the
  pending table size (`:ets.info/2`) is exactly the number of published,
  unswept records. Synchronous `Store.stats/1`/`lookup/3` sweep first.
  Process deaths use `Process.exit(pid, :kill)` at explicit, acknowledged
  points and wait for `:DOWN`.

  Rerun: scripts/mix test test/telelem/receipts --seed 0 --warnings-as-errors
  """
  use ExUnit.Case, async: true

  alias Telelem.Receipts.{Producer, Receipt, Store, Writer}

  @marker "SYNTHETIC-MARKER-c41d"
  @principal %{
    tenant_id: "local",
    subject_id: "operator",
    mode: :sandbox,
    permissions: MapSet.new([:receipt_read])
  }

  defp ev(request, kind, overrides \\ []) do
    Map.merge(
      %{
        tenant_id: "local",
        request_id: request,
        route_rev: 1,
        attempt: if(kind == :ingress, do: 0, else: 1),
        deployment_id: if(kind == :ingress, do: :none, else: "a"),
        at_ms: 0,
        kind: kind,
        outcome: if(kind == :terminal, do: :done, else: :none),
        exposure: :none
      },
      Map.new(overrides)
    )
  end

  defp start(opts \\ []) do
    store = start_supervised!({Store, Keyword.merge([mode: :sandbox], opts)})
    {store, Store.producer(store)}
  end

  defp open(p, request, tenant \\ "local") do
    {:ok, w} = Writer.open(p, tenant, request)
    w
  end

  # Record and assert the result; returns the advanced writer.
  defp rec(w, result, kind, overrides \\ []) do
    {tenant, request} = w.key
    assert {^result, w} = Writer.record(w, ev(request, kind, [tenant_id: tenant] ++ overrides))
    w
  end

  defp ok(w, kind, overrides \\ []), do: rec(w, :ok, kind, overrides)

  defp pending(p), do: :ets.info(p.table, :size)

  defp lookup(store, request), do: Store.lookup(store, @principal, request)

  # Fill every free pending slot with ingresses of fresh requests.
  defp fill(p, prefix) do
    for i <- 1..p.max_pending, pending(p) < p.max_pending do
      ok(open(p, "#{prefix}#{i}"), :ingress)
    end
  end

  # A coordinator process that runs `steps` (fun(writer) -> writer) one at a
  # time on `:step`, acknowledging each with `{:stepped, n, result}`.
  defp coordinator(p, request) do
    test = self()

    pid =
      spawn(fn ->
        {:ok, w} = Writer.open(p, "local", request)
        send(test, {:opened, self(), w})
        loop(w, test, 1)
      end)

    ref = Process.monitor(pid)
    assert_receive {:opened, ^pid, w}
    {pid, ref, w}
  end

  defp loop(w, test, n) do
    receive do
      {:record, input} ->
        {result, w} = Writer.record(w, input)
        send(test, {:stepped, n, result})
        loop(w, test, n + 1)
    end
  end

  defp step(pid, input) do
    send(pid, {:record, input})
    assert_receive {:stepped, _, result}
    result
  end

  defp req({_, _, %Writer{key: {_, request}}}), do: request

  defp kill({pid, ref, _}) do
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
  end

  describe "atomic publication and pending bound" do
    test "admission is one insert; full pending overflows immediately and recovers" do
      {store, p} = start()
      :ok = :sys.suspend(store)

      results =
        for i <- 1..300 do
          {result, _} = Writer.record(open(p, "r#{i}"), ev("r#{i}", :ingress))
          result
        end

      assert Enum.take(results, 256) |> Enum.all?(&(&1 == :ok))
      assert Enum.drop(results, 256) |> Enum.all?(&(&1 == {:dropped, :overflow}))
      assert pending(p) == 256
      assert %{overflow: 44, loss?: true} = Producer.stats(p)

      :ok = :sys.resume(store)
      assert %{pending: 0, retained: 256, overflow: 44} = Store.stats(store)

      # Admission reopens once swept.
      ok(open(p, "late"), :ingress)
      assert %{pending: 0, retained: 257} = Store.stats(store)
      assert {:error, :not_found} = lookup(store, "r300")
    end

    test "holds exactly under concurrent writers" do
      {store, p} = start()
      :ok = :sys.suspend(store)

      results =
        for t <- 1..32 do
          Task.async(fn ->
            for i <- 1..100 do
              {:ok, w} = Writer.open(p, "local", "t#{t}_#{i}")
              {result, _} = Writer.record(w, ev("t#{t}_#{i}", :ingress))
              result
            end
          end)
        end
        |> Task.await_many(5_000)
        |> List.flatten()

      assert Enum.count(results, &(&1 == :ok)) == 256
      assert Enum.count(results, &(&1 == {:dropped, :overflow})) == 3_200 - 256
      assert pending(p) == 256

      :ok = :sys.resume(store)
      assert %{pending: 0, retained: 256, overflow: 2_944} = Store.stats(store)
    end

    test "failed admission and dead producers strand no slot" do
      {store, p} = start(max_pending: 8)
      :ok = :sys.suspend(store)

      # Eight coordinators publish one record each and die after publication;
      # four more die right after a failed (overflowing) attempt.
      published = for i <- 1..8, do: coordinator(p, "pub#{i}")
      for {pid, _, _} = c <- published, do: assert(step(pid, ev(req(c), :ingress)) == :ok)

      refused = for i <- 1..4, do: coordinator(p, "ref#{i}")

      for {pid, _, _} = c <- refused,
          do: assert(step(pid, ev(req(c), :ingress)) == {:dropped, :overflow})

      Enum.each(published ++ refused, &kill/1)
      assert pending(p) == 8

      :ok = :sys.resume(store)
      assert %{pending: 0, retained: 8, overflow: 4} = Store.stats(store)

      # Full capacity is available again: exactly eight more fit.
      :ok = :sys.suspend(store)
      assert length(fill(p, "again")) == 8
      assert {{:dropped, :overflow}, _} = Writer.record(open(p, "ninth"), ev("ninth", :ingress))
      :ok = :sys.resume(store)
      assert %{pending: 0, retained: 16} = Store.stats(store)
    end

    test "periodic sweep moves pending records without a read" do
      {store, p} = start(sweep_ms: 60_000)
      ok(open(p, "s1"), :ingress)
      assert pending(p) == 1
      send(store, :sweep)
      assert %{retained: 1} = :sys.get_state(store)
      assert pending(p) == 0
    end

    test "the pending table holds whole records, not messages" do
      {store, p} = start()
      :ok = :sys.suspend(store)
      ok(open(p, "m"), :ingress)
      assert {:message_queue_len, 0} = Process.info(store, :message_queue_len)
      assert pending(p) == 1
      :ok = :sys.resume(store)
    end
  end

  describe "coordinator death" do
    test "dies before publishing a terminal: no terminal, never complete" do
      {store, p} = start()
      c = {pid, _, _} = coordinator(p, "c1")
      assert step(pid, ev("c1", :ingress)) == :ok
      assert step(pid, ev("c1", :dispatch, exposure: :possible)) == :ok
      kill(c)

      assert {:ok, r} = lookup(store, "c1")
      assert r.completeness == :incomplete
      assert r.gaps == [:no_terminal]
      assert r.observed_outcome == :not_observed
      assert r.exposure == :unknown
      assert r.usage == :unknown
      assert %{pending: 0} = Store.stats(store)
    end

    test "dies right after atomic publication, before the sweep: the record is kept" do
      {store, p} = start()
      :ok = :sys.suspend(store)
      c = {pid, _, _} = coordinator(p, "c2")
      assert step(pid, ev("c2", :ingress)) == :ok
      assert step(pid, ev("c2", :terminal)) == :ok
      kill(c)
      assert pending(p) == 2

      :ok = :sys.resume(store)

      assert {:ok, %Receipt{completeness: :complete, gaps: [], observed_outcome: :done}} =
               lookup(store, "c2")
    end

    test "dies after a failed publication: the gap stays, no later terminal exists" do
      {store, p} = start(max_pending: 4)
      c = {pid, _, _} = coordinator(p, "c3")
      assert step(pid, ev("c3", :ingress)) == :ok
      Store.stats(store)

      :ok = :sys.suspend(store)
      fill(p, "f")
      assert step(pid, ev("c3", :dispatch)) == {:dropped, :overflow}
      # Marked before record/2 returned, while the store could not run.
      assert Producer.lost?(p, {"local", "c3"})
      kill(c)
      :ok = :sys.resume(store)

      assert {:ok, %{completeness: :incomplete, gaps: [:no_terminal, :loss]}} =
               lookup(store, "c3")
    end

    test "no second process can complete a dead coordinator's evidence" do
      {store, p} = start()
      c = {pid, _, w} = coordinator(p, "c4")
      assert step(pid, ev("c4", :ingress)) == :ok
      kill(c)

      # The dead coordinator's writer cannot be used by another process.
      assert {{:rejected, :not_owner}, ^w} = Writer.record(w, ev("c4", :terminal))

      # A new writer for the same request id is a conflicting second writer.
      w2 = open(p, "c4")
      _ = w2 |> ok(:ingress) |> ok(:terminal)

      assert {:ok, r} = lookup(store, "c4")
      assert r.completeness == :incomplete
      assert :writer_conflict in r.gaps
      assert :duplicate_sequence in r.gaps
      assert r.observed_outcome == :done
      assert r.exposure == :unknown
    end
  end

  describe "sequenced evidence" do
    test "full lifecycle with no loss is complete, in sequence order" do
      {store, p} = start()

      open(p, "r1")
      |> ok(:ingress, at_ms: 1)
      |> ok(:connect_refused, at_ms: 2)
      |> ok(:dispatch, attempt: 2, deployment_id: "b", at_ms: 3, exposure: :possible)
      |> ok(:commit, attempt: 2, deployment_id: "b", at_ms: 4, exposure: :possible)
      |> ok(:terminal, attempt: 2, deployment_id: "b", at_ms: 5, exposure: :possible)

      assert {:ok, r} = lookup(store, "r1")

      assert %Receipt{
               completeness: :complete,
               gaps: [],
               observed_outcome: :done,
               exposure: :possible,
               usage: :unknown
             } = r

      assert Enum.map(r.events, & &1.at_ms) == [1, 2, 3, 4, 5]
      # Sequence metadata is not an event field.
      assert Enum.all?(r.events, &(Map.keys(Map.from_struct(&1)) |> length() == 9))
      assert %{loss?: false} = Store.stats(store)
    end

    test "overflow before a subsequent terminal makes the terminal incomplete" do
      {store, p} = start(max_pending: 4)
      w = open(p, "r1") |> ok(:ingress)
      Store.stats(store)

      :ok = :sys.suspend(store)
      fill(p, "f")
      w = rec(w, {:dropped, :overflow}, :dispatch, exposure: :possible)
      assert Producer.lost?(p, {"local", "r1"})
      :ok = :sys.resume(store)
      Store.stats(store)

      _ = ok(w, :terminal)

      assert {:ok, r} = lookup(store, "r1")
      assert r.completeness == :incomplete
      assert r.gaps == [:missing_sequence, :reported_drop, :loss]
      assert r.observed_outcome == :done
      assert r.exposure == :unknown
      assert Enum.map(r.events, & &1.kind) == [:ingress, :terminal]
    end

    test "a rejected event consumes its sequence number and marks only its receipt" do
      {store, p} = start()
      _ = open(p, "ok") |> ok(:ingress) |> ok(:terminal)

      _ =
        open(p, "bad")
        |> ok(:ingress)
        |> rec({:rejected, {:invalid, :exposure}}, :commit, exposure: :maybe)
        |> rec({:rejected, {:invalid, :request}}, :commit, request_id: "other")
        |> ok(:terminal)

      assert {:ok, %{completeness: :complete}} = lookup(store, "ok")

      assert {:ok, %{gaps: [:missing_sequence, :reported_drop, :loss], exposure: :unknown}} =
               lookup(store, "bad")

      assert {:error, :not_found} = lookup(store, "other")
      assert %{rejected: 2} = Store.stats(store)
    end

    test "a stale writer copy produces a duplicate sequence" do
      {store, p} = start()
      w1 = open(p, "r1") |> ok(:ingress)
      _w2 = ok(w1, :dispatch)
      _ = ok(w1, :terminal)

      assert {:ok, %{gaps: [:duplicate_sequence], observed_outcome: :done}} = lookup(store, "r1")
    end

    test "records after the terminal are exposed and make evidence incomplete" do
      {store, p} = start()
      _ = open(p, "r1") |> ok(:ingress) |> ok(:terminal, outcome: :failed) |> ok(:dispatch)

      assert {:ok, r} = lookup(store, "r1")
      assert r.gaps == [:after_terminal]
      assert r.observed_outcome == :failed
      assert Enum.map(r.events, & &1.kind) == [:ingress, :terminal, :dispatch]
    end

    test "a recorded terminal stays the observed outcome; a second one is a gap" do
      {store, p} = start()

      _ =
        open(p, "r1")
        |> ok(:ingress)
        |> ok(:terminal, outcome: :incomplete)
        |> ok(:terminal, outcome: :done)

      assert {:ok, %{gaps: [:duplicate_terminal, :after_terminal], observed_outcome: :incomplete}} =
               lookup(store, "r1")
    end

    test "never infers success from an absent terminal or a missing ingress" do
      {store, p} = start()
      _ = open(p, "r1") |> ok(:ingress) |> ok(:dispatch, exposure: :possible)
      _ = open(p, "r2") |> ok(:dispatch) |> ok(:terminal)

      assert {:ok, %{gaps: [:no_terminal], observed_outcome: :not_observed, exposure: :unknown}} =
               lookup(store, "r1")

      assert {:ok, %{gaps: [:no_ingress], completeness: :incomplete}} = lookup(store, "r2")
    end

    test "a non-owner process cannot record" do
      {store, p} = start()
      w = open(p, "r1") |> ok(:ingress)

      assert {{:rejected, :not_owner}, ^w} =
               Task.async(fn -> Writer.record(w, ev("r1", :terminal)) end) |> Task.await()

      assert {:ok, %{gaps: [:no_terminal], events: [_]}} = lookup(store, "r1")
    end

    test "a read from another process sees records published before it" do
      {store, p} = start()
      :ok = :sys.suspend(store)
      _ = open(p, "r1") |> ok(:ingress) |> ok(:terminal)
      reader = Task.async(fn -> lookup(store, "r1") end)
      :ok = :sys.resume(store)
      assert {:ok, %{completeness: :complete, events: [_, _]}} = Task.await(reader)
    end
  end

  describe "store death" do
    test "publication to a dead store is explicit loss, never a crash or a wait" do
      {store, p} = start()
      w = open(p, "d1") |> ok(:ingress)
      :ok = stop_supervised(Store)
      refute Process.alive?(store)

      w = rec(w, {:dropped, :store_down}, :dispatch)
      w = rec(w, {:dropped, :store_down}, :terminal)
      assert w.next_seq == 4 and w.dropped == 2

      assert %{store_down: 2, overflow: 0, loss?: true} = Producer.stats(p)
      assert Producer.lost?(p, {"local", "d1"})
    end

    test "evidence is not durable: a store's records die with it" do
      {_store, p} = start()
      _ = open(p, "d1") |> ok(:ingress) |> ok(:terminal)
      :ok = stop_supervised(Store)

      {store2, _p2} = start()
      assert {:error, :not_found} = lookup(store2, "d1")
      assert {{:dropped, :store_down}, _} = Writer.record(open(p, "d2"), ev("d2", :ingress))
    end
  end

  describe "retention limit" do
    test "defaults keep 1,024 records and count each eviction" do
      {store, p} = start()

      for batch <- Enum.chunk_every(1..1_100, 200) do
        for i <- batch, do: ok(open(p, "e#{i}"), :ingress)
        Store.stats(store)
      end

      # Ordinary retention is counted but is not loss.
      assert %{retained: 1_024, receipts: 1_024, evicted: 76, overflow: 0, loss?: false} =
               Store.stats(store)

      assert {:error, :not_found} = lookup(store, "e76")
      assert {:ok, %Receipt{}} = lookup(store, "e77")
    end

    # Regression (cold review of ca5427c): eviction used to set shared,
    # never-cleared hashed loss slots, so with default limits sustained
    # lossless traffic eventually left no new receipt complete.
    test "sustained lossless traffic at default limits keeps fresh receipts complete" do
      {store, p} = start()
      lifecycle = [ingress: [], dispatch: [exposure: :possible], commit: [], terminal: []]

      for n <- 1..6_000 do
        Enum.reduce(lifecycle, open(p, "s#{n}"), fn {kind, o}, w -> ok(w, kind, o) end)
        # Sweep each request so pending never overflows; isolates retention.
        Store.stats(store)

        if rem(n, 1_000) == 0 do
          # 1,024 retained / 4 records: the newest 256 are fully retained.
          for i <- (n - 255)..n do
            assert {:ok, %Receipt{completeness: :complete, gaps: [], exposure: :possible}} =
                     lookup(store, "s#{i}")
          end

          assert {:error, :not_found} = lookup(store, "s#{n - 256}")
        end
      end

      assert %{
               retained: 1_024,
               receipts: 256,
               evicted: 22_976,
               overflow: 0,
               store_down: 0,
               rejected: 0,
               loss?: false
             } = Store.stats(store)
    end

    test "eviction marks only its own receipt, even when every loss slot collides" do
      # One shared loss slot: any hashed mark would hit every receipt.
      {store, p} = start(max_retained: 4, loss_slots: 1)
      _ = open(p, "old") |> ok(:ingress) |> ok(:terminal)
      Store.stats(store)
      _ = open(p, "new") |> ok(:ingress) |> ok(:dispatch) |> ok(:terminal)

      assert %{evicted: 1, loss?: false} = Store.stats(store)
      assert {:ok, %{gaps: [:missing_sequence, :no_ingress, :evicted]}} = lookup(store, "old")
      assert {:ok, %{completeness: :complete, gaps: []}} = lookup(store, "new")
    end

    test "actual overflow still marks a colliding receipt (shared-slot limitation)" do
      {store, p} = start(max_pending: 1, loss_slots: 1)
      w = open(p, "victim") |> ok(:ingress)
      Store.stats(store)
      :ok = :sys.suspend(store)
      fill(p, "f")

      assert {{:dropped, :overflow}, _} =
               Writer.record(open(p, "dropped"), ev("dropped", :ingress))

      :ok = :sys.resume(store)
      Store.stats(store)
      _ = ok(w, :terminal)

      assert %{overflow: 1, loss?: true} = Store.stats(store)
      # Conservative false positive: never more complete, possibly less.
      assert {:ok, %{completeness: :incomplete, gaps: [:loss]}} = lookup(store, "victim")
    end

    test "partial eviction, then a late record from the same live writer" do
      {store, p} = start(max_retained: 4)
      # Publication order: part.ingress, other.ingress, other.dispatch,
      # part.dispatch, other.terminal; the fifth evicts part's ingress, and the
      # late terminal later evicts other's ingress (not part's dispatch).
      w = open(p, "part") |> ok(:ingress)
      Store.stats(store)
      o = open(p, "other") |> ok(:ingress) |> ok(:dispatch)
      Store.stats(store)
      w = ok(w, :dispatch)
      Store.stats(store)
      _ = ok(o, :terminal)

      assert %{evicted: 1} = Store.stats(store)

      assert {:ok, %{gaps: [:missing_sequence, :no_ingress, :no_terminal, :evicted]}} =
               lookup(store, "part")

      _ = ok(w, :terminal)

      assert {:ok, r} = lookup(store, "part")
      assert r.gaps == [:missing_sequence, :no_ingress, :evicted]
      assert r.completeness == :incomplete
      assert r.observed_outcome == :done
      assert r.exposure == :unknown
      assert Enum.map(r.events, & &1.kind) == [:dispatch, :terminal]
      refute Producer.lost?(p, {"local", "part"})
    end

    test "full eviction is not found; a late record from the same live writer is not complete" do
      {store, p} = start(max_retained: 4)
      w = open(p, "full") |> ok(:ingress) |> ok(:dispatch)
      Store.stats(store)
      _ = open(p, "other") |> ok(:ingress) |> ok(:dispatch) |> ok(:commit) |> ok(:terminal)

      assert %{evicted: 2, retained: 4, loss?: false} = Store.stats(store)
      assert {:error, :not_found} = lookup(store, "full")

      _ = ok(w, :terminal)

      assert {:ok, r} = lookup(store, "full")
      assert r.gaps == [:missing_sequence, :no_ingress]
      assert r.completeness == :incomplete
      assert r.observed_outcome == :done
      assert r.exposure == :unknown
      assert [%{kind: :terminal}] = r.events
      # The late record evicted other's oldest record: entry-local, no loss.
      assert {:ok, %{gaps: [:missing_sequence, :no_ingress, :evicted]}} = lookup(store, "other")
      assert %{loss?: false} = Store.stats(store)
    end

    test "evicting part of a receipt makes it incomplete and keeps its terminal" do
      {store, p} = start(max_retained: 4)
      _ = open(p, "old") |> ok(:ingress) |> ok(:terminal, outcome: :failed)
      Store.stats(store)
      _ = open(p, "new") |> ok(:ingress) |> ok(:dispatch) |> ok(:terminal)

      assert %{retained: 4, evicted: 1} = Store.stats(store)
      assert {:ok, old} = lookup(store, "old")
      assert old.gaps == [:missing_sequence, :no_ingress, :evicted]
      assert old.completeness == :incomplete
      assert old.observed_outcome == :failed
      assert old.exposure == :unknown
      assert [%{kind: :terminal}] = old.events
      assert {:ok, %Receipt{completeness: :complete}} = lookup(store, "new")
    end
  end

  describe "invalid data" do
    test "is never retained, is counted, and leaves no marker in state or replies" do
      {store, p} = start(max_event_bytes: 64)
      w = open(p, "m1")

      inputs = [
        Map.put(ev("m1", :ingress), :body, @marker),
        %{ev("m1", :ingress) | deployment_id: "Bearer " <> @marker},
        # valid shape, but the canonical encoding exceeds 64 bytes
        ev("m1", :ingress, at_ms: 1_000_000_000_000_000_000),
        [authorization: @marker],
        @marker
      ]

      w =
        Enum.reduce(inputs, w, fn input, w ->
          assert {{:rejected, {:invalid, _}}, w} = Writer.record(w, input)
          w
        end)

      assert {:error, {:invalid, :request_id}} = Writer.open(p, "local", @marker <> "/x")
      assert {:error, {:invalid, :tenant_id}} = Writer.open(p, @marker <> "/x", "r")

      stats = Store.stats(store)
      assert %{retained: 0, receipts: 0, rejected: 5, pending: 0, loss?: true} = stats

      for probe <- [stats, w, :sys.get_state(store), :sys.get_status(store)],
          do: refute(inspect(probe, limit: :infinity, printable_limit: :infinity) =~ @marker)

      assert {:error, :not_found} = lookup(store, "m1")
    end

    test "malformed rows written straight to the public table are discarded" do
      {store, p} = start()
      :ok = :sys.suspend(store)
      {:ok, e} = Telelem.Receipts.Event.new(ev("forged", :terminal))
      true = :ets.insert_new(p.table, {1, 1, {"local", "forged"}, self(), 0, 0, e})
      true = :ets.insert_new(p.table, {2, 2, {"local", "other"}, self(), 1, 0, e})
      :ok = :sys.resume(store)

      assert %{retained: 0, rejected: 2} = Store.stats(store)
      assert {:error, :not_found} = lookup(store, "forged")
    end
  end

  describe "authenticated tenant reads" do
    test "only the authenticated tenant's request is readable" do
      {store, p} = start()
      _ = open(p, "r1") |> ok(:ingress)
      _ = open(p, "r1", "other") |> ok(:ingress, at_ms: 99)
      _ = open(p, "r2", "other") |> ok(:ingress)

      assert {:ok, %{tenant_id: "local", events: [%{tenant_id: "local", at_ms: 0}]}} =
               lookup(store, "r1")

      # Another tenant's request is indistinguishable from a missing one.
      assert lookup(store, "r2") == {:error, :not_found}
      assert lookup(store, "never") == {:error, :not_found}

      assert Store.lookup(store, %{@principal | tenant_id: "third"}, "r1") ==
               {:error, :not_found}

      assert Store.lookup(store, @principal, %{tenant_id: "other", request_id: "r2"}) ==
               {:error, :not_found}
    end

    test "requires receipt_read, matching mode and a well-formed principal" do
      {store, p} = start()
      _ = open(p, "r1") |> ok(:ingress) |> ok(:terminal)

      assert {:ok, _} = Store.lookup(store, %{@principal | permissions: [:receipt_read]}, "r1")

      for perms <- [MapSet.new(), MapSet.new([:route_read, :dispatch]), [:route_read], nil] do
        assert Store.lookup(store, %{@principal | permissions: perms}, "r1") ==
                 {:error, {:denied, :missing_permission}}
      end

      # Denial does not depend on whether the request exists.
      assert Store.lookup(store, %{@principal | permissions: MapSet.new()}, "never") ==
               {:error, {:denied, :missing_permission}}

      assert Store.lookup(store, %{@principal | mode: :serving}, "r1") ==
               {:error, {:denied, :mode_mismatch}}

      for bad <- [
            %{},
            nil,
            "local",
            Map.delete(@principal, :subject_id),
            %{@principal | tenant_id: "a/b"},
            %{@principal | tenant_id: :local},
            %{@principal | mode: :admin},
            Map.to_list(@principal)
          ],
          do: assert(Store.lookup(store, bad, "r1") == {:error, {:denied, :unauthenticated}})
    end

    test "a serving store serves serving principals only" do
      {store, p} = start(mode: :serving)
      _ = open(p, "r1") |> ok(:ingress) |> ok(:terminal)
      serving = %{@principal | mode: :serving}

      assert {:ok, %{completeness: :complete}} = Store.lookup(store, serving, "r1")
      assert {:error, {:denied, :mode_mismatch}} = Store.lookup(store, @principal, "r1")
    end

    test "the store refuses to start without an explicit mode" do
      Process.flag(:trap_exit, true)
      assert {:error, {%ArgumentError{}, _}} = Store.start_link([])
      assert {:error, {%ArgumentError{}, _}} = Store.start_link(mode: :admin)
    end
  end
end
