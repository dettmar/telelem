defmodule Telelem.Receipts.StoreTest do
  @moduledoc """
  M2a §2 evidence store: hard pending admission bound before the
  asynchronous enqueue, retention limit, invalid data never retained,
  fixed-size loss indicator, conservative completeness and tenant-scoped
  reads.

  A suspended store (`:sys.suspend/1`) cannot drain its mailbox, so the
  mailbox length is the number of enqueued event messages. Synchronous
  `Store.stats/1` from the producing process orders after that process's
  earlier sends.

  Rerun: scripts/mix test test/telelem/receipts --seed 0 --warnings-as-errors
  """
  use ExUnit.Case, async: true

  alias Telelem.Receipts.{Producer, Receipt, Store}

  @marker "SYNTHETIC-MARKER-c41d"
  @principal %{
    tenant_id: "local",
    subject_id: "operator",
    mode: :sandbox,
    permissions: MapSet.new()
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
    store = start_supervised!({Store, opts})
    {store, Store.producer(store)}
  end

  defp queue_len(pid), do: elem(Process.info(pid, :message_queue_len), 1)

  describe "pending admission bound" do
    test "is enforced before enqueue and the producer never waits" do
      {store, p} = start()
      :ok = :sys.suspend(store)

      results = for i <- 1..300, do: Producer.record(p, ev("r#{i}", :ingress))

      assert Enum.count(results, &(&1 == :ok)) == 256
      assert Enum.count(results, &(&1 == {:dropped, :overflow})) == 44
      assert Enum.take(results, 256) |> Enum.all?(&(&1 == :ok))
      assert queue_len(store) == 256
      assert %{pending: 256, dropped: 44, loss?: true} = Producer.stats(p)

      :ok = :sys.resume(store)
      assert %{retained: 256, pending: 0, dropped: 44} = Store.stats(store)

      # Admission reopens once the store has drained.
      assert Producer.record(p, ev("late", :ingress)) == :ok
      assert %{retained: 257, pending: 0} = Store.stats(store)

      # A dropped request's receipt is marked lossy, not silently complete.
      Producer.record(p, ev("r300", :terminal))
      Producer.record(p, ev("r300", :ingress))

      assert {:ok, %Receipt{completeness: :incomplete, gaps: gaps}} =
               Store.lookup(store, @principal, "r300")

      assert :loss in gaps
    end

    test "holds exactly under concurrent producers" do
      {store, p} = start()
      :ok = :sys.suspend(store)

      tasks =
        for t <- 1..32 do
          Task.async(fn ->
            for i <- 1..100, do: Producer.record(p, ev("t#{t}_#{i}", :ingress))
          end)
        end

      results = tasks |> Task.await_many(5_000) |> List.flatten()

      assert length(results) == 3_200
      assert Enum.count(results, &(&1 == :ok)) == 256
      assert Enum.count(results, &(&1 == {:dropped, :overflow})) == 3_200 - 256
      assert queue_len(store) == 256
      assert %{pending: 256, dropped: 2_944} = Producer.stats(p)

      :ok = :sys.resume(store)
      assert %{retained: 256, pending: 0, dropped: 2_944} = Store.stats(store)
    end

    test "configured bound applies and forged messages bypassing admission are ignored" do
      {store, p} = start(max_pending: 3)
      :ok = :sys.suspend(store)

      # Not admitted: wrong token. Must not be retained or release a slot.
      {:ok, forged} = Telelem.Receipts.Event.new(ev("forged", :ingress))
      send(store, {:telelem_receipt_event, make_ref(), 0, forged})

      assert [:ok, :ok, :ok, {:dropped, :overflow}] =
               for(i <- 1..4, do: Producer.record(p, ev("q#{i}", :ingress)))

      :ok = :sys.resume(store)
      assert %{retained: 3, pending: 0, dropped: 1} = Store.stats(store)
      assert {:error, :not_found} = Store.lookup(store, @principal, "forged")
    end

    test "a dead store does not block or crash producers; loss stays visible" do
      {store, p} = start(max_pending: 2)
      :ok = stop_supervised(Store)
      refute Process.alive?(store)

      assert [:ok, :ok, {:dropped, :overflow}] =
               for(i <- 1..3, do: Producer.record(p, ev("d#{i}", :ingress)))

      assert %{pending: 2, dropped: 1, loss?: true} = Producer.stats(p)
    end
  end

  describe "retention limit" do
    test "defaults keep 1,024 events and count each eviction" do
      {store, p} = start()

      for batch <- Enum.chunk_every(1..1_100, 200) do
        for i <- batch, do: :ok = Producer.record(p, ev("e#{i}", :ingress))
        Store.stats(store)
      end

      assert %{retained: 1_024, receipts: 1_024, evicted: 76, dropped: 0, loss?: true} =
               Store.stats(store)

      assert {:error, :not_found} = Store.lookup(store, @principal, "e76")
      assert {:ok, %Receipt{}} = Store.lookup(store, @principal, "e77")
    end

    test "evicting part of a receipt makes it incomplete and keeps its recorded terminal" do
      {store, p} = start(max_retained: 4)

      :ok = Producer.record(p, ev("old", :ingress))
      :ok = Producer.record(p, ev("old", :terminal, outcome: :failed))

      for kind <- [:ingress, :dispatch, :terminal],
          do: :ok = Producer.record(p, ev("new", kind))

      assert %{retained: 4, evicted: 1} = Store.stats(store)

      assert {:ok, old} = Store.lookup(store, @principal, "old")
      assert old.completeness == :incomplete
      assert old.gaps == [:no_ingress, :loss, :evicted]
      assert old.outcome == :failed
      assert old.exposure == :unknown
      assert old.usage == :unknown
      assert [%{kind: :terminal}] = old.events

      assert {:ok, %Receipt{completeness: :complete}} = Store.lookup(store, @principal, "new")
    end
  end

  describe "invalid data" do
    test "is never retained, is counted, and leaves no marker in state or replies" do
      {store, p} = start(max_event_bytes: 64)

      inputs = [
        Map.put(ev("m1", :ingress), :body, @marker),
        %{ev("m1", :ingress) | deployment_id: "Bearer " <> @marker},
        # valid shape, but its canonical encoding exceeds the configured 64 bytes
        ev(String.duplicate("x", 40), :ingress),
        [authorization: @marker],
        @marker
      ]

      for input <- inputs, do: assert({:rejected, {:invalid, _}} = Producer.record(p, input))

      stats = Store.stats(store)
      assert %{retained: 0, receipts: 0, rejected: 5, pending: 0, loss?: true} = stats

      for probe <- [stats, :sys.get_state(store), :sys.get_status(store)],
          do: refute(inspect(probe, limit: :infinity, printable_limit: :infinity) =~ @marker)

      assert {:error, :not_found} = Store.lookup(store, @principal, "m1")
    end

    test "attributable rejection marks only the affected receipt (by loss slot)" do
      {store, p} = start()

      for r <- ["ok", "bad"],
          kind <- [:ingress, :terminal],
          do: :ok = Producer.record(p, ev(r, kind))

      assert {:rejected, {:invalid, :exposure}} =
               Producer.record(p, ev("bad", :commit, exposure: :maybe))

      assert {:ok, %{completeness: :complete, gaps: []}} = Store.lookup(store, @principal, "ok")

      assert {:ok, %{completeness: :incomplete, gaps: [:loss], exposure: :unknown}} =
               Store.lookup(store, @principal, "bad")
    end

    test "unattributable rejection marks receipts whose ingress came before it" do
      {store, p} = start()

      :ok = Producer.record(p, ev("before", :ingress))
      assert {:rejected, _} = Producer.record(p, %{ev("x", :commit) | request_id: "no/slash"})
      :ok = Producer.record(p, ev("before", :terminal))
      :ok = Producer.record(p, ev("after", :ingress))
      :ok = Producer.record(p, ev("after", :terminal))

      assert %{unattributed: 1} = Store.stats(store)

      assert {:ok, %{completeness: :incomplete, gaps: [:unattributed_loss]}} =
               Store.lookup(store, @principal, "before")

      assert {:ok, %{completeness: :complete}} = Store.lookup(store, @principal, "after")
    end
  end

  describe "conservative completeness" do
    test "full lifecycle with no loss is complete" do
      {store, p} = start()

      :ok = Producer.record(p, ev("r1", :ingress, at_ms: 1))
      :ok = Producer.record(p, ev("r1", :connect_refused, at_ms: 2))

      :ok =
        Producer.record(
          p,
          ev("r1", :dispatch, attempt: 2, deployment_id: "b", at_ms: 3, exposure: :possible)
        )

      :ok =
        Producer.record(
          p,
          ev("r1", :commit, attempt: 2, deployment_id: "b", at_ms: 4, exposure: :possible)
        )

      :ok =
        Producer.record(
          p,
          ev("r1", :terminal, attempt: 2, deployment_id: "b", at_ms: 5, exposure: :possible)
        )

      assert {:ok, r} = Store.lookup(store, @principal, "r1")

      assert %Receipt{
               completeness: :complete,
               gaps: [],
               outcome: :done,
               exposure: :possible,
               usage: :unknown
             } = r

      assert Enum.map(r.events, & &1.at_ms) == [1, 2, 3, 4, 5]
      assert %{loss?: false} = Store.stats(store)
    end

    test "never infers success from an absent terminal" do
      {store, p} = start()
      :ok = Producer.record(p, ev("r1", :ingress))
      :ok = Producer.record(p, ev("r1", :dispatch, exposure: :possible))

      assert {:ok,
              %{
                completeness: :incomplete,
                gaps: [:no_terminal],
                outcome: :unknown,
                exposure: :unknown
              }} =
               Store.lookup(store, @principal, "r1")
    end

    test "a recorded terminal stays terminal; a second terminal is a gap" do
      {store, p} = start()
      :ok = Producer.record(p, ev("r1", :ingress))
      :ok = Producer.record(p, ev("r1", :terminal, outcome: :incomplete))
      :ok = Producer.record(p, ev("r1", :terminal, outcome: :done))

      assert {:ok,
              %{completeness: :incomplete, gaps: [:duplicate_terminal], outcome: :incomplete}} =
               Store.lookup(store, @principal, "r1")
    end

    test "a read from another process sees events admitted before it" do
      {store, p} = start()
      :ok = :sys.suspend(store)
      :ok = Producer.record(p, ev("r1", :ingress))
      :ok = Producer.record(p, ev("r1", :terminal))
      reader = Task.async(fn -> Store.lookup(store, @principal, "r1") end)
      :ok = :sys.resume(store)
      assert {:ok, %{completeness: :complete, events: [_, _]}} = Task.await(reader)
    end
  end

  describe "tenant-scoped reads" do
    test "only the authenticated tenant's request is readable" do
      {store, p} = start()
      :ok = Producer.record(p, ev("r1", :ingress, tenant_id: "local"))
      :ok = Producer.record(p, ev("r1", :ingress, tenant_id: "other", at_ms: 99))
      :ok = Producer.record(p, ev("r2", :ingress, tenant_id: "other"))

      assert {:ok, %{tenant_id: "local", events: [%{tenant_id: "local", at_ms: 0}]}} =
               Store.lookup(store, @principal, "r1")

      # Another tenant's request is indistinguishable from a missing one.
      assert Store.lookup(store, @principal, "r2") == {:error, :not_found}
      assert Store.lookup(store, @principal, "never") == {:error, :not_found}
      assert Store.lookup(store, %{@principal | tenant_id: "third"}, "r1") == {:error, :not_found}

      assert Store.lookup(store, @principal, %{tenant_id: "other", request_id: "r2"}) ==
               {:error, :not_found}

      for bad <- [
            %{},
            nil,
            "other",
            %{tenant_id: "a/b"},
            %{tenant_id: :other},
            [tenant_id: "other"]
          ],
          do: assert(Store.lookup(store, bad, "r1") == {:error, :denied})
    end
  end
end
