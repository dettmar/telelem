defmodule Telelem.Receipts.StoreStatusRedactionTest do
  @moduledoc """
  TEL-013 diagnostics: `Telelem.Receipts.Store.format_status/1` is a total
  allowlist projection of the status map OTP passes it.

  Covered, on a real store fed synthetic marker data (`@mark` in tenant and
  request ids, the reader's subject and a cast): `:sys.get_status/1`'s
  formatted `"State"` and `"Logged events"`, and the `{gen_server,
  terminate}` error report (`state`, `last_message`, `reason`, `log`, and
  Elixir's translation of it) read raw from a test logger handler. Positive
  controls show the marker really was in the state and the sys log.

  Pinned as raw, i.e. NOT covered: `:sys.get_state/1` and the outer `Debug`
  element of `:sys.get_status/1`.

  Listener-free, no sockets, no sleeps. `async: false` only because a logger
  handler is global.

      scripts/mix test test/telelem/receipts/store_status_redaction_test.exs --seed 0
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Telelem.Receipts.{Store, Writer}

  @moduletag :capture_log

  @mark "MARK-77b1"
  @tenant "tenant-" <> @mark
  @reader %{
    tenant_id: @tenant,
    subject_id: "subject-" <> @mark,
    mode: :sandbox,
    permissions: [:receipt_read]
  }

  @allowed MapSet.new([
             true,
             false,
             nil,
             :redacted,
             :other,
             :mode,
             :retained,
             :receipts,
             :sandbox,
             :serving,
             :call,
             :cast,
             :system,
             :exit,
             :sweep,
             :producer,
             :stats,
             :lookup,
             :normal,
             :shutdown,
             :kill,
             :killed,
             :noproc,
             :noconnection,
             :timeout,
             :function_clause,
             :undef,
             :badarg,
             :badarith,
             :system_limit,
             :badmatch,
             :case_clause,
             :try_clause,
             :with_clause,
             :badkey,
             :badmap,
             :bad_return_value,
             :nocatch,
             :exception,
             :in,
             :out,
             :noreply,
             :continue,
             :code_change,
             :postpone,
             :consume,
             :enter
           ])

  defmodule Forward do
    @moduledoc false
    def log(event, %{config: %{pid: pid}}), do: send(pid, {:log_event, event})
  end

  setup do
    id = :"store_status_redaction_#{System.unique_integer([:positive])}"
    :ok = :logger.add_handler(id, Forward, %{config: %{pid: self()}, level: :all})
    on_exit(fn -> :logger.remove_handler(id) end)
    :ok
  end

  test ":sys.get_status shows mode and counts; raw state and sys log keep the marker" do
    store = start_supervised!({Store, mode: :sandbox})
    :ok = :sys.log(store, true)
    populate!(store)
    assert {:ok, %{}} = Store.lookup(store, @reader, @mark)
    assert {:error, :not_found} = Store.lookup(store, @reader, "other-" <> @mark)

    # Suspended, the periodic :sweep can't add log entries between the two
    # reads below; system messages are still served.
    :ok = :sys.suspend(store)

    # Positive controls.
    assert raw?(:sys.get_state(store))
    {:ok, raw_log} = :sys.log(store, :get)
    assert raw?(raw_log)

    {:status, ^store, {:module, :gen_server}, [_pdict, :suspended, _parent, debug, misc]} =
      :sys.get_status(store)

    :ok = :sys.resume(store)

    [header: _, data: data, data: [{~c"State", state}]] = misc
    {_, events} = List.keyfind(data, ~c"Logged events", 0)

    assert state == %{mode: :sandbox, retained: 2, receipts: 1}
    assert length(events) == length(raw_log) and events != []
    assert Enum.all?(events, &match?({tag, :redacted} when tag in [:in, :out, :noreply], &1))
    assert safe?(state) and safe?(events)
    refute raw?(misc)

    # Limit, pinned: the outer Debug element is OTP's raw sys debug state.
    assert raw?(debug)
  end

  test "terminate report after a callback exception: kinds and counts only" do
    {:ok, store} = GenServer.start(Store, mode: :sandbox)
    on_exit(fn -> Process.exit(store, :kill) end)
    mref = Process.monitor(store)
    :ok = :sys.log(store, true)
    populate!(store)
    assert {:ok, _} = Store.lookup(store, @reader, @mark)
    assert raw?(:sys.get_state(store))

    # Elixir's default handle_cast/2 raises; the cast carries the marker.
    GenServer.cast(store, {:probe, @mark})
    assert_receive {:DOWN, ^mref, :process, ^store, {%RuntimeError{}, _}}

    assert_receive {:log_event,
                    %{
                      meta: %{pid: ^store},
                      msg: {:report, %{label: {:gen_server, :terminate}} = report}
                    } = ev}

    assert report.last_message == {:cast, :other}
    assert {:exception, st} = report.reason
    assert is_list(st)
    assert report.state == %{mode: :sandbox, retained: 2, receipts: 1}
    assert report.log != [] and Enum.all?(report.log, &match?({_, :redacted}, &1))
    assert safe?(report.state) and safe?(report.last_message) and safe?(report.log)

    {fmt, args} = :gen_server.format_log(report)

    rendered =
      IO.chardata_to_string([
        :logger_formatter.format(ev, %{single_line: false}),
        :gen_server.format_log(report, %{}),
        :io_lib.format(fmt, args),
        report.elixir_translation
      ])

    assert rendered =~ "(stop) :exception"
    refute rendered =~ @mark
    refute raw?(report)
  end

  test "same keys, no 'EXIT', only allowlisted values for odd inputs through gen's merge" do
    base = %{
      state: %{retained: 1, by_key: %{{@tenant, @mark} => @mark}, mode: @mark, producer: @mark},
      message: {:"$gen_call", {self(), make_ref()}, {:lookup, @reader, @mark}},
      reason: {:badmatch, @mark},
      log: [{:in, @mark}, {:out, @mark, self(), %{x: @mark}}, @mark],
      future_key: @mark
    }

    assert %{
             state: %{mode: :other, retained: 1, receipts: 1},
             message: {:call, :lookup},
             reason: {:badmatch, :redacted},
             log: [{:in, :redacted}, {:out, :redacted}, :redacted],
             future_key: :redacted
           } = Store.format_status(base)

    inputs = [
      base,
      %{base | state: %{retained: @mark, by_key: %{}}},
      %{base | state: @mark, log: @mark},
      %{base | log: [{:in, @mark} | @mark]},
      %{message: :sweep, reason: %ArgumentError{message: @mark}},
      %{message: {:"$gen_call", {self(), make_ref()}, @mark}},
      %{}
    ]

    for input <- inputs, do: assert_projected(input)
  end

  property "arbitrary values under real and unknown keys never pass through" do
    val =
      StreamData.one_of([
        StreamData.resize(StreamData.term(), 6),
        StreamData.constant(@mark),
        StreamData.tuple(
          {StreamData.member_of([:shutdown, :in, :lookup]), StreamData.constant(@mark)}
        )
      ])

    state =
      StreamData.one_of([
        val,
        StreamData.fixed_map(%{
          retained: StreamData.one_of([val, StreamData.integer()]),
          by_key: StreamData.one_of([val, marked_map(val)]),
          mode: val
        })
      ])

    check all(
            status <-
              StreamData.fixed_map(%{
                state: state,
                message: val,
                reason: val,
                log: StreamData.one_of([val, StreamData.list_of(val, max_length: 4)]),
                extra: val
              }),
            max_runs: 300
          ) do
      assert_projected(status)
    end
  end

  # ── helpers ─────────────────────────────────────────────────────────────

  # Up to three entries under distinct marker keys (map_of/2 can run out
  # of unique keys at small sizes).
  defp marked_map(values) do
    values
    |> StreamData.list_of(max_length: 3)
    |> StreamData.map(fn vs ->
      vs |> Enum.with_index() |> Map.new(fn {v, i} -> {{@mark, i}, v} end)
    end)
  end

  # One receipt (two records) under marker ids.
  defp populate!(store) do
    {:ok, w} = Writer.open(Store.producer(store), @tenant, @mark)
    {:ok, w} = record(w, :ingress)
    {:ok, _} = record(w, :dispatch)
  end

  defp record(w, kind) do
    attempt? = kind != :ingress

    event = %{
      tenant_id: @tenant,
      request_id: @mark,
      route_rev: 1,
      attempt: if(attempt?, do: 1, else: 0),
      deployment_id: if(attempt?, do: "dep-" <> @mark, else: :none),
      at_ms: 0,
      kind: kind,
      outcome: :none,
      exposure: if(attempt?, do: :possible, else: :none)
    }

    {:ok, w} = Writer.record(w, event)
    {:ok, w}
  end

  # gen applies the callback only if the module is loaded
  # (`function_exported/3`); a live server's module always is.
  defp assert_projected(status) do
    Code.ensure_loaded!(Store)
    out = :gen.format_status(Store, :terminate, status, [])
    refute Map.has_key?(out, :EXIT), inspect(out)
    assert Map.keys(out) |> Enum.sort() == Map.keys(status) |> Enum.sort()
    for {_k, v} <- out, do: assert(safe?(v), inspect(v))
    refute raw?(out)
  end

  # Only allowlisted atoms, non-negative integers, proper lists, tuples and
  # plain maps: no binary, pid, ref, fun, float or unknown atom.
  defp safe?(t) when is_atom(t), do: MapSet.member?(@allowed, t)
  defp safe?(t) when is_integer(t), do: t >= 0
  defp safe?([]), do: true
  defp safe?([h | t]), do: safe?(h) and safe?(t)
  defp safe?(t) when is_tuple(t), do: t |> Tuple.to_list() |> Enum.all?(&safe?/1)

  defp safe?(%{} = m) when not is_struct(m),
    do: Enum.all?(m, fn {k, v} -> safe?(k) and safe?(v) end)

  defp safe?(_), do: false

  defp raw?(t), do: inspect(t, limit: :infinity, printable_limit: :infinity) =~ @mark
end
