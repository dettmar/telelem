defmodule Telelem.HandoffStatusRedactionTest do
  @moduledoc """
  TEL-013 diagnostics: `Telelem.Handoff.format_status/1` is a total
  allowlist projection of the status map OTP passes it.

  Covered, on real coordinators fed synthetic marker data (`@mark` in attempt
  ids, requester reasons, chunks, terminal values, receipt metadata and exit
  reasons): `:sys.get_status/1`'s formatted `"State"` and `"Logged events"`
  and the `{gen_server, terminate}` error report (`state`, `last_message`,
  `reason`, `log`), read raw from a test logger handler and rendered with
  OTP's formatter. Every case has a positive control showing the marker was
  really present (`:sys.get_state/1`, the raw sys log, `status/1`).

  Pinned as raw, i.e. NOT covered by the callback: `:sys.get_state/1`, the
  outer `Debug` element of `:sys.get_status/1`, the exit reason seen by a
  monitor, and the proc_lib crash report (when the logger delivers it).

  Listener-free, no sockets, no sleeps. `async: false` only because a logger
  handler is global.

      scripts/mix test test/telelem/handoff_status_redaction_test.exs --seed 0
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  @moduletag :tmp_dir
  @moduletag :capture_log

  alias Telelem.Handoff
  alias Telelem.Instance
  alias Telelem.Receipts.Store

  @mark "MARK-3e9d"
  @a1 {:attempt, @mark}
  @a2 "second-" <> @mark
  @chunk "data: {\"x\":\"" <> @mark <> "\"}\n\n"

  # Every atom the projection may emit (keys and values). A new atom in the
  # output, i.e. an unknown value passed through, fails `safe?/1`.
  @allowed MapSet.new([
             true,
             false,
             nil,
             :redacted,
             :other,
             # state keys
             :phase,
             :winner?,
             :outcome,
             :terminal,
             :terminal_ack?,
             :terminal_deadline?,
             :terminal_pending?,
             :authority,
             :authorization,
             :observer,
             :unreported,
             :attempts,
             :total,
             :attached,
             :dispatched,
             :ready,
             :cancelled,
             :closed,
             :possible,
             :grant,
             :request?,
             :receipts,
             :state,
             :permitted?,
             :terminal?,
             :upstream,
             :downstream,
             # categories
             :open,
             :elected,
             :committed,
             :done,
             :incomplete,
             :failed,
             :complete,
             :unconfirmed,
             :written,
             :not_tracked,
             :error,
             :up,
             :lost,
             :none,
             :admission,
             :fence_only,
             :idle,
             :reserving,
             :reserved,
             :authorizing,
             :authorized,
             :refused,
             :abandoned,
             :recording,
             :disabled,
             # message kinds
             :call,
             :cast,
             :system,
             :exit,
             :down,
             :terminal_grant,
             :terminal_deadline,
             :register_deadline,
             :cleanup_deadline,
             :attach,
             :begin_dispatch,
             :transfer,
             :finish,
             :terminal_written,
             :cleaned,
             :cancel,
             :status,
             # reasons
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
             # sys log tags
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
    # Logger handler: forwards raw events to the test process.
    def log(event, %{config: %{pid: pid}}), do: send(pid, {:log_event, event})
  end

  setup do
    id = :"handoff_status_redaction_#{System.unique_integer([:positive])}"
    :ok = :logger.add_handler(id, Forward, %{config: %{pid: self()}, level: :all})
    on_exit(fn -> :logger.remove_handler(id) end)
    :ok
  end

  # ── :sys.get_status on a live coordinator ───────────────────────────────

  test ":sys.get_status shows fixed categories and counts; raw state, sys log and status/1 keep the marker" do
    h = start_legacy!()
    :ok = :sys.log(h, true)
    att = attempt!(h, @a1)

    assert :release = run(att, &Handoff.ready(&1, @a1))
    assert_receive {:telelem_handoff, ^h, {:elected, @a1}}
    assert :ok = run(att, &Handoff.transfer(&1, @chunk))
    assert_receive {:telelem_handoff, ^h, {:data, @chunk}}
    assert :ok = Handoff.cancel(h, {:client_gone, @mark})
    assert_receive {:telelem_handoff, ^h, {:incomplete, {:client_gone, @mark}}}

    # Positive controls: the marker really is in the (unchanged) public
    # status, the state and the sys log (system messages are not logged).
    assert %{
             outcome: {:incomplete, {:client_gone, @mark}},
             winner: @a1,
             attempts: %{@a1 => %{cancelled: {:client_gone, @mark}}, @a2 => _}
           } = Handoff.status(h)

    assert raw?(:sys.get_state(h))
    {:ok, raw_log} = :sys.log(h, :get)
    assert raw?(raw_log)

    {:status, ^h, {:module, :gen_server}, [_pdict, :running, _parent, debug, misc]} =
      :sys.get_status(h)

    [header: _, data: data, data: [{~c"State", state}]] = misc
    {_, events} = List.keyfind(data, ~c"Logged events", 0)

    assert state == %{
             phase: :committed,
             winner?: true,
             outcome: :incomplete,
             terminal: nil,
             terminal_ack?: false,
             terminal_deadline?: false,
             terminal_pending?: false,
             authority: :none,
             authorization: :none,
             observer: :none,
             unreported: 0,
             attempts: %{
               total: 2,
               attached: 1,
               dispatched: 1,
               ready: 1,
               cancelled: 1,
               closed: 1,
               possible: 0
             },
             grant: nil,
             receipts: nil
           }

    # Same number of logged events, tags only.
    assert length(events) == length(raw_log) and events != []
    assert Enum.all?(events, &match?({tag, :redacted} when tag in [:in, :out, :noreply], &1))
    assert safe?(state) and safe?(events)
    refute raw?(misc)

    # Limit, pinned: the outer Debug element is OTP's raw sys debug state.
    assert raw?(debug)
  end

  test ":sys.get_status of a grant + receipts coordinator hides grant tokens and receipt metadata",
       ctx do
    {auth, store} = instance!(ctx.tmp_dir)
    d = System.monotonic_time(:millisecond) + 60_000

    {:ok, h} =
      Handoff.start(
        requester: idle(),
        attempts: [:primary],
        admission_authority: auth,
        terminal_deadline: d,
        coordinator_grant: {:one_primary, 4},
        terminal_ack: true,
        receipts: [
          store: store,
          producer: Store.producer(store),
          store_mode: :sandbox,
          meta: %{
            tenant_id: "local",
            request_id: @mark,
            route_rev: 1,
            deployment_id: "dep-" <> @mark,
            mode: :sandbox
          }
        ],
        cleanup_timeout: 60_000,
        register_timeout: 60_000
      )

    on_exit(fn -> Process.exit(h, :kill) end)
    att = spawn_attempt(h)
    assert {:ok, _} = run(att, &Handoff.attach(&1, :primary, deadline: d))

    s = :sys.get_state(h)
    assert raw?(s) and s.grant.gid != nil

    {:status, ^h, _, [_, _, _, _, [header: _, data: _, data: [{~c"State", state}]]]} =
      :sys.get_status(h)

    assert %{
             phase: :open,
             authorization: :admission,
             authority: :up,
             terminal_ack?: true,
             terminal_deadline?: true,
             grant: %{phase: :reserved, request?: false},
             receipts: %{state: :recording, permitted?: false, terminal?: false},
             attempts: %{total: 1, attached: 1, dispatched: 0}
           } = state

    assert safe?(state)
    refute inspect(state, limit: :infinity) =~ inspect(s.grant.gid)
    assert %{grant: :reserved, receipts: :recording} = Handoff.status(h)
  end

  # ── {gen_server, terminate} error reports ───────────────────────────────

  test "terminate report on a parent exit: projected state/message/reason/log; raw exit and proc_lib report keep the marker" do
    test = self()

    parent =
      spawn(fn ->
        {:ok, h} =
          Handoff.start_link(requester: test, attempts: [@a1, @a2], cleanup_timeout: 60_000)

        send(test, {:started, h})
        receive do: (:die -> exit({:parent_gone, @mark}))
      end)

    assert_receive {:started, h}
    mref = Process.monitor(h)
    :ok = :sys.log(h, true)
    att = attempt!(h, @a1)
    assert :release = run(att, &Handoff.ready(&1, @a1))
    assert raw?(:sys.get_state(h))

    # Isolated trace session: every report as the coordinator hands it to
    # logger, before any primary filter.
    session = :trace.session_create(:handoff_status_redaction, self(), [])
    1 = :trace.process(session, h, true, [:call])
    1 = :trace.function(session, {:logger, :macro_log, 4}, true, [])

    send(parent, :die)
    assert_receive {:DOWN, ^mref, :process, ^h, reason}
    # Raw exit reason, unchanged (not a format_status field).
    assert reason == {:parent_gone, @mark}

    ref = :trace.delivered(session, h)
    assert_receive {:trace_delivered, ^h, ^ref}
    :trace.session_destroy(session)
    logged = logged_reports(h)

    {report, ev} = terminate_report!(h)
    # The same projected fields reach logger's call and the handler.
    fields = [:state, :last_message, :reason, :log]
    assert [logged_terminate] = for({{:gen_server, :terminate}, r} <- logged, do: r)
    assert Map.take(logged_terminate, fields) == Map.take(report, fields)

    # A parent EXIT is reported without a stacktrace.
    assert report.last_message == :exit
    assert report.reason == :other
    assert %{phase: :elected, winner?: true, outcome: nil} = report.state
    assert safe?(report.state) and safe?(report.last_message) and safe?(report.log)
    assert report.log != [] and Enum.all?(report.log, &match?({_, :redacted}, &1))
    assert report.client_info == :undefined

    # Rendered by OTP's own formatters (logger_formatter and the report_cb)
    # and by Elixir's translator (added to the report at the handler).
    refute rendered(report, ev) =~ @mark
    assert translated(report) =~ "terminating"
    refute raw?(report)

    # Limit, pinned: proc_lib's crash report is a separate report built from
    # the raw exit reason. With the pinned runner's logger configuration
    # (Elixir's primary translator filter, `sasl: false`) it never reaches a
    # handler; it would be logged raw with SASL reports enabled.
    assert [crash] = for({{:proc_lib, :crash}, r} <- logged, do: r)
    assert raw?(crash)

    refute_received {:log_event,
                     %{meta: %{pid: ^h}, msg: {:report, %{label: {:proc_lib, :crash}}}}}
  end

  test "terminate report on a callback exception: message kind only, reason :exception" do
    {:ok, h} = Handoff.start(requester: self(), attempts: [@a1], cleanup_timeout: 60_000)
    mref = Process.monitor(h)
    att = attempt!(h, @a1)
    assert :release = run(att, &Handoff.ready(&1, @a1))

    # Elixir's default handle_cast/2 raises; the cast carries the marker.
    GenServer.cast(h, {:probe, @mark})
    assert_receive {:DOWN, ^mref, :process, ^h, {%RuntimeError{}, _}}

    {report, ev} = terminate_report!(h)
    assert report.last_message == {:cast, :other}
    assert {:exception, st} = report.reason
    assert is_list(st)
    assert %{phase: :elected, outcome: nil} = report.state
    assert safe?(report.state) and report.log == []
    refute rendered(report, ev) =~ @mark
    assert translated(report) =~ "(stop) :exception"
    refute raw?(report)
  end

  # ── Totality ────────────────────────────────────────────────────────────

  test "same keys, no 'EXIT', only allowlisted values for odd inputs through gen's merge" do
    base = %{
      state: :not_a_handoff_state,
      message: {@mark},
      reason: {:shutdown, @mark},
      log: [{:in, @mark}, {:out, @mark, self(), %{x: @mark}}, @mark, {:noreply, @mark}],
      future_key: @mark
    }

    inputs = [
      base,
      %{base | log: [{:in, @mark} | @mark]},
      %{base | log: @mark, state: %{attempts: %{@mark => @mark}, phase: @mark}},
      %{
        base
        | state: %{attempts: %{}, receipts: @mark, grant: @mark, terminal: {:authorized, @mark}}
      },
      %{state: %{attempts: :bad}},
      %{reason: %RuntimeError{message: @mark}},
      %{reason: {:timeout, {GenServer, :call, [self(), @mark, 5_000]}}},
      %{message: {:"$gen_call", {self(), make_ref()}, {:finish, @mark, @mark, make_ref()}}},
      %{}
    ]

    for input <- inputs, do: assert_projected(input)

    assert %{message: {:call, :finish}} =
             Handoff.format_status(%{
               message: {:"$gen_call", {self(), make_ref()}, {:finish, 1, 2, 3}}
             })

    assert %{
             reason: {:shutdown, :redacted},
             log: [{:in, :redacted}, {:out, :redacted}, :redacted, {:noreply, :redacted}]
           } =
             Handoff.format_status(base)

    # Improper list: the whole map falls back to fixed atoms, never a crash.
    assert %{state: :redacted, message: :redacted, reason: :redacted, log: []} =
             Handoff.format_status(%{state: 1, message: 1, reason: 1, log: [{:in, 1} | :tail]})
  end

  property "arbitrary values under real and unknown keys never pass through" do
    check all(status <- status_gen(), max_runs: 300) do
      assert_projected(status)
    end
  end

  # ── helpers ─────────────────────────────────────────────────────────────

  # gen applies the callback only if the module is loaded
  # (`function_exported/3`); a live server's module always is.
  defp assert_projected(status) do
    Code.ensure_loaded!(Handoff)
    out = :gen.format_status(Handoff, :terminate, status, [])
    refute Map.has_key?(out, :EXIT), inspect(out)
    assert Map.keys(out) |> Enum.sort() == Map.keys(status) |> Enum.sort()
    for {_k, v} <- out, do: assert(safe?(v), inspect(v))
    refute raw?(out)
  end

  defp status_gen do
    val =
      StreamData.one_of([
        StreamData.resize(StreamData.term(), 6),
        StreamData.constant(@mark),
        StreamData.tuple(
          {StreamData.member_of([:done, :failed, :shutdown, :in, :out]),
           StreamData.constant({@mark})}
        ),
        StreamData.tuple(
          {StreamData.member_of([:"$gen_call", :DOWN]), StreamData.constant(@mark),
           StreamData.constant([@mark])}
        )
      ])

    attempt =
      StreamData.fixed_map(%{
        pid: val,
        dispatched: val,
        ready: val,
        cancelled: val,
        cleanup: val,
        charge: val
      })

    state =
      StreamData.one_of([
        val,
        StreamData.fixed_map(%{
          attempts: marked_map(attempt),
          phase: val,
          winner: val,
          outcome: val,
          terminal:
            StreamData.one_of([val, StreamData.tuple({StreamData.constant(:authorized), val})]),
          downstream: val,
          terminal_ack: val,
          terminal_deadline: val,
          pending: val,
          authority_state: val,
          authorization: val,
          observer_state: val,
          unreported: StreamData.one_of([val, StreamData.list_of(val, max_length: 3)]),
          grant: StreamData.one_of([val, StreamData.fixed_map(%{phase: val, req: val})]),
          receipts:
            StreamData.one_of([
              val,
              StreamData.fixed_map(%{state: val, permitted: val, terminal: val})
            ])
        })
      ])

    StreamData.fixed_map(%{
      state: state,
      message: val,
      reason: val,
      log: StreamData.one_of([val, StreamData.list_of(val, max_length: 4)]),
      extra: val
    })
  end

  # Up to three entries under distinct marker keys (map_of/2 can run out
  # of unique keys at small sizes).
  defp marked_map(values) do
    values
    |> StreamData.list_of(max_length: 3)
    |> StreamData.map(fn vs ->
      vs |> Enum.with_index() |> Map.new(fn {v, i} -> {{@mark, i}, v} end)
    end)
  end

  defp terminate_report!(h) do
    assert_receive {:log_event,
                    %{meta: %{pid: ^h}, msg: {:report, %{label: {:gen_server, :terminate}} = r}} =
                      ev}

    {r, ev}
  end

  defp logged_reports(h) do
    receive do
      {:trace, ^h, :call, {:logger, :macro_log, [_loc, _level, %{label: label} = r, _meta]}} ->
        [{label, r} | logged_reports(h)]

      {:trace, ^h, :call, {:logger, :macro_log, _}} ->
        [:other | logged_reports(h)]
    after
      0 -> []
    end
  end

  defp translated(%{elixir_translation: t}), do: IO.chardata_to_string(t)

  # OTP's renderings of the report: the event through logger_formatter, and
  # gen_server's report callbacks (logger and legacy error_logger).
  defp rendered(report, ev) do
    {fmt, args} = :gen_server.format_log(report)

    [
      :logger_formatter.format(ev, %{single_line: false}),
      :gen_server.format_log(report, %{}),
      :io_lib.format(fmt, args)
    ]
    |> IO.chardata_to_string()
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

  defp start_legacy! do
    {:ok, h} =
      Handoff.start(
        requester: self(),
        attempts: [@a1, @a2],
        cleanup_timeout: 60_000,
        register_timeout: 60_000
      )

    on_exit(fn -> Process.exit(h, :kill) end)
    h
  end

  # An attempt process that runs Handoff calls on request (calls must come
  # from the attached pid).
  defp attempt!(h, id) do
    att = spawn_attempt(h)
    assert {:ok, _} = run(att, &Handoff.attach(&1, id))
    assert :ok = run(att, &Handoff.begin_dispatch(&1, id))
    att
  end

  defp spawn_attempt(h) do
    pid =
      spawn(fn ->
        loop = fn loop ->
          receive do
            {:run, from, fun} ->
              send(from, {:ran, self(), fun.(h)})
              loop.(loop)
          end
        end

        loop.(loop)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp run(att, fun) do
    send(att, {:run, self(), fun})
    assert_receive {:ran, ^att, result}
    result
  end

  defp idle do
    pid = spawn(fn -> receive do: (:never -> :ok) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp instance!(dir) do
    sup =
      start_supervised!({
        Instance,
        id: :handoff_status_redaction_gw,
        limits: %{slots: 4, units: 40},
        spool_dir: Path.join(dir, "spool"),
        transport: {:unused_no_workers, nil},
        reserve_ttl: nil
      })

    auth = Instance.authority(Instance.handle(sup))
    store = start_supervised!({Store, mode: :sandbox}, id: :status_store)
    {auth, store}
  end
end
