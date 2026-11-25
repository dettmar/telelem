defmodule Telelem.Serve.ListenerSpecTest do
  # Pure policy checks plus read-only checks against the pinned dependency
  # definitions (source attribute lists, compiled typespecs, struct keys). No
  # listener is started and no socket is opened.
  use ExUnit.Case, async: true

  alias Telelem.Serve.ListenerSpec

  @expected [
    scheme: :https,
    startup_log: false,
    http_options: [
      compress: false,
      compress_chunked: false,
      log_exceptions_with_status_codes: [],
      log_protocol_errors: false,
      log_client_closures: false
    ],
    http_1_options: [enabled: true, max_requests: 1, log_unknown_messages: false],
    http_2_options: [enabled: false],
    websocket_options: [enabled: false, compress: false, log_protocol_errors: false],
    thousand_island_options: [silent_terminate_on_error: true]
  ]

  @groups [:http_options, :http_1_options, :http_2_options, :websocket_options]

  test "the policy is exactly the fixed value, every time" do
    assert ListenerSpec.options() == @expected
    assert ListenerSpec.options() === ListenerSpec.options()
    assert function_exported?(ListenerSpec, :options, 0)
    refute function_exported?(ListenerSpec, :options, 1)
    refute function_exported?(ListenerSpec, :child_spec, 1)
  end

  test "no key appears twice at any level" do
    opts = ListenerSpec.options()
    assert Keyword.keys(opts) == Enum.uniq(Keyword.keys(opts))

    for {_group, sub} <- opts, is_list(sub) do
      assert Keyword.keys(sub) == Enum.uniq(Keyword.keys(sub))
    end
  end

  test "it is not runnable and carries no bind, TLS, plug or handler config" do
    opts = ListenerSpec.options()

    for key <- [
          :plug,
          :port,
          :ip,
          :inet,
          :inet6,
          :keyfile,
          :certfile,
          :otp_app,
          :cipher_suite,
          :display_plug
        ] do
      refute Keyword.has_key?(opts, key)
    end

    refute :inet in opts
    refute :inet6 in opts

    ti = Keyword.fetch!(opts, :thousand_island_options)
    assert Keyword.keys(ti) == [:silent_terminate_on_error]

    # Only atoms, booleans, integers and empty lists: no binaries (paths, PEM
    # or DER), PIDs, references, functions, ranges or maps.
    assert leaves(opts) |> Enum.all?(&(is_atom(&1) or is_integer(&1) or &1 == []))
  end

  test "policy semantics: HTTPS HTTP/1.1, one request, no HTTP/2, WebSocket or compression" do
    opts = ListenerSpec.options()
    assert opts[:scheme] == :https
    assert opts[:startup_log] == false
    assert opts[:http_1_options][:enabled] == true
    assert opts[:http_1_options][:max_requests] == 1
    assert opts[:http_1_options][:log_unknown_messages] == false
    assert opts[:http_2_options][:enabled] == false
    assert opts[:websocket_options][:enabled] == false
    assert opts[:websocket_options][:compress] == false
    assert opts[:websocket_options][:log_protocol_errors] == false
    assert opts[:http_options][:compress] == false
    assert opts[:http_options][:compress_chunked] == false
    assert opts[:http_options][:log_exceptions_with_status_codes] == []
    assert opts[:http_options][:log_protocol_errors] == false
    assert opts[:http_options][:log_client_closures] == false
    assert opts[:thousand_island_options][:silent_terminate_on_error] == true
  end

  test "dependency versions are the pinned ones" do
    assert Application.spec(:bandit, :vsn) == ~c"1.14.1"
    assert Application.spec(:thousand_island, :vsn) == ~c"1.6.0"
    assert Application.spec(:telemetry, :vsn) == ~c"1.4.3"
  end

  test "every name is accepted by the pinned Bandit option validation lists" do
    source = File.read!(Path.join(Mix.Project.deps_paths()[:bandit], "lib/bandit.ex"))

    valid = %{
      top: attr_keys(source, "top_level_keys"),
      http_options: attr_keys(source, "http_keys"),
      http_1_options: attr_keys(source, "http_1_keys"),
      http_2_options: attr_keys(source, "http_2_keys"),
      websocket_options: attr_keys(source, "websocket_keys")
    }

    opts = ListenerSpec.options()
    assert Keyword.keys(opts) -- valid.top == []

    for group <- @groups do
      assert Keyword.keys(opts[group]) -- valid[group] == [], "#{group}"
    end
  end

  test "every name and value type matches the pinned Bandit typespecs" do
    {:ok, types} = Code.Typespec.fetch_types(Bandit)

    typed =
      for {:type, {name, ast, []}} <- types,
          name in [:options, :http_options, :http_1_options, :http_2_options, :websocket_options],
          into: %{},
          do: {name, typed_keys(ast)}

    opts = ListenerSpec.options()
    assert Keyword.keys(opts) -- Map.keys(typed.options) == []

    for group <- @groups do
      assert Keyword.keys(opts[group]) -- Map.keys(typed[group]) == [], "#{group}"

      for {key, value} <- opts[group] do
        assert matches_type?(value, typed[group][key]), "#{group}.#{key}"
      end
    end

    assert matches_type?(opts[:scheme], typed.options[:scheme])
    assert matches_type?(opts[:startup_log], typed.options[:startup_log])
  end

  test "the Thousand Island option exists in the pinned server and handler configs" do
    server_keys = ThousandIsland.ServerConfig.__struct__() |> Map.from_struct() |> Map.keys()
    handler_keys = ThousandIsland.HandlerConfig.__struct__() |> Map.from_struct() |> Map.keys()

    for key <- Keyword.keys(ListenerSpec.options()[:thousand_island_options]) do
      assert key in server_keys
      assert key in handler_keys
    end

    assert ThousandIsland.ServerConfig.__struct__().silent_terminate_on_error == false
  end

  defp leaves(list) when is_list(list) and list != [] do
    Enum.flat_map(list, fn
      {k, v} when is_atom(k) -> leaves(v)
      other -> leaves(other)
    end)
  end

  defp leaves(other), do: [other]

  defp attr_keys(source, name) do
    [_, words] = Regex.run(~r/@#{name} ~w\(([^)]*)\)a/, source)
    words |> String.split() |> Enum.map(&String.to_atom/1)
  end

  # Collects `{key_atom, value_type}` from the 2-tuple members of a keyword
  # typespec's union.
  defp typed_keys(ast) do
    ast
    |> tuples()
    |> Map.new()
  end

  defp tuples({:type, _, :tuple, [{:atom, _, key}, value]}), do: [{key, value}]
  defp tuples({:type, _, _, args}) when is_list(args), do: Enum.flat_map(args, &tuples/1)
  defp tuples(_), do: []

  defp matches_type?(value, {:type, _, :boolean, []}), do: is_boolean(value)

  defp matches_type?(value, {:type, _, :pos_integer, []}),
    do: is_integer(value) and value > 0

  defp matches_type?(value, {:type, _, :list, []}), do: is_list(value)
  defp matches_type?(value, {:atom, _, atom}), do: value === atom

  defp matches_type?(value, {:type, _, :union, members}),
    do: Enum.any?(members, &matches_type?(value, &1))

  defp matches_type?(_value, _type), do: false
end
