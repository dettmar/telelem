defmodule Telelem.WireSSETest do
  # TEL-005 unit tests: SSE framing and declared limits. No sockets.
  use ExUnit.Case, async: true

  alias Telelem.Wire.{Limits, SSE}

  defp ev(obj), do: "data: " <> JSON.encode!(obj) <> "\n\n"

  defp delta(d, fin \\ nil),
    do: %{"choices" => [%{"index" => 0, "delta" => d, "finish_reason" => fin}]}

  test "splits only at event boundaries and keeps the partial frame as rest" do
    a = ev(delta(%{"content" => "hi"}))
    b = ev(delta(%{"content" => "there"}))
    stream = a <> b <> "data: {\"choices\":[{\"del"

    # Feed byte by byte: the same events come out, the tail stays partial.
    {events, rest} =
      stream
      |> :binary.bin_to_list()
      |> Enum.reduce({[], ""}, fn byte, {acc, rest} ->
        {:ok, evs, rest} = SSE.feed(rest, <<byte>>, 4096)
        {acc ++ evs, rest}
      end)

    assert Enum.map(events, &elem(&1, 1)) == [a, b]
    assert rest == "data: {\"choices\":[{\"del"
  end

  test "accepts LF, CRLF and CR terminators byte for byte" do
    for sep <- ["\n\n", "\r\n\r\n", "\r\r"] do
      e = "data: [DONE]" <> sep
      assert {:ok, [{:done, ^e}], ""} = SSE.feed("", e, 100)
    end
  end

  test "classifies heartbeat, preamble, semantic, done and error events" do
    assert SSE.classify(": keep-alive\n\n") == :comment
    assert SSE.classify(ev(delta(%{"role" => "assistant", "content" => ""}))) == :preamble
    assert SSE.classify(ev(delta(%{"role" => "assistant", "content" => nil}))) == :preamble
    assert SSE.classify(ev(delta(%{"content" => "x"}))) == :semantic

    tool = %{"tool_calls" => [%{"index" => 0, "function" => %{"arguments" => "{\"ci"}}]}
    assert SSE.classify(ev(delta(tool))) == :semantic
    assert SSE.classify(ev(delta(%{}, "stop"))) == :semantic
    assert SSE.classify(ev(%{"choices" => [], "usage" => %{"total_tokens" => 1}})) == :semantic
    assert SSE.classify(ev(%{"error" => %{"message" => "boom"}})) == :error_event
    assert SSE.classify("data: [DONE]\n\n") == :done
    assert SSE.classify("data: not json\n\n") == :preamble
  end

  test "an event or partial frame over max_event overflows instead of growing" do
    big = "data: " <> String.duplicate("x", 200)
    assert {:overflow, n} = SSE.feed("", big, 100)
    assert n > 100
    assert {:overflow, _} = SSE.feed("", big <> "\n\n", 100)
    assert {:ok, [_], ""} = SSE.feed("", big <> "\n\n", 1000)
  end

  test "limits reject an inconsistent budget" do
    assert %Limits{overflow: :abort} = Limits.new()

    assert_raise ArgumentError, ~r/window/, fn ->
      Limits.new(max_event: 100_000, window: 1_000)
    end

    assert_raise ArgumentError, ~r/high_water/, fn ->
      Limits.new(max_event: 1_000, window: 2_000, high_water: 1_000)
    end

    assert_raise ArgumentError, ~r/hard_cap/, fn -> Limits.new(hard_cap: 1) end
    assert_raise ArgumentError, ~r/cleanup/, fn -> Limits.new(cleanup_timeout_ms: 5_000) end
  end
end
