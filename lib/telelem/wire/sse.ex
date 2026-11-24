defmodule Telelem.Wire.SSE do
  @moduledoc """
  Incremental, size-checked Server-Sent Events splitter and classifier for the
  narrow M1 chat-completions grammar (TEL-005).

  It cuts the byte stream at event boundaries (a blank line: `\\n\\n`,
  `\\r\\n\\r\\n` or `\\r\\r`) and classifies each complete event. It never
  rewrites event bytes: the gateway forwards exactly what the upstream sent.

  Bytes after the last boundary stay in `rest`. They are a **partial frame**:
  if the stream ends or breaks they are discarded and counted, never forwarded
  and never completed.

  `feed/3` returns `{:overflow, size}` when an event, or the partial frame,
  is larger than `max_event`. The check runs on the concatenated buffer
  *after* a delivery has been appended. It is an abort threshold, not a cap on
  memory already held.

  ## Grammar (M1 only, deliberately narrow)

  Lines are parsed the way the pinned OpenAI SDK's SSE decoder does: a line
  starting with `:` is a comment, otherwise `field:value` with one leading
  space dropped from the value, and `data` values joined with `\\n`. The SDK
  stops iterating (a clean end) on **any** event whose data *starts with*
  `[DONE]`, and raises `APIError` on a JSON object whose `"error"` is truthy.
  So:

    * `:comment`: only comment lines (heartbeats).
    * `:done`: data is exactly `[DONE]`. This is the only success sentinel.
    * `:bad_sentinel`: data starts with `[DONE]` but is not exactly that. The
      SDK would treat it as a success marker, so it must never be forwarded.
    * `:error_event`: a JSON object with a truthy top-level `"error"`.
    * `:semantic`: a `chat.completion.chunk`-shaped object carrying content,
      tool calls, refusal, a finish reason or usage.
    * `:preamble`: a JSON object with none of those (e.g. the role-only chunk).
    * `:unsupported`: anything else: non-data fields (`event:`, `id:`,
      `retry:`), data that is not a JSON object, or a mix of comment and data
      lines. This is outside the M1 grammar and is treated as a protocol
      violation, not forwarded.
  """

  @type kind ::
          :comment | :done | :bad_sentinel | :error_event | :semantic | :preamble | :unsupported
  @type event :: {kind(), binary()}

  @doc """
  Appends `data` to `rest` and splits off complete events.

  Returns `{:ok, [event], rest}` or `{:overflow, bytes}`.
  """
  @spec feed(binary(), binary(), pos_integer()) ::
          {:ok, [event()], binary()} | {:overflow, non_neg_integer()}
  def feed(rest, data, max_event) do
    split(rest <> data, [], max_event)
  end

  defp split(buf, acc, max_event) do
    case boundary(buf) do
      nil ->
        if byte_size(buf) > max_event,
          do: {:overflow, byte_size(buf)},
          else: {:ok, Enum.reverse(acc), buf}

      pos when pos > max_event ->
        {:overflow, pos}

      pos ->
        <<ev::binary-size(^pos), more::binary>> = buf
        split(more, [{classify(ev), ev} | acc], max_event)
    end
  end

  # Position just after the first event terminator, or nil.
  defp boundary(buf) do
    case :binary.match(buf, ["\n\n", "\r\n\r\n", "\r\r"]) do
      {start, len} -> start + len
      :nomatch -> nil
    end
  end

  @doc "Classifies one complete event (see the moduledoc grammar)."
  @spec classify(binary()) :: kind()
  def classify(ev) do
    lines = String.split(ev, ["\r\n", "\n", "\r"], trim: true)
    {comments, fields} = Enum.split_with(lines, &String.starts_with?(&1, ":"))
    parsed = Enum.map(fields, &field/1)

    cond do
      fields == [] and comments != [] -> :comment
      fields == [] -> :unsupported
      comments != [] -> :unsupported
      Enum.any?(parsed, fn {name, _} -> name != "data" end) -> :unsupported
      true -> classify_data(Enum.map_join(parsed, "\n", &elem(&1, 1)))
    end
  end

  defp field(line) do
    case String.split(line, ":", parts: 2) do
      [name, " " <> value] -> {name, value}
      [name, value] -> {name, value}
      [name] -> {name, ""}
    end
  end

  defp classify_data("[DONE]"), do: :done
  defp classify_data("[DONE]" <> _), do: :bad_sentinel

  defp classify_data(payload) do
    case JSON.decode(payload) do
      {:ok, %{} = obj} ->
        cond do
          truthy?(obj["error"]) -> :error_event
          semantic?(obj) -> :semantic
          true -> :preamble
        end

      _ ->
        :unsupported
    end
  end

  @doc """
  The `finish_reason` values carried by a data event (empty if none or if it
  is not a JSON object). Used to check the terminal grammar.
  """
  @spec finish_reasons(binary()) :: [term()]
  def finish_reasons(ev) do
    payload =
      ev
      |> String.split(["\r\n", "\n", "\r"], trim: true)
      |> Enum.map_join("\n", &(&1 |> field() |> elem(1)))

    case JSON.decode(payload) do
      {:ok, %{"choices" => choices}} when is_list(choices) ->
        for %{} = ch <- choices, present?(ch["finish_reason"]), do: ch["finish_reason"]

      _ ->
        []
    end
  end

  defp semantic?(obj) do
    usage? = is_map(obj["usage"])

    choices? =
      Enum.any?(List.wrap(obj["choices"]), fn
        %{} = ch ->
          d = ch["delta"] || %{}

          present?(d["content"]) or present?(d["tool_calls"]) or present?(d["refusal"]) or
            present?(ch["finish_reason"])

        _ ->
          false
      end)

    usage? or choices?
  end

  # Python truthiness, as the SDK's `data.get("error")` test.
  defp truthy?(v), do: v not in [nil, false, 0, +0.0, "", [], %{}]

  defp present?(nil), do: false
  defp present?(""), do: false
  defp present?([]), do: false
  defp present?(_), do: true
end
