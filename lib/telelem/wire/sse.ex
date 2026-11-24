defmodule Telelem.Wire.SSE do
  @moduledoc """
  Incremental, size-bounded Server-Sent Events splitter (TEL-005).

  It only cuts the byte stream at event boundaries (a blank line: `\\n\\n`,
  `\\r\\n\\r\\n` or `\\r\\r`) and classifies each complete event. It never
  rewrites event bytes: the gateway forwards exactly what the upstream sent.

  Bytes after the last boundary stay in `rest`. They are a **partial frame**:
  if the stream ends or breaks, they are discarded and counted, never forwarded
  and never completed.

  `feed/3` returns `{:overflow, size}` when `rest` would exceed `max_event`
  bytes, so one event can never grow without bound.
  """

  @type kind :: :comment | :done | :semantic | :preamble | :error_event
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

  @doc """
  Classifies one complete event.

    * `:comment`: only comment lines (`:` prefix), such as heartbeats.
    * `:done`: a `data: [DONE]` event.
    * `:semantic`: a data event that carries content, tool calls, a finish
      reason or usage. This is what makes an attempt `ready`.
    * `:error_event`: a data event with a top-level `"error"` object.
    * `:preamble`: any other data event (e.g. the role-only first chunk).
      It is kept and forwarded in order, but it does not make an attempt ready.
  """
  @spec classify(binary()) :: kind()
  def classify(ev) do
    lines = ev |> String.split(["\r\n", "\n", "\r"], trim: true)

    data =
      for line <- lines, String.starts_with?(line, "data:") do
        line |> String.replace_prefix("data:", "") |> String.replace_prefix(" ", "")
      end

    cond do
      data == [] and Enum.all?(lines, &String.starts_with?(&1, ":")) -> :comment
      data == [] -> :preamble
      true -> classify_data(Enum.join(data, "\n"))
    end
  end

  defp classify_data("[DONE]" <> _), do: :done

  defp classify_data(payload) do
    case JSON.decode(payload) do
      {:ok, %{"error" => e}} when is_map(e) -> :error_event
      {:ok, %{} = obj} -> if semantic?(obj), do: :semantic, else: :preamble
      _ -> :preamble
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

  defp present?(nil), do: false
  defp present?(""), do: false
  defp present?([]), do: false
  defp present?(_), do: true
end
