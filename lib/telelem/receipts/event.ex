defmodule Telelem.Receipts.Event do
  @moduledoc """
  Payload-free receipt event (M2a contract §2, `docs/m2a-contracts.md`).

      Event {tenant_id: Id, request_id: Id, route_rev: positive integer,
             attempt: 0 | 1 | 2, deployment_id: Id | none, at_ms: monotonic integer,
             kind: ingress | dispatch | connect_refused | commit | terminal | authority_down,
             outcome: none | done | incomplete | failed, exposure: none | possible | unknown}

  `new/1` accepts a map, keyword list or `%Event{}` with **exactly** these
  atom keys. Anything else is rejected, including extra keys, so there is no
  place for free-form messages, bodies, headers, secrets, URLs or external
  identifiers. `Id` is ASCII `[a-zA-Z0-9_-]{1,64}` (the §1 rule); `at_ms` is a
  signed 64-bit integer (BEAM monotonic time may be negative).

  One cross-field rule is enforced (proposed for day-two review): a
  `:terminal` event must carry outcome `:done | :incomplete | :failed`.

  Error reasons are fixed terms `{:invalid, field}` and never echo the
  rejected value, so a rejected marker cannot leak through an error.

  ## Canonical encoding

  Compact UTF-8 JSON array, version-prefixed, fields in contract order:

      ["telelem.receipt.event/1","local","r1",1,1,"a",12,"connect_refused","none","none"]

  `deployment_id: :none` encodes as `null` (a deployment may legally be
  named `"none"`). The byte size of this encoding is what the store's
  per-event ceiling (512 bytes by default) is checked against.
  """

  @version "telelem.receipt.event/1"
  @fields [
    :tenant_id,
    :request_id,
    :route_rev,
    :attempt,
    :deployment_id,
    :at_ms,
    :kind,
    :outcome,
    :exposure
  ]
  @kinds [:ingress, :dispatch, :connect_refused, :commit, :terminal, :authority_down]
  @outcomes [:none, :done, :incomplete, :failed]
  @exposures [:none, :possible, :unknown]
  @int64_min -0x8000000000000000
  @int64_max 0x7FFFFFFFFFFFFFFF

  @enforce_keys @fields
  defstruct @fields

  @type id :: String.t()
  @type kind :: :ingress | :dispatch | :connect_refused | :commit | :terminal | :authority_down
  @type outcome :: :none | :done | :incomplete | :failed
  @type exposure :: :none | :possible | :unknown
  @type t :: %__MODULE__{
          tenant_id: id(),
          request_id: id(),
          route_rev: pos_integer(),
          attempt: 0 | 1 | 2,
          deployment_id: id() | :none,
          at_ms: integer(),
          kind: kind(),
          outcome: outcome(),
          exposure: exposure()
        }
  @type reason :: {:invalid, atom()}

  @doc "Contract field order."
  def fields, do: @fields

  @doc "Encoding version prefix."
  def version, do: @version

  @doc "True for an ASCII `[a-zA-Z0-9_-]{1,64}` binary."
  @spec valid_id?(term()) :: boolean()
  def valid_id?(id) when is_binary(id) and byte_size(id) in 1..64, do: id_chars?(id)
  def valid_id?(_), do: false

  defp id_chars?(<<>>), do: true

  defp id_chars?(<<c, rest::binary>>)
       when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_ or c == ?-,
       do: id_chars?(rest)

  defp id_chars?(_), do: false

  @doc """
  Validate input into an `%Event{}`. Never raises.
  """
  @spec new(term()) :: {:ok, t()} | {:error, reason()}
  def new(%__MODULE__{} = event), do: event |> Map.from_struct() |> new()

  def new(input) when is_list(input) do
    if Keyword.keyword?(input) and length(input) == length(Enum.uniq_by(input, &elem(&1, 0))),
      do: new(Map.new(input)),
      else: {:error, {:invalid, :shape}}
  end

  def new(input) when is_map(input) and not is_struct(input) do
    cond do
      map_size(input) != length(@fields) or not Enum.all?(@fields, &Map.has_key?(input, &1)) ->
        {:error, {:invalid, :shape}}

      true ->
        validate(input)
    end
  end

  def new(_), do: {:error, {:invalid, :shape}}

  defp validate(m) do
    with :ok <- check(:tenant_id, valid_id?(m.tenant_id)),
         :ok <- check(:request_id, valid_id?(m.request_id)),
         :ok <-
           check(
             :route_rev,
             is_integer(m.route_rev) and m.route_rev > 0 and m.route_rev <= @int64_max
           ),
         :ok <- check(:attempt, m.attempt in [0, 1, 2]),
         :ok <- check(:deployment_id, m.deployment_id == :none or valid_id?(m.deployment_id)),
         :ok <-
           check(:at_ms, is_integer(m.at_ms) and m.at_ms >= @int64_min and m.at_ms <= @int64_max),
         :ok <- check(:kind, m.kind in @kinds),
         :ok <- check(:outcome, m.outcome in @outcomes),
         :ok <- check(:exposure, m.exposure in @exposures),
         :ok <- check(:outcome, m.kind != :terminal or m.outcome != :none) do
      {:ok, struct!(__MODULE__, m)}
    end
  end

  defp check(_field, true), do: :ok
  defp check(field, _), do: {:error, {:invalid, field}}

  @doc """
  Best-effort `{tenant_id, request_id}` of rejected input, used only to
  attribute loss to a receipt. Returns `:unattributed` unless both ids are
  valid.
  """
  @spec key_of(term()) :: {id(), id()} | :unattributed
  def key_of(input) do
    {t, r} =
      case input do
        %{tenant_id: t, request_id: r} ->
          {t, r}

        l when is_list(l) ->
          if Keyword.keyword?(l), do: {l[:tenant_id], l[:request_id]}, else: {nil, nil}

        _ ->
          {nil, nil}
      end

    if valid_id?(t) and valid_id?(r), do: {t, r}, else: :unattributed
  end

  @doc "Canonical compact JSON encoding (see moduledoc)."
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{} = e) do
    JSON.encode!([
      @version,
      e.tenant_id,
      e.request_id,
      e.route_rev,
      e.attempt,
      if(e.deployment_id == :none, do: nil, else: e.deployment_id),
      e.at_ms,
      Atom.to_string(e.kind),
      Atom.to_string(e.outcome),
      Atom.to_string(e.exposure)
    ])
  end
end
