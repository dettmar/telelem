defmodule Telelem.Sandbox.Id do
  @moduledoc """
  Contract §1/§3 identifier: ASCII `[a-zA-Z0-9_-]{1,64}`.

  Local copy for the sandbox lane so this boundary has no dependency on the
  route lane's module; the orchestrator may fold both into one at day two.
  """

  @spec valid?(term()) :: boolean()
  def valid?(id) when is_binary(id) and byte_size(id) in 1..64, do: chars?(id)
  def valid?(_), do: false

  defp chars?(<<>>), do: true

  defp chars?(<<c, rest::binary>>)
       when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_ or c == ?-,
       do: chars?(rest)

  defp chars?(_), do: false
end
