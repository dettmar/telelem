defmodule Telelem.Instance.Transport do
  @moduledoc """
  Injected transport for an instance's workers (TEL-002). Configure it as
  `{module, arg}`.

  `checkout/2` runs **in the worker process**, so the worker owns any socket
  or port it opens. A brutal kill of the worker still closes that socket;
  `close/1` is the orderly path from `terminate/2`. Real Mint/Bandit ownership
  belongs to TEL-005.
  """

  @callback checkout(arg :: term(), meta :: map()) :: {:ok, conn :: term()} | {:error, term()}
  @callback dispatch(conn :: term(), request :: term()) :: :ok | {:error, term()}
  @callback close(conn :: term()) :: :ok
end
