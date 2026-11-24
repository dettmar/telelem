defmodule Telelem.Sandbox.VerifiedTarget do
  @moduledoc """
  Contract §3 `VerifiedTarget`: one explicit fixture destination for a
  deployment ID, with its own fixture CA. Produced only by
  `Telelem.Sandbox.Bootstrap`; `check/2` re-validates on every resolve.

  Policy for this slice (fail closed, day-two review point):

    * `https` only; `host` is an IP literal on loopback (`127.0.0.0/8`, `::1`)
      in canonical form (`:inet.ntoa/1` of `address`, e.g. `"::1"`, never
      `"0:0:0:0:0:0:0:1"`). No hostnames, so a sandbox target never needs DNS,
      and the exact IP the verifier enrolls is the one the client dials.
    * `port` is an integer in `1..65535`.
    * `cacerts` is the fixture CA set: 1..8 distinct, canonically DER-encoded
      certificates, each with the CA basic constraint. `ca_sha256` is their
      lowercase-hex SHA-256 in the same order, for verifier enrollment. There
      is no fallback to the OS trust store: `tls_opts/1` passes `cacerts:`
      only.

  `check/2` accepts only a `%VerifiedTarget{}` whose every field satisfies the
  above; anything else (wrong struct, extra or wrong-typed fields) is denied.
  """

  require Record

  Record.defrecordp(
    :otp_cert,
    :OTPCertificate,
    Record.extract(:OTPCertificate, from_lib: "public_key/include/public_key.hrl")
  )

  Record.defrecordp(
    :otp_tbs,
    :OTPTBSCertificate,
    Record.extract(:OTPTBSCertificate, from_lib: "public_key/include/public_key.hrl")
  )

  alias Telelem.Sandbox.Id

  @basic_constraints {2, 5, 29, 19}

  @enforce_keys [:deployment_id, :scheme, :host, :address, :port, :cacerts, :ca_sha256]
  defstruct @enforce_keys
  @all_keys Enum.sort([:__struct__ | @enforce_keys])

  @type t :: %__MODULE__{
          deployment_id: String.t(),
          scheme: :https,
          host: String.t(),
          address: :inet.ip_address(),
          port: 1..65535,
          cacerts: [binary()],
          ca_sha256: [String.t()]
        }

  @doc "Build from already-decoded parts; returns the same errors `check/2` would."
  @spec build(String.t(), String.t(), term(), [binary()]) :: {:ok, t()} | {:error, atom()}
  def build(id, host, port, cacerts) do
    with {:ok, addr} <- loopback(host) do
      t = %__MODULE__{
        deployment_id: id,
        scheme: :https,
        host: host,
        address: addr,
        port: port,
        cacerts: cacerts,
        ca_sha256: Enum.map(List.wrap(cacerts), &sha256/1)
      }

      with :ok <- check(t, id), do: {:ok, t}
    end
  end

  @doc "Re-validate a target bound under `id`. Pure; no I/O."
  @spec check(term(), String.t()) :: :ok | {:error, atom()}
  def check(%__MODULE__{} = t, id) do
    cond do
      Enum.sort(Map.keys(t)) != @all_keys -> {:error, :target_unverified}
      not Id.valid?(id) or t.deployment_id != id -> {:error, :target_id_mismatch}
      t.scheme != :https -> {:error, :scheme_not_https}
      loopback(t.host) != {:ok, t.address} -> {:error, :non_loopback_host}
      not (is_integer(t.port) and t.port in 1..65535) -> {:error, :invalid_port}
      not ca_set?(t.cacerts) -> {:error, :invalid_fixture_ca}
      t.ca_sha256 != Enum.map(t.cacerts, &sha256/1) -> {:error, :ca_digest_mismatch}
      true -> :ok
    end
  end

  def check(_, _), do: {:error, :target_unverified}

  @doc "TLS options for Mint/`:ssl`: fixture CA only, peer verification on."
  @spec tls_opts(t()) :: keyword()
  def tls_opts(%__MODULE__{cacerts: cacerts}) do
    [cacerts: cacerts, verify: :verify_peer, versions: [:"tlsv1.3", :"tlsv1.2"]]
  end

  @doc false
  def loopback(host) when is_binary(host) and byte_size(host) in 1..45 do
    with true <- String.valid?(host),
         {:ok, a} <- :inet.parse_strict_address(String.to_charlist(host)) do
      cond do
        not loopback_addr?(a) -> {:error, :non_loopback_host}
        List.to_string(:inet.ntoa(a)) != host -> {:error, :non_canonical_host}
        true -> {:ok, a}
      end
    else
      _ -> {:error, :host_not_ip_literal}
    end
  end

  def loopback(_), do: {:error, :host_not_ip_literal}

  defp loopback_addr?({127, _, _, _}), do: true
  defp loopback_addr?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  defp loopback_addr?(_), do: false

  @doc false
  def ca_set?(ders) when is_list(ders) and length(ders) in 1..8,
    do: Enum.all?(ders, &ca_cert?/1) and length(Enum.uniq(ders)) == length(ders)

  def ca_set?(_), do: false

  @doc false
  def ca_cert?(der) when is_binary(der) do
    # Canonical DER only: no trailing bytes, no alternative encodings.
    true = :public_key.der_encode(:Certificate, :public_key.pkix_decode_cert(der, :plain)) == der
    otp_cert(tbsCertificate: tbs) = :public_key.pkix_decode_cert(der, :otp)
    exts = otp_tbs(tbs, :extensions)

    is_list(exts) and
      Enum.any?(exts, fn
        {:Extension, @basic_constraints, _crit, {:BasicConstraints, true, _}} -> true
        _ -> false
      end)
  rescue
    _ -> false
  end

  def ca_cert?(_), do: false

  defp sha256(der) when is_binary(der),
    do: Base.encode16(:crypto.hash(:sha256, der), case: :lower)

  defp sha256(_), do: ""
end
