# Prints the toolchain, TLS stack and dependency versions actually loaded.
# Run offline:  MIX_ENV=test scripts/mix run --no-start scripts/versions.exs
otp_version =
  [:code.root_dir(), "releases", System.otp_release(), "OTP_VERSION"]
  |> Path.join()
  |> File.read!()
  |> String.trim()

vsn = fn app ->
  _ = Application.load(app)

  case Application.spec(app, :vsn) do
    nil -> "not available"
    v -> List.to_string(v)
  end
end

[{_, _, openssl}] = :crypto.info_lib()

runtime = [
  {"elixir", System.version()},
  {"otp", otp_version},
  {"erts", List.to_string(:erlang.system_info(:version))},
  {"ssl", vsn.(:ssl)},
  {"public_key", vsn.(:public_key)},
  {"crypto", vsn.(:crypto)},
  {"openssl", openssl},
  {"tls versions (default)", inspect(:ssl.versions()[:supported])},
  {"os cacerts", "#{length(:public_key.cacerts_get())} certs via :public_key.cacerts_get/0"}
]

deps =
  for {app, _} <- Mix.Dep.Lock.read() |> Enum.sort() do
    {Atom.to_string(app), vsn.(app)}
  end

for {section, rows} <- [{"runtime", runtime}, {"deps (loaded from _build)", deps}] do
  IO.puts("## #{section}")
  for {k, v} <- rows, do: IO.puts("  #{String.pad_trailing(k, 24)} #{v}")
end
