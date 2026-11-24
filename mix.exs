defmodule Telelem.MixProject do
  use Mix.Project

  # Toolchain is pinned by the runner image (scripts/Dockerfile):
  # Elixir 1.20.5 / OTP 28.5.0.8. See docs/runner.md.
  def project do
    [
      app: :telelem,
      version: "0.1.0",
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto, :public_key, :ssl]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Exact pins; transitive versions and checksums live in mix.lock.
  # Cooldown (same policy as fixtures/wire): only Hex releases published
  # before 2026-11-17T00:00:00Z, except security fixes. Plug is pinned directly
  # to 1.20.4 (2026-11-17), which fixes EEF-CVE-2026-120287 in 1.20.3.
  defp deps do
    [
      {:mint, "== 1.11.0"},
      {:bandit, "== 1.14.1"},
      {:plug, "== 1.20.4"},
      {:stream_data, "== 1.4.0", only: [:dev, :test]}
    ]
  end
end
