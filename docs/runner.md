# Test runner (TEL-001)

All Elixir work runs through one command, which passes its arguments to `mix`:

```sh
scripts/mix <any mix args>      # e.g. scripts/mix test --seed 0 test/foo_test.exs
```

By default it runs in a pinned Docker image with **no network access** (`--network none`, `HEX_OFFLINE=1`), as your host uid, with the checkout mounted at `/work`. No Elixir is needed on the host.

## Two phases

| Phase | Command | Network | What it does |
| --- | --- | --- | --- |
| 1. Dependency setup (once per checkout or per lock change) | `scripts/setup` | yes | Builds the runner image (pulls the pinned base if missing and installs Hex 2.7.0), runs `mix deps.get --check-locked` (fails if `mix.lock` would change), then compiles deps for `dev` and `test` offline and prints the versions it loaded. |
| 2. Test run | `scripts/mix test` | **none** | Compiles and tests offline. Needs no credentials, external inference or provider. Reproduce a run with `--seed N`. |

### Runner smoke test

```sh
scripts/mix test test/runner_smoke_test.exs --seed 0 --warnings-as-errors
```

This test checks that the pinned socket stack works. It does not test wire or SDK behaviour; that belongs to the fixture tests.

- **Setup:** it generates a throwaway CA and certificates in memory for each run, so no key files exist on disk.
- **Server:** it starts Bandit over HTTPS on `127.0.0.1` with an ephemeral port. Bandit starts under `start_supervised!`, so the listener is bound before the test begins and is stopped when the test ends.
- **Client:** Mint connects in passive mode with certificate checking on, and it is closed explicitly.
- **Checks:** the connection uses TLS 1.3, the response is a chunked 200 with the expected body, and a client that doesn't trust the CA is refused with `unknown_ca`.
- **Expected output:** `2 passed` in about 0.5 s. It needs no network access or credentials.

If phase 2 runs before phase 1, it fails rather than fetching anything: either the image is missing or deps are absent and Hex is offline.

If `--check-locked` fails, Mix may already have fetched the newer package into `deps/`. Restore `mix.exs`, then rerun `scripts/setup`, which brings `deps/` back to the lock.

Maintainers who change deps: edit `mix.exs`, run `scripts/setup --relock`, review the `mix.lock` diff, then run `TELELEM_NETWORK=bridge scripts/mix hex.audit`.

## Pins (actual, as reported by `MIX_ENV=test scripts/mix run --no-start scripts/versions.exs`)

| Component | Version |
| --- | --- |
| Base image | `elixir:1.20.5-otp-28@sha256:9deb9fe8bf7c8cfa02bd3b7b6b1932fbb9c313151eb1e76eba112fdde7628337` (Debian 13 trixie, linux/amd64 verified) |
| Elixir / OTP / ERTS | 1.20.5 / 28.5.0.8 / 16.4.0.7 |
| Hex / rebar3 | 2.7.0 / 3.27.0 (from the image) |
| TLS | OTP `ssl` 11.6.0.7, `public_key` 1.20.3.6, `crypto` 5.8.3.3 linked to OpenSSL 3.5.7 (Debian `libssl3t64` 3.5.7-1~deb13u3); TLS 1.3 and 1.2 enabled by default |
| Trust store | OS store through `:public_key.cacerts_get/0` (Debian `ca-certificates` in the image, 148 certs). No `castore`. Local fixture tests should pass their own CA (`cacerts:`/`cacertfile:`) instead of using the OS store. |
| Direct deps | `mint` 1.11.0, `bandit` 1.14.1, `plug` 1.20.4, `stream_data` 1.4.0 (`:dev`/`:test` only), all exact `==` pins |
| Transitive (`mix.lock`) | `thousand_island` 1.6.0, `hpax` 1.1.0, `telemetry` 1.4.3, `websock` 0.5.3, `mime` 2.0.7, `plug_crypto` 2.2.1. That makes 10 Hex packages in total. |

Dependency policy: we use the same supply-chain cooldown as the fixture (`exclude-newer = 2026-11-17T00:00:00Z`) and allow only Hex releases published before it, except for security fixes. The one exception is `plug` 1.20.4 (published 2026-11-17), because 1.20.3 is affected by EEF-CVE-2026-120287 (multipart length limit). `bandit` 1.15.0 is newer than the cutoff, so it is not used.

The image's `apt` packages are not upgraded at build time, so OS libraries change only when the base digest changes.

## Options

| Variable | Values | Meaning |
| --- | --- | --- |
| `TELELEM_RUNNER` | `auto` (default), `docker`, `local` | `auto` uses Docker if a daemon answers, otherwise `mix` on `PATH`. `local` warns if the toolchain isn't 1.20.5 / OTP 28. It sets only `HEX_OFFLINE=1` and cannot remove network access. |
| `TELELEM_NETWORK` | `none` (default), `host`, `bridge` | `host` lets tests reach loopback fixtures started on the host (Hex stays offline). `bridge` is for fetching deps only. |
| `TELELEM_IMAGE` | tag | Runner image tag. Default: `telelem-mix:elixir-1.20.5-otp-28.5.0.8-hex-2.7.0`. |
| passthrough | `MIX_ENV`, `ERL_AFLAGS`, `ELIXIR_ERL_OPTIONS`, `TELELEM_TEST_*` | Forwarded into the container. Nothing else from the host environment is forwarded, so host provider keys never reach tests. |

Per-checkout state lives in `deps/`, `_build/` and `.cache/` (container `HOME` and Hex cache). All three are git-ignored, and deleting them returns the checkout to a clean state.

## Python wire fixture (TEL-004)

The fixture is owned separately in `fixtures/wire/`. It runs on the host with `uv` and Python 3.13. It is not part of this image. Its environment is pinned by `fixtures/wire/uv.lock`, which currently records `openai` 3.52.0, `cryptography` 50.0.2 and `pytest` 9.2.0. The fixture's README documents its setup and run commands, and those are the authoritative ones. Elixir tests that talk to a running fixture use `TELELEM_NETWORK=host scripts/mix test ...`.
