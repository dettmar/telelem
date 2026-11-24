# Contributing to telelem

Start with the [README](README.md) for current capabilities. Agents can use [llms.txt](llms.txt) as a short documentation index and [AGENTS.md](AGENTS.md) for instructions when editing the repo. If the roadmap and the code disagree, report the difference; don't promise an unimplemented command or endpoint.

## From a fresh Linux checkout

Install Docker and `uv` with Python 3.13 support. No provider credentials are needed. The setup commands can fetch the pinned runner and locked dependencies; subsequent test commands use the pinned tools and do not call inference providers.

```sh
export TELELEM_RUNNER=docker
scripts/setup
(cd fixtures/wire && uv sync --locked)

scripts/mix format --check-formatted
MIX_ENV=test scripts/mix compile --force --warnings-as-errors
scripts/mix test --seed 424242 --warnings-as-errors
(cd fixtures/wire && uv run --locked --offline pytest -q)
integration/m1/run
```

The default ExUnit suite **skips** the real-SDK cases. Run the last command to exercise them: it uses loopback-only SDK and fixture endpoints plus the gateway's real HTTP/TLS stack. The evidence directory it prints is local and ignored by Git. Do not commit raw evidence, generated TLS material, provider credentials or customer payloads.

For smaller changes, run the relevant test file with `scripts/mix test PATH --seed 424242 --warnings-as-errors`. Use the [runner guide](docs/runner.md) to distinguish a failed dependency setup from a failed test.

## Make a change easy to verify

- Put the expected behavior and a reproducible check in the PR. Name the relevant `TEL-…` work item if there is one.
- For changes in admission, routing or transport, include a negative test and say what the SDK and independent upstream actually observe. Separate actual wire evidence from mocks.
- For public commands, document one working example, required permissions or inputs, and what failure looks like. Agent-facing instructions and human docs should point to the same command or operation, not divergent wrappers.
- Explain any skips or unsupported behavior; don't count them as passes. Keep lockfile changes intentional and reviewable.

Report suspected vulnerabilities as described in [SECURITY.md](SECURITY.md), not in ordinary PRs or public issues.
