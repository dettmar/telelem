# CI and source releases

M1 is tagged `m1-experiment`. It is an experiment checkpoint, not a production release. A source release packages a pinned commit and checksums; it does not imply deployable binaries, supported providers or production readiness.

## Checks

| Check | What it establishes |
| --- | --- |
| Locked setup and compile | Repeatable inputs and compiler diagnostics treated as failures. |
| Elixir formatting, compile graph and tests | Consistent formatting, inspectable compile dependencies, and ExUnit unit/property coverage. The compile graph is not a type checker. |
| Python fixture tests and real-SDK launcher | TLS, independent lane counters, and the expected SDK outcomes; default ExUnit skips aren't wire passes. |
| Security workflow | Dependency and secret checks as configured. Read the job summary for tool coverage and distinguish a scanner failure from a clean report. |
| Release rehearsal | The same tagged commit is used for checks and source packaging. A read-only verifier downloads the exact artifact by ID, checks the files and hashes, and confirms the tag has not moved at verification time. |

The workflow files are the source of truth for events, pins, commands and permissions. Dependency and action pins should be intentionally reviewed when changed. Keep PR jobs free of credentials and do not add `pull_request_target` jobs that execute contributor code.

Run `scripts/dialyzer` after setup for offline success-typing diagnostics. The M1 baseline currently reports two warnings in `wire/upstream.ex`: an overflow helper that intentionally does not return, and an error fallback the pinned Mint return type makes unreachable. Dialyzer exits `2` for warnings and `1` for a tool error. It is not a required check yet; review those warnings before making it one.

A style linter such as Credo and container/base-image vulnerability coverage are separate checks. If not present in the workflows, they are **not active**. Add them under the existing dependency-pin policy. A report without a working scanner is not a passing check.

## Rehearse before publishing

The manual workflow is **dry-run only**. It cannot create tags, drafts or releases. No GitHub job has release write permission.

1. Finish review and document the supported capability, versions and limits. With publication approval, create a `vX.Y.Z` tag matching `mix.exs` on the intended commit on `main`, then run the workflow from `main`. Never move an existing tag to rerun checks.
2. Download the artifact for that exact run and attempt. Run `sha256sum --strict -c SHA256SUMS` beside the assets; compare the hashes with the verify-job summary and the commit with the tagged commit.
3. Before creating a draft or publishing it, confirm the version tag is protected against updates and deletion with no bypass and still points to that commit. The [workflow header](../.github/workflows/release.yml) documents the maintainer handoff. Do not attach raw test evidence, keys or private planning files.

Confirm the project's license before inviting reuse or calling a public source release open source. Public visibility is not a license grant.

## One-time GitHub settings to verify

- Require review and the stable CI and security checks on `main`; disallow force-pushes and branch deletion. A workflow file alone cannot enforce branch protection.
- Require a no-bypass ruleset against updating or deleting version tags, restrict their creation to maintainers, and consider immutable releases. Keep GitHub Actions read-only; automating draft creation would require a separate reviewed change.
- Enable private vulnerability reporting, Dependabot alerts and security updates, and secret scanning with push protection where available. Check which ecosystems GitHub recognizes rather than assuming `mix.lock` is covered. Review available code-scanning and dependency-graph support separately.

`dependabot.yml` requests weekly pull requests for GitHub Actions pins. It does not enable alerts, change repository settings, or bypass dependency review.
