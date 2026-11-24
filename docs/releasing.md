# CI and source releases

M1 is tagged `m1-experiment`. It is an experiment checkpoint, not a production release. A source release packages a pinned commit and checksums; it does not imply deployable binaries, supported providers or production readiness.

## Checks

| Check | What it establishes |
| --- | --- |
| Locked setup and compile | Repeatable inputs and compiler diagnostics treated as failures. |
| Elixir formatting, compile graph and tests | Consistent formatting, inspectable compile dependencies, and ExUnit unit/property coverage. The compile graph is not a type checker. |
| Python fixture tests and real-SDK launcher | TLS, independent lane counters, and the expected SDK outcomes; default ExUnit skips aren't wire passes. |
| Security workflow | Dependency and secret checks as configured. Read the job summary for tool coverage and distinguish a scanner failure from a clean report. |
| Release packaging | The same reviewed commit is used for checks, the source archive and its SHA-256 manifest. |

The workflow files are the source of truth for events, pins, commands and permissions. Dependency and action pins should be intentionally reviewed when changed. Keep PR jobs free of credentials and do not add `pull_request_target` jobs that execute contributor code.

Dialyzer (success typing), a style linter such as Credo, and container/base-image vulnerability coverage are distinct checks. If not present in the workflows, they are **not active**. Add them under the existing dependency-pin policy; address actionable warnings or record a narrowly scoped, expiring exception. A report without a working scanner is not a passing check.

## Publish deliberately

1. Finish the milestone and confirm the supported capability, versions and limitations in the README and release notes. A passing pipeline does not authorize release.
2. After review, create a version tag pointing to the intended commit on `main`. Run the manual release workflow for that version. Never move an existing published tag to rerun checks.
3. Inspect the release, commit/tag, checksums and attached source archive. Verify the archive with `sha256sum -c SHA256SUMS` from the directory containing the assets. Upload no raw test evidence, keys or private planning files.

Confirm the project's license before inviting reuse or calling a public source release open source. Public visibility is not a license grant.

## One-time GitHub settings to verify

- Require review and the stable CI and security checks on `main`; disallow force-pushes and branch deletion. A workflow file alone cannot enforce branch protection.
- Restrict release-tag creation and the publish environment to maintainers where available. Keep the workflow default token read-only; grant release writes only to the publication job.
- Enable private vulnerability reporting, Dependabot alerts and security updates, and secret scanning with push protection where available. Check which ecosystems GitHub recognizes rather than assuming `mix.lock` is covered. Review available code-scanning and dependency-graph support separately.

`dependabot.yml` requests weekly pull requests for GitHub Actions pins. It does not enable alerts, change repository settings, or bypass dependency review.
