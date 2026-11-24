# Working on telelem

Start from the assigned `TEL-…` row in `docs/roadmap.md`. Read the test and the files you own; follow other references when the contract is unclear. Do not ingest a full design history to make a local change.

The orchestrator delegates by failure boundary: runner, independent fixture, supervision/admission, transport ownership. Each task packet gives an observable result, file ownership, constraints, dependency contract and rerun command or expected form of evidence. One writer per path. The orchestrator handles integration and cross-boundary decisions. Separate changes that can proceed in parallel from results that must be verified in dependency order.

Use Opus 5.5 at **high** effort for races, transport behavior, accounting invariants, fault design and independent verification of those results. Use **medium** effort for mechanical setup, documentation or reproducing a well-defined command. Escalate when an ordinary task uncovers an architectural decision. If the requested model/effort is unavailable, report that before substituting for a critical assignment.

Review cold: give a fresh reviewer the pinned checkout, the ticket, the fault and the expected observable result. Let them run it before showing the author's explanation. Report input, command, observations, counterexample or gaps. A passing mock is not evidence about the real socket or SDK. Return short handoffs (changed paths, interface, tests, limits), not transcripts. Keep synthetic test traffic and credentials local.
