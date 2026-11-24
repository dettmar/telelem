# M2a day-zero contracts

These are boundary stubs, not a launch promise. Scope: one native `POST /v1/chat/completions` stream, one node, two same-protocol deployments. Use the M1 chunked HTTP/1.1 terminal rules and pinned Python SDK `openai==3.52.0`, `max_retries=0`. No new dependencies or network access needed for the first assignments. The orchestrator owns this contract and resolves changes at the day-two review.

## 1. Route revision

```text
RouteRevision {route_id: Id, rev: positive integer, deployments: [Id, Id],
               preferred: Id, fallback_on_refusal: boolean,
               deadline_ms: positive integer, max_attempts: 1 | 2, digest: hex SHA-256}
select(revision, eligible: set<Id>, monotonic_now_ms) -> Plan | NoEligibleDeployment
Plan {rev, digest, primary: Id, backup: Id | none, absolute_deadline_ms, max_attempts}
```

Example: `chat, rev 1, [a,b], preferred a, true, 5000ms, 2` with both lanes eligible selects `a`, with `b` available only on positively proved pre-send refusal. Select once per ingress; no reselection after an ambiguous send or commit. The runtime rechecks deadline, remaining attempt budget and the pinned authority before dispatch. Authority loss fences an open committed stream as incomplete and never refunds dispatched exposure. An already recorded valid terminal stays terminal. Digest input is the ordered, version-prefixed list of the revision fields shown above (except the digest), encoded as compact UTF-8 JSON; restrict IDs to ASCII `[a-zA-Z0-9_-]{1,64}`. Keep selection independent of transport, store and global clock.

Use the literal prefix `telelem.route_revision.v1`. Ignore eligibility IDs outside the revision. If the preferred lane is ineligible, the eligible alternate is the initial primary (not a fallback). A backup requires both lanes, refusal permission and a configured budget of two; `Plan.max_attempts` is the effective budget (one when there is no backup). `Revision.with_preferred/2` builds a validated revision successor without a store dependency. An explicit apply advances `rev` once even when the preferred lane is unchanged.

Keep the selector free of socket and worker PIDs. The coordinator binds the plan to grants and attempts locally; the transport owns the socket for its lifetime. Deployment IDs identify destinations, not model identity. These seams preserve future placement and multi-model policy options without adding a hop, pooling, migration or fusion to M2a.

## 2. Receipt event

```text
Event {tenant_id: Id, request_id: Id, route_rev: positive integer,
       attempt: 0 | 1 | 2, deployment_id: Id | none, at_ms: monotonic integer,
       kind: ingress | dispatch | connect_refused | commit | terminal | authority_down,
       outcome: none | done | incomplete | failed, exposure: none | possible | unknown}
```

Example: `{tenant_id: local, request_id: r1, route_rev: 1, attempt: 1, deployment_id: a, at_ms: 12, kind: connect_refused, outcome: none, exposure: none}`. No free-form messages, bodies, headers, secrets, URLs, or external identifiers. Use bounded admission to the evidence queue **before** enqueueing; initial ceilings: 256 pending events, 1,024 retained events, 512 bytes canonical encoding per event. These bound event storage, not VM allocation. Producers must not wait on a full evidence queue. Overflow drops evidence, not the inference request; record a fixed-size global loss indicator/counter, and when data may be missing mark receipts incomplete and absent usage `unknown`. Never infer success from an absent event. Receipt lookup requires the authenticated tenant; allow only matching tenant and request ID. The M1 `Recorder` stays in the reference harness, never on the M2a serving path. Observe through receipts, external fixture counters and, only if necessary, a bounded opt-in observer attached outside serving code and disabled by default.

## 3. Principal and sandbox binding

```text
Principal {tenant_id: Id, subject_id: Id, mode: sandbox | serving, permissions: set<enum>}
Bindings {mode, fixture_targets: map<Id, VerifiedTarget>, sandbox_store_root, sandbox_signer_ref}
resolve(principal, bindings, deployment_id) -> VerifiedTarget | Denied
```

Example: authenticated `local/operator/sandbox` may apply a route revision, read only `local` receipts, and resolve `a` to its explicit fixture target and fixture CA. Never trust tenant from request data. For sandbox, missing or mismatched bindings fail closed; no path to production resolver, secret store or signer. Pass bindings to the same route, grant, transport and operation pipeline used for serving.

**Isolation acceptance, checked outside the gateway.** In a disposable network boundary, the gateway can reach only the two registered fixture targets. An independently observed canary sink beyond that boundary must receive zero gateway connections; an allowed fixture request and a verifier control request to the canary sink prove the observers are working. A missing binding or disallowed target must fail closed with no new lane or sink arrival. Plant distinct, non-secret sentinel production config/key/store files reachable to the verifier; independently audit opens and writes from the gateway and verify content hashes unchanged. Prove the audit sees a deliberate control access. A test environment without a functioning boundary or observers does not pass. Keep all controls on the disposable local runner; no host firewall edits, provider endpoints or actual production secrets.

**Receipt hygiene acceptance.** Put different synthetic markers in request/response bodies and authorization. Independently verify marker delivery at the intended test edge, then verify there are no marker matches in receipt responses, captured event data, logs or retained artifacts. Run a positive scanner control in a disposable file excluded from capture. Test tenant denial and overflow separately.

## 4. Route operation envelope

```text
plan_route(principal, route_id, expected_rev, preferred: Id) -> {expected_rev, plan_digest, proposed}
apply_route(principal, route_id, expected_rev, preferred: Id, plan_digest)
  -> {effective_revision, plan_digest} | Conflict | Denied
read_route(principal, route_id) -> {effective_revision, last_plan_digest}
```

Example: prefer `b` against revision `1`; plan the proposal, apply with the same inputs and digest, read back revision `2`. An old revision conflicts. All entry points call the same handlers. Authenticate the principal outside the handler; require `route_read` to read, and `route_write` to plan or apply. Never accept tenant or permissions from the operation body.

Hash the compact UTF-8 JSON array `["telelem.ops.route_plan/1",tenant_id,route_id,expected_rev,preferred,proposed_revision_digest]` using SHA-256, rendered in lowercase hex. Recompute it on apply and compare-and-write atomically against both the observed revision number and its digest. Apply is not an idempotency API: after a lost reply, read back and compare `last_plan_digest`. A match confirms the current applied plan; a mismatch cannot establish that the write never landed because a later write may have superseded it.

The one editable field is `preferred`; other controls remain fixed in this slice. CLI/JSON first. MCP and the single-field UI wait for day two and may not be dropped to hit the timebox.

## First assignments and ownership

| Lane | Owns | Before day two |
| --- | --- | --- |
| A1 Runtime | `lib/telelem/route/`, `test/telelem/route/`; later, sole writer of `lib/telelem/wire/gateway.ex` and serving wiring | Pure selector and focused tests only. |
| A2 Sandbox | `lib/telelem/sandbox/`, `test/telelem/sandbox/`, `integration/m2a/isolation/` | Bootstrap/binding skeleton and disposable isolation harness with positive controls; no gateway edits. |
| A3 Evidence | `lib/telelem/receipts/`, `test/telelem/receipts/` | Standalone bounded evidence store and tests; no recorder or gateway replacement yet. |
| A4 Operations | `lib/telelem/ops/`, `test/telelem/ops/` | Shared operation handler and JSON CLI adapter; no dependencies, MCP or UI work. |
| A5 Independent verification | Acceptance card outside the implementation paths | Define cold replay, expected invariants and arrival counts; no implementation review or edits in this phase. |

For day two: review the four stubs, confirm their types and examples match the first implementations, require a usable isolation harness and acceptance card, then sequence serving integration. Preserve the original M1 harness as a regression baseline; repeat its SDK/TLS behavior and terminal probes **against the ordinary M2a serving path** for merge. Keep pinned authority PID/`:DOWN`, requester outcome, no replacement dispatch and grant/exposure evidence for the authority-loss case. Count ingress separately from lane arrivals (any SDK re-send is another ingress).
