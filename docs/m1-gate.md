# M1 acceptance card

Use this card to review a pinned product checkout. A reviewer should see the acceptance conditions before seeing an implementer’s results. Record the actual commands, build and SDK versions, status and smallest failing trace. Build/download dependencies separately; run the experiment with local endpoints and no inference credentials.

| Case | Independent observation |
| --- | --- |
| Two instances | Hold work in A; B serves the same logical scope. Kill A's authority and deliver an old callback. Identify the old PID, restarted descendants and what was closed; B keeps its identities and state. |
| Election and death | Force each order of two ready attempts. Kill the requester before election, the winner during transfer and the authority after commit. Distinguish release of occupancy from refund of possibly billed work. A failed downstream write cannot reopen election. |
| Refusal before connection | The client sends one ingress call, with SDK retries disabled. Show proof of refused connect on the primary, **zero** arrivals there, **one** backup arrival, and a complete SDK terminal. Distinguish connect attempts from upstream arrivals. |
| Reset after commit | Observe a tool argument fragment in the public SDK iterator before an incomplete termination. One primary arrival, zero backup arrivals, no synthetic success terminator. HTTP status alone does not pass. |
| Slow consumer and cancellation | Record the enforced application buffer/credit limits and overflow action **before** running. Stop reading until pressure reaches the gateway, then disconnect. Measure peak retained application bytes or bounded outstanding credit with a size bound, time from detected close to local cleanup, and remaining workers/sockets. A kernel socket buffer is not an application-buffer measurement; local close does not prove remote work or billing stopped. |
| Admission model | Compare externally observable grants, occupancy and unresolved exposure after every generated transition. Include duplicate operations, owner death and stale work; retain a seed for replay and shrink failures. |

The independent reviewer marks each case *reproduced*, *failed* or *not observed*. The PM may choose the proven handoff or a proven credit-driven reader; unbounded buffering, missing SDK evidence, or fallback after commit keeps the gate open. Passing M1 is not a provider compatibility or performance claim.
