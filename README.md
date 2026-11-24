# telelem

telelem is a proposed Elixir/OTP gateway for LLM requests. Its first goal is to let a developer run their own SDK against two local deployments, interrupt a stream, and see exactly what the gateway did and what the client received.

**Status: M1 engineering gate passed.** [Rerun the real-SDK experiment](docs/m1-results.md) to see why the gateway switches deployments on a refused connection but reports an interrupted committed stream to the caller. The [engineering roadmap](docs/roadmap.md) tracks the work beyond the experiment.
