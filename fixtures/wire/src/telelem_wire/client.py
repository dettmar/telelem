"""Reference-run SDK client: the pinned ``openai`` package, no SDK retries, fixture-only trust.

Import lazily: the fixture itself does not require the SDK (``pip install telelem-wire[sdk]``).
"""

from __future__ import annotations

import ssl
from typing import Any


def sdk_client(base_url: str, ca_file: str, *, timeout: float = 10.0, api_key: str = "telelem-wire-test-key") -> Any:
    """An ``openai.OpenAI`` client that trusts only the fixture CA, never retries and ignores
    proxy/env settings (``trust_env=False``) so nothing leaves loopback."""
    import openai

    ctx = ssl.create_default_context(cafile=ca_file)  # default CA store NOT loaded: cafile only
    http = openai.DefaultHttpx2Client(verify=ctx, trust_env=False)
    return openai.OpenAI(api_key=api_key, base_url=base_url, max_retries=0, timeout=timeout, http_client=http)


CHAT_ARGS: dict[str, Any] = {
    "model": "telelem-wire-model",
    "messages": [{"role": "user", "content": "What is the weather in Stockholm?"}],
    "tools": [{
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Weather for a city",
            "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
        },
    }],
}


def run_stream(client: Any, **overrides: Any) -> dict[str, Any]:
    """Consume one streaming chat completion and report exactly what the SDK surfaced.

    Returns {"chunks", "content", "tool_args", "finish_reason", "exception": {"type", "mro", "cause", "message"} | None,
             "request_id"}. Never raises for transport errors: those are the observation."""
    args = {**CHAT_ARGS, "stream": True, **overrides}
    out: dict[str, Any] = {"chunks": 0, "content": "", "tool_args": "", "tool_name": None, "finish_reason": None,
                           "exception": None, "request_id": None, "usage": None}
    try:
        stream = client.chat.completions.create(**args)
        out["request_id"] = stream.response.headers.get("x-request-id")
        with stream:
            for chunk in stream:
                out["chunks"] += 1
                if chunk.usage is not None:
                    out["usage"] = chunk.usage.model_dump()
                for ch in chunk.choices:
                    d = ch.delta
                    if d.content:
                        out["content"] += d.content
                    for tc in d.tool_calls or []:
                        if tc.function and tc.function.name:
                            out["tool_name"] = tc.function.name
                        if tc.function and tc.function.arguments:
                            out["tool_args"] += tc.function.arguments
                    if ch.finish_reason:
                        out["finish_reason"] = ch.finish_reason
    except Exception as e:  # the exception IS the observation
        out["exception"] = describe_exception(e)
    return out


def describe_exception(e: BaseException) -> dict[str, Any]:
    chain = []
    cur: BaseException | None = e.__cause__ or e.__context__
    while cur is not None and len(chain) < 6:
        chain.append(f"{type(cur).__module__}.{type(cur).__qualname__}: {cur}")
        cur = cur.__cause__ or cur.__context__
    return {
        "type": f"{type(e).__module__}.{type(e).__qualname__}",
        "message": str(e),
        "mro": [f"{c.__module__}.{c.__qualname__}" for c in type(e).__mro__[:-1]],
        "cause_chain": chain,
    }
