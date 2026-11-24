"""telelem-wire: TEL-004 counted two-lane loopback TLS SSE wire fixture.

Public API::

    from telelem_wire import WireFixture, fault_spec, sdk_client, run_stream
"""

from .server import FAULT_KINDS, SCHEMA, TOOL_ARGS, TOOL_FRAGMENT, TOOL_NAME, WireFixture, fault_spec

__all__ = ["WireFixture", "fault_spec", "FAULT_KINDS", "SCHEMA", "TOOL_ARGS", "TOOL_FRAGMENT", "TOOL_NAME",
           "sdk_client", "run_stream"]


def __getattr__(name: str):  # SDK helpers import openai lazily
    if name in ("sdk_client", "run_stream"):
        from . import client

        return getattr(client, name)
    raise AttributeError(name)
