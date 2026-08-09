import asyncio

import httpx
import pytest
from starlette.responses import PlainTextResponse
from starlette.types import Receive, Scope, Send

from perfcho.api.tracing import TraceContextMiddleware
from perfcho.infra.tracing import current_trace_id, new_trace_id, trace_id_from_traceparent


def test_traceparent_parser_accepts_valid_trace_and_rejects_invalid_values() -> None:
    trace_id = "0123456789abcdef0123456789abcdef"

    assert trace_id_from_traceparent(f"00-{trace_id}-0123456789abcdef-01") == trace_id
    assert trace_id_from_traceparent(f"00-{'0' * 32}-0123456789abcdef-01") is None
    assert trace_id_from_traceparent(f"00-{trace_id}-{'0' * 16}-01") is None
    assert trace_id_from_traceparent("invalid") is None
    assert len(new_trace_id()) == 32


@pytest.mark.asyncio
async def test_trace_middleware_returns_trace_header_and_isolates_concurrent_requests() -> None:
    observed: dict[str, str | None] = {}

    async def endpoint(scope: Scope, receive: Receive, send: Send) -> None:
        path = str(scope["path"])
        await asyncio.sleep(0)
        observed[path] = current_trace_id()
        response = PlainTextResponse("ok")
        await response(scope, receive, send)

    app = TraceContextMiddleware(endpoint)
    transport = httpx.ASGITransport(app=app)
    inherited = "0123456789abcdef0123456789abcdef"
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        inherited_response, generated_response = await asyncio.gather(
            client.get("/inherited", headers={"traceparent": f"00-{inherited}-0123456789abcdef-01"}),
            client.get("/generated"),
        )

    assert inherited_response.headers["x-trace-id"] == inherited
    assert observed["/inherited"] == inherited
    generated = generated_response.headers["x-trace-id"]
    assert len(generated) == 32 and generated != inherited
    assert observed["/generated"] == generated
    assert current_trace_id() is None
