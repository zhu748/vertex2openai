"""POST /v1/responses: stateless text/function generation and buffered SSE."""

import asyncio
from dataclasses import dataclass, field
from functools import partial
import hashlib
import json

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import ValidationError
from starlette.background import BackgroundTask
from starlette.requests import ClientDisconnect

from admission import GenerationCapacityError, ManagedStreamingResponse, SETUP_FAST_WINDOW_SECONDS
from auth import get_api_key
from json_encoding import json_response_offloop
from logger import defer_producer_outcomes, stats
from media_tasks import run_media_task
from request_connection import ClientConnection
from request_errors import MAX_RESPONSES_REQUEST_BYTES, body_too_large_response, ingress_error_response
from request_ingress import hold_ingress
from request_tracking import configure as track_configure, current_trace
from response_validation import IncompleteResponseError
from responses_protocol import (
    MAX_RESPONSE_BYTES,
    ResponsesEvents,
    ResponsesRequestError,
    completion_to_response,
    parse_responses_request,
)
from retry_policy import advisory_headers, bounded_sse_advisory, response_retry_after
from routes.chat_api import call_upstream
from runtime_state import app_state
from schema_validation import SchemaValidationError, preflight_schema_contracts, validate_request_schemas
from signature_origin import (
    OriginBatch,
    bind_signature_origins,
    publish_completed_origins,
    publish_responses_item_origins,
)
from stream_cleanup import await_owned_close
from streaming import SSE_NO_BUFFER_HEADERS, close_stream_response
from tool_policy import validate_parallel_tool_calls
from upstream_errors import create_openai_error_response, extract_upstream_error


router = APIRouter()
IDLE_KEEPALIVE_SECONDS = 10.0
_PARSE_INLINE_BYTES = 64 * 1024


@dataclass
class _Result:
    content: dict
    status: int = 200
    headers: dict = field(default_factory=dict)
    proof: dict | None = None
    destination: bytes | None = None
    diagnostic_fallback: bool = False


def _failure(status, message, *, param=None, error_type="upstream_error", headers=None):
    payload = create_openai_error_response(status, message[:1024], error_type)
    payload["error"]["param"] = param
    return _Result(payload, status=status, headers=headers or {})


def _invalid_json(constant):
    raise ValueError("Non-finite JSON constant")


def _parse_request(raw):
    try:
        body = json.loads(raw, parse_constant=_invalid_json)
    except (ValueError, UnicodeError, RecursionError):
        raise ResponsesRequestError("body", "invalid JSON request body") from None
    parsed = parse_responses_request(body)
    validate_parallel_tool_calls(parsed.chat)
    validate_request_schemas(parsed.chat)  # inexpensive shape checks are still before SSE
    preflight_schema_contracts(parsed.chat)
    return parsed


def _parse_response(raw, status, hint, base):
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_RESPONSE_BYTES:
        if status >= 400:
            return _failure(status, f"Upstream generation request failed (HTTP {status}).",
                            headers={"retry-after": hint} if hint else None)
        raise IncompleteResponseError("Upstream completion was empty or exceeded the 8 MiB Responses limit.")
    try:
        payload = json.loads(raw)
    except (UnicodeError, ValueError, RecursionError):
        if status >= 400:
            return _failure(status, f"Upstream generation request failed (HTTP {status}).",
                            headers={"retry-after": hint} if hint else None)
        raise IncompleteResponseError("Upstream completion was not a valid JSON response.") from None
    error = payload.get("error") if isinstance(payload, dict) else None
    if status >= 400 or error:
        status = status if status >= 400 else 502
        message = error.get("message") if isinstance(error, dict) else None
        error_type = error.get("type") if isinstance(error, dict) else None
        return _failure(status, message if isinstance(message, str) and message else
                        f"Upstream generation request failed (HTTP {status}).",
                        error_type=error_type if error_type in ("invalid_request_error", "client_closed_request",
                                                               "authentication_error", "rate_limit_error") else "upstream_error",
                        headers={"retry-after": hint} if hint else None)
    if not 200 <= status < 300:
        raise IncompleteResponseError("Upstream returned an unexpected response status.")
    content, proof = completion_to_response(payload, base)
    return _Result(content, proof=proof)


async def _generate(parsed, fastapi_request):
    destination = None

    def accepted(value):
        nonlocal destination
        destination = value

    # The internal JSON producer can retry, rotate or switch channels. Only
    # its accepted attempt reports a destination, and only the Responses
    # projection can publish the public tool turn after it was delivered.
    parsed.chat._signature_output_destination = accepted
    try:
        # Internal attempt outcomes, including a losing channel's proposal,
        # cannot settle the public Responses request. Token totals still accrue.
        with defer_producer_outcomes():
            source = await call_upstream(parsed.chat, fastapi_request)
            if isinstance(source, StreamingResponse):
                # The dispatcher was asked for complete JSON. Never leave a
                # wrongly returned live owned stream or invent a successful body.
                await await_owned_close(close_stream_response(source))
                raise IncompleteResponseError("Expected a complete upstream JSON response.")
        raw = getattr(source, "body", None)
        status, hint = source.status_code, response_retry_after(source)
        if isinstance(raw, bytes) and _PARSE_INLINE_BYTES < len(raw) <= MAX_RESPONSE_BYTES:
            result = await run_media_task(_parse_response, raw, status, hint, parsed.base, heavy=False)
        else:
            result = _parse_response(raw, status, hint, parsed.base)
        result.destination = destination
        result.diagnostic_fallback = getattr(source, "_chat_diagnostic_fallback", False) is True
        return result
    except Exception as exc:
        code, message = extract_upstream_error(exc)
        return _failure(code, message, headers=advisory_headers(exc))


async def _publish(prefix, proof, connection, destination):
    if not prefix or not proof or (connection is not None and not connection.delivery_possible):
        return

    def publish(_finish, keys, signatures, accepted_destination):
        publish_responses_item_origins(keys, signatures, accepted_destination, proof["item_witness"])

    if proof["withheld"]:
        message = proof["message"]
        batch = OriginBatch(prefix)
        # This shares the exact visible prefix, order and same proof budgets
        # as successful replay, without retaining any source signature bytes.
        batch.withhold()
        batch.text.feed(message.get("content"))
        batch.reasoning.feed(message.get("reasoning_content"))
        for call in message["tool_calls"]:
            batch.add(call)
        batch.finish(stage_origin=publish, destination=destination)
    elif destination is not None:
        publish_completed_origins(prefix, (proof,), stage_origin=publish, destination=destination)


async def _finish_delivery(prefix, proof, connection, destination, diagnostic_fallback):
    if connection is not None and not connection.delivery_possible:
        return
    if prefix and proof:
        await _publish(prefix, proof, connection, destination)
    if diagnostic_fallback:
        stats.add_error()
    else:
        stats.add_success()


async def _as_json(result, prefix, connection):
    # HTTP errors are counted by the middleware. For 2xx, success (including
    # a valid incomplete envelope) requires a verified final ASGI send.
    background = (BackgroundTask(_finish_delivery, prefix, result.proof, connection, result.destination,
                                 result.diagnostic_fallback) if result.status < 400 else None)
    return await json_response_offloop(result.content, status_code=result.status, headers=result.headers,
                                       background=background)


def _job_result(job):
    if job.cancelled():
        return _failure(502, "Upstream generation ended without a response.")
    return job.result()


async def _stream(parsed, fastapi_request, prefix, connection):
    # One producer, owned by this connection. There are no replay entries or
    # detached generations. The lease is inside the shared dispatcher.
    job = hold_ingress(asyncio.create_task(_generate(parsed, fastapi_request)))

    async def stop():
        if not job.done() and not job.cancelling():
            job.cancel()
        await asyncio.gather(job, return_exceptions=True)

    delivery = None

    async def events():
        nonlocal delivery
        writer = ResponsesEvents()
        for kind in ("response.created", "response.in_progress"):
            for chunk in writer.event(kind, response=parsed.base):
                yield chunk
        while not job.done():
            done, _ = await asyncio.wait({job}, timeout=IDLE_KEEPALIVE_SECONDS)
            if not done:
                yield b": keepalive\n\n"
        result = _job_result(job)
        if result.status >= 400:
            # The HTTP status is already committed. Keep the original hint in
            # an optional comment, never as SSE's automatic retry instruction.
            error = result.content["error"]
            event_code = ("rate_limit_exceeded" if result.status == 429 else
                          "invalid_prompt" if result.status == 400 else "server_error")
            failed = {**parsed.base, "status": "failed",
                      "error": {"code": event_code, "message": error["message"]}}
            if trace := current_trace.get():
                trace.fail(error["message"], result.status)
            if hint := bounded_sse_advisory(result.headers.get("retry-after")):
                yield f": retry-after {hint}\n\n".encode("ascii")
            for chunk in writer.event("response.failed", response=failed):
                yield chunk
            # A renderer failure before this terminal belongs to middleware;
            # a delivered SSE error cannot also be counted as HTTP failure.
            if connection is None or connection.delivery_possible:
                stats.add_error()
        else:
            for chunk in writer.output(result.content):
                yield chunk
            delivery = (result.proof, result.destination, result.diagnostic_fallback)

    async def finish_delivery():
        if delivery is not None:
            proof, destination, diagnostic_fallback = delivery
            await _finish_delivery(prefix, proof, connection, destination, diagnostic_fallback)

    try:
        done, _ = await asyncio.wait({job}, timeout=SETUP_FAST_WINDOW_SECONDS)
        if done:
            result = _job_result(job)
            if result.status >= 400:
                return await _as_json(result, prefix, connection)
        response = ManagedStreamingResponse(events(), media_type="text/event-stream",
                                            headers=SSE_NO_BUFFER_HEADERS, cleanup=stop,
                                            background=BackgroundTask(finish_delivery))
        response.client_connection = connection
        return response
    except BaseException:
        await await_owned_close(stop())
        raise


@router.post("/v1/responses")
async def responses(fastapi_request: Request, api_key: str = Depends(get_api_key)):
    connection = ClientConnection.for_request(fastapi_request)
    try:
        resume = fastapi_request.headers.get("x-resumable", "").strip().lower()
        if any(fastapi_request.headers.get(name, "").strip() for name in
               ("last-event-id", "x-resume-id", "idempotency-key", "x-idempotency-key")) or (
            resume not in ("", "false", "0", "no")
        ) or any(fastapi_request.query_params.get(name, "").strip() for name in ("resume_id", "resume_from")):
            raise ResponsesRequestError("request", "response replay, resumption and idempotency keys are not supported")
        raw = await fastapi_request.body()
        if len(raw) > MAX_RESPONSES_REQUEST_BYTES:
            return body_too_large_response("/v1/responses", MAX_RESPONSES_REQUEST_BYTES)
        if len(raw) > _PARSE_INLINE_BYTES:
            parse = partial(run_media_task, _parse_request, raw, heavy=False)
            parsed = await connection.while_connected(parse) if connection is not None else await parse()
        else:
            parsed = _parse_request(raw)
        del raw
    except (SchemaValidationError, ValidationError, UnicodeError) as exc:
        message = ("Request contains an invalid field." if isinstance(exc, ValidationError) else str(exc))
        result = _failure(400, message, param=getattr(exc, "param", None), error_type="invalid_request_error")
        return JSONResponse(result.content, status_code=result.status)
    except GenerationCapacityError as exc:
        return ingress_error_response("/v1/responses", 503, str(exc))
    except TimeoutError:
        return ingress_error_response("/v1/responses", 408, "Request parsing took too long.")
    except ClientDisconnect:
        return ingress_error_response("/v1/responses", 499, "Client disconnected.")

    # Domain-separate the normal proof by protocol and authenticated credential.
    # Do not let the internal Chat response register a public Responses receipt.
    scope = "responses:" + hashlib.sha256(api_key.encode("utf-8")).hexdigest()
    bind_signature_origins(parsed.chat, scope)
    prefix, parsed.chat._signature_output_prefix = parsed.chat._signature_output_prefix, None
    track_configure(model=parsed.chat.model, protocol="responses", stream=parsed.stream,
                    channel="mixed" if app_state.get_channel_mode() == "mixed" else
                    "cookie" if app_state.is_web_proxy_enabled() else "express")
    if parsed.stream:
        return await _stream(parsed, fastapi_request, prefix, connection)
    result = await _generate(parsed, fastapi_request)
    return await _as_json(result, prefix, connection)
