"""Bounded Cookie health probes, independent of generation traffic."""

import asyncio
import time

import httpx

import config
from cookie_auth import BATCH_GRAPHQL_URL, build_headers
from credential_manager import credential_manager
from http_io import read_limited_bytes
from impersonate import open_upstream_client
from json_stream import JsonObjects
from proxy_failover import ProxyChain
from request_tracking import redact
from runtime_state import app_state
from upstreams.cookie_request import build_probe_body


MAX_CONCURRENT_PROBES = 4
PROBE_TOTAL_TIMEOUT_SECONDS = 45.0
MAX_PROBE_RESPONSE_BYTES = 256 * 1024
_probe_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PROBES)


async def probe_account(record: dict) -> dict:
    """Use a nonexistent model; reject ambiguous responses without quarantining."""
    from upstreams.cookie_transport import grpc_status_code, is_cookie_expired_error

    started = time.monotonic()
    account_id = record.get("id", "")

    def done(status, detail):
        if status == "valid":
            credential_manager.record_cookie_success(account_id, credential=record)
        elif status == "invalid":
            credential_manager.record_cookie_auth_failure(account_id, credential=record)
        return {"id": account_id, "label": record.get("note", "") or account_id,
                "status": status, "detail": redact(detail)[:600],
                "elapsed_ms": round((time.monotonic() - started) * 1000)}

    headers = build_headers(record.get("cookie", ""))
    if headers is None:
        return done("invalid", "Cookie 缺少 SAPISID 族字段，无法计算认证头。")
    project = (record.get("project_id", "") or app_state.get_project_id() or "").strip()
    if not project:
        return done("error", "该账号未填写 Project ID，无法构造探测请求。")

    options = {"timeout": httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0),
               "follow_redirects": False, "trust_env": False,
               "_proxy_chain": ProxyChain(record)}
    if config.SSL_CERT_FILE:
        options["verify"] = config.SSL_CERT_FILE
    try:
        # The outer deadline includes every proxy attempt and the entire body.
        async with _probe_semaphore:
            async with asyncio.timeout(PROBE_TOTAL_TIMEOUT_SECONDS):
                async with open_upstream_client(options) as client:
                    async with client.stream("POST", BATCH_GRAPHQL_URL, headers=headers,
                                             json=build_probe_body(project)) as response:
                        status_code = response.status_code
                        if status_code in (401, 403):
                            return done("invalid", f"HTTP {status_code}：会话已失效或无权访问该 Project。")
                        data = await read_limited_bytes(response, MAX_PROBE_RESPONSE_BYTES)
    except Exception as exc:
        return done("error", f"网络/代理或响应读取错误：{type(exc).__name__}: {exc}")

    if status_code >= 500 or status_code == 429 or 300 <= status_code < 400:
        return done("error", f"上游暂时不可用（HTTP {status_code}），请稍后重试。")

    try:
        parser = JsonObjects(limit=MAX_PROBE_RESPONSE_BYTES)
        objects = list(parser.feed(data.decode("utf-8")))
        parser.finish()
        errors = []
        recognized = False
        for obj in objects:
            results = obj.get("results")
            if not isinstance(results, list):
                continue
            for result in results:
                if not isinstance(result, dict):
                    raise ValueError("探测返回无效的 results 记录")
                recognized |= isinstance(result.get("data"), dict) and bool(result["data"])
                for error in result.get("errors") or []:
                    if not isinstance(error, dict):
                        raise ValueError("探测返回无效的 errors 记录")
                    errors.append(error)
        # Evaluate all errors: a later NOT_FOUND cannot hide an auth rejection.
        codes = {grpc_status_code(error) for error in errors}
        detail = next((str(error["message"]) for error in errors if error.get("message")), "")
        if codes & {7, 16} or any(is_cookie_expired_error(str(e.get("message", ""))) for e in errors):
            return done("invalid", f"鉴权失败：{detail or 'permission/unauthenticated'}")
        if codes & {3, 5, 8}:
            return done("valid", f"会话有效：{detail or '模型不存在属预期'}")
        if status_code == 200 and recognized and not errors:
            return done("valid", "会话有效（上游 200 且无错误块）。")
        return done("error", f"无法判定（HTTP {status_code}）：{detail or '未收到预期的探测结果'}")
    except (ValueError, TypeError, AttributeError, RecursionError) as exc:
        return done("error", f"上游探测响应格式无效：{exc}")


async def probe_all(records: list) -> list:
    """A fixed worker set preserves order and isolates per-account failures."""
    pending = iter(enumerate(records))
    results = [None] * len(records)

    async def worker():
        for index, record in pending:
            try:
                results[index] = await probe_account(record)
            except Exception as exc:
                results[index] = {"id": record.get("id", ""),
                                  "label": record.get("note", "") or record.get("id", ""),
                                  "status": "error", "detail": redact(f"探测异常：{exc}")[:600],
                                  "elapsed_ms": 0}

    await asyncio.gather(*(worker() for _ in range(min(MAX_CONCURRENT_PROBES, len(records)))))
    return results
