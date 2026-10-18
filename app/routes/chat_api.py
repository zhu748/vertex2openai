from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask
from models import OpenAIRequest
from auth import get_api_key

import asyncio
import json

# 引入运行状态管理器与多通道分发策略
from runtime_state import app_state
from upstreams.express_sdk import ExpressSDKUpstream
from upstreams.cookie_proxy import CookieProxyUpstream
from api_helpers import extract_upstream_error, create_openai_error_response
from logger import stats
from streaming import keepalive_sse

# 第二轮：弱网可靠传输 —— 服务端响应缓冲 + 序列号事件 + 断线重放 / 分片轮询。
# 上游的读取由后台泵任务驱动，与前端连接解绑；Google 的回复总能完整落缓冲，
# 前端凭 Last-Event-ID / ?resume_from= 续传，或轮询 /v1/resumable/{id} 拉分片。
# SSE 空闲时发送注释保活；断线后的完整性交由缓冲和续传保证。
from resumable import (
    resumable_store,
    compute_stream_id,
    is_enabled as resumable_enabled,
    DetachedRequest,
    pump_stream,
    sse_from_buffer,
    frame_with_id,
    DONE_FRAME,
)

router = APIRouter()

# 实例化多通道策略
express_upstream = ExpressSDKUpstream()
cookie_upstream = CookieProxyUpstream()


async def _call_upstream(request_obj: OpenAIRequest, fastapi_request: Request):
    """按控制台开关分流到两条上游通道（与原来完全一致）。"""
    if app_state.is_web_proxy_enabled():
        response = await cookie_upstream.chat_completions(request_obj, fastapi_request)
    else:
        response = await express_upstream.chat_completions(request_obj, fastapi_request)
    if isinstance(response, StreamingResponse):
        original = response.body_iterator

        async def relay():
            stream = keepalive_sse(original, fastapi_request)
            try:
                async for frame in stream:
                    yield frame
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                stats.add_error()
                code, message = extract_upstream_error(exc)
                print(f"❌ [流式转发] {request_obj.model} | HTTP {code} | {message[:200]}")
                error = create_openai_error_response(code, message[:1024], "upstream_error")
                yield f"data: {json.dumps(error, ensure_ascii=False)}\n\n"
                yield DONE_FRAME
            finally:
                await stream.aclose()
        response.body_iterator = relay()
    return response


def _request_fingerprint(request_obj: OpenAIRequest) -> str:
    """稳定请求指纹：同一请求原样重发即命中同一缓冲区（不再烧上游配额）。"""
    try:
        return json.dumps(request_obj.model_dump(mode="json"), sort_keys=True,
                          ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return json.dumps(request_obj.model_dump(), sort_keys=True, default=str)


def _resume_start_seq(fastapi_request: Request) -> int:
    """续传起点：优先 Last-Event-ID 请求头，其次 ?resume_from= 查询参数，缺省 0。"""
    try:
        leid = (fastapi_request.headers.get("last-event-id") or "").strip()
        if leid:
            # Last-Event-ID 是客户端已收到的最后一帧；查询参数仍是起点游标。
            return max(0, int(leid) + 1)
    except (TypeError, ValueError):
        pass
    try:
        q = fastapi_request.query_params.get("resume_from")
        if q is not None:
            return max(0, int(str(q).strip()))
    except (TypeError, ValueError):
        pass
    return 0


def _resume_headers(stream_id: str, replay: bool = False) -> dict:
    headers = {"X-Resume-Id": stream_id, "Cache-Control": "no-store"}
    if replay:
        headers["X-Resumable-Replay"] = "1"
    return headers


async def _abort_placeholder(buf, stream_id: str, message: str, code: int = 500) -> None:
    """占位缓冲没能接上上游：给已经在跟读它的并发请求补一个错误帧 + 终止帧，再撤掉占位。

    撤掉占位是为了不让一个「空壳」缓冲挡住后续重试（跟读者手里还拿着缓冲对象，读得完）。
    """
    err = {"error": {"message": message[:1024], "type": "upstream_error", "code": code}}
    await buf.append(frame_with_id(buf.next_seq, f"data: {json.dumps(err, ensure_ascii=False)}\n\n"))
    await buf.append(frame_with_id(buf.next_seq, DONE_FRAME))
    await buf.finish("error", error=message, status_code=code)
    await resumable_store.drop(stream_id, expected=buf)


async def _resumable_stream(fastapi_request: Request, request_obj: OpenAIRequest, api_key: str):
    """流式：缓冲命中则重放/续传；否则建缓冲、后台泵抽帧，当前连接只做缓冲的读者。

    建缓冲走 `create_if_absent` 原子占位：并发的同一请求只有第一个去打上游，
    其余直接跟读同一份缓冲（不重复烧配额，也不会把先建的缓冲/泵任务覆盖掉）。
    """
    stream_id, id_src = compute_stream_id(
        fastapi_request.headers, api_key or "", _request_fingerprint(request_obj))

    start = _resume_start_seq(fastapi_request)
    has_cursor = bool((fastapi_request.headers.get("last-event-id") or "").strip()) or (
        fastapi_request.query_params.get("resume_from") is not None)
    if has_cursor:
        # 显式续传始终读取旧副本；不能生成另一轮再跳过前 start 帧。
        buf = await resumable_store.get(stream_id)
        if buf is None or buf.kind != "stream":
            return JSONResponse(
                status_code=404,
                content=create_openai_error_response(
                    404, "续传缓冲不存在或已过期；如需重新生成，请移除续传游标。", "not_found"),
                headers=_resume_headers(stream_id))
        if start > buf.next_seq:
            return JSONResponse(
                status_code=409,
                content=create_openai_error_response(
                    409, f"续传起点 {start} 超过缓冲末尾 {buf.next_seq}。", "invalid_resume_cursor"),
                headers=_resume_headers(stream_id))
        is_new = False
    else:
        buf, is_new = await resumable_store.create_if_absent(
            stream_id, model=request_obj.model, kind="stream", replace_delivered=True)
    if not is_new:
        # 命中在用的缓冲：重放已缓冲的部分并继续跟随上游直到本轮结束，不再请求上游。
        print(f"↩️ [续传命中] {stream_id}（{id_src}）：直接重放/续传，不再请求上游。")
        return StreamingResponse(
            sse_from_buffer(buf, start),
            media_type="text/event-stream",
            headers=_resume_headers(stream_id, replay=True),
        )

    # 连准备阶段也由后台任务持有：前端在鉴权/预处理/首次连接时断开，
    # 不会取消上游或迫使重连者重复生成；淘汰/停机仍可经 store 取消它。
    detached = DetachedRequest(fastapi_request)

    async def start_stream():
        try:
            resp = await _call_upstream(request_obj, detached)
        except asyncio.CancelledError:
            await _abort_placeholder(buf, stream_id, "上游准备任务已取消。", 503)
            raise
        except Exception as exc:
            code, message = extract_upstream_error(exc)
            await _abort_placeholder(buf, stream_id, message, code)
            raise
        if not isinstance(resp, StreamingResponse):
            # 初始化错误仍向发起者返回原 HTTP 响应，并唤醒已有的跟读者。
            status = getattr(resp, "status_code", 500)
            await _abort_placeholder(buf, stream_id, f"上游返回非流式响应（HTTP {status}）。", status)
            return resp
        if buf.done:
            close = getattr(resp.body_iterator, "aclose", None)
            if close:
                await close()
            return JSONResponse(status_code=buf.status_code, content=buf.payload or
                                create_openai_error_response(503, "响应缓冲已释放。", "buffer_evicted"))
        pump_task = asyncio.create_task(pump_stream(buf, resp.body_iterator))
        resumable_store.register_pump(stream_id, pump_task, buffer=buf)
        print(f"📦 [续传缓冲] {stream_id}（{id_src}）：后台泵已启动，上游读取与前端连接解绑。")
        return resp

    setup_task = asyncio.create_task(start_stream())
    resumable_store.register_pump(stream_id, setup_task, buffer=buf)
    resp = await asyncio.shield(setup_task)
    if not isinstance(resp, StreamingResponse):
        return resp

    return StreamingResponse(
        sse_from_buffer(buf, 0),
        media_type="text/event-stream",
        headers=_resume_headers(stream_id),
    )


async def _resumable_json(fastapi_request: Request, request_obj: OpenAIRequest, api_key: str):
    """非流式：完整回复落缓冲；原样重发命中未交付缓冲则直接重放。

    同样走 `create_if_absent` 原子占位：并发的同一请求只打一次上游，
    后来的请求等同一个后台泵的结果。
    """
    stream_id, id_src = compute_stream_id(
        fastapi_request.headers, api_key or "", _request_fingerprint(request_obj))

    buf, is_new = await resumable_store.create_if_absent(
        stream_id, model=request_obj.model, kind="json", replace_delivered=True)
    if not is_new:
        # 命中在用的缓冲：已出结果的直接重放；上游还在算的，等同一个后台泵的结果。
        print(f"↩️ [重放命中] {stream_id}（{id_src}）：直接返回已缓冲的完整回复，不再请求上游。")
        if buf.payload is None:
            await buf.wait_for(2 ** 62, 600.0)
        if buf.payload is None:
            return JSONResponse(
                status_code=504,
                content=create_openai_error_response(504, "上游响应超时，缓冲未完成。", "timeout"),
                headers=_resume_headers(stream_id, replay=True))
        return JSONResponse(status_code=buf.status_code, content=buf.payload,
                            headers=_resume_headers(stream_id, replay=True),
                            background=BackgroundTask(buf.mark_delivered))

    detached = DetachedRequest(fastapi_request)

    async def _pump_json():
        # 后台跑上游调用：这条前端连接断了，Google 的回复也要收完进缓冲。
        try:
            resp = await _call_upstream(request_obj, detached)
        except asyncio.CancelledError:
            if not buf.done:
                await buf.finish("error", error="上游调用已取消。",
                                 payload=create_openai_error_response(503, "上游调用已取消。", "upstream_error"),
                                 status_code=503)
            raise
        except Exception as e:
            code, msg = extract_upstream_error(e)
            print(f"❌ [响应缓冲] {stream_id} 上游调用失败：{msg[:200]}")
            await buf.finish("error", error=msg,
                             payload=create_openai_error_response(code, msg[:1024], "upstream_error"),
                             status_code=code)
            return
        body = getattr(resp, "body", b"") or b""
        status = getattr(resp, "status_code", 200)
        try:
            payload = json.loads(body.decode("utf-8")) if body else {}
        except Exception:
            payload = {"_raw": body.decode("utf-8", "replace")[:4000]}
        await buf.finish("done" if 200 <= status < 300 else "error",
                         payload=payload, status_code=status)

    pump_task = asyncio.create_task(_pump_json())
    resumable_store.register_pump(stream_id, pump_task, buffer=buf)

    # 当前连接等待缓冲完成（等同于原来的直通等待；断线不影响后台泵）。
    # wait_for(seq=极大值) 等价于“等到 done 或超时”。
    await buf.wait_for(2 ** 62, 600.0)
    if buf.payload is None:
        return JSONResponse(
            status_code=504,
            content=create_openai_error_response(504, "上游响应超时，缓冲未完成。", "timeout"),
            headers=_resume_headers(stream_id))
    # 响应体发送完成后才标记交付；发送失败时保留同一份回复供重试。
    print(f"📦 [响应缓冲] {stream_id}（{id_src}）：非流式完整回复已落缓冲（HTTP {buf.status_code}）。")
    return JSONResponse(status_code=buf.status_code, content=buf.payload,
                        headers=_resume_headers(stream_id),
                        background=BackgroundTask(buf.mark_delivered))


@router.post("/v1/chat/completions")
async def chat_completions(fastapi_request: Request, request: OpenAIRequest, api_key: str = Depends(get_api_key)):
    """
    /v1/chat/completions 动态分流路由器
    根据大盘设置的全局开关，决定将 OpenAI 请求路由至：
    - True  -> CookieProxyUpstream (Cookie 直连反代通道，规避 429 限流)
    - False -> ExpressSDKUpstream (官方 API Key 标准通道)

    弱网可靠传输（resumable_enabled 开启时，默认开）：
    响应先落服务端缓冲（带序列号），上游读取与前端连接解绑；
    前端断线可凭 Last-Event-ID / ?resume_from= 续传，或轮询 /v1/resumable/{id} 拉分片。

    统一异常兜底：把上游抛出的 404/403/400 等如实转成 OpenAI 错误格式，
    避免笼统的 500 Internal Server Error（非流式路径此前直接 raise）。
    统计由 main.py 的中间件按响应状态码统一计入，这里不重复计数。
    """
    try:
        if resumable_enabled():
            if request.stream:
                return await _resumable_stream(fastapi_request, request, api_key)
            return await _resumable_json(fastapi_request, request, api_key)
        return await _call_upstream(request, fastapi_request)
    except Exception as e:
        code, msg = extract_upstream_error(e)
        print(f"❌ [路由兜底] 模型 {request.model} 调用失败 | HTTP {code} | {msg[:200]}")
        return JSONResponse(status_code=code, content=create_openai_error_response(code, msg, "upstream_error"))


@router.get("/v1/resumable/{stream_id}")
async def resumable_poll(stream_id: str, fastapi_request: Request,
                         api_key: str = Depends(get_api_key),
                         from_: int = Query(0, alias="from"),
                         limit: int = Query(200),
                         wait: float = Query(0.0)):
    """
    分片轮询拉取：GET /v1/resumable/{id}?from=<seq>&limit=200&wait=10

    短连接天生抗抖动；返回带序号的事件分片、next 游标与 done 标志，
    客户端按 seq 去重、按 done 判定本轮完整结束。wait>0 时为长轮询（最多等 25 秒）。
    响应头 X-Resume-Id 即为这里的 {id}。
    """
    buf = await resumable_store.get(stream_id)
    if buf is None:
        return JSONResponse(
            status_code=404,
            content=create_openai_error_response(
                404, f"未找到响应缓冲 {stream_id}（可能已过期被清理）。", "not_found"))
    limit = max(1, min(limit, 500))
    if wait > 0 and not buf.done:
        await buf.wait_for(max(0, from_), min(wait, 25.0))
    start = max(0, from_)
    events = buf.snapshot(start, limit)
    end = start + len(events)
    done = buf.done and end >= buf.next_seq
    delivered = done and start <= buf.next_seq
    content = {
        "id": stream_id,
        "model": buf.model,
        "kind": buf.kind,
        "status": buf.status,
        "error": buf.error,
        "events": [{"seq": start + i, "data": frame} for i, frame in enumerate(events)],
        "next": end,
        "done": done,
    }
    if buf.kind == "json" and buf.done:
        # JSON 没有事件分片；轮询必须实际交付正文，不能只报告 done。
        content.update(payload=buf.payload, status_code=buf.status_code)
    return JSONResponse(
        content=content,
        headers=_resume_headers(stream_id),
        background=BackgroundTask(buf.mark_delivered) if delivered else None)
