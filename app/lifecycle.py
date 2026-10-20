"""应用启动、后台任务登记与资源关闭。"""

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

import config
import express_key_probe
from client_pool import close_all_clients
from console_auth import DEFAULT_API_KEY
from model_loader import close_models_refresh, get_models_config, schedule_models_refresh
from proxy_resolver import (bridge_manager, find_bridge_binary, singbox_available,
                            singbox_manager, singbox_version)
from resumable import resumable_store
from runtime_state import app_state, get_effective_proxy_url, mask_proxy_url


_background_tasks: set[asyncio.Task] = set()


def start_background_task(coro) -> asyncio.Task:
    """持有后台任务，报告异常，并在应用关闭时统一取消和等待。"""
    task = asyncio.create_task(coro)
    _background_tasks.add(task)

    def finished(completed: asyncio.Task) -> None:
        _background_tasks.discard(completed)
        if not completed.cancelled():
            error = completed.exception()
            if error is not None:
                print(f"⚠️ [后台任务] {type(error).__name__}: {error}")

    task.add_done_callback(finished)
    return task


async def _probe_express_projects_on_startup() -> None:
    """启动后异步探测尚无 Project ID 的 Express Key（失败不影响服务）。"""
    try:
        pending = [r["key"] for r in app_state.get_express_keys() if not r.get("project_id")]
        if not pending:
            return
        print(
            f"🔎 [密钥探测] 启动探测：{len(pending)} 个 Key 尚无 Project ID，正在用不存在的模型名调 generateContent 探测。"
        )
        await express_key_probe.probe_all(keys=pending)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"⚠️ [密钥探测] 启动探测失败（不影响服务）：{type(e).__name__} - {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    singbox_manager.reopen()
    bridge_manager.reopen()
    print("🚀 [服务启动] agentplatform2api 适配器已启动（Express API Key / Cookie 直连 双通道）。")

    # S-1：默认口令 + 公开托管 + 明文 Cookie 是很危险的组合，必须让人看见。
    if config.API_KEY == DEFAULT_API_KEY:
        public_host = any(os.environ.get(k) for k in ("SPACE_ID", "SPACE_HOST", "HF_SPACE_ID"))
        if public_host and os.environ.get("ALLOW_DEFAULT_KEY", "").lower() not in ("1", "true", "yes"):
            raise RuntimeError(
                "检测到公开托管环境（HuggingFace Space 等）且 API_KEY 仍为默认值 123456。\n"
                "该口令同时是控制台登录密码，而控制台可以读写完整的 Google 会话 Cookie。\n"
                "请设置一个强 API_KEY 后重启；确需临时放行可设 ALLOW_DEFAULT_KEY=true。"
            )
        print(
            "🔴 [安全警告] API_KEY 仍是默认值 123456！它既是本代理的 Key，也是控制台登录口令，"
            "请立刻改成强口令。"
        )
    express_key_manager = app.state.express_key_manager

    # 多账号 Express Key：环境变量只做**一次性**初始导入，之后以控制台保存的列表为准。
    # （用户在控制台把列表清空并保存后，这里不会再把环境变量里的 Key 复种回来。）
    app_state.import_env_express_keys_once(config.ENV_EXPRESS_KEYS)
    app_state.import_env_cookie_accounts_once()
    app_state.initialize_channel_mode_once()
    print(f"🔀 [通道调度] {app_state.get_channel_mode()}；"
          f"Cookie 账号 {sum(r['enabled'] for r in app_state.get_cookie_accounts())} 个启用。")
    if express_key_manager.get_total_keys() > 0:
        print(f"✅ [密钥配置] 已加载 {express_key_manager.get_total_keys()} 个 Express API Key。")
    else:
        print(
            "⚠️ [密钥配置] 未配置任何 Express API Key（控制台「通道与凭证」页可随时添加）。"
            "若不启用 Cookie 直连模式，聊天请求将会报错。"
        )
    # 本地模型列表立即可用；远程刷新不再阻塞启动和健康检查。
    await get_models_config()
    schedule_models_refresh()

    # 网络出口状态：让“代理到底启没启用”一眼可见（控制台「通道与凭证」页可热改）。
    _proxy_now = get_effective_proxy_url()
    if _proxy_now:
        print(
            f"🌐 [网络] 出站代理已启用：{mask_proxy_url(_proxy_now)}（控制台「通道与凭证」页可热改，保存后无需重启）。"
        )
    else:
        print(
            "🌐 [网络] 未配置出站代理，直连 Google 上游（控制台「通道与凭证」页可配置，或设环境变量 PROXY_URL）。"
        )

    # 多协议代理支持（socks4/vless/vmess/trojan/ss/hysteria2/tuic → 本地 sing-box 转换）：
    # 提前报告 sing-box 可用性，用户填 vless:// 链接而环境缺 sing-box 时
    # 能在启动日志里直接看到原因（请求时也会明确报错，绝不静默直连）。
    try:
        if singbox_available():
            print(
                f"🛰️ [多协议代理] sing-box 就绪（{singbox_version() or '版本未知'}）："
                "vless/vmess/trojan/ss/socks4/hysteria2/tuic 分享链接可直接作为代理使用，"
                "首个请求时自动转换成本地 socks5。"
            )
        else:
            print(
                "⚠️ [多协议代理] 未找到 sing-box：vless/vmess/trojan/ss/socks4/hysteria2/tuic "
                "等协议不可用（http/socks5 不受影响）。Docker 镜像已内置；本地部署请安装 "
                "sing-box 或设 SINGBOX_BIN 指向可执行文件。"
            )
    except Exception as _e:
        print(f"⚠️ [多协议代理] sing-box 状态探测失败：{_e}")
    print("🛰️ [TURN/SSTP] 转换器就绪，节点首次使用时启动。" if find_bridge_binary()
          else "ℹ️ [TURN/SSTP] 转换器未安装；使用新 Docker 镜像或设置 PROXY_BRIDGE_BIN 可启用。")

    # Cookie 通道指纹伪装状态（curl_cffi）
    try:
        from impersonate import get_impersonate_profile

        _fp = get_impersonate_profile()
        if _fp != "off":
            print(
                f"🎭 [指纹伪装] Cookie 通道已启用 curl_cffi 浏览器伪装：{_fp}"
                "（TLS 指纹 + 浏览器请求头，控制台「通道与凭证」页可热改/off 关闭）。"
            )
        else:
            print(
                "🎭 [指纹伪装] Cookie 通道指纹伪装已关闭（httpx 原始行为，可在控制台「通道与凭证」页开启）。"
            )
    except Exception as _e:
        print(f"⚠️ [指纹伪装] 状态读取失败：{_e}")

    # Express 通道请求头指纹状态（SDK 通道 + 全部裸 httpx 出站点）
    try:
        from express_fingerprint import describe_current_profile

        print(
            f"🪪 [请求头指纹] Express API Key 通道：{describe_current_profile()}"
            "（控制台「通道与凭证」页可热改；同时作用于 Gemini 原生透传 / Key 探测 / 模型配置 / 代理测试）。"
        )
    except Exception as _e:
        print(f"⚠️ [请求头指纹] 状态读取失败：{_e}")
    print(
        "🔁 [Gemini 透传] 原生格式端点已开启：POST /v1beta/models/{model}:generateContent"
        " / :streamGenerateContent / :countTokens（鉴权与 OpenAI 端点一致，三种原生 Key 写法均支持）。"
    )

    # 默认口令告警：API_KEY 未通过环境变量覆盖时，对外 API 与控制台共用
    # 已知默认值 "123456"，公网部署形同裸奔（可被任意人消耗凭据配额、
    # 读取控制台明文 Key）。这里在启动日志里给一个醒目提示。
    try:
        from config import API_KEY as _deployed_key, DEFAULT_API_KEY as _default_key
        if _deployed_key == _default_key:
            print(
                "⚠️ [安全告警] 当前仍在使用默认 API_KEY（123456），对外接口与控制台登录"
                "均可用该默认值访问！公网部署请立即通过环境变量 API_KEY 设置强口令。"
            )
    except Exception:
        pass

    # 启动时自动探测各 Key 归属的 GCP Project ID（location 钉定要用）。
    # 放后台任务里跑：探测走外网，绝不能阻塞服务启动。
    start_background_task(_probe_express_projects_on_startup())
    # 过期缓冲清理器（弱网续传用，见 resumable.py）
    start_background_task(resumable_store.sweeper_loop())
    try:
        yield
    finally:
        tasks = list(_background_tasks)
        for _t in tasks:
            if not _t.done():
                _t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await resumable_store.aclose()
        await close_models_refresh()
        await close_all_clients()
        # 终止全部 sing-box 子进程（vless/vmess 等协议转换进程；
        # atexit 里还有一道兑底，这里保证 FastAPI 优雅停机时先收掉）。
        await asyncio.gather(asyncio.to_thread(singbox_manager.shutdown_all),
                             asyncio.to_thread(bridge_manager.shutdown_all), return_exceptions=True)
