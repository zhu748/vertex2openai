"""兼容入口：Express 管线本体已迁至 upstreams/express_pipeline（2026-11 P2-3）。

历史 import 路径（``api_helpers.execute_gemini_call`` 等）继续可用，等价于
直接使用 ``upstreams.express_pipeline``。注意 mock.patch 的落点：本模块只是
re-export 层，patch 本模块的属性不会影响管线内部绑定——需要替换 stats /
wait_for_retry / timeout_seconds / track_configure 等 seam 时，请 patch
``upstreams.express_pipeline``（测试已同步迁移）。
"""

from upstreams.express_pipeline import (  # noqa: F401
    SDKGeneration,
    ToolCallIndexer,
    OpenAIRequest,
    _build_chunk_payload,
    _call_retry_factory,
    _chunk_openai_response_dict_for_sse,
    _record_usage,
    _warn_bad_credential,
    app_state,
    convert_chunk_to_openai,
    convert_to_openai_format,
    execute_gemini_call,
    stats,
    timeout_seconds,
    track_configure,
    wait_for_retry,
)
