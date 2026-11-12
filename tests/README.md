# 测试分类与定位

测试按行为和资源归属查找，新增文件使用具体行为名称。已有历史轮次文件继续
保留路径，定位其中的测试应查看类名及文件顶部的覆盖说明。

| 分类 | 主要文件或入口 | 关注点 |
| --- | --- | --- |
| 模型目录与生成参数 | `test_protocol_alignment.py`、`test_anti_truncation.py`、`test_generation_config.py` | 三协议模型目录、搜索/分辨率别名、采样与思考、抗截断启用条件 |
| 消息与工具转换 | `test_claude_protocol.py`、`test_protocol_conversion_edges.py`、`test_tool_roundtrip_audit.py` | 角色、工具往返、签名、finish reason 与错误形状 |
| 请求生命周期与续传 | `test_resumable_delivery.py`、`test_request_stability.py`、`test_tool_stream_deadline.py` | 幂等、取消、断连、重试期限、发送与生成计数 |
| JSON 与图片内存 | `test_buffer_json_accounting.py`、`test_image_memory_review.py`、`test_image_memory_framing.py` | 精确字节预算、峰值内存、编码分块、图片资源回收 |
| 线程准备与日志投递 | `test_preparation_isolation.py`、`test_log_stream.py` | 准入隔离、线程取消、日志积压、订阅独立与消息顺序 |
| 请求诊断 | `test_request_tracking.py`、`test_state_and_protocol_regressions.py` | SSE 分帧、禁用诊断、历史清理、重连游标与脱敏 |
| 凭证、状态与缓存 | `test_multi_credentials.py`、`test_effective_settings_cache.py`、`test_persistence_and_media_integrity.py` | 热更新、版本冲突、落盘回滚、配置快照与缓存失效 |
| 代理与连接池 | `test_proxy*.py`、`test_native_proxy_relays.py`、`test_capability_and_pool_lifecycle.py` | 出口切换、协议转换、客户端复用、租约与关闭 |
| 控制台与鉴权 | `test_console_routes.py`、`test_console_usability.py`、`test_security*.py` | 设置校验、会话、CSRF、凭证导出与页面接口 |
| 模块依赖与工程约束 | `test_architecture.py`、`test_qua_round18_contracts.py` | 反向依赖、导入环、公共契约和检查脚本 |

完整离线检查：

```sh
python scripts/check_project.py
```

本轮性能与模块拆分的定向回归：

```sh
python -m pytest -q tests/test_log_stream.py tests/test_preparation_isolation.py tests/test_buffer_json_accounting.py tests/test_request_tracking.py tests/test_architecture.py tests/test_protocol_alignment.py tests/test_anti_truncation.py
```

控制台前端：

```sh
npm ci
npm run check:format
npm run test:console
```

真实浏览器与布局用 `npm run test:browser`，需要 Playwright Chromium。
Go 桥接在 `proxybridge/` 中独立运行 `go test -race -count=1 -timeout=90s ./...`。

根目录 `conftest.py` 统一初始化应用单例，并在测试模块之间清理状态。
新增测试优先创建独立对象；修改共享单例时使用 fixture 或 cleanup 恢复。
测试直接引用所属模块；旧私有名的兼容导出只供历史调用者迁移。
