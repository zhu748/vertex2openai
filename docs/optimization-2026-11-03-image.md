# 生图链路全面审查与优化（2026-11-03，第三轮）

前两轮（同日）集中解决 4K 生图在 512MB 容器上的 OOMKill；本轮按「全面优化、
不限于 4K」的要求通读生图全链路（输入摄取/压缩 → 尺寸与比例解析 → 两通道
请求构建 → 上游并发闸 → 响应收集/转换 → SSE 发射），覆盖 512/1K/2K/4K
全部档位。优化前统一检查 68/68 组通过；未调用真实 Google 上游。

> 第四轮（同日续，见文末「分辨率后缀目录可发现性」一节）：用户确认
> 「分辨率此前只在控制台调、模型列表里看不到分辨率变体」后补齐的
> 目录可发现性与 Gemini 原生透传支持。

## 审查范围

| 环节 | 文件 | 本轮落点 |
| --- | --- | --- |
| 输入图压缩/解码/抓取 | `image_processing.py` | 透明抹平按模式与实际透明度分流 |
| 尺寸/比例/档位解析 | `model_capabilities.py` | 分辨率别名生效、size 映射补全 |
| 通道模型名归一 | `express_sdk.py`、`cookie_proxy.py`、`cookie_request.py` | 上游模型名剥离分辨率后缀 |
| Express 假流式发射 | `express_response.py` | 巨帧切片零步进 |
| Cookie 生图整取 | `cookie_proxy.py` | 解析前释放 bytes 缓冲 |
| 并发闸/媒体线程池/续传绕过 | `admission.py`、`media_tasks.py` | 检查后沿用（前两轮已落） |

## 已实施改进

### 1. 模型名分辨率别名（-1k/-2k/-4k/-512）端到端生效

`_strip_known_suffixes` 早已为**家族识别**剥离这些后缀，但后缀从未转成
`imageConfig.imageSize`，也从未从发往上游的模型名里剥离——目录
（vertexModels.json）只有三个裸生图 ID，客户端发 `gemini-3.1-flash-image-4k`
会得到「档案判定正确、上游 404」的半截行为。

现在：

- 新增 `mc.image_resolution_suffix(name) -> (基础名, 档位)`，仅当剥离后仍是
  生图档案时生效（与家族识别口径一致，不误伤文本模型）；
- Express（`_normalize_model_name`，Claude 路由共用）与 Cookie
  （`chat_completions` 解析处 + `build_request_body` 幂等二次剥离）都把后缀
  从上游模型名剥掉；
- `resolve_image_size` 优先级更新为：请求 `image_size` 字段 > 模型名后缀 >
  OpenAI `size` 档位意图 > 控制台设置 > 1K；Cookie 通道传入的 model_name
  已剥离时从请求原始模型名兜底解析，两通道行为一致。

### 2. OpenAI images 风格 size 映射补全

- 宽高比：旧映射只有 1024x1024/1024x768/768x1024 三个值，dall-e-3 与
  gpt-image-1 的竖横版（1536x1024、1024x1536、1792x1024、1024x1792）
  落到「无比例」分支被静默丢弃；补全为 3:2/2:3/16:9/9:16，并兼容大小写。
- 档位：仅接受无歧义意图——长边 ≥2048 → 就近 2K/4K，长边 ≤640 → 512；
  1024/1536/1792 保持默认 1K（dall-e-3 客户端按 1792 拿 1K 是既定行为，
  不因补映射而悄悄放大 4 倍像素成本）。`auto` 与未知格式不生效。

### 3. 输入图透明抹平按实际透明度分流（内存）

旧实现对 RGBA/LA/P 一律 `convert("RGBA")`（一份全量拷贝）→ 建白底 →
`getchannel("A")`（再一份）→ paste；24MP RGBA 输入抹平段实测新增峰值
RSS 206MB，512MB 容器单图即近 OOM——与 4K 输出路径无关、纯输入侧风险，
任何档位的图生图/上传修图都会走到。

现在按模式与 alpha 极值分流，且极值探测用多波段 `getextrema()` 不物化
掩码通道：

- P 无 transparency / 其它非 RGB 模式 → 直接 `convert("RGB")`（调色板 LUT）；
- 全不透明（alpha 恒 255，网页截图/导出图常见）→ 仅丢通道；
- 全透明（alpha 恒 0）→ 直接白底；
- 真半透明 → 才物化掩码做白底合成，输出颜色与旧实现一致（回归验证）。

子进程高水位差实测（6000×4000，5 次中位数）：全不透明 206→92MB（-56%）、
真半透明 206→115MB（-45%，行为不变）、全透明 206→92MB（-56%）。

### 4. Express 假流式巨帧切片零步进（时延）

巨帧（>1MiB 生图 base64）切片沿用文本假流式的逐帧 `sleep(0.01)`——
data URL 对前端不可增量渲染（收完才能显示），步进是纯延迟，且 Cookie
通道同路径从不步进。改为 `sleep(0)`（仅让出事件循环，慢客户端由 TCP
背压限速）；文本假流式 10 段节奏保持 0.01 不变。实测 2.25MiB（12 帧）
发射 120ms 下限 → 4.2ms；4K（40MB，~160 帧）节省 0.5-1.6s，2K 节省
~160ms。切片拼接逐字节一致由回归保证。

### 5. Cookie 生图整取在解析前释放 bytes 缓冲（内存）

httpx 整取会同时持有 `response.content`（bytes）与 `response.text`（str），
4K 单图各 ~30-50MB；旧实现解析全程两者并存，叠加 JSON 解出的 data 串、
markdown 与 full_text 后收集期 4-5 份全量。现在状态码/重试判断完成后
取 `response_text` 本地引用并置空 `response`，bytes 缓冲在解析开始前
回收（引用计数级断言验证，见回归测试）。所有档位均少一份 bytes→str
双驻留。

## 审查后保留的设计（未改，记录结论）

- **输入压缩默认 1.5MB/1536px 对 2K/4K 编辑往返的降采样**：控制台
  `img_compress_*` 可调的既定产品决策（防多轮修图内存卡死）；生成图回传
  会经压缩降采样，需要原画质的部署应调高 `img_compress_max_mb`/`max_dim`
  或关闭开关。
- **内联 data URL 20MB 上限**：4K PNG（25-40MB）base64 回传超限会被跳过
  （占位「图片无法处理」）。上限是内存安全边界（解码后叠加 PIL 全量解码，
  512MB 容器承受不了 40MP PNG），提高上限需先有真机容量测量，本轮不动。
- **`-think-*` 模型名思考档位别名**：与分辨率后缀同构的「识别但不生效」
  缺口（`_strip_known_suffixes` 剥、resolve_thinking 不读、上游名不剥）。
  属文本模型链路，超出本轮生图范围，留待后续。
- **`fetch_remote_image` 每图新建 httpx Client**：多图同域无连接复用，但
  换来每跳钉定 IP + 每请求代理链的隔离正确性，收益小于风险，不动。
- **`_prompt_aspect_ratio` 的宽泛回退 `\b(\d+[:：]\d+)\b`**：正文任意
  数字对（时间/比例说明）可能被当成 AR，但有 15 档白名单兜底；既有
  文档化设计，不动。

## 本机基准

`python scripts/benchmark_image_path.py`（Python 3.12.16）：

| 用例 | 旧 | 新 |
| --- | ---: | ---: |
| 透明抹平段新增峰值 RSS，24MP 全不透明 RGBA | 206.4MB | 91.7MB |
| 同上，真半透明（行为不变） | 206.4MB | 114.6MB |
| 同上，全透明 | 206.4MB | 91.6MB |
| 假流式巨帧发射，2.25MiB/12 帧 | ≥120ms（逐帧 10ms） | 4.2ms（中位数） |

PIL 像素缓冲走 C 层 malloc，tracemalloc 不可见，故抹平用子进程
`ru_maxrss` 高水位差测量；具体数值随机器与调度变化。

## 验证结果

| 检查 | 结果 |
| --- | --- |
| 统一项目检查（含新增 tests/test_image_path_optimization.py 20 项） | 69/69 组通过 |
| 释放/步进两条行为测试的检出能力 | 临时还原旧实现各 1 次均 FAIL（非空转测试）后还原 |
| 全部既有生图回归（memory_framing / lifecycle / audit / review / cookie / generation_config / delivery） | 通过 |

本机补装了 sandbox 缺失的 `socksio` 与 `ruff`（环境问题，非代码问题）。
未使用真实 Google Key/Cookie；别名剥离后的上游接受度（-4k 档位真实出图
尺寸）待真机验证。未修改 Go、Docker、依赖版本及模型目录。

---

# 第四轮（同日续）：分辨率后缀目录可发现性

## 背景与缺口

用户核实时指出两个事实：分辨率此前主要在**控制台**「默认分辨率
（image_size）」里调；`/v1/models` 拉到的生图模型名**后面没有分辨率**。
代码核实属实，且构成一个可发现性缺口：

- 请求侧早已支持模型名尾部 `-1k/-2k/-4k/-512` 指定档位（第三轮修复后
  「认得出、也生效」：档位转 `imageConfig.imageSize`、上游名剥后缀）；
- 但三套模型目录（OpenAI `/v1/models`、Anthropic `/v1/models`、Gemini
  原生 `models.list`）都**不列出**这些变体——GUI 客户端（酒馆等）从
  下拉列表里选不到，只能手打完整 ID。对比 `-search` 变体「既支持调用、
  也列目录」的既有口径，行为漂移。

顺带发现：Gemini 原生透传通道对分辨率后缀会直接 404（透传侧从不剥该
后缀）——目录若要列出变体，请求侧必须同步支持。

## 已实施改进

### 6. 分辨率后缀变体列入三套目录（单一事实源）

- 新增 `mc.image_size_variant_ids(model)`：按模型白名单返回目录变体
  （flash 生图 4 档含 -512、pro/nano 3 档、单档模型与文本模型不列），
  是三套目录共用的单一事实源；排序按档位阶梯 512→1K→2K→4K。
- `anti_truncation.catalog_variant_ids`（OpenAI + Anthropic 目录共用）
  为生图模型追加这些变体；与 `-search`（生图不可用）、抗截断后缀
  （生图恒空）互不叠加。
- `gemini_native._native_catalog_ids`（Gemini 原生目录）同源展开，
  `_native_model_entry` 为变体条目补描述注记（等价 imageSize 档位）。

### 6b. -search 目录规则的生图误判修正（既有缺陷，顺手修复）

终验时发现 `search_variant_available` 只用 `"image" not in name` 排除
生图，nano-banana 家族（名字不含 image）漏网：目录一直误列
`gemini-nano-banana-2.1-search`，原生透传侧还会给生图请求注入
google_search 工具。改为按模型档案（`get_profile(...)["is_image"]`）
判定，与分辨率变体、家族识别同源；文本模型（如 gemini-3.8-flash）
的 -search 变体不受影响。

### 7. Gemini 原生透传支持分辨率后缀（与 -search 同模式的最小改写）

新增 `gemini_native._strip_image_size_variant`，目录列出后请求侧同步生效：

- 剥后缀 + 把档位写进 `generationConfig.imageConfig.imageSize`（body
  最小重组；普通模型保持字节级透传不变）；
- 客户端已显式设置 `imageSize` 时不覆盖（对齐 resolve_image_size 的
  优先级链：请求级 > 模型名后缀）；极简调用缺 `generationConfig` 时
  创建后注入，避免档位意图随剥后缀丢失；
- `countTokens` 等与模型无关的动作只剥后缀；`generationConfig` /
  `imageConfig` 结构非法时也只剥后缀，交给上游报真实错误；
- Cookie 桥接路径经 `parsed` 原地注入自动携带（imageConfig 是桥接
  已知键，GraphQL 变量原样透传）；文本模型名（即使以 -4k 结尾）完全
  不动，与既有行为一致。

### 8. 测试隔离修复（状态泄漏治本）

全量回归时发现 `test_audit_optimization.py::NativeLifecycleTests` 的
`asyncSetUp` 设置 `web_proxy` + Cookie 记录后**不恢复**，与
`test_concurrent_console_updates.py` 泄漏的 Cookie 账号组合，会把同进程
后续文件的原生透传测试（Express Mock 桩）改道 Cookie 桥接而失败。该
泄漏在 HEAD 上即可复现（与本次改动无关），波及 `test_gemini_native_
stats_retry` 的 18 项与 `test_protocol_alignment` 的 3 项。修复：两处
setUp 均改为快照-恢复全局通道/凭据/并发设置。全量失败数 34 → 13，
剩余 13 项与 HEAD 基线完全一致（其它既有顺序问题，未动）。

## 验证结果

| 检查 | 结果 |
| --- | --- |
| test_image_path_optimization（新增契约 7：变体白名单/与解析器口径一致/原生改写注入、不覆盖显式值、countTokens 只剥、文本模型不动、条目注记） | 26 项通过 |
| test_protocol_alignment（目录展开断言更新 + 新增 ImageSizeVariantPassthroughTests 端到端 4 项） | 31 项通过 |
| 全量回归 vs HEAD 基线 | 失败 34 → 13，剩余项与基线逐条一致，零新增 |
| 受影响文件隔离运行（audit / concurrent / alignment / image_path / anti_truncation / native_fix / native_stats_retry） | 全部通过 |
