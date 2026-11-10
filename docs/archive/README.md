# docs/archive/

历史审查与优化轮次的过程快照（2026-11 审查归档）。

这些文档记录的是**归档时点的基线行为**（多数带有 `基线 <commit>` 声明），
不描述当前代码；各轮变更的权威叙述见根目录 `CHANGES.md`。
保留在此供回溯决策过程与历史性能数据。

## 目录

| 文档 | 内容 |
| --- | --- |
| optimization-2026-10-25.md / optimization-2026-10-30.md | 早期优化轮次 |
| optimization-2026-11-03-image.md | 生图链路五轮优化（4K OOMKill 等） |
| image-memory-audit-2026-11-03.md | 生图内存专项复核 |
| review-2026-10-25.md / review-2026-11-03-iterative.md | 深入审查轮次 |
| comprehensive-review.md / optimization-review.md | 全面审查记录 |
| maintainability-2026-10-25.md | 可维护性重构记录 |
| protocol-review-2026-10-30.md | 协议审查 |
| cookie-tool-deltas-2026-10-30.md / cookie-tool-stream-fix-2026-10-30.md | Cookie 工具流修复 |
| audit-2ace593/ | 基线 2ace593 的审计证据包（含 repro 脚本，描述的是**修复前**缺陷行为，不是当前版本的回归断言） |

当前有效的活文档在 `docs/` 根层：`architecture.md`（架构约定）、
`proxy-turn-sstp.md`（TURN/SSTP 接入）、`proxy-protocol-validation.md`
（协议验证证据）、`image-build.md`（镜像构建）、`console-usability.md`（控制台）。
