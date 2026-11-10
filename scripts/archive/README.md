# scripts/archive/

历史优化轮次的基准测试与一次性复现脚本（2026-11 审查归档）。

这些脚本对应已完成的优化轮次，结论已沉淀在 `docs/archive/` 的对应快照与
`CHANGES.md` 的轮次记录里；保留在此供回溯历史性能数据，不再参与任何 CI
或日常验证链。

| 脚本 | 对应轮次 |
| --- | --- |
| benchmark_hot_paths.py | 热路径基准（2026-10-25） |
| benchmark_image_path.py | 生图路径内存基准（2026-11-03 image 轮） |
| benchmark_image_memory_review.py | 生图内存复审基准（2026-11-03） |
| benchmark_native_assembly.py | 原生事件重组基准 |
| benchmark_native_part_size.py | 原生分片尺寸基准 |
| repro_first_reply_wholecall.py | 首回复整调用计时复现（CHANGES.md 第六轮） |

日常验证入口不变：`python scripts/check_project.py`（CI 同款）。
