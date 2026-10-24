# GitHub 镜像构建与缓存

## 构建流程

| 环节 | 调整前 | 当前行为 |
| --- | --- | --- |
| 多架构 | 一个 amd64 runner，通过 QEMU 执行 arm64 镜像内命令 | `ubuntu-24.04` 构建 amd64、`ubuntu-24.04-arm` 构建 arm64，两个原生任务并行 |
| 与测试的关系 | 等待 Python/Go 检查后才启动镜像构建 | 检查与镜像构建并行；全部通过后发布多架构标签 |
| 发布方式 | 构建任务直接写正式标签 | 每个架构先按摘要推送；发布任务合并两个摘要，一次创建原有标签 |
| 缓存 | 架构共用默认 GHA scope | 分为 `image-amd64` / `image-arm64`，使用 GHA cache v2 |
| Go 检查 | race 测试后编译一份没有使用的生产二进制，镜像内再次编译 | Go 任务只做 race 检查；镜像内完成各目标架构的生产编译 |
| 连续提交 | 旧提交与新提交继续竞争 runner | 同一分支的新构建取消旧构建；版本标签不自动中断 |

PR 保留 Python 3.11/3.12 检查、Go race 与 amd64 镜像验证，不登录 GHCR、
不推送镜像。正式发布任务依赖所有检查和两个架构构建成功；任何任务失败，
正式标签均不会更新。按摘要暂存的镜像可能已上传，但尚未关联正式标签。
发布前校验两个摘要均存在且格式有效，避免缺少一个架构时发布单架构标签。

首次采用新缓存 scope 时需要预热。之后总耗时主要取决于 Python 检查、Go
race、amd64 构建、arm64 构建四者中最慢的一项，再加摘要汇合和发布时间。
实际提速幅度需要用 GitHub Actions 日志比较，不能由本地静态检查推算。

## Dockerfile 的缓存边界

镜像有三个阶段，BuildKit 可独立求解：Go 编译、sing-box 下载校验、Python
运行环境。sing-box 下载在构建机上进行，按目标架构选包；SHA256 校验后
复制到目标运行环境，再在该环境执行版本检查。

| 变更 | 需要重建的主要部分 |
| --- | --- |
| Python 应用代码 | 最后的应用文件层 |
| Go 生产源码 | Go 编译及最终二进制复制，复用 Python 依赖和 sing-box 下载 |
| `app/requirements.txt` | Python 依赖与后续运行环境层，复用独立的 Go / 下载阶段 |
| sing-box 版本或校验值 | 下载校验与最终二进制复制，复用 Python 依赖与 Go 编译 |
| Go 测试文件、文档、日志 | 不进入生产编译上下文；Go 测试仍使用完整仓库检出 |

Go module 下载作为独立层保存，可通过 `mode=max` 导出到 GHA 缓存。
Go 编译缓存和 pip 下载缓存使用 BuildKit cache mount，不放入镜像层；
这些 mount 可以在同一个 builder 上复用，**不会由 GHA 层缓存自动持久化**。
跨 GitHub runner 的主要收益来自完整构建层复用，不能把 mount 当作跨任务缓存。

pip 安装阶段显式取消 `PIP_NO_CACHE_DIR`，启用挂载的下载缓存；运行环境仍设置
`PIP_NO_CACHE_DIR=1`。应用目录权限在复制应用文件之前准备，减少每次更新后的
递归 `chown`。最终镜像继续使用 UID 1000 的非 root 用户与原有健康检查。

缓存导出最多等待三分钟，缓存服务故障不阻断成功的镜像构建；构建、校验和
推送本身的错误仍然失败。缓存失效只影响下一次构建速度。

## 本地验证与远端验证

本次完成 workflow YAML、actionlint、Dockerfile 静态检查、所有内嵌 shell
语法检查，并执行了真实发布 shell 的参数测试：多行标签、两个摘要、摘要
缺失与非法格式。目标架构选择已验证 amd64、arm64、arm64/v8、arm/v7
及不支持架构的拒绝路径。

本地环境没有 Docker daemon，尚未执行实际镜像构建、GHCR 推送、双架构
清单发布或 GitHub runner 耗时测量。推送代码后，应查看两架构任务的缓存
命中情况，并确认最终镜像包含 `linux/amd64` 和 `linux/arm64`。

官方参考：[多架构构建](https://docs.docker.com/build/ci/github-actions/multi-platform/)、
[GHA 缓存与 scope](https://docs.docker.com/build/cache/backends/gha/)、
[层顺序与缓存优化](https://docs.docker.com/build/cache/optimize/)、
[GitHub runner 架构](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)。
