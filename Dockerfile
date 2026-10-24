# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.27.3-bookworm AS proxy-bridge-build
WORKDIR /src
COPY proxybridge/go.mod proxybridge/go.sum ./
RUN go mod download
COPY proxybridge/*.go ./
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT
RUN --mount=type=cache,target=/root/.cache/go-build \
    GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} GOARM=${TARGETVARIANT#v} \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /vertex-proxy-bridge .


# 下载阶段仅依赖版本与目标架构，和 Go / Python 应用代码独立缓存。
FROM --platform=$BUILDPLATFORM python:3.11-slim AS singbox-download
# sing-box 版本与各架构 tar.gz 的 SHA256（出站代理多协议转换引擎：vless/vmess/
# trojan/ss/socks4/hysteria2/tuic 分享链接 → 本地 socks5，见 app/proxy_resolver.py）。
# 需要更新时改这几个 ARG 即可；构建时可 --build-arg SINGBOX_VERSION=vX.Y.Z 覆盖，
# 自定义版本时必须同步提供 --build-arg SINGBOX_SHA256_<arch>（留空则跳过校验，不推荐）。
ARG SINGBOX_VERSION=1.14.3
ARG SINGBOX_SHA256_amd64=e2bdf179c15a3652955dc44867e33ca0965c9c9b91b34267dcc5a5d639a5feee
ARG SINGBOX_SHA256_arm64=29dae24d76bea3bc62d07dd3e60cfc12003de7debbd6b768df9be9c68360174e
ARG SINGBOX_SHA256_armv7=8e81ee6bc1db6fec6e0342360e1de7bfabecec8d2e9c480a3563d129f84fb034

# Install sing-box（多协议代理转换，静态二进制约 30MB）。
# slim 镜像没有 curl/wget，用 Python 标准库下载（跟随 GitHub 302 跳转）；
# 在构建机上下载目标架构的包，无需模拟执行目标平台的 Python。
# 国内构建环境若访问 GitHub 困难，可 --build-arg SINGBOX_MIRROR=https://ghproxy.example/
# 走镜像前缀（须以 / 结尾、支持原样透传路径）。
ARG SINGBOX_MIRROR=""
ARG TARGETARCH
ARG TARGETVARIANT
# 下载后做 SHA256 校验（哈希与镜像前缀无关，SINGBOX_MIRROR 的文件内容不同
# 会被直接拒绝——sing-box 是流量出口安全组件，镜像被劫持/中间人会直接进镜像）。
RUN set -eux; \
    case "${TARGETARCH}/${TARGETVARIANT}" in \
        amd64/) SINGBOX_ARCH=amd64; SINGBOX_SHA256="$SINGBOX_SHA256_amd64" ;; \
        arm64/|arm64/v8) SINGBOX_ARCH=arm64; SINGBOX_SHA256="$SINGBOX_SHA256_arm64" ;; \
        arm/v7) SINGBOX_ARCH=armv7; SINGBOX_SHA256="$SINGBOX_SHA256_armv7" ;; \
        *) echo "unsupported architecture for sing-box: ${TARGETARCH}${TARGETVARIANT}"; exit 1 ;; \
    esac; \
    SINGBOX_URL="${SINGBOX_MIRROR}https://github.com/SagerNet/sing-box/releases/download/v${SINGBOX_VERSION}/sing-box-${SINGBOX_VERSION}-linux-${SINGBOX_ARCH}.tar.gz"; \
    python -c "import urllib.request; urllib.request.urlretrieve('${SINGBOX_URL}', '/tmp/singbox.tgz')"; \
    if [ -n "$SINGBOX_SHA256" ]; then \
        printf '%s  /tmp/singbox.tgz\n' "$SINGBOX_SHA256" > /tmp/singbox.sha256; \
        sha256sum -c /tmp/singbox.sha256; \
    else \
        echo "WARNING: SINGBOX_SHA256_${SINGBOX_ARCH} is empty; integrity check skipped"; \
    fi; \
    tar -xzf /tmp/singbox.tgz -C /tmp; \
    mv "/tmp/sing-box-${SINGBOX_VERSION}-linux-${SINGBOX_ARCH}/sing-box" /usr/local/bin/sing-box; \
    chmod 0755 /usr/local/bin/sing-box; \
    rm -rf /tmp/singbox.tgz /tmp/singbox.sha256 "/tmp/sing-box-${SINGBOX_VERSION}-linux-${SINGBOX_ARCH}"


FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 在应用代码进入镜像前准备权限，避免每次更新都递归 chown 整个应用层。
RUN useradd --system --uid 1000 --create-home app \
    && mkdir -p /app/data \
    && chown app:app /app /app/data

# Python 依赖不依赖代理二进制或 sing-box 版本，可独立命中缓存。
COPY app/requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    env -u PIP_NO_CACHE_DIR pip install -r requirements.txt

# 三个阶段由 BuildKit 并行求解；更新 Go 代码不再触发 pip 或 sing-box 下载。
COPY --from=singbox-download /usr/local/bin/sing-box /usr/local/bin/sing-box
RUN /usr/local/bin/sing-box version
COPY --from=proxy-bridge-build /vertex-proxy-bridge /usr/local/bin/vertex-proxy-bridge
COPY proxybridge/NOTICE.txt /usr/local/share/vertex-proxy-bridge/NOTICE.txt

# Copy application code and local model fallback
COPY --chown=app:app app/ .
COPY --chown=app:app vertexModels.json .

# 非 root 运行：容器内进程不再持有 root 权限。
# web_state.json（含 Cookie/Key）落在 /app/data（STATE_DIR）或 /app，
# 目录所有权交给 app 用户，避免凭据文件被同容器其他进程随意读写。
# 自托管挂卷时请确保卷目录对 UID 1000 可写（如 chown 1000 data/）。
USER app

# Expose the port
EXPOSE 7860

# 容器级健康检查：复用应用内 /healthz（公开、零上游请求、开销≈0）。
# slim 镜像没有 curl/wget，用 Python 标准库实现；HEAD 方法不下载 body，开销最小。
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request,sys; req=urllib.request.Request('http://127.0.0.1:7860/healthz',method='HEAD'); sys.exit(0 if urllib.request.urlopen(req,timeout=4).status<500 else 1)"]

# Command to run the application
# Run the FastAPI service on the container port
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7860"]
