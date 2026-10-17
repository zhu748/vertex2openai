FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# sing-box 版本（出站代理多协议转换引擎：vless/vmess/trojan/ss/socks4/
# hysteria2/tuic 分享链接 → 本地 socks5，见 app/proxy_resolver.py）。
# 需要更新时改这一个 ARG 即可；构建时可 --build-arg SINGBOX_VERSION=vX.Y.Z 覆盖。
ARG SINGBOX_VERSION=1.14.3

WORKDIR /app

# 先建运行用户（后面的 COPY --chown 依赖它存在）
RUN useradd --system --uid 1000 --create-home app

# Install sing-box（多协议代理转换，静态二进制约 30MB）。
# slim 镜像没有 curl/wget，用 Python 标准库下载（跟随 GitHub 302 跳转）；
# 按构建机架构选包（amd64 / arm64 / armv7）。
# 国内构建环境若访问 GitHub 困难，可 --build-arg SINGBOX_MIRROR=https://ghproxy.example/
# 走镜像前缀（须以 / 结尾、支持原样透传路径）。
ARG SINGBOX_MIRROR=""
RUN set -eux; \
    case "$(dpkg --print-architecture)" in \
        amd64) SINGBOX_ARCH=amd64 ;; \
        arm64) SINGBOX_ARCH=arm64 ;; \
        armhf) SINGBOX_ARCH=armv7 ;; \
        *) echo "unsupported architecture for sing-box: $(dpkg --print-architecture)"; exit 1 ;; \
    esac; \
    SINGBOX_URL="${SINGBOX_MIRROR}https://github.com/SagerNet/sing-box/releases/download/v${SINGBOX_VERSION}/sing-box-${SINGBOX_VERSION}-linux-${SINGBOX_ARCH}.tar.gz"; \
    python -c "import sys, urllib.request; urllib.request.urlretrieve('${SINGBOX_URL}', '/tmp/singbox.tgz')"; \
    tar -xzf /tmp/singbox.tgz -C /tmp; \
    mv "/tmp/sing-box-${SINGBOX_VERSION}-linux-${SINGBOX_ARCH}/sing-box" /usr/local/bin/sing-box; \
    chmod 0755 /usr/local/bin/sing-box; \
    rm -rf /tmp/singbox.tgz "/tmp/sing-box-${SINGBOX_VERSION}-linux-${SINGBOX_ARCH}"; \
    /usr/local/bin/sing-box version

# Install dependencies（单独一层，代码变动不触发重装）
COPY app/requirements.txt .
RUN pip install -r requirements.txt

# Copy application code and local model fallback
COPY --chown=app:app app/ .
COPY --chown=app:app vertexModels.json .

# 非 root 运行：容器内进程不再持有 root 权限。
# web_state.json（含 Cookie/Key）落在 /app/data（STATE_DIR）或 /app，
# 目录所有权交给 app 用户，避免凭据文件被同容器其他进程随意读写。
# 自托管挂卷时请确保卷目录对 UID 1000 可写（如 chown 1000 data/）。
RUN mkdir -p /app/data && chown -R app:app /app
USER app

# Expose the port
EXPOSE 7860

# 容器级健康检查：复用应用内 /healthz（公开、零上游请求、开销≈0）。
# slim 镜像没有 curl/wget，用 Python 标准库实现；HEAD 方法不下载 body，开销最小。
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; \
req=urllib.request.Request('http://127.0.0.1:7860/healthz',method='HEAD'); \
sys.exit(0 if urllib.request.urlopen(req,timeout=4).status<500 else 1)"

# Command to run the application
# Run the FastAPI service on the container port
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7860"]
