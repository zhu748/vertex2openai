FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 先建运行用户（后面的 COPY --chown 依赖它存在）
RUN useradd --system --uid 1000 --create-home app

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
