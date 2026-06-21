# Hybrid search API image. Uses uv for fast, lockfile-pinned installs.
FROM python:3.14-slim

# uv: standalone binary, copied from the official distroless image.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    # Cache the (heavy) sentence-transformers model under the app dir.
    HF_HOME=/app/.cache/huggingface

WORKDIR /app

# Install deps first, in their own layer, so code edits don't bust the cache.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# App code. Same image serves both the API (default CMD) and the Streamlit FE
# (the `fe` compose service overrides the command).
COPY main.py streamlit_app.py ./
COPY hybridsearch ./hybridsearch
# Streamlit theme (green accent) for the FE service.
COPY .streamlit ./.streamlit

# Connection (OpenSearch host, S3 bucket, ...) is provided at runtime via .env
# (docker-compose env_file). No connection defaults are baked into the image.

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health').status==200 else 1)"

CMD ["uv", "run", "--no-dev", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
