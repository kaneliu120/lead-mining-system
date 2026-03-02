FROM python:3.11-slim

# ── 系统依赖（Playwright Chromium 需要）────────────────────────────────────────
# 注：Debian trixie ARM64 上 libasound2 → libasound2t64，ttf-* → fonts-*
RUN apt-get update && apt-get install -y \
    curl wget gnupg ca-certificates \
    fonts-unifont fonts-noto \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libdbus-1-3 libxkbcommon0 libxcomposite1 libxdamage1 \
    libxfixes3 libxrandr2 libgbm1 libpangocairo-1.0-0 \
    libcairo2 libx11-xcb1 libxcb1 libxext6 libxshmfence1 \
    && (apt-get install -y libasound2 || apt-get install -y libasound2t64 || true) \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── 安装 Python 依赖 ───────────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── 安装 Playwright Chromium（仅浏览器二进制，系统依赖已在上方安装）──────────────
# 不使用 --with-deps，避免 Debian trixie ARM64 字体包名称兼容问题
RUN playwright install chromium

# ── 复制应用代码 ───────────────────────────────────────────────────────────────
COPY . .

# ── 创建非 root 用户运行（安全实践）──────────────────────────────────────────────
RUN useradd -m appuser && chown -R appuser /app
USER appuser

EXPOSE 8000

# ── 健康检查 ───────────────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "-m", "uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
