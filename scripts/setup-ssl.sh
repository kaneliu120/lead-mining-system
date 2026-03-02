#!/bin/bash
# setup-ssl.sh — 在 VM 上初始化/更新 Nginx + Let's Encrypt SSL
# 用法: bash scripts/setup-ssl.sh [DOMAIN]
# 若不传参数, 自动使用 .env 中的 DOMAIN, 或从公网 IP 生成 nip.io 域名
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# ── 1. 确定域名 ───────────────────────────────────────────────────────────────
if [ -n "${1:-}" ]; then
  DOMAIN="$1"
elif grep -q "^DOMAIN=" .env 2>/dev/null; then
  DOMAIN="$(grep '^DOMAIN=' .env | cut -d= -f2 | tr -d ' \r')"
fi

if [ -z "${DOMAIN:-}" ]; then
  # 自动获取 VM 公网 IP, 生成 nip.io 域名（无需购买域名，DNS 自动生效）
  PUB_IP="$(curl -s --max-time 5 https://api.ipify.org || curl -s --max-time 5 https://ifconfig.me)"
  if [ -z "$PUB_IP" ]; then
    echo "❌ 无法获取公网 IP，请手动设置 DOMAIN 变量" >&2
    exit 1
  fi
  DOMAIN="${PUB_IP}.nip.io"
  echo "📡 自动检测到公网 IP: $PUB_IP"
fi

echo "🌐 使用域名: $DOMAIN"
echo "   - n8n:      https://n8n.${DOMAIN}"
echo "   - 采集面板: https://mine.${DOMAIN}"
echo "   - 外展 API: https://outreach.${DOMAIN}"

# ── 2. 更新 .env 中的域名和 n8n URL ──────────────────────────────────────────
if ! grep -q "^DOMAIN=" .env 2>/dev/null; then
  echo "DOMAIN=${DOMAIN}" >> .env
else
  sed -i "s|^DOMAIN=.*|DOMAIN=${DOMAIN}|" .env
fi

# 更新 n8n webhook/editor URL
sed -i "s|^WEBHOOK_URL=.*|WEBHOOK_URL=https://n8n.${DOMAIN}|"           .env
sed -i "s|^N8N_EDITOR_BASE_URL=.*|N8N_EDITOR_BASE_URL=https://n8n.${DOMAIN}|" .env

# ── 3. 确保 certbot 挂载目录存在 ─────────────────────────────────────────────
mkdir -p nginx/certbot/www nginx/certbot/conf

# ── 4. 申请/续期 SSL 证书 ────────────────────────────────────────────────────
echo "🔐 申请 Let's Encrypt SSL 证书..."

# 先确保 nginx 在 HTTP 模式下运行（用于 ACME challenge）
docker compose -f docker-compose.prod.yml up -d nginx
sleep 3

# 申请包含三个子域名的单张证书
# 注意: 使用 docker run 而非 docker compose run，避免 service entrypoint 的无限续期循环阻塞
docker run --rm \
  -v lead-mining-system_certbot_www:/var/www/certbot \
  -v lead-mining-system_certbot_certs:/etc/letsencrypt \
  --network lead-mining-net \
  certbot/certbot certonly \
  --webroot \
  --webroot-path /var/www/certbot \
  --email "kaneliu10@gmail.com" \
  --agree-tos \
  --no-eff-email \
  --non-interactive \
  -d "n8n.${DOMAIN}" \
  -d "mine.${DOMAIN}" \
  -d "outreach.${DOMAIN}" \
  -d "${DOMAIN}" \
  --cert-name "lead-mining" \
  || {
    echo "⚠️  SSL 申请失败（可能是首次或 nip.io 限速），切换到仅 HTTP 模式"
    SSL_FAILED=1
  }

# ── 5. 生成完整 nginx 配置 ────────────────────────────────────────────────────
echo "📝 生成 nginx 配置..."

if [ "${SSL_FAILED:-0}" = "1" ]; then
  # ── HTTP only fallback ────────────────────────────────────────────────────
  cat > nginx/conf.d/default.conf <<NGINXEOF
# HTTP 模式（SSL 证书获取失败或待申请）

# 安全响应头（HTTP 模式也需要）
add_header X-Content-Type-Options "nosniff" always;
add_header X-Frame-Options "SAMEORIGIN" always;
add_header Referrer-Policy "strict-origin-when-cross-origin" always;

server {
    listen 80;
    server_name n8n.${DOMAIN};
    location /.well-known/acme-challenge/ { root /var/www/certbot; }
    location /robots.txt { return 200 "User-agent: *\nDisallow: /\n"; add_header Content-Type text/plain; }
    location / {
        proxy_pass http://n8n:5678;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_read_timeout 86400;
    }
}
server {
    listen 80;
    server_name mine.${DOMAIN};
    location /.well-known/acme-challenge/ { root /var/www/certbot; }
    location /robots.txt { return 200 "User-agent: *\nDisallow: /\n"; add_header Content-Type text/plain; }
    location / {
        proxy_pass http://lead-miner:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header X-Real-IP \$remote_addr;
    }
}
server {
    listen 80;
    server_name outreach.${DOMAIN};
    location /.well-known/acme-challenge/ { root /var/www/certbot; }
    location /robots.txt { return 200 "User-agent: *\nDisallow: /\n"; add_header Content-Type text/plain; }
    location / {
        proxy_pass http://sales-outreach:8080;
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header X-Real-IP \$remote_addr;
    }
}
server {
    listen 80;
    server_name ${DOMAIN};
    location /.well-known/acme-challenge/ { root /var/www/certbot; }
    location /robots.txt { return 200 "User-agent: *\nDisallow: /\n"; add_header Content-Type text/plain; }
    location / { return 200 "Lead Mining System"; add_header Content-Type text/plain; }
}
NGINXEOF
  echo "⚠️  已生成 HTTP 模式配置，之后运行 'bash scripts/setup-ssl.sh' 可补充 SSL"
else
  # ── HTTP → HTTPS redirect + HTTPS virtual hosts ───────────────────────────
  cat > nginx/conf.d/default.conf <<NGINXEOF
# ── HTTP → HTTPS 强制跳转 + ACME challenge ───────────────────────────────────
server {
    listen 80;
    server_name n8n.${DOMAIN} mine.${DOMAIN} outreach.${DOMAIN} ${DOMAIN};
    location /.well-known/acme-challenge/ { root /var/www/certbot; }
    location / { return 301 https://\$host\$request_uri; }
}

# ── 全局安全响应头（所有 HTTPS 虚拟主机共享）────────────────────────────────
map \$sent_http_content_type \$csp_header {
    default "default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; connect-src 'self' wss:; font-src 'self'; object-src 'none'; frame-ancestors 'none';";
}

# SSL 通用参数
ssl_protocols TLSv1.2 TLSv1.3;
ssl_prefer_server_ciphers on;
ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384;
ssl_session_cache shared:SSL:10m;
ssl_session_timeout 10m;

# ── n8n ───────────────────────────────────────────────────────────────────────
server {
    listen 443 ssl;
    server_name n8n.${DOMAIN};
    ssl_certificate     /etc/letsencrypt/live/lead-mining/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/lead-mining/privkey.pem;

    # 安全响应头
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;
    add_header Permissions-Policy "geolocation=(), microphone=(), camera=()" always;

    # robots.txt — 禁止爬虫
    location = /robots.txt {
        return 200 "User-agent: *\nDisallow: /\n";
        add_header Content-Type text/plain;
    }

    # WebSocket 支持（n8n 编辑器必须）
    location / {
        proxy_pass http://n8n:5678;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_read_timeout 86400;
    }
}

# ── Lead Mining 控制台 ───────────────────────────────────────────────────────
server {
    listen 443 ssl;
    server_name mine.${DOMAIN};
    ssl_certificate     /etc/letsencrypt/live/lead-mining/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/lead-mining/privkey.pem;

    # 安全响应头
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;
    add_header Permissions-Policy "geolocation=(), microphone=(), camera=()" always;
    add_header Content-Security-Policy \$csp_header always;

    # robots.txt — 禁止爬虫访问所有路径
    location = /robots.txt {
        return 200 "User-agent: *\nDisallow: /\n";
        add_header Content-Type text/plain;
    }

    location / {
        proxy_pass http://lead-miner:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_read_timeout 120;
    }
}

# ── Sales Outreach API ────────────────────────────────────────────────────────
server {
    listen 443 ssl;
    server_name outreach.${DOMAIN};
    ssl_certificate     /etc/letsencrypt/live/lead-mining/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/lead-mining/privkey.pem;

    # 安全响应头
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;
    add_header Permissions-Policy "geolocation=(), microphone=(), camera=()" always;

    # robots.txt — 禁止爬虫
    location = /robots.txt {
        return 200 "User-agent: *\nDisallow: /\n";
        add_header Content-Type text/plain;
    }

    location / {
        proxy_pass http://sales-outreach:8080;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_read_timeout 120;
    }
}

# ── 根域名 — 中性落地页（不跳转到采集后台）──────────────────────────────────
server {
    listen 443 ssl;
    server_name ${DOMAIN};
    ssl_certificate     /etc/letsencrypt/live/lead-mining/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/lead-mining/privkey.pem;

    # 安全响应头
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "DENY" always;
    add_header Referrer-Policy "no-referrer" always;
    add_header Content-Security-Policy "default-src 'none'; frame-ancestors 'none';" always;

    # robots.txt — 允许爬虫爬到此页面（中性内容，帮助建立良好声誉）
    location = /robots.txt {
        return 200 "User-agent: *\nAllow: /\nDisallow: /admin\nDisallow: /login\nDisallow: /mine\n";
        add_header Content-Type text/plain;
    }

    # 中性落地页（不跳转到采集后台）
    location / {
        return 200 "<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'><title>SkillStore</title></head><body><h1>SkillStore</h1><p>Business automation tools.</p></body></html>";
        add_header Content-Type "text/html; charset=utf-8";
    }
}
NGINXEOF
  echo "✅ 已生成 HTTPS 配置"

  # SSL 成功后更新 n8n 环境变量，启用安全 cookie
  sed -i "s|^N8N_PROTOCOL=.*|N8N_PROTOCOL=https|"         .env
  sed -i "s|^N8N_SECURE_COOKIE=.*|N8N_SECURE_COOKIE=true|" .env
  # 重启 n8n 以应用新的 WEBHOOK_URL 和 PROTOCOL
  docker compose -f docker-compose.prod.yml up -d --no-deps n8n
fi

# ── 6. 重载 nginx ─────────────────────────────────────────────────────────────
docker compose -f docker-compose.prod.yml exec nginx nginx -s reload 2>/dev/null \
  || docker compose -f docker-compose.prod.yml restart nginx

# ── 7. 打印访问地址 ───────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║            ✅  Lead Mining System — 访问地址             ║"
echo "╠══════════════════════════════════════════════════════════╣"
if [ "${SSL_FAILED:-0}" = "1" ]; then
echo "║  🔄 n8n 工作流     http://n8n.${DOMAIN}"
echo "║  🎛️  采集控制台   http://mine.${DOMAIN}/admin"
echo "║  📤 外展 API      http://outreach.${DOMAIN}"
else
echo "║  🔄 n8n 工作流     https://n8n.${DOMAIN}"
echo "║  🎛️  采集控制台   https://mine.${DOMAIN}/admin"
echo "║  📤 外展 API      https://outreach.${DOMAIN}"
echo "║  🌐 主域名        https://${DOMAIN} → n8n"
fi
echo "╚══════════════════════════════════════════════════════════╝"
