#!/bin/bash
# setup-ssl.sh — Initialize/update Nginx + Let's Encrypt SSL on VM
# Usage: bash scripts/setup-ssl.sh [DOMAIN]
# If no argument provided, automatically uses DOMAIN from .env, or generates a nip.io domain from public IP
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# ── 1. Determine domain ───────────────────────────────────────────────────────
if [ -n "${1:-}" ]; then
  DOMAIN="$1"
elif grep -q "^DOMAIN=" .env 2>/dev/null; then
  DOMAIN="$(grep '^DOMAIN=' .env | cut -d= -f2 | tr -d ' \r')"
fi

if [ -z "${DOMAIN:-}" ]; then
  # Automatically get VM public IP, generate nip.io domain (no domain purchase needed, DNS auto-resolves)
  PUB_IP="$(curl -s --max-time 5 https://api.ipify.org || curl -s --max-time 5 https://ifconfig.me)"
  if [ -z "$PUB_IP" ]; then
    echo "❌ Unable to get public IP, please set DOMAIN variable manually" >&2
    exit 1
  fi
  DOMAIN="${PUB_IP}.nip.io"
  echo "📡 Auto-detected public IP: $PUB_IP"
fi

echo "🌐 Using domain: $DOMAIN"
echo "   - n8n:            https://n8n.${DOMAIN}"
echo "   - Mining dashboard: https://mine.${DOMAIN}"
echo "   - Outreach API:   https://outreach.${DOMAIN}"

# ── 2. Update domain and n8n URL in .env ─────────────────────────────────────
if ! grep -q "^DOMAIN=" .env 2>/dev/null; then
  echo "DOMAIN=${DOMAIN}" >> .env
else
  sed -i "s|^DOMAIN=.*|DOMAIN=${DOMAIN}|" .env
fi

# Update n8n webhook/editor URL
sed -i "s|^WEBHOOK_URL=.*|WEBHOOK_URL=https://n8n.${DOMAIN}|"           .env
sed -i "s|^N8N_EDITOR_BASE_URL=.*|N8N_EDITOR_BASE_URL=https://n8n.${DOMAIN}|" .env

# ── 3. Ensure certbot mount directories exist ─────────────────────────────────
mkdir -p nginx/certbot/www nginx/certbot/conf

# ── 4. Request/renew SSL certificate ─────────────────────────────────────────
echo "🔐 Requesting Let's Encrypt SSL certificate..."

# First ensure nginx is running in HTTP mode (for ACME challenge)
docker compose -f docker-compose.prod.yml up -d nginx
sleep 3

# Request a single certificate covering three subdomains
# Note: use docker run instead of docker compose run to avoid infinite renewal loop blocking from service entrypoint
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
    echo "⚠️  SSL request failed (may be first attempt or nip.io rate limit), switching to HTTP-only mode"
    SSL_FAILED=1
  }

# ── 5. Generate full nginx configuration ─────────────────────────────────────
echo "📝 Generating nginx configuration..."

if [ "${SSL_FAILED:-0}" = "1" ]; then
  # ── HTTP only fallback ────────────────────────────────────────────────────
  cat > nginx/conf.d/default.conf <<NGINXEOF
# HTTP mode (SSL certificate failed or pending)

# Security response headers (required even in HTTP mode)
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
  echo "⚠️  HTTP mode configuration generated. Run 'bash scripts/setup-ssl.sh' later to add SSL"
else
  # ── HTTP → HTTPS redirect + HTTPS virtual hosts ───────────────────────────
  cat > nginx/conf.d/default.conf <<NGINXEOF
# ── HTTP → HTTPS redirect + ACME challenge ───────────────────────────────────
server {
    listen 80;
    server_name n8n.${DOMAIN} mine.${DOMAIN} outreach.${DOMAIN} ${DOMAIN};
    location /.well-known/acme-challenge/ { root /var/www/certbot; }
    location / { return 301 https://\$host\$request_uri; }
}

# ── Global security response headers (shared by all HTTPS virtual hosts) ─────
map \$sent_http_content_type \$csp_header {
    default "default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; connect-src 'self' wss:; font-src 'self'; object-src 'none'; frame-ancestors 'none';";
}

# SSL common parameters
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

    # Security response headers
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;
    add_header Permissions-Policy "geolocation=(), microphone=(), camera=()" always;

    # robots.txt — block crawlers
    location = /robots.txt {
        return 200 "User-agent: *\nDisallow: /\n";
        add_header Content-Type text/plain;
    }

    # WebSocket support (required for n8n editor)
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

# ── Lead Mining Dashboard ─────────────────────────────────────────────────────
server {
    listen 443 ssl;
    server_name mine.${DOMAIN};
    ssl_certificate     /etc/letsencrypt/live/lead-mining/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/lead-mining/privkey.pem;

    # Security response headers
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;
    add_header Permissions-Policy "geolocation=(), microphone=(), camera=()" always;
    add_header Content-Security-Policy \$csp_header always;

    # robots.txt — block crawlers from all paths
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

    # Security response headers
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;
    add_header Permissions-Policy "geolocation=(), microphone=(), camera=()" always;

    # robots.txt — block crawlers
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

# ── Root domain — neutral landing page (does not redirect to mining dashboard) ──
server {
    listen 443 ssl;
    server_name ${DOMAIN};
    ssl_certificate     /etc/letsencrypt/live/lead-mining/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/lead-mining/privkey.pem;

    # Security response headers
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "DENY" always;
    add_header Referrer-Policy "no-referrer" always;
    add_header Content-Security-Policy "default-src 'none'; frame-ancestors 'none';" always;

    # robots.txt — allow crawlers to reach this page (neutral content, helps build reputation)
    location = /robots.txt {
        return 200 "User-agent: *\nAllow: /\nDisallow: /admin\nDisallow: /login\nDisallow: /mine\n";
        add_header Content-Type text/plain;
    }

    # Neutral landing page (does not redirect to mining dashboard)
    location / {
        return 200 "<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'><title>SkillStore</title></head><body><h1>SkillStore</h1><p>Business automation tools.</p></body></html>";
        add_header Content-Type "text/html; charset=utf-8";
    }
}
NGINXEOF
  echo "✅ HTTPS configuration generated"

  # After SSL success, update n8n env vars to enable secure cookies
  sed -i "s|^N8N_PROTOCOL=.*|N8N_PROTOCOL=https|"         .env
  sed -i "s|^N8N_SECURE_COOKIE=.*|N8N_SECURE_COOKIE=true|" .env
  # Restart n8n to apply new WEBHOOK_URL and PROTOCOL
  docker compose -f docker-compose.prod.yml up -d --no-deps n8n
fi

# ── 6. Reload nginx ───────────────────────────────────────────────────────────
docker compose -f docker-compose.prod.yml exec nginx nginx -s reload 2>/dev/null \
  || docker compose -f docker-compose.prod.yml restart nginx

# ── 7. Print access URLs ──────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║         ✅  Lead Mining System — Access URLs             ║"
echo "╠══════════════════════════════════════════════════════════╣"
if [ "${SSL_FAILED:-0}" = "1" ]; then
echo "║  🔄 n8n Workflow       http://n8n.${DOMAIN}"
echo "║  🎛️  Mining Dashboard  http://mine.${DOMAIN}/admin"
echo "║  📤 Outreach API       http://outreach.${DOMAIN}"
else
echo "║  🔄 n8n Workflow       https://n8n.${DOMAIN}"
echo "║  🎛️  Mining Dashboard  https://mine.${DOMAIN}/admin"
echo "║  📤 Outreach API       https://outreach.${DOMAIN}"
echo "║  🌐 Root Domain        https://${DOMAIN} → n8n"
fi
echo "╚══════════════════════════════════════════════════════════╝"
