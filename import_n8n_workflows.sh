#!/usr/bin/env bash
# ============================================================
# n8n 工作流自动导入脚本
# 用法：./import_n8n_workflows.sh
# 前提：n8n 容器已 healthy
# ============================================================
set -euo pipefail

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
WORKFLOWS_DIR="./n8n-workflows"

echo -e "${GREEN}[n8n] Importing workflows via CLI...${NC}"

# 等待 n8n 完全就绪
wait_n8n() {
  local max=60 elapsed=0
  echo -n "  Waiting for n8n"
  while [[ $elapsed -lt $max ]]; do
    if curl -sf http://localhost:5678/healthz >/dev/null 2>&1; then
      echo " ready"
      return 0
    fi
    echo -n "."; sleep 2; ((elapsed+=2))
  done
  echo " TIMEOUT"; return 1
}
wait_n8n

# 使用 n8n CLI 导入每个工作流（在容器内执行）
imported=0
failed=0
for wf_file in "$WORKFLOWS_DIR"/*.json; do
  wf_name=$(basename "$wf_file" .json)
  
  # 将 JSON 写入容器临时路径
  docker compose cp "$wf_file" "n8n:/tmp/${wf_name}.json" 2>/dev/null
  
  # 使用 n8n CLI 导入
  if docker compose exec -T n8n n8n import:workflow --input="/tmp/${wf_name}.json" 2>&1 | grep -qiE "import|success|already"; then
    echo -e "  ${GREEN}✓${NC} Imported: $wf_name"
    ((imported++))
  else
    # 捕获详细输出
    output=$(docker compose exec -T n8n n8n import:workflow --input="/tmp/${wf_name}.json" 2>&1 || true)
    echo -e "  ${YELLOW}⚠${NC}  $wf_name: $output"
    ((failed++))
  fi
done

echo ""
echo -e "  Imported: ${GREEN}$imported${NC}  Failed: ${failed}"

# 列出已导入的工作流（通过 DB 查询）
echo ""
echo -e "${GREEN}[n8n] Workflows in database:${NC}"
docker compose exec -T postgres psql \
  -U "${POSTGRES_USER:-postgres}" \
  -d "${POSTGRES_DB:-leads}" \
  -t -c "SELECT id, name, active FROM n8n.workflow_entity ORDER BY id;" 2>/dev/null \
  | grep -v "^$" || echo "  (none yet — DB schema may not be initialized)"

echo ""
echo -e "${GREEN}✅ Done. Open n8n at: http://localhost:5678${NC}"
echo -e "   Login: ${N8N_OWNER_EMAIL:-admin@leadminer.local} / ${N8N_OWNER_PASSWORD:-LeadMiner2024!}"
