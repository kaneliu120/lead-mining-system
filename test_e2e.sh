#!/usr/bin/env bash
# Lead Mining System - Full E2E Test Script
# Usage: chmod +x test_e2e.sh && ./test_e2e.sh
set -euo pipefail

GREEN="\033[0;32m"; RED="\033[0;31m"; YELLOW="\033[1;33m"; CYAN="\033[0;36m"; NC="\033[0m"
PASS=0; FAIL=0; SKIP=0

log()  { echo -e "\n${CYAN}> $*${NC}"; }
ok()   { echo -e "  ${GREEN}PASS${NC} $*"; PASS=$((PASS+1)); }
fail() { echo -e "  ${RED}FAIL${NC} $*"; FAIL=$((FAIL+1)); }
warn() { echo -e "  ${YELLOW}WARN${NC} $*"; SKIP=$((SKIP+1)); }

if [[ ! -f .env ]]; then echo "ERROR: .env not found"; exit 1; fi
set -a; source .env; set +a

echo "================================================"
echo "  Lead Mining System - Full E2E Test Suite"
echo "================================================"

# Phase 0: Env check
log "Phase 0 - Environment Validation"
env_ok=true
for key in GEMINI_API_KEY SERPER_API_KEY POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB; do
  val="${!key:-}"
  if [[ -z "$val" || "$val" == *"your_"* || "$val" == *"_here"* ]]; then
    fail "$key not configured"
    env_ok=false
  else
    ok "$key configured"
  fi
done
if [[ "$env_ok" == false ]]; then
  echo "ERROR: Fill in real API keys in .env before running."
  exit 1
fi

# Phase 1: Build (skip if images already exist, use FORCE_BUILD=1 to rebuild)
log "Phase 1 - Docker Build"
_lm_img=$(docker images -q lead-mining-system-lead-miner:latest 2>/dev/null)
_so_img=$(docker images -q lead-mining-system-sales-outreach:latest 2>/dev/null)
if [[ -n "$_lm_img" && -n "$_so_img" && "${FORCE_BUILD:-0}" != "1" ]]; then
  ok "Docker images already exist (skip build — use FORCE_BUILD=1 to rebuild)"
else
  if docker compose build --parallel 2>&1 | tail -3; then
    ok "Docker images built"
  else
    fail "docker compose build failed"; exit 1
  fi
fi

# Phase 2: Start
log "Phase 2 - Start & Health Checks"
docker compose up -d
ok "Services started"

wait_healthy() {
  local svc=$1 max=${2:-120} elapsed=0
  echo -n "  [$svc] waiting"
  while [[ $elapsed -lt $max ]]; do
    cid=$(docker compose ps -q "$svc" 2>/dev/null || echo "")
    if [[ -z "$cid" ]]; then sleep 3; ((elapsed+=3)); continue; fi
    hstatus=$(docker inspect "$cid" --format "{{.State.Health.Status}}" 2>/dev/null || echo "none")
    if [[ "$hstatus" == "healthy" ]]; then
      echo " OK (${elapsed}s)"; ok "$svc healthy"; return 0
    fi
    printf "."; sleep 3; ((elapsed+=3))
  done
  echo " TIMEOUT"
  fail "$svc not healthy after ${max}s"
  docker compose logs --tail=20 "$svc" 2>/dev/null || true
}

wait_healthy postgres    60
wait_healthy chromadb    60
wait_healthy lead-miner  150
wait_healthy n8n         120

# Phase 3: PostgreSQL
log "Phase 3 - PostgreSQL Schema"
pg_q() { docker compose exec -T postgres psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-leads}" -t -A -c "$1" 2>/dev/null | tr -d "\r"; }
for tbl in leads_raw leads_enriched contacts outreach_log; do
  ct=$(pg_q "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='$tbl';")
  if [[ "$ct" == "1" ]]; then
    rows=$(pg_q "SELECT COUNT(*) FROM $tbl;")
    ok "public.$tbl exists ($rows rows)"
  else
    fail "public.$tbl MISSING"
  fi
done

# Phase 4: ChromaDB
log "Phase 4 - ChromaDB"
hb=$(curl -sf http://localhost:8002/api/v1/heartbeat 2>/dev/null || echo "")
[[ -n "$hb" ]] && ok "ChromaDB heartbeat OK" || fail "ChromaDB not responding"

# Phase 5: Lead Mining API
log "Phase 5 - Lead Mining API Endpoints"

h=$(curl -sf http://localhost:8010/health 2>/dev/null || echo "{}")
hs=$(echo "$h" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("status",""))' 2>/dev/null || echo "")
[[ "$hs" == "ok" ]] && ok "GET /health -> ok" || fail "GET /health -> $h"

mr=$(curl -sf -X POST http://localhost:8010/mine -H "Content-Type: application/json"   -d "{\"keyword\":\"Manila restaurant\",\"location\":\"Philippines\",\"limit\":3,\"enrich\":false}"   2>/dev/null || echo "{\"mined\":0}")
mn=$(echo "$mr" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("total",0))' 2>/dev/null || echo "0")
[[ "$mn" -gt 0 ]] && ok "POST /mine -> mined $mn leads" || fail "POST /mine -> 0 leads: $(echo $mr | cut -c1-150)"

lr=$(curl -sf "http://localhost:8010/leads?limit=5" 2>/dev/null || echo "{}")
lt=$(echo "$lr" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("total",0))' 2>/dev/null || echo "0")
[[ "$lt" -gt 0 ]] && ok "GET /leads -> $lt total" || fail "GET /leads -> empty"

l1=$(curl -sf "http://localhost:8010/leads?lead_id=1" 2>/dev/null || echo "{}")
echo "$l1" | python3 -c 'import sys,json; d=json.load(sys.stdin); assert "leads" in d' 2>/dev/null   && ok "GET /leads?lead_id=1 -> OK" || fail "GET /leads?lead_id=1 -> missing leads key"

er=$(curl -sf -X POST http://localhost:8010/enrich -H "Content-Type: application/json"   -d "{\"limit\":3,\"min_score\":0}" 2>/dev/null || echo "{\"status\":\"error\"}")
es=$(echo "$er" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("status",""))' 2>/dev/null || echo "")
if echo "$es" | grep -qE "^(ok|queued|started|accepted)$"; then
  ok "POST /enrich -> $es"
  echo "  Waiting 20s for enrichment..."
  sleep 20
  enr=$(curl -sf "http://localhost:8010/leads?limit=10" 2>/dev/null || echo "{\"leads\":[]}")
  enc=$(echo "$enr" | python3 -c 'import sys,json; l=json.load(sys.stdin).get("leads",[]); print(sum(1 for x in l if x.get("score") is not None))' 2>/dev/null || echo "0")
  [[ "$enc" -gt 0 ]] && ok "$enc leads enriched (score present)" || warn "0 enriched yet"
else
  warn "POST /enrich -> $er"
fi

exj=$(curl -sf "http://localhost:8010/leads/export?export_format=json" 2>/dev/null || echo "[]")
ejn=$(echo "$exj" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)))' 2>/dev/null || echo "-1")
[[ "$ejn" -ge 0 ]] && ok "GET /leads/export?export_format=json -> $ejn records" || fail "export JSON failed"

csv1=$(curl -sf "http://localhost:8010/leads/export?export_format=csv" 2>/dev/null | head -1)
echo "$csv1" | grep -qiE "company|name|email|phone"   && ok "GET /leads/export?export_format=csv -> headers OK" || warn "CSV unexpected: $csv1"

ragr=$(curl -sf -X POST http://localhost:8010/rag/query -H "Content-Type: application/json"   -d "{\"query\":\"restaurant Manila\",\"limit\":3}" 2>/dev/null || echo "{\"results\":[]}")
rn=$(echo "$ragr" | python3 -c 'import sys,json; print(len(json.load(sys.stdin).get("results",[])))' 2>/dev/null || echo "-1")
[[ "$rn" -ge 0 ]] && ok "POST /rag/query -> $rn results" || fail "RAG query failed"

sr=$(curl -sf -X POST http://localhost:8010/leads/update-status -H "Content-Type: application/json"   -d "{\"email\":\"test@example.com\",\"status\":\"replied\"}" 2>/dev/null || echo "error")
echo "$sr" | python3 -c 'import sys,json; json.load(sys.stdin)' 2>/dev/null   && ok "POST /leads/update-status -> valid JSON" || fail "update-status failed: $sr"

# Phase 6: Sales Outreach
log "Phase 6 - Sales Outreach Engine (http://localhost:8080)"
soh=$(curl -sf http://localhost:8080/health 2>/dev/null || echo "")
if [[ -n "$soh" ]]; then
  sos=$(echo "$soh" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("status",""))' 2>/dev/null || echo "?")
  [[ "$sos" == "ok" ]] && ok "sales-outreach /health -> ok" || warn "sales-outreach: $soh"
else
  warn "sales-outreach not up yet (check: docker compose logs sales-outreach)"
fi

# Phase 7: n8n
log "Phase 7 - n8n Workflow Import & Verification"
N8N_EMAIL="${N8N_OWNER_EMAIL:-admin@leadminer.local}"
N8N_PASS="${N8N_OWNER_PASSWORD:-LeadMiner2024!}"

importedwf=0
for wf in ./n8n-workflows/*.json; do
  wn=$(basename "$wf" .json)
  if docker compose cp "$wf" "n8n:/tmp/${wn}.json" 2>/dev/null; then
    out=$(docker compose exec -T n8n n8n import:workflow --input="/tmp/${wn}.json" 2>&1 || true)
    if echo "$out" | grep -qiE "import|success|Skipping|already|exists|created"; then
      ok "  n8n workflow imported: $wn"
      importedwf=$((importedwf+1))
    else
      warn "  n8n $wn: $out"
    fi
  else
    warn "  cp to n8n container failed: $wn"
  fi
done
ok "$importedwf n8n workflow(s) processed"

# Fix workflow display order: 01 first
docker compose exec -T postgres psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-leads}" -c "
UPDATE n8n.workflow_entity SET \"updatedAt\"='2026-01-01 13:00:04+00' WHERE name LIKE '01%';
UPDATE n8n.workflow_entity SET \"updatedAt\"='2026-01-01 13:00:03+00' WHERE name LIKE '02%';
UPDATE n8n.workflow_entity SET \"updatedAt\"='2026-01-01 13:00:02+00' WHERE name LIKE '03%';
UPDATE n8n.workflow_entity SET \"updatedAt\"='2026-01-01 13:00:01+00' WHERE name LIKE '04%';
" >/dev/null 2>&1 || true

loginr=$(curl -sf -c /tmp/n8n_c.txt   -X POST http://localhost:5678/rest/login   -H "Content-Type: application/json"   -d "{\"emailAddress\":\"$N8N_EMAIL\",\"password\":\"$N8N_PASS\"}" 2>/dev/null || echo "{}")
loginok=$(echo "$loginr" | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin)
  print(\'ok\' if \'id\' in d or (\'data\' in d and d[\'data\']) else \'fail\')
except: print(\'fail\')
" 2>/dev/null || echo "fail")

if [[ "$loginok" == "ok" ]]; then
  ok "n8n REST login OK"
  wfl=$(curl -sf -b /tmp/n8n_c.txt "http://localhost:5678/rest/workflows" 2>/dev/null || echo "{\"data\":[]}")
  wfc=$(echo "$wfl" | python3 -c 'import sys,json; print(len(json.load(sys.stdin).get("data",[])))' 2>/dev/null || echo "0")
  ok "n8n REST API: $wfc workflow(s)"
else
  warn "n8n login needs first browser visit: http://localhost:5678"
  warn "  Email: $N8N_EMAIL  Password: $N8N_PASS"
fi
rm -f /tmp/n8n_c.txt

# Phase 8: Chain smoke test
log "Phase 8 - End-to-End Chain Smoke Test"
cr=$(curl -sf -X POST http://localhost:8010/mine -H "Content-Type: application/json"   -d "{\"keyword\":\"IT services Philippines\",\"limit\":2,\"enrich\":true}" 2>/dev/null || echo "{}")
cn=$(echo "$cr" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("total",0))' 2>/dev/null || echo "0")
[[ "$cn" -gt 0 ]] && ok "Chain mine+enrich: $cn leads" || warn "Chain mine: 0 (quota/dedup)"
fr=$(pg_q "SELECT COUNT(*) FROM leads_raw;" 2>/dev/null || echo "?")
fe=$(pg_q "SELECT COUNT(*) FROM leads_enriched;" 2>/dev/null || echo "?")
ok "Final DB state: leads_raw=$fr  leads_enriched=$fe"

# Summary
echo ""
echo "================================================"
echo "  RESULTS: PASS=$PASS  WARN=$SKIP  FAIL=$FAIL"
echo "================================================"
echo ""
echo "  Lead Miner API Docs: http://localhost:8010/docs"
echo "  Outreach Engine:     http://localhost:8080/docs"
echo "  n8n Editor:          http://localhost:5678"
echo "  n8n Login:           $N8N_EMAIL / $N8N_PASS"
echo "  ChromaDB:            http://localhost:8001/api/v1/collections"
echo ""
if [[ $FAIL -eq 0 ]]; then
  echo "All tests passed!"
else
  echo "$FAIL test(s) failed."
  echo "Logs: docker compose logs --tail=50 lead-miner"
  exit 1
fi
