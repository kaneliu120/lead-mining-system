# Lead Mining System — Philippines SME automated sales lead & outreach system

## System Architecture

```
lead-mining-system/
├── lead-mining-engine/          # FastAPI Lead Mining Engine
│   ├── app/
│   │   ├── miners/plugins/      # 6 data source plugins
│   │   ├── enrichers/           # Gemini AI Enrichment Engine
│   │   ├── writers/             # PostgreSQL + ChromaDB + CSV writer
│   │   ├── orchestrator.py      # Orchestrator (deduplication + Fallback)
│   │   ├── config.py            # Plugin factory
│   │   └── api.py               # FastAPI service
│   ├── config/miners.yaml       # Plugin configuration
│   ├── tests/                   # Unit tests
│   └── Dockerfile
├── sales-outreach-engine/       # LangGraph Outreach Engine
│   ├── src/
│   │   ├── state.py             # LangGraph state definition
│   │   ├── nodes.py             # 5 processing nodes
│   │   ├── graph.py             # Workflow graph
│   │   └── tools/               # RAG tools + PostgresLoader
│   └── Dockerfile
├── n8n-workflows/               # 4 n8n orchestration workflows
│   ├── 01-lead-mining.json      # Daily mining (every day at 8 AM)
│   ├── 02-lead-enrichment.json  # AI enrichment (every 6 hours)
│   ├── 03-sales-outreach.json   # Email outreach (weekdays at 9 AM)
│   └── 04-reply-detection.json  # Reply detection (every 2 hours)
├── docker-compose.yml           # 5-service orchestration
└── .env.example                 # Environment variable template
```

## Quick Start

### 1. Prepare API Keys (Phase 1 required)

```bash
cp .env.example .env
# Edit .env and fill in:
# SERPER_API_KEY   - https://serper.dev (free 2500 requests/month)
# APOLLO_API_KEY   - https://app.apollo.io (free 10K credits/month)
# GEMINI_API_KEY   - https://aistudio.google.com/app/apikey (free)
```

### 2. Start all services

```bash
docker compose up -d

# Wait ~30 seconds, check health status
docker compose ps
curl http://localhost:8000/health
```

### 3. Test the mining endpoint

```bash
curl -X POST http://localhost:8000/mine \
  -H "Content-Type: application/json" \
  -d '{
    "keyword": "restaurant",
    "location": "Manila, Philippines",
    "limit": 20,
    "enrich": true
  }'
```

### 4. Import n8n workflows

1. Visit http://localhost:5678 (admin / changeme)
2. Go to `Settings → Workflows → Import from File`
3. Import the 4 JSON files under `n8n-workflows/` in order
4. Configure Gmail OAuth2 credentials (required by workflows 03, 04)
5. Activate the workflows

## API Endpoints

| Endpoint | Method | Description |
|------|------|------|
| `/mine` | POST | Trigger mining (supports immediate enrichment) |
| `/leads` | GET | Query leads (supports filtering and score sorting) |
| `/rag/query` | POST | Semantic similarity search (used by LangGraph) |
| `/leads/export` | GET | Export CSV / JSON |
| `/health` | GET | System health check |

## Data Source Plugins (Phased)

| Plugin | Phase | Cost | Description |
|------|-------|------|------|
| SerperMiner | 1 | $1/1K requests | Google Maps business search |
| ApolloMiner | 1 | Free 10K/month | B2B contact enrichment |
| GeminiEnricher | 1 | ~$0.13/1K records | AI scoring + outreach suggestions |
| GoogleCSEMiner | 2 | 100/day free | Search engine supplemental data |
| SECPhilippinesMiner | 2 | Completely free | Philippines SEC registered companies |
| RedditMiner | 2 | Completely free | Social media leads |
| PhilGEPSMiner | 3 | Completely free | Government procurement suppliers |

## Monthly Cost Estimate

| Service | Cost/month |
|------|---------|
| Serper API (~5K requests) | $5 |
| Apollo (free tier) | $0 |
| Gemini 2.0 Flash (~50K records enriched) | ~$10 |
| VPS (2-core 4GB, Hetzner recommended) | $8-15 |
| **Total** | **~$23-30/month** |

## Running Tests

```bash
cd lead-mining-engine
pip install -r requirements.txt -r tests/requirements-test.txt
pytest tests/ -v
```

## LangGraph Outreach Flowchart

```
Lead (from PostgreSQL)
    │
    ▼
rag_retrieve     # Retrieve similar case context from ChromaDB
    │
    ▼
find_contacts    # Query decision-maker email by domain via Apollo API
    │
    ▼
generate_email   # Gemini generates personalized email
    │
    ├─ (no email/error) → END
    │
    ▼
send_email       # SMTP/Gmail send
    │
    ▼
log_outreach     # Write to outreach_log to prevent duplicates
    │
    ▼
   END
```
