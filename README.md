# Lead Mining System — 菲律宾 SME 自动化销售线索 & 外展系统

## 系统架构

```
lead-mining-system/
├── lead-mining-engine/          # FastAPI 线索采集引擎
│   ├── app/
│   │   ├── miners/plugins/      # 6个数据源插件
│   │   ├── enrichers/           # Gemini AI 富化引擎
│   │   ├── writers/             # PostgreSQL + ChromaDB + CSV 写入
│   │   ├── orchestrator.py      # 调度器（去重+Fallback）
│   │   ├── config.py            # 插件工厂
│   │   └── api.py               # FastAPI 服务
│   ├── config/miners.yaml       # 插件配置
│   ├── tests/                   # 单元测试
│   └── Dockerfile
├── sales-outreach-engine/       # LangGraph 外展引擎
│   ├── src/
│   │   ├── state.py             # LangGraph 状态定义
│   │   ├── nodes.py             # 5个处理节点
│   │   ├── graph.py             # 工作流图
│   │   └── tools/               # RAG工具 + PostgresLoader
│   └── Dockerfile
├── n8n-workflows/               # 4条 n8n 编排工作流
│   ├── 01-lead-mining.json      # 每日采集（每天早8AM）
│   ├── 02-lead-enrichment.json  # AI富化（每6小时）
│   ├── 03-sales-outreach.json   # 邮件外展（工作日早9AM）
│   └── 04-reply-detection.json  # 回复识别（每2小时）
├── docker-compose.yml           # 5服务编排
└── .env.example                 # 环境变量模板
```

## 快速启动

### 1. 准备 API Keys（Phase 1 必填）

```bash
cp .env.example .env
# 编辑 .env，填入：
# SERPER_API_KEY   - https://serper.dev（免费2500次/月）
# APOLLO_API_KEY   - https://app.apollo.io（免费10K credits/月）
# GEMINI_API_KEY   - https://aistudio.google.com/app/apikey（免费）
```

### 2. 启动所有服务

```bash
docker compose up -d

# 等待约30秒，检查健康状态
docker compose ps
curl http://localhost:8000/health
```

### 3. 测试采集接口

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

### 4. 导入 n8n 工作流

1. 访问 http://localhost:5678（admin / changeme）
2. 进入 `Settings → Workflows → Import from File`
3. 依次导入 `n8n-workflows/` 下的 4 个 JSON 文件
4. 配置 Gmail OAuth2 凭证（工作流 03, 04 需要）
5. 激活工作流

## API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/mine` | POST | 触发采集（支持立即富化）|
| `/leads` | GET | 查询线索（支持过滤和评分排序）|
| `/rag/query` | POST | 语义相似度搜索（供 LangGraph 使用）|
| `/leads/export` | GET | 导出 CSV / JSON |
| `/health` | GET | 系统健康检查 |

## 数据源插件（Phase 分期）

| 插件 | Phase | 费用 | 说明 |
|------|-------|------|------|
| SerperMiner | 1 | $1/1K 次 | Google Maps 商户搜索 |
| ApolloMiner | 1 | 免费 10K/月 | B2B 联系人富化 |
| GeminiEnricher | 1 | ~$0.13/千条 | AI 评分+外展建议 |
| GoogleCSEMiner | 2 | 100次/天免费 | 搜索引擎补充数据 |
| SECPhilippinesMiner | 2 | 完全免费 | 菲律宾 SEC 注册企业 |
| RedditMiner | 2 | 完全免费 | 社交媒体线索 |
| PhilGEPSMiner | 3 | 完全免费 | 政府采购供应商 |

## 月成本估算

| 服务 | 费用/月 |
|------|---------|
| Serper API（~5K 次） | $5 |
| Apollo（免费层）| $0 |
| Gemini 2.0 Flash（~50K条富化）| ~$10 |
| VPS（2核4G，建议 Hetzner）| $8-15 |
| **总计** | **~$23-30/月** |

## 运行测试

```bash
cd lead-mining-engine
pip install -r requirements.txt -r tests/requirements-test.txt
pytest tests/ -v
```

## LangGraph 外展流程图

```
Lead (from PostgreSQL)
    │
    ▼
rag_retrieve     # 从 ChromaDB 检索相似案例上下文
    │
    ▼
find_contacts    # Apollo API 查询域名决策者邮箱
    │
    ▼
generate_email   # Gemini 生成个性化邮件
    │
    ├─ (无邮箱/错误) → END
    │
    ▼
send_email       # SMTP/Gmail 发送
    │
    ▼
log_outreach     # 写 outreach_log 防重复
    │
    ▼
   END
```
