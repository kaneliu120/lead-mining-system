"""
outreach_log 表 DDL — 在 lead-mining-engine init_tables 时一并创建
或在 sales-outreach-engine 启动时单独创建
"""

CREATE_OUTREACH_LOG = """
CREATE TABLE IF NOT EXISTS outreach_log (
    id          BIGSERIAL PRIMARY KEY,
    lead_id     BIGINT    REFERENCES leads_raw(id),
    email       TEXT      NOT NULL,
    subject     TEXT,
    status      TEXT      NOT NULL DEFAULT 'sent',   -- 'sent','replied','bounced','unsubscribed'
    thread_id   TEXT,                                 -- Gmail/SMTP thread ID
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replied_at  TIMESTAMPTZ,
    metadata    JSONB
);

CREATE INDEX IF NOT EXISTS idx_outreach_log_lead_id ON outreach_log(lead_id);
CREATE INDEX IF NOT EXISTS idx_outreach_log_email   ON outreach_log(email);
CREATE INDEX IF NOT EXISTS idx_outreach_log_status  ON outreach_log(status);
"""
