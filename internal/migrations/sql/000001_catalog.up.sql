CREATE TYPE duckmesh_tenant_status AS ENUM ('active', 'disabled');
CREATE TYPE duckmesh_ingest_op AS ENUM ('insert', 'upsert', 'delete');
CREATE TYPE duckmesh_ingest_state AS ENUM ('accepted', 'claimed', 'committed', 'failed');
CREATE TYPE duckmesh_change_type AS ENUM ('add', 'remove');

CREATE TABLE tenant (
    tenant_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status duckmesh_tenant_status NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE api_key (
    key_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    key_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ
);

CREATE TABLE table_def (
    table_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    primary_key_cols JSONB NOT NULL DEFAULT '[]'::JSONB,
    partition_spec JSONB NOT NULL DEFAULT '{}'::JSONB,
    schema_version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, table_name)
);

CREATE TABLE table_schema_version (
    table_id BIGINT NOT NULL REFERENCES table_def(table_id) ON DELETE CASCADE,
    schema_version INT NOT NULL,
    schema_json JSONB NOT NULL,
    compatibility_mode TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_id, schema_version)
);

CREATE TABLE ingest_event (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    table_id BIGINT NOT NULL REFERENCES table_def(table_id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL,
    op duckmesh_ingest_op NOT NULL,
    payload_json JSONB NOT NULL,
    event_time TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    state duckmesh_ingest_state NOT NULL DEFAULT 'accepted',
    UNIQUE (tenant_id, table_id, idempotency_key)
);

CREATE TABLE ingest_claim_batch (
    batch_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    consumer_id TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_until TIMESTAMPTZ NOT NULL,
    state duckmesh_ingest_state NOT NULL
);

CREATE TABLE ingest_claim_item (
    batch_id BIGINT NOT NULL REFERENCES ingest_claim_batch(batch_id) ON DELETE CASCADE,
    event_id BIGINT NOT NULL REFERENCES ingest_event(event_id) ON DELETE CASCADE,
    PRIMARY KEY (batch_id, event_id)
);

CREATE TABLE snapshot (
    snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT NOT NULL,
    max_visibility_token BIGINT NOT NULL,
    parent_snapshot_id BIGINT REFERENCES snapshot(snapshot_id)
);

CREATE TABLE snapshot_table_watermark (
    snapshot_id BIGINT NOT NULL REFERENCES snapshot(snapshot_id) ON DELETE CASCADE,
    table_id BIGINT NOT NULL REFERENCES table_def(table_id) ON DELETE CASCADE,
    max_visibility_token BIGINT NOT NULL,
    PRIMARY KEY (snapshot_id, table_id)
);

CREATE TABLE data_file (
    file_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    table_id BIGINT NOT NULL REFERENCES table_def(table_id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    format TEXT NOT NULL CHECK (format = 'parquet'),
    record_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    min_event_time TIMESTAMPTZ,
    max_event_time TIMESTAMPTZ,
    stats_json JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE snapshot_file (
    snapshot_id BIGINT NOT NULL REFERENCES snapshot(snapshot_id) ON DELETE CASCADE,
    table_id BIGINT NOT NULL REFERENCES table_def(table_id) ON DELETE CASCADE,
    file_id BIGINT NOT NULL REFERENCES data_file(file_id) ON DELETE CASCADE,
    change_type duckmesh_change_type NOT NULL,
    PRIMARY KEY (snapshot_id, table_id, file_id, change_type)
);

CREATE TABLE compaction_run (
    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    table_id BIGINT NOT NULL REFERENCES table_def(table_id) ON DELETE CASCADE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    details_json JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE TABLE gc_run (
    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    details_json JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE TABLE query_audit (
    query_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    query_text TEXT NOT NULL,
    snapshot_id BIGINT REFERENCES snapshot(snapshot_id),
    trace_id TEXT,
    duration_ms BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE incident_audit (
    incident_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id TEXT REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    incident_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    summary TEXT NOT NULL,
    details_json JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ingest_event_state_lease_table ON ingest_event (state, lease_until, table_id);
CREATE UNIQUE INDEX idx_ingest_event_idempotency ON ingest_event (tenant_id, table_id, idempotency_key);
CREATE INDEX idx_snapshot_tenant_id_desc ON snapshot (tenant_id, snapshot_id DESC);
CREATE INDEX idx_snapshot_table_watermark_table_snapshot_desc ON snapshot_table_watermark (table_id, snapshot_id DESC);
CREATE INDEX idx_snapshot_file_snapshot_table ON snapshot_file (snapshot_id, table_id);
