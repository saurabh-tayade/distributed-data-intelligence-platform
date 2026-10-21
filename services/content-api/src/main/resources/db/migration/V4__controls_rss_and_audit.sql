CREATE TABLE rss_deliveries (
    run_id uuid NOT NULL REFERENCES runs ON DELETE RESTRICT, source_id source_key NOT NULL,
    product_id product_id NOT NULL DEFAULT 'content-intelligence' CHECK(product_id='content-intelligence'), revision integer NOT NULL,
    owning_attempt_id uuid NOT NULL, journal_address relative_address NOT NULL,
    journal_descriptor bytea NOT NULL CHECK(valid_object_bytes(journal_descriptor,65536)),
    journal_sha256 sha256_hex NOT NULL CHECK(journal_sha256=encode(sha256(journal_descriptor),'hex')),
    ingestion_id uuid NOT NULL UNIQUE,
    original_ingested_at_text varchar(40) NOT NULL CHECK(original_ingested_at_text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$'),
    expected_evidence bytea NOT NULL CHECK(valid_object_bytes(expected_evidence,65536)),
    expected_sha256 sha256_hex NOT NULL CHECK(expected_sha256=encode(sha256(expected_evidence),'hex')),
    payload_length bigint NOT NULL CHECK(payload_length BETWEEN 0 AND 5242880), payload_sha256 sha256_hex NOT NULL,
    state varchar(24) NOT NULL CHECK(state IN ('FETCH_JOURNALED','BRONZE_WRITING','BRONZE_VERIFIED')),
    metadata_sha256 sha256_hex, receipt_bytes bytea CHECK(valid_object_bytes(receipt_bytes,65536)),
    row_version bigint NOT NULL DEFAULT 0 CHECK(row_version >= 0),
    PRIMARY KEY(run_id,source_id),
    FOREIGN KEY(run_id,product_id,source_id,revision) REFERENCES run_sources(run_id,product_id,source_id,revision) ON DELETE RESTRICT,
    FOREIGN KEY(product_id,source_id,revision) REFERENCES source_revisions ON DELETE RESTRICT,
    FOREIGN KEY(run_id,owning_attempt_id) REFERENCES attempts(run_id,attempt_id) ON DELETE RESTRICT,
    CHECK(state <> 'BRONZE_VERIFIED' OR (metadata_sha256 IS NOT NULL AND receipt_bytes IS NOT NULL))
);
CREATE TABLE controls (
    control_id uuid PRIMARY KEY, run_id uuid NOT NULL REFERENCES runs ON DELETE RESTRICT,
    type varchar(16) NOT NULL CHECK(type IN ('CANCEL','RETRY','RECONCILE')), idempotency_key token_key NOT NULL,
    normalized_request bytea NOT NULL CHECK(valid_object_bytes(normalized_request,65536)),
    request_sha256 sha256_hex NOT NULL CHECK(request_sha256=encode(sha256(normalized_request),'hex')),
    expected_version bigint NOT NULL CHECK(expected_version >= 0), target_attempt_id uuid, quiescence_confirmed boolean,
    state varchar(16) NOT NULL CHECK(state IN ('REQUESTED','APPLIED','TOO_LATE','REJECTED')), reason token_key,
    requester varchar(32) NOT NULL CHECK(requester='local-operator'), requested_at timestamptz NOT NULL DEFAULT clock_timestamp(), applied_at timestamptz,
    row_version bigint NOT NULL DEFAULT 0 CHECK(row_version >= 0),
    UNIQUE(run_id,idempotency_key),
    FOREIGN KEY(run_id,target_attempt_id) REFERENCES attempts(run_id,attempt_id) ON DELETE RESTRICT,
    CHECK((type='RECONCILE' AND target_attempt_id IS NOT NULL AND quiescence_confirmed IS TRUE) OR
          (type<>'RECONCILE' AND target_attempt_id IS NULL AND quiescence_confirmed IS NULL)),
    CHECK(state='REQUESTED' OR (reason IS NOT NULL AND applied_at IS NOT NULL))
);
CREATE INDEX controls_history_idx ON controls(run_id,requested_at,control_id);
CREATE TABLE command_bindings (
    scope token_key NOT NULL, idempotency_key token_key NOT NULL,
    normalized_request bytea NOT NULL CHECK(valid_object_bytes(normalized_request,65536)),
    request_sha256 sha256_hex NOT NULL CHECK(request_sha256=encode(sha256(normalized_request),'hex')),
    response_object_id varchar(256) NOT NULL CHECK(octet_length(response_object_id) BETWEEN 1 AND 256),
    response_status smallint NOT NULL CHECK(response_status BETWEEN 200 AND 299),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(scope,idempotency_key)
);
CREATE TABLE audit_events (
    event_seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, time timestamptz NOT NULL DEFAULT clock_timestamp(),
    actor_scope varchar(32) NOT NULL CHECK(actor_scope='local-operator'),
    run_id uuid REFERENCES runs ON DELETE RESTRICT, source_product product_id, source_id source_key,
    control_id uuid REFERENCES controls ON DELETE RESTRICT, attempt_id uuid REFERENCES attempts ON DELETE RESTRICT,
    action token_key NOT NULL, before_state varchar(24), after_state varchar(24),
    before_version bigint CHECK(before_version >= 0), after_version bigint CHECK(after_version >= 0), safe_code token_key,
    details_bytes bytea NOT NULL CHECK(valid_object_bytes(details_bytes,8192)),
    FOREIGN KEY(source_product,source_id) REFERENCES sources ON DELETE RESTRICT,
    CHECK((source_product IS NULL)=(source_id IS NULL))
);
CREATE INDEX audit_run_history_idx ON audit_events(run_id,event_seq);
