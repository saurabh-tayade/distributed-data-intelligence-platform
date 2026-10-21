CREATE TABLE runs (
    run_id uuid PRIMARY KEY, submission_seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    product_id product_id NOT NULL, pipeline varchar(32) NOT NULL CHECK(pipeline IN ('CONTENT_RSS_V1','RELIABILITY_V2')),
    pipeline_version smallint NOT NULL CHECK(pipeline_version=1),
    principal_scope varchar(32) NOT NULL CHECK(principal_scope='local-operator'), idempotency_key token_key NOT NULL,
    normalized_request bytea NOT NULL CHECK(valid_object_bytes(normalized_request,65536)),
    request_sha256 sha256_hex NOT NULL CHECK(request_sha256=encode(sha256(normalized_request),'hex')),
    captured_selection bytea NOT NULL CHECK(valid_object_bytes(captured_selection,16777216)),
    selection_sha256 sha256_hex NOT NULL CHECK(selection_sha256=encode(sha256(captured_selection),'hex')),
    execution_profile_id token_key NOT NULL REFERENCES execution_profiles ON DELETE RESTRICT,
    state varchar(20) NOT NULL CHECK(state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','SUCCEEDED','FAILED','CANCELLED','RECOVERY_REQUIRED')),
    current_stage varchar(32) CHECK(current_stage IN ('CHECK_INPUTS','FETCH_SOURCE','STORE_BRONZE','SILVER','PREPARE_GOLD','PUBLISH_GOLD','PUBLISH_CONTENT','PUBLISH_RELIABILITY','VERIFY_RESULT')), current_attempt_id uuid,
    row_version bigint NOT NULL DEFAULT 0 CHECK(row_version >= 0),
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
    result_artifact_id uuid REFERENCES artifacts ON DELETE RESTRICT,
    outcome_code token_key, counts_bytes bytea CHECK(valid_object_bytes(counts_bytes,65536)),
    retry_eligible boolean NOT NULL DEFAULT false,
    UNIQUE(product_id,principal_scope,idempotency_key), UNIQUE(run_id,product_id),
    CHECK((product_id='content-intelligence')=(pipeline='CONTENT_RSS_V1')),
    FOREIGN KEY(result_artifact_id,product_id) REFERENCES artifacts(artifact_id,product_id),
    CHECK(state <> 'SUCCEEDED' OR (result_artifact_id IS NOT NULL AND finished_at IS NOT NULL)),
    CHECK(state NOT IN ('FAILED','CANCELLED') OR finished_at IS NOT NULL),
    CHECK(state NOT IN ('RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED') OR current_attempt_id IS NOT NULL)
);
CREATE UNIQUE INDEX one_active_run ON runs((true)) WHERE state IN ('RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED');
CREATE INDEX runs_queue_idx ON runs(state,submission_seq);
CREATE INDEX runs_history_idx ON runs(product_id,requested_at DESC,run_id);
CREATE TABLE run_sources (
    run_id uuid NOT NULL, product_id product_id NOT NULL, source_id source_key NOT NULL,
    revision integer NOT NULL, captured_gate_version bigint NOT NULL CHECK(captured_gate_version >= 0), captured_enabled boolean NOT NULL,
    PRIMARY KEY(run_id,product_id,source_id), UNIQUE(run_id,product_id,source_id,revision),
    FOREIGN KEY(run_id,product_id) REFERENCES runs(run_id,product_id) ON DELETE RESTRICT,
    FOREIGN KEY(product_id,source_id,revision) REFERENCES source_revisions ON DELETE RESTRICT
);
CREATE INDEX run_sources_history_idx ON run_sources(product_id,source_id,run_id);
CREATE TABLE run_inputs (
    run_id uuid NOT NULL REFERENCES runs ON DELETE RESTRICT, role token_key NOT NULL,
    ordinal integer NOT NULL CHECK(ordinal BETWEEN 0 AND 127), artifact_id uuid NOT NULL REFERENCES artifacts ON DELETE RESTRICT,
    evidence_bytes bytea NOT NULL CHECK(valid_object_bytes(evidence_bytes,16777216)),
    evidence_sha256 sha256_hex NOT NULL CHECK(evidence_sha256=encode(sha256(evidence_bytes),'hex')),
    PRIMARY KEY(run_id,role,ordinal), UNIQUE(run_id,role,artifact_id)
);
CREATE TABLE attempts (
    attempt_id uuid PRIMARY KEY, run_id uuid NOT NULL REFERENCES runs ON DELETE RESTRICT,
    attempt_no integer NOT NULL CHECK(attempt_no BETWEEN 1 AND 20), kind varchar(16) NOT NULL CHECK(kind IN ('EXECUTE','RECONCILE')),
    instance_id uuid NOT NULL REFERENCES worker_instances ON DELETE RESTRICT, owner_epoch bigint NOT NULL CHECK(owner_epoch > 0),
    state varchar(16) NOT NULL CHECK(state IN ('CLAIMED','RUNNING','SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
    reused_checkpoint_id uuid, outcome_code token_key, row_version bigint NOT NULL DEFAULT 0 CHECK(row_version >= 0),
    UNIQUE(run_id,attempt_no), UNIQUE(run_id,attempt_id), UNIQUE(attempt_id,instance_id,owner_epoch),
    CHECK(state IN ('CLAIMED','RUNNING') OR finished_at IS NOT NULL)
);
CREATE UNIQUE INDEX one_active_attempt ON attempts((true)) WHERE state IN ('CLAIMED','RUNNING');
-- Deferred composite links permit a single atomic ownership transaction, never cross-run pointers.
ALTER TABLE runs ADD CONSTRAINT current_attempt_fk FOREIGN KEY(run_id,current_attempt_id)
    REFERENCES attempts(run_id,attempt_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE coordinator ADD CONSTRAINT coordinator_attempt_owner_fk FOREIGN KEY(active_attempt_id,owner_instance_id,epoch)
    REFERENCES attempts(attempt_id,instance_id,owner_epoch) DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE stage_invocations (
    attempt_id uuid NOT NULL REFERENCES attempts ON DELETE RESTRICT,
    stage varchar(32) NOT NULL CHECK(stage IN ('CHECK_INPUTS','FETCH_SOURCE','STORE_BRONZE','SILVER','PREPARE_GOLD','PUBLISH_GOLD','PUBLISH_CONTENT','PUBLISH_RELIABILITY','VERIFY_RESULT')),
    item_ordinal integer NOT NULL CHECK(item_ordinal BETWEEN 0 AND 511), checkpoint_id uuid NOT NULL UNIQUE,
    state varchar(16) NOT NULL CHECK(state IN ('INTENT','RUNNING','VERIFIED','FAILED','UNKNOWN')),
    input_evidence_sha256 sha256_hex NOT NULL, intended_identity varchar(512), intended_namespace relative_address,
    command_address relative_address, command_sha256 sha256_hex, result_address relative_address, result_sha256 sha256_hex,
    child_pid bigint CHECK(child_pid > 0), child_started_at timestamptz, child_instance_id uuid,
    started_at timestamptz, finished_at timestamptz, outcome_code token_key,
    counts_bytes bytea CHECK(valid_object_bytes(counts_bytes,65536)), row_version bigint NOT NULL DEFAULT 0 CHECK(row_version >= 0),
    PRIMARY KEY(attempt_id,stage,item_ordinal),
    CHECK((command_address IS NULL)=(command_sha256 IS NULL)),
    CHECK((result_address IS NULL)=(result_sha256 IS NULL)),
    CHECK((child_pid IS NULL)=(child_started_at IS NULL)), CHECK((child_pid IS NULL)=(child_instance_id IS NULL)),
    CHECK(state NOT IN ('VERIFIED','FAILED') OR finished_at IS NOT NULL)
);
ALTER TABLE attempts ADD CONSTRAINT reused_checkpoint_fk FOREIGN KEY(reused_checkpoint_id) REFERENCES stage_invocations(checkpoint_id);
CREATE TABLE stage_artifacts (
    attempt_id uuid NOT NULL, stage varchar(32) NOT NULL, item_ordinal integer NOT NULL,
    role token_key NOT NULL, ordinal integer NOT NULL CHECK(ordinal BETWEEN 0 AND 127),
    artifact_id uuid NOT NULL REFERENCES artifacts ON DELETE RESTRICT,
    PRIMARY KEY(attempt_id,stage,item_ordinal,role,ordinal),
    FOREIGN KEY(attempt_id,stage,item_ordinal) REFERENCES stage_invocations ON DELETE RESTRICT
);
