-- No native manifest interpretation/backfill. Unknown legacy format scope needs operator review.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM artifacts WHERE kind IN ('RELIABILITY_BATCH_V2','RELIABILITY_V2')) THEN
        RAISE EXCEPTION USING MESSAGE='ARTIFACT_NATIVE_FORMAT_UNRESOLVED', ERRCODE='23514';
    END IF;
    IF EXISTS (SELECT 1 FROM artifacts GROUP BY product_id,kind,contract_version,processor_revision,native_identity HAVING count(*)>1) THEN
        RAISE EXCEPTION USING MESSAGE='ARTIFACT_NATIVE_IDENTITY_CONFLICT', ERRCODE='23514';
    END IF;
END $$;
ALTER TABLE artifacts ADD COLUMN native_format varchar(16);
ALTER TABLE artifacts ADD CONSTRAINT artifact_format_scope CHECK (
    (kind IN ('RELIABILITY_BATCH_V2','RELIABILITY_V2') AND native_format IS NOT NULL AND native_format IN ('JSONL','PARQUET')) OR
    (kind NOT IN ('RELIABILITY_BATCH_V2','RELIABILITY_V2') AND native_format IS NULL));
ALTER TABLE artifacts ADD CONSTRAINT artifact_native_scope_unique UNIQUE NULLS NOT DISTINCT
    (product_id,kind,contract_version,processor_revision,native_format,native_identity);

CREATE TABLE artifact_import_operations (
    import_id uuid PRIMARY KEY,
    installation_id uuid NOT NULL REFERENCES coordinator(installation_id) ON DELETE RESTRICT,
    kind varchar(16) NOT NULL CHECK(kind IN ('IMPORT','RECOVERY')),
    predecessor_id uuid UNIQUE REFERENCES artifact_import_operations ON DELETE RESTRICT,
    quiescence_confirmed boolean NOT NULL,
    process_instance_id uuid NOT NULL, owner_pid bigint NOT NULL CHECK(owner_pid>0), owner_started_at timestamptz NOT NULL,
    execution_profile_id token_key NOT NULL, profile_sha256 sha256_hex NOT NULL,
    host_binding_id token_key NOT NULL REFERENCES storage_bindings ON DELETE RESTRICT, host_binding_sha256 sha256_hex NOT NULL,
    owner_epoch bigint NOT NULL CHECK(owner_epoch>0),
    request_bytes bytea NOT NULL CHECK(valid_object_bytes(request_bytes,65536)),
    request_sha256 sha256_hex NOT NULL CHECK(request_sha256=encode(sha256(request_bytes),'hex')),
    state varchar(16) NOT NULL CHECK(state IN ('ACTIVE','SUCCEEDED','FAILED','INTERRUPTED')),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
    outcome_code varchar(32) CHECK(outcome_code IN ('SAFE_COMPLETION','SAFE_FAILURE','RECOVERY_HANDOFF','RECOVERY_CLOSED')),
    result_bytes bytea CHECK(result_bytes IS NULL OR valid_object_bytes(result_bytes,65536)), result_sha256 sha256_hex,
    row_version bigint NOT NULL DEFAULT 0 CHECK(row_version>=0),
    UNIQUE(import_id,installation_id,owner_epoch),
    FOREIGN KEY(execution_profile_id,profile_sha256) REFERENCES execution_profiles(profile_id,configuration_sha256) ON DELETE RESTRICT,
    CHECK((kind='IMPORT' AND predecessor_id IS NULL AND NOT quiescence_confirmed) OR
          (kind='RECOVERY' AND predecessor_id IS NOT NULL AND predecessor_id<>import_id AND quiescence_confirmed)),
    CHECK((state='ACTIVE' AND finished_at IS NULL AND outcome_code IS NULL AND result_bytes IS NULL AND result_sha256 IS NULL) OR
          (state<>'ACTIVE' AND finished_at IS NOT NULL AND finished_at>=started_at AND outcome_code IS NOT NULL AND result_bytes IS NOT NULL AND result_sha256 IS NOT NULL)),
    CHECK(result_sha256 IS NULL OR result_sha256=encode(sha256(result_bytes),'hex')),
    CHECK(state='ACTIVE' OR (state='INTERRUPTED' AND outcome_code='RECOVERY_HANDOFF') OR
          (kind='IMPORT' AND ((state='SUCCEEDED' AND outcome_code='SAFE_COMPLETION') OR (state='FAILED' AND outcome_code='SAFE_FAILURE'))) OR
          (kind='RECOVERY' AND state='SUCCEEDED' AND outcome_code='RECOVERY_CLOSED'))
);
CREATE UNIQUE INDEX one_active_import ON artifact_import_operations((true)) WHERE state='ACTIVE';
ALTER TABLE coordinator ADD COLUMN active_import_id uuid;
ALTER TABLE coordinator ADD CONSTRAINT coordinator_owner_exclusion CHECK(active_import_id IS NULL OR (active_attempt_id IS NULL AND owner_instance_id IS NULL));
ALTER TABLE coordinator ADD CONSTRAINT coordinator_import_owner_fk FOREIGN KEY(active_import_id,installation_id,epoch)
    REFERENCES artifact_import_operations(import_id,installation_id,owner_epoch) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE audit_events ADD COLUMN import_id uuid REFERENCES artifact_import_operations ON DELETE RESTRICT;
CREATE INDEX audit_import_history_idx ON audit_events(import_id,event_seq) WHERE import_id IS NOT NULL;

DROP TRIGGER coordinator_guard ON coordinator;
CREATE TRIGGER coordinator_guard BEFORE UPDATE OR DELETE ON coordinator FOR EACH ROW EXECUTE FUNCTION guard_mutable_row
    ('mode','epoch','active_attempt_id','owner_instance_id','heartbeat_at','row_version','active_import_id');
CREATE TRIGGER import_identity_guard BEFORE UPDATE OR DELETE ON artifact_import_operations FOR EACH ROW EXECUTE FUNCTION guard_mutable_row
    ('state','finished_at','outcome_code','result_bytes','result_sha256','row_version');
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON artifact_import_operations FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation();

-- All application transactions lock coordinator first. These guards also serialize direct SQL
-- admission, so READ COMMITTED cannot admit an attempt and import using disjoint row locks.
CREATE FUNCTION serialize_import_ownership() RETURNS trigger LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE c coordinator%ROWTYPE; previous artifact_import_operations%ROWTYPE;
BEGIN
    SELECT * INTO c FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION USING MESSAGE='OWNERSHIP_COORDINATOR_MISSING', ERRCODE='23514'; END IF;
    IF TG_TABLE_NAME='artifact_import_operations' THEN
        IF TG_OP='INSERT' THEN
            IF NEW.state<>'ACTIVE' OR NEW.row_version<>0 THEN
                RAISE EXCEPTION USING MESSAGE='IMPORT_INITIAL_STATE_INVALID', ERRCODE='23514';
            END IF;
            IF NEW.installation_id<>c.installation_id OR NEW.owner_epoch<>c.epoch+1 OR
               EXISTS(SELECT 1 FROM attempts WHERE state IN ('CLAIMED','RUNNING')) OR
               EXISTS(SELECT 1 FROM runs WHERE state IN ('RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')) THEN
                RAISE EXCEPTION USING MESSAGE='IMPORT_OWNERSHIP_CONFLICT', ERRCODE='23514';
            END IF;
            IF NOT EXISTS(SELECT 1 FROM storage_bindings b JOIN execution_profile_storage p USING(binding_id)
                WHERE b.binding_id=NEW.host_binding_id AND b.role='CONTROL' AND b.installation_id=NEW.installation_id
                  AND b.configuration_sha256=NEW.host_binding_sha256 AND p.profile_id=NEW.execution_profile_id) OR
               EXISTS(SELECT 1 FROM artifact_import_operations WHERE installation_id=NEW.installation_id AND host_binding_sha256<>NEW.host_binding_sha256) THEN
                RAISE EXCEPTION USING MESSAGE='IMPORT_HOST_BINDING_CONFLICT', ERRCODE='23514';
            END IF;
            IF NEW.kind='IMPORT' THEN
                IF c.mode<>'READY' OR c.active_attempt_id IS NOT NULL OR c.active_import_id IS NOT NULL THEN
                    RAISE EXCEPTION USING MESSAGE='IMPORT_OWNERSHIP_CONFLICT', ERRCODE='23514';
                END IF;
            ELSE
                SELECT * INTO previous FROM artifact_import_operations WHERE import_id=NEW.predecessor_id;
                IF NOT FOUND OR c.active_import_id IS DISTINCT FROM previous.import_id OR previous.state<>'INTERRUPTED' OR
                   previous.owner_epoch<>c.epoch OR previous.installation_id<>NEW.installation_id OR
                   previous.request_bytes<>NEW.request_bytes OR previous.execution_profile_id<>NEW.execution_profile_id OR
                   previous.profile_sha256<>NEW.profile_sha256 OR previous.host_binding_id<>NEW.host_binding_id OR
                   previous.host_binding_sha256<>NEW.host_binding_sha256 THEN
                    RAISE EXCEPTION USING MESSAGE='IMPORT_RECOVERY_BINDING_CONFLICT', ERRCODE='23514';
                END IF;
            END IF;
        ELSE
            IF OLD.state<>'ACTIVE' OR NEW.state='ACTIVE' THEN
                RAISE EXCEPTION USING MESSAGE='IMPORT_TRANSITION_INVALID', ERRCODE='23514';
            END IF;
            IF c.active_import_id IS DISTINCT FROM OLD.import_id OR c.epoch<>OLD.owner_epoch THEN
                RAISE EXCEPTION USING MESSAGE='IMPORT_OWNER_STALE', ERRCODE='23514';
            END IF;
        END IF;
    ELSIF c.active_import_id IS NOT NULL OR EXISTS(SELECT 1 FROM artifact_import_operations WHERE state='ACTIVE') THEN
        RAISE EXCEPTION USING MESSAGE='IMPORT_EXECUTION_EXCLUSION', ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER import_serialization BEFORE INSERT OR UPDATE ON artifact_import_operations FOR EACH ROW EXECUTE FUNCTION serialize_import_ownership();
CREATE TRIGGER import_exclusion BEFORE INSERT OR UPDATE ON attempts FOR EACH ROW WHEN(NEW.state IN ('CLAIMED','RUNNING')) EXECUTE FUNCTION serialize_import_ownership();
CREATE TRIGGER import_exclusion BEFORE INSERT OR UPDATE ON runs FOR EACH ROW WHEN(NEW.state IN ('RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')) EXECUTE FUNCTION serialize_import_ownership();

CREATE FUNCTION guard_import_pointer() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.active_import_id IS DISTINCT FROM OLD.active_import_id THEN
        IF (NEW.active_import_id IS NOT NULL AND NEW.epoch<>OLD.epoch+1) OR
           (NEW.active_import_id IS NULL AND NEW.epoch<>OLD.epoch) THEN
            RAISE EXCEPTION USING MESSAGE='IMPORT_EPOCH_INVALID', ERRCODE='23514';
        END IF;
    ELSIF NEW.active_import_id IS NOT NULL AND NEW.epoch<>OLD.epoch THEN
        RAISE EXCEPTION USING MESSAGE='IMPORT_EPOCH_INVALID', ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER import_pointer_guard BEFORE UPDATE ON coordinator FOR EACH ROW EXECUTE FUNCTION guard_import_pointer();

-- Deferred checks permit old ACTIVE -> INTERRUPTED, successor insertion and pointer transfer
-- in one transaction. Other sessions see the occupied old binding or the occupied new one.
CREATE FUNCTION check_import_binding() RETURNS trigger LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE c coordinator%ROWTYPE;
BEGIN
    SELECT * INTO c FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    IF c.active_import_id IS NULL THEN
        IF EXISTS(SELECT 1 FROM artifact_import_operations WHERE state='ACTIVE') THEN
            RAISE EXCEPTION USING MESSAGE='IMPORT_OWNER_REQUIRED', ERRCODE='23514';
        END IF;
    ELSIF NOT EXISTS(SELECT 1 FROM artifact_import_operations WHERE import_id=c.active_import_id AND installation_id=c.installation_id AND owner_epoch=c.epoch AND state='ACTIVE') OR
          EXISTS(SELECT 1 FROM attempts WHERE state IN ('CLAIMED','RUNNING')) OR
          EXISTS(SELECT 1 FROM runs WHERE state IN ('RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')) THEN
        RAISE EXCEPTION USING MESSAGE='IMPORT_BINDING_INVALID', ERRCODE='23514';
    END IF;
    IF EXISTS(SELECT 1 FROM artifact_import_operations p WHERE p.state='INTERRUPTED' AND NOT EXISTS
        (SELECT 1 FROM artifact_import_operations n WHERE n.predecessor_id=p.import_id)) THEN
        RAISE EXCEPTION USING MESSAGE='IMPORT_RECOVERY_SUCCESSOR_REQUIRED', ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER import_binding AFTER INSERT OR UPDATE ON coordinator DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_import_binding();
CREATE CONSTRAINT TRIGGER import_binding AFTER INSERT OR UPDATE ON artifact_import_operations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_import_binding();
