-- Opt-in admission evidence only. Existing attempt states and all unmarked attempts are unchanged.
-- V5's immutable-identity guard also makes this new marker immutable after attempt insertion.
ALTER TABLE attempts ADD COLUMN content_admission_policy varchar(64)
    CHECK(content_admission_policy IS NULL OR content_admission_policy='ddip-content-narrow-v1');
ALTER TABLE attempts ADD CONSTRAINT narrow_content_attempt_capacity CHECK (
    content_admission_policy IS NULL OR (kind='EXECUTE' AND attempt_no<=19));

CREATE TABLE content_attempt_admissions (
    attempt_id uuid PRIMARY KEY REFERENCES attempts ON DELETE RESTRICT,
    policy varchar(64) NOT NULL CHECK(policy='ddip-content-narrow-v1'),
    admission_bytes bytea NOT NULL CHECK(valid_object_bytes(admission_bytes,65536)),
    admission_sha256 sha256_hex NOT NULL CHECK(admission_sha256=encode(sha256(admission_bytes),'hex')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(attempt_id,policy)
);
-- The claim may insert its attempt before enrollment, but cannot commit a marked orphan.
ALTER TABLE attempts ADD CONSTRAINT content_attempt_admission_fk
    FOREIGN KEY(attempt_id,content_admission_policy)
    REFERENCES content_attempt_admissions(attempt_id,policy) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE content_admission_milestones (
    attempt_id uuid NOT NULL REFERENCES content_attempt_admissions ON DELETE RESTRICT,
    phase varchar(16) NOT NULL CHECK(phase IN ('PREPARING','READY')),
    receipt_bytes bytea NOT NULL CHECK(valid_object_bytes(receipt_bytes,65536)),
    receipt_sha256 sha256_hex NOT NULL CHECK(receipt_sha256=encode(sha256(receipt_bytes),'hex')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(attempt_id,phase)
);
DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['content_attempt_admissions','content_admission_milestones'] LOOP
        EXECUTE format('CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation()',t);
        EXECUTE format('CREATE TRIGGER no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation()',t);
    END LOOP;
END $$;

-- JSON/JSONB below checks shape and identity only. Digests always cover the supplied bytes;
-- canonical encoding, filesystem identities and runtime verification remain application checks.
CREATE FUNCTION guard_content_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE
    c coordinator%ROWTYPE; r runs%ROWTYPE; a attempts%ROWTYPE; w worker_instances%ROWTYPE;
    n jsonb; raw bytea; fields text[]; key text; admission_sha sha256_hex;
BEGIN
    -- The application takes these locks before inserting. Direct SQL uses the same serialization.
    SELECT * INTO c FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_OWNER_REQUIRED', ERRCODE='23514'; END IF;
    SELECT r0.* INTO r FROM runs r0 JOIN attempts a0 ON a0.run_id=r0.run_id
        WHERE a0.attempt_id=NEW.attempt_id FOR UPDATE OF r0;
    IF NOT FOUND THEN RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_OWNER_REQUIRED', ERRCODE='23514'; END IF;
    SELECT * INTO a FROM attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
    SELECT * INTO w FROM worker_instances WHERE instance_id=a.instance_id;
    IF a.content_admission_policy IS DISTINCT FROM 'ddip-content-narrow-v1' OR
       a.kind<>'EXECUTE' OR a.state<>'CLAIMED' OR a.attempt_no>19 OR
       r.pipeline<>'CONTENT_RSS_V1' OR r.state<>'RUNNING' OR
       r.current_attempt_id IS DISTINCT FROM a.attempt_id OR
       c.mode<>'READY' OR c.active_import_id IS NOT NULL OR
       c.active_attempt_id IS DISTINCT FROM a.attempt_id OR
       c.owner_instance_id IS DISTINCT FROM a.instance_id OR c.epoch<>a.owner_epoch OR
       w.installation_id IS DISTINCT FROM c.installation_id OR w.status IS DISTINCT FROM 'READY' OR
       w.execution_profile_id IS DISTINCT FROM r.execution_profile_id OR
       EXISTS(SELECT 1 FROM stage_invocations WHERE attempt_id=a.attempt_id) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_OWNER_REQUIRED', ERRCODE='23514';
    END IF;

    IF TG_TABLE_NAME='content_attempt_admissions' THEN
        raw:=NEW.admission_bytes;
        fields:=ARRAY['admissionVersion','policy','installationId','runId','attemptId','workerInstanceId',
            'ownerEpoch','planKind','sourceId','controlBinding','controlBindingSha256','selectionSha256',
            'profileId','profileSha256','runtimeId','runtimeSha256'];
    ELSE
        raw:=NEW.receipt_bytes;
        fields:=ARRAY['protocolVersion','admissionSha256'];
        IF NEW.phase='READY' THEN
            fields:=fields || ARRAY['rootDevice','rootInode','lockDevice','lockInode','ledgerSha256'];
        END IF;
    END IF;
    n:=convert_from(raw,'UTF8')::jsonb;
    IF NOT (n ?& fields) OR n-fields<>'{}'::jsonb OR
       (SELECT count(*) FROM json_each(convert_from(raw,'UTF8')::json))<>cardinality(fields) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_SHAPE_INVALID', ERRCODE='23514';
    END IF;
    FOREACH key IN ARRAY fields LOOP
        IF key NOT IN ('admissionVersion','protocolVersion','controlBinding') AND
           jsonb_typeof(n->key) IS DISTINCT FROM 'string' THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_SHAPE_INVALID', ERRCODE='23514';
        END IF;
    END LOOP;

    IF TG_TABLE_NAME='content_attempt_admissions' THEN
        IF NEW.policy IS DISTINCT FROM a.content_admission_policy OR
           jsonb_typeof(n->'admissionVersion') IS DISTINCT FROM 'number' OR
           n->>'admissionVersion'<>'1' OR n->>'policy' IS DISTINCT FROM NEW.policy OR
           n->>'installationId' IS DISTINCT FROM c.installation_id::text OR
           n->>'runId' IS DISTINCT FROM a.run_id::text OR
           n->>'attemptId' IS DISTINCT FROM a.attempt_id::text OR
           n->>'workerInstanceId' IS DISTINCT FROM a.instance_id::text OR
           n->>'ownerEpoch' IS DISTINCT FROM a.owner_epoch::text OR
           n->>'planKind'<>'ORIGINAL' OR n->>'selectionSha256' IS DISTINCT FROM r.selection_sha256::text OR
           n->>'profileId' IS DISTINCT FROM r.execution_profile_id::text OR
           n->>'profileSha256' IS DISTINCT FROM w.profile_sha256::text OR
           n->>'controlBindingSha256' IS DISTINCT FROM w.host_binding_sha256::text OR
           jsonb_typeof(n->'controlBinding') IS DISTINCT FROM 'object' OR
           NOT EXISTS(SELECT 1 FROM run_sources WHERE run_id=a.run_id AND source_id=n->>'sourceId') OR
           NOT EXISTS(SELECT 1 FROM storage_bindings b JOIN execution_profile_storage p USING(binding_id)
               WHERE p.profile_id=r.execution_profile_id AND b.role='CONTROL' AND
                   b.installation_id=c.installation_id AND b.configuration_sha256=w.host_binding_sha256 AND
                   convert_from(b.configuration_bytes,'UTF8')::jsonb=n->'controlBinding') OR
           NOT EXISTS(SELECT 1 FROM execution_profiles p JOIN execution_profiles runtime
               ON runtime.profile_id=n->>'runtimeId' AND runtime.configuration_sha256::text=n->>'runtimeSha256'
               WHERE p.profile_id=r.execution_profile_id AND p.configuration_sha256=w.profile_sha256 AND
                   convert_from(p.configuration_bytes,'UTF8')::jsonb->>'contentRuntimeId'=n->>'runtimeId') THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_BINDING_INVALID', ERRCODE='23514';
        END IF;
    ELSE
        SELECT admission_sha256 INTO admission_sha FROM content_attempt_admissions WHERE attempt_id=a.attempt_id;
        IF admission_sha IS NULL OR jsonb_typeof(n->'protocolVersion') IS DISTINCT FROM 'number' OR
           n->>'protocolVersion'<>'1' OR n->>'admissionSha256' IS DISTINCT FROM admission_sha::text OR
           (NEW.phase='PREPARING' AND EXISTS(SELECT 1 FROM content_admission_milestones WHERE attempt_id=a.attempt_id)) OR
           (NEW.phase='READY' AND NOT EXISTS(SELECT 1 FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='PREPARING')) THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_PHASE_INVALID', ERRCODE='23514';
        END IF;
        IF NEW.phase='READY' THEN
            IF n->>'ledgerSha256' !~ '^[0-9a-f]{64}$' THEN
                RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_BINDING_INVALID', ERRCODE='23514';
            END IF;
            FOREACH key IN ARRAY ARRAY['rootDevice','rootInode','lockDevice','lockInode'] LOOP
                BEGIN
                    IF ((n->>key)::bigint)::text IS DISTINCT FROM n->>key THEN
                        RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_BINDING_INVALID', ERRCODE='23514';
                    END IF;
                EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
                    RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_BINDING_INVALID', ERRCODE='23514';
                END;
            END LOOP;
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_admission_insert BEFORE INSERT ON content_attempt_admissions
    FOR EACH ROW EXECUTE FUNCTION guard_content_admission();
CREATE TRIGGER content_admission_milestone_insert BEFORE INSERT ON content_admission_milestones
    FOR EACH ROW EXECUTE FUNCTION guard_content_admission();

CREATE FUNCTION guard_content_attempt_progress() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
BEGIN
    IF NEW.content_admission_policy IS NULL THEN RETURN NEW; END IF;
    PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    IF TG_OP='INSERT' THEN
        IF NEW.state<>'CLAIMED' THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_INITIAL_STATE_INVALID', ERRCODE='23514';
        END IF;
    ELSE
        IF NEW.state='RUNNING' AND NOT EXISTS(SELECT 1 FROM content_admission_milestones
                WHERE attempt_id=NEW.attempt_id AND phase='READY') THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_NOT_READY', ERRCODE='23514';
        END IF;
        -- Preparation has durable filesystem effects. The old clean-prelaunch release is unsafe.
        -- Interruption remains possible; this does not add a new terminal/release protocol.
        IF OLD.state='CLAIMED' AND NEW.state IN ('FAILED','CANCELLED','SUCCEEDED') AND
           EXISTS(SELECT 1 FROM content_admission_milestones WHERE attempt_id=NEW.attempt_id AND phase='PREPARING') THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_PREPARATION_RETAINED', ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_attempt_progress BEFORE INSERT OR UPDATE ON attempts
    FOR EACH ROW EXECUTE FUNCTION guard_content_attempt_progress();

CREATE FUNCTION guard_content_stage_readiness() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE policy varchar(64);
BEGIN
    -- A visible attempt's immutable marker cannot change from legacy to enrolled later.
    -- Do not add coordinator locking to ordinary legacy stage writes.
    SELECT content_admission_policy INTO policy FROM attempts WHERE attempt_id=NEW.attempt_id;
    IF NOT FOUND THEN
        -- An unseen uncommitted attempt is not a legacy attempt. Otherwise the FK could
        -- wait for its claim to commit after this trigger had already skipped readiness.
        RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_ATTEMPT_REQUIRED', ERRCODE='23503';
    END IF;
    IF policy IS NULL THEN RETURN NEW; END IF;
    PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    IF NOT EXISTS(SELECT 1 FROM content_admission_milestones
            WHERE attempt_id=NEW.attempt_id AND phase='READY') THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_NOT_READY', ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_stage_readiness BEFORE INSERT OR UPDATE ON stage_invocations
    FOR EACH ROW EXECUTE FUNCTION guard_content_stage_readiness();
