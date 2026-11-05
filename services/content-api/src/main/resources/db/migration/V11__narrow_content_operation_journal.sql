-- Inactive ORIGINAL CHECK_INPUTS/0 read-only probe. No legacy stage/history changes.
CREATE TABLE content_operations (
    operation_id uuid PRIMARY KEY,
    attempt_id uuid NOT NULL REFERENCES content_attempt_admissions ON DELETE RESTRICT,
    stage varchar(32) NOT NULL CHECK(stage='CHECK_INPUTS'),
    item_ordinal integer NOT NULL CHECK(item_ordinal=0),
    checkpoint_id uuid NOT NULL,
    bootstrap_bytes bytea NOT NULL CHECK(valid_object_bytes(bootstrap_bytes,65536)),
    bootstrap_sha256 sha256_hex NOT NULL CHECK(bootstrap_sha256=encode(sha256(bootstrap_bytes),'hex')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(attempt_id,stage,item_ordinal)
);
CREATE TABLE content_operation_phases (
    operation_id uuid NOT NULL REFERENCES content_operations ON DELETE RESTRICT,
    phase varchar(32) NOT NULL CHECK(phase IN ('FILES_READY','START_AUTHORIZED','CHILD_RECORDED','RELEASE_AUTHORIZED','PROBE_COMPLETED')),
    child_instance_id uuid,
    child_pid bigint CHECK(child_pid>0),
    child_started_at timestamptz,
    bootstrap_sha256 sha256_hex,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(operation_id,phase),
    CHECK((phase='CHILD_RECORDED' AND child_instance_id IS NOT NULL AND child_pid IS NOT NULL AND child_started_at IS NOT NULL)
       OR (phase<>'CHILD_RECORDED' AND child_instance_id IS NULL AND child_pid IS NULL AND child_started_at IS NULL)),
    CHECK((phase='RELEASE_AUTHORIZED')=(bootstrap_sha256 IS NOT NULL))
);
DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['content_operations','content_operation_phases'] LOOP
        EXECUTE format('CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation()',t);
        EXECUTE format('CREATE TRIGGER no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation()',t);
    END LOOP;
END $$;

CREATE FUNCTION guard_content_operation() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE
    c coordinator%ROWTYPE; r runs%ROWTYPE; a attempts%ROWTYPE; w worker_instances%ROWTYPE;
    op content_operations%ROWTYPE; admission jsonb; ready jsonb; n jsonb; fields text[];
    target uuid; previous text; key text; cp bytea;
BEGIN
    SELECT * INTO c FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    IF TG_TABLE_NAME='content_operations' THEN target:=NEW.attempt_id;
    ELSE
        SELECT * INTO op FROM content_operations WHERE operation_id=NEW.operation_id;
        target:=op.attempt_id;
    END IF;
    SELECT r0.* INTO r FROM runs r0 JOIN attempts a0 ON a0.run_id=r0.run_id
        WHERE a0.attempt_id=target FOR UPDATE OF r0;
    SELECT * INTO a FROM attempts WHERE attempt_id=target FOR UPDATE;
    SELECT * INTO w FROM worker_instances WHERE instance_id=a.instance_id;
    IF a.attempt_id IS NULL OR a.content_admission_policy IS DISTINCT FROM 'ddip-content-narrow-v1'
       OR a.kind<>'EXECUTE' OR a.state<>'CLAIMED' OR a.attempt_no>19
       OR clock_timestamp()>=a.started_at+interval '2 hours'
       OR r.pipeline<>'CONTENT_RSS_V1' OR r.state<>'RUNNING' OR r.current_attempt_id IS DISTINCT FROM a.attempt_id
       OR c.mode<>'READY' OR c.active_import_id IS NOT NULL OR c.active_attempt_id IS DISTINCT FROM a.attempt_id
       OR c.owner_instance_id IS DISTINCT FROM a.instance_id OR c.epoch<>a.owner_epoch
       OR w.installation_id IS DISTINCT FROM c.installation_id OR w.status IS DISTINCT FROM 'READY'
       OR w.execution_profile_id IS DISTINCT FROM r.execution_profile_id
       OR EXISTS(SELECT 1 FROM controls WHERE run_id=a.run_id AND (state='REQUESTED' OR (type='CANCEL' AND state='APPLIED')))
       OR EXISTS(SELECT 1 FROM stage_invocations WHERE attempt_id=a.attempt_id) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_OPERATION_OWNER_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT convert_from(admission_bytes,'UTF8')::jsonb INTO admission FROM content_attempt_admissions WHERE attempt_id=a.attempt_id;
    SELECT convert_from(receipt_bytes,'UTF8')::jsonb INTO ready FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='READY';
    IF admission IS NULL OR ready IS NULL THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_OPERATION_READY_REQUIRED',ERRCODE='23514';
    END IF;
    IF TG_TABLE_NAME='content_operations' THEN
        n:=convert_from(NEW.bootstrap_bytes,'UTF8')::jsonb;
        fields:=ARRAY['bootstrapVersion','mode','operationId','installationId','policy','planKind','runId','attemptId','sourceId',
            'controlRoot','controlDevice','controlInode','rootDevice','rootInode','lockDevice','lockInode','ledgerSha256',
            'selectionSha256','profileSha256','runtimeSha256','workerInstanceId','ownerEpoch','stage','ordinal','checkpointId','commandSha256'];
        IF NOT (n ?& fields) OR n-fields<>'{}'::jsonb OR
           (SELECT count(*) FROM json_each(convert_from(NEW.bootstrap_bytes,'UTF8')::json))<>cardinality(fields) THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_OPERATION_BINDING_INVALID',ERRCODE='23514';
        END IF;
        FOREACH key IN ARRAY fields LOOP
            IF key NOT IN ('bootstrapVersion','ordinal') AND jsonb_typeof(n->key) IS DISTINCT FROM 'string' THEN
                RAISE EXCEPTION USING MESSAGE='CONTENT_OPERATION_BINDING_INVALID',ERRCODE='23514';
            END IF;
        END LOOP;
        cp:=decode(md5('ddip-content-narrow-v1'||chr(10)||a.run_id::text||chr(10)||a.attempt_id::text||chr(10)||'CHECK_INPUTS:0'),'hex');
        cp:=set_byte(cp,6,(get_byte(cp,6)&15)|48);cp:=set_byte(cp,8,(get_byte(cp,8)&63)|128);
        IF jsonb_typeof(n->'bootstrapVersion') IS DISTINCT FROM 'number' OR n->>'bootstrapVersion'<>'1'
           OR jsonb_typeof(n->'ordinal') IS DISTINCT FROM 'number' OR n->>'ordinal'<>'0' OR n->>'mode'<>'READ_ONLY_PROBE'
           OR n->>'operationId' IS DISTINCT FROM NEW.operation_id::text OR n->>'stage' IS DISTINCT FROM NEW.stage
           OR n->>'checkpointId' IS DISTINCT FROM NEW.checkpoint_id::text OR NEW.checkpoint_id<>encode(cp,'hex')::uuid
           OR n->>'commandSha256' !~ '^[0-9a-f]{64}$'
           OR n->>'controlRoot' IS DISTINCT FROM admission->'controlBinding'->>'root'
           OR n->>'controlDevice' IS DISTINCT FROM admission->'controlBinding'->>'rootDevice'
           OR n->>'controlInode' IS DISTINCT FROM admission->'controlBinding'->>'rootInode' THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_OPERATION_BINDING_INVALID',ERRCODE='23514';
        END IF;
        FOREACH key IN ARRAY ARRAY['installationId','policy','planKind','runId','attemptId','sourceId','selectionSha256',
                'profileSha256','runtimeSha256','workerInstanceId','ownerEpoch'] LOOP
            IF n->key IS DISTINCT FROM admission->key THEN
                RAISE EXCEPTION USING MESSAGE='CONTENT_OPERATION_BINDING_INVALID',ERRCODE='23514';
            END IF;
        END LOOP;
        FOREACH key IN ARRAY ARRAY['rootDevice','rootInode','lockDevice','lockInode','ledgerSha256'] LOOP
            IF n->key IS DISTINCT FROM ready->key THEN
                RAISE EXCEPTION USING MESSAGE='CONTENT_OPERATION_BINDING_INVALID',ERRCODE='23514';
            END IF;
        END LOOP;
    ELSE
        previous:=CASE NEW.phase WHEN 'FILES_READY' THEN NULL WHEN 'START_AUTHORIZED' THEN 'FILES_READY'
            WHEN 'CHILD_RECORDED' THEN 'START_AUTHORIZED' WHEN 'RELEASE_AUTHORIZED' THEN 'CHILD_RECORDED'
            WHEN 'PROBE_COMPLETED' THEN 'RELEASE_AUTHORIZED' END;
        IF (previous IS NULL AND EXISTS(SELECT 1 FROM content_operation_phases WHERE operation_id=NEW.operation_id))
           OR (previous IS NOT NULL AND NOT EXISTS(SELECT 1 FROM content_operation_phases WHERE operation_id=NEW.operation_id AND phase=previous)) THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_OPERATION_PHASE_INVALID',ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_operation_insert BEFORE INSERT ON content_operations FOR EACH ROW EXECUTE FUNCTION guard_content_operation();
CREATE TRIGGER content_operation_phase_insert BEFORE INSERT ON content_operation_phases FOR EACH ROW EXECUTE FUNCTION guard_content_operation();
