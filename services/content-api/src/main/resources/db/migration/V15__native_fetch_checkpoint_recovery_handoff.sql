-- A distinct, inactive recovery identity. This does not adopt legacy checkpoints or authorize work.
ALTER TABLE attempts ADD COLUMN content_recovery_policy varchar(64)
    CHECK(content_recovery_policy IS NULL OR content_recovery_policy='ddip-content-fetch-checkpoint-recovery-v1');
ALTER TABLE attempts ADD CONSTRAINT content_recovery_attempt_shape CHECK (
    content_recovery_policy IS NULL OR
    (kind='RECONCILE' AND content_admission_policy IS NULL AND reused_checkpoint_id IS NULL));

CREATE TABLE content_fetch_recovery_handoffs (
    handoff_id uuid PRIMARY KEY,
    identity_version smallint NOT NULL CHECK(identity_version=1),
    policy varchar(64) NOT NULL CHECK(policy='ddip-content-fetch-checkpoint-recovery-v1'),
    run_id uuid NOT NULL UNIQUE REFERENCES runs ON DELETE RESTRICT,
    control_id uuid NOT NULL UNIQUE REFERENCES controls ON DELETE RESTRICT,
    original_attempt_id uuid NOT NULL UNIQUE,
    recovery_attempt_id uuid NOT NULL UNIQUE,
    original_instance_id uuid NOT NULL REFERENCES worker_instances ON DELETE RESTRICT,
    recovery_instance_id uuid NOT NULL REFERENCES worker_instances ON DELETE RESTRICT,
    original_epoch bigint NOT NULL CHECK(original_epoch>0),
    operation_id uuid NOT NULL UNIQUE REFERENCES content_fetch_checkpoints(operation_id) ON DELETE RESTRICT,
    checkpoint_id uuid NOT NULL UNIQUE REFERENCES content_fetch_checkpoints(checkpoint_id) ON DELETE RESTRICT,
    original_coordinator_version bigint NOT NULL CHECK(original_coordinator_version>=0),
    original_run_version bigint NOT NULL CHECK(original_run_version>=0),
    original_attempt_version bigint NOT NULL CHECK(original_attempt_version>=0),
    original_worker_version bigint NOT NULL CHECK(original_worker_version>=0),
    recovery_worker_version bigint NOT NULL CHECK(recovery_worker_version>=0),
    control_version bigint NOT NULL CHECK(control_version>=0),
    review_bytes bytea NOT NULL CHECK(valid_object_bytes(review_bytes,8192)),
    review_sha256 sha256_hex NOT NULL CHECK(review_sha256=encode(sha256(review_bytes),'hex')),
    ancestry_bytes bytea NOT NULL CHECK(valid_object_bytes(ancestry_bytes,65536)),
    ancestry_sha256 sha256_hex NOT NULL CHECK(ancestry_sha256=encode(sha256(ancestry_bytes),'hex')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(recovery_attempt_id,policy),
    FOREIGN KEY(run_id,original_attempt_id) REFERENCES attempts(run_id,attempt_id) ON DELETE RESTRICT,
    FOREIGN KEY(run_id,recovery_attempt_id) REFERENCES attempts(run_id,attempt_id) ON DELETE RESTRICT,
    CHECK(original_attempt_id<>recovery_attempt_id AND recovery_attempt_id<>control_id
        AND handoff_id<>original_attempt_id AND handoff_id<>recovery_attempt_id AND handoff_id<>control_id),
    CHECK(original_instance_id<>recovery_instance_id)
);
ALTER TABLE attempts ADD CONSTRAINT content_recovery_handoff_fk
    FOREIGN KEY(attempt_id,content_recovery_policy)
    REFERENCES content_fetch_recovery_handoffs(recovery_attempt_id,policy) DEFERRABLE INITIALLY DEFERRED;
CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON content_fetch_recovery_handoffs
    FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON content_fetch_recovery_handoffs
    FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation();

-- Insert last in the ownership transaction. All checks observe the transferred, still occupied slot.
-- Historical native rows remain untouched. SQL validates structure; the application validates the
-- exact canonical snapshot and operator review before transfer, outside filesystem IO transactions.
CREATE FUNCTION guard_content_fetch_recovery_handoff() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE c coordinator%ROWTYPE; r runs%ROWTYPE; a attempts%ROWTYPE; successor attempts%ROWTYPE;
    ow worker_instances%ROWTYPE; rw worker_instances%ROWTYPE; ctl controls%ROWTYPE;
    op content_native_operations%ROWTYPE; cp content_fetch_checkpoints%ROWTYPE; ancestry jsonb; review jsonb; expected jsonb;
BEGIN
    IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_ISOLATION_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT * INTO c FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    SELECT * INTO r FROM runs WHERE run_id=NEW.run_id FOR UPDATE;
    SELECT * INTO a FROM attempts WHERE attempt_id=NEW.original_attempt_id FOR UPDATE;
    SELECT * INTO successor FROM attempts WHERE attempt_id=NEW.recovery_attempt_id FOR UPDATE;
    SELECT * INTO ow FROM worker_instances WHERE instance_id=NEW.original_instance_id;
    SELECT * INTO rw FROM worker_instances WHERE instance_id=NEW.recovery_instance_id;
    SELECT * INTO ctl FROM controls WHERE control_id=NEW.control_id FOR UPDATE;
    SELECT * INTO op FROM content_native_operations WHERE operation_id=NEW.operation_id;
    SELECT * INTO cp FROM content_fetch_checkpoints WHERE operation_id=NEW.operation_id;
    IF c.singleton_id IS NULL OR r.run_id IS NULL OR a.attempt_id IS NULL OR successor.attempt_id IS NULL
       OR ow.instance_id IS NULL OR rw.instance_id IS NULL OR ctl.control_id IS NULL
       OR op.operation_id IS NULL OR cp.operation_id IS NULL
       OR a.run_id<>NEW.run_id OR a.kind<>'EXECUTE' OR a.state<>'INTERRUPTED'
       OR a.content_admission_policy IS DISTINCT FROM 'ddip-content-narrow-v1' OR a.content_recovery_policy IS NOT NULL
       OR a.instance_id<>NEW.original_instance_id OR a.owner_epoch<>NEW.original_epoch
       OR a.row_version<>NEW.original_attempt_version+1 OR a.finished_at IS NULL
       OR a.outcome_code IS DISTINCT FROM 'INTERRUPTED'
       OR successor.run_id<>NEW.run_id OR successor.kind<>'RECONCILE' OR successor.state<>'CLAIMED'
       OR successor.content_recovery_policy IS DISTINCT FROM NEW.policy
       OR successor.instance_id<>NEW.recovery_instance_id OR successor.owner_epoch<>NEW.original_epoch+1
       OR successor.row_version<>0 OR successor.finished_at IS NOT NULL OR successor.outcome_code IS NOT NULL
       OR successor.content_admission_policy IS NOT NULL OR successor.reused_checkpoint_id IS NOT NULL
       OR successor.attempt_no<>(SELECT max(attempt_no)+1 FROM attempts
           WHERE run_id=NEW.run_id AND attempt_id<>successor.attempt_id)
       OR r.pipeline<>'CONTENT_RSS_V1' OR r.state<>'RECOVERY_REQUIRED'
       OR r.outcome_code IS DISTINCT FROM 'RECOVERY_REQUIRED' OR r.retry_eligible
       OR r.current_attempt_id IS DISTINCT FROM successor.attempt_id OR r.row_version<>NEW.original_run_version+1
       OR r.result_artifact_id IS NOT NULL OR r.finished_at IS NOT NULL
       OR c.mode<>'RECOVERY_ONLY' OR c.active_import_id IS NOT NULL
       OR c.active_attempt_id IS DISTINCT FROM successor.attempt_id OR c.owner_instance_id IS DISTINCT FROM rw.instance_id
       OR c.epoch<>NEW.original_epoch+1 OR c.row_version<>NEW.original_coordinator_version+1
       OR ow.installation_id<>c.installation_id OR rw.installation_id<>c.installation_id
       OR ow.execution_profile_id<>r.execution_profile_id OR rw.execution_profile_id<>r.execution_profile_id
       OR ow.profile_sha256<>rw.profile_sha256 OR ow.host_binding_sha256<>rw.host_binding_sha256
       OR ow.row_version<>NEW.original_worker_version OR rw.row_version<>NEW.recovery_worker_version
       OR rw.status<>'RECOVERY_ONLY'
       OR ctl.run_id<>NEW.run_id OR ctl.type<>'RECONCILE' OR ctl.state<>'REQUESTED'
       OR ctl.target_attempt_id IS DISTINCT FROM a.attempt_id OR ctl.quiescence_confirmed IS DISTINCT FROM true
       OR ctl.row_version<>NEW.control_version OR ctl.expected_version<>NEW.original_run_version
       OR ctl.requester<>'local-operator'
       OR op.attempt_id<>a.attempt_id OR cp.checkpoint_id<>NEW.checkpoint_id OR op.checkpoint_id<>NEW.checkpoint_id
       OR (SELECT count(*) FROM content_native_launch_phases WHERE operation_id=NEW.operation_id)<>6
       OR NOT EXISTS(SELECT 1 FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='READY')
       OR EXISTS(SELECT 1 FROM attempts prior WHERE prior.run_id=NEW.run_id
           AND prior.attempt_id NOT IN (a.attempt_id,successor.attempt_id)
           AND (prior.kind<>'EXECUTE' OR prior.state NOT IN ('FAILED','CANCELLED')
               OR prior.content_admission_policy IS NOT NULL OR prior.content_recovery_policy IS NOT NULL
               OR prior.reused_checkpoint_id IS NOT NULL))
       OR EXISTS(SELECT 1 FROM stage_invocations s JOIN attempts x USING(attempt_id) WHERE x.run_id=NEW.run_id)
       OR EXISTS(SELECT 1 FROM rss_deliveries WHERE run_id=NEW.run_id)
       OR EXISTS(SELECT 1 FROM content_operations WHERE attempt_id=a.attempt_id)
       OR EXISTS(SELECT 1 FROM controls WHERE run_id=NEW.run_id AND control_id<>NEW.control_id
           AND (state='REQUESTED' OR (type='CANCEL' AND state='APPLIED'))) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_BINDING_INVALID',ERRCODE='23514';
    END IF;
    ancestry:=convert_from(NEW.ancestry_bytes,'UTF8')::jsonb;
    review:=convert_from(NEW.review_bytes,'UTF8')::jsonb;
    expected:=jsonb_build_object(
        'identityVersion',1,'policy',NEW.policy,'handoffId',NEW.handoff_id::text,'controlId',NEW.control_id::text,
        'runId',NEW.run_id::text,'originalAttemptId',NEW.original_attempt_id::text,
        'recoveryAttemptId',NEW.recovery_attempt_id::text,'operationId',NEW.operation_id::text,
        'checkpointId',NEW.checkpoint_id::text,
        'admissionSha256',(SELECT admission_sha256::text FROM content_attempt_admissions WHERE attempt_id=a.attempt_id),
        'readySha256',(SELECT receipt_sha256::text FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='READY'),
        'commandSha256',op.command_sha256::text,'bootstrapSha256',op.bootstrap_sha256::text,
        'candidateSha256',(SELECT candidate_sha256::text FROM content_native_launch_phases WHERE operation_id=NEW.operation_id AND phase='CANDIDATE_EXITED'),
        'completionSha256',cp.completion_sha256::text,'journalSha256',cp.journal_sha256::text,'expectedSha256',cp.expected_sha256::text,
        'snapshot',ancestry->'snapshot','snapshotSha256',ancestry->'snapshotSha256');
    IF NOT native_reservation_json_matches(convert_from(NEW.ancestry_bytes,'UTF8')::json,expected)
       OR jsonb_typeof(ancestry->'snapshot') IS DISTINCT FROM 'object'
       OR jsonb_typeof(ancestry->'snapshotSha256') IS DISTINCT FROM 'string'
       OR ancestry->>'snapshotSha256' !~ '^[0-9a-f]{64}$'
       OR NOT native_reservation_json_matches(convert_from(NEW.review_bytes,'UTF8')::json,review)
       OR NOT review ?& ARRAY['targetSha256','inspectedAt','handlersStopped','unmanagedWritersStopped',
           'orphanReviewComplete','reviewedCheckpoints','descendants','reviewVersion','descendantsReviewed','launchGapsReviewed']
       OR review-ARRAY['targetSha256','inspectedAt','handlersStopped','unmanagedWritersStopped',
           'orphanReviewComplete','reviewedCheckpoints','descendants','reviewVersion','descendantsReviewed','launchGapsReviewed']<>'{}'::jsonb
       OR review->'reviewVersion' IS DISTINCT FROM '1'::jsonb OR review->>'reviewVersion' IS DISTINCT FROM '1'
       OR review->'handlersStopped' IS DISTINCT FROM 'true'::jsonb
       OR review->'unmanagedWritersStopped' IS DISTINCT FROM 'true'::jsonb
       OR review->'orphanReviewComplete' IS DISTINCT FROM 'true'::jsonb
       OR review->'descendantsReviewed' IS DISTINCT FROM 'true'::jsonb
       OR review->'launchGapsReviewed' IS DISTINCT FROM 'true'::jsonb
       OR review->'targetSha256' IS DISTINCT FROM to_jsonb(NEW.ancestry_sha256::text)
       OR jsonb_typeof(review->'inspectedAt') IS DISTINCT FROM 'string'
       OR review->'reviewedCheckpoints' IS DISTINCT FROM jsonb_build_array(NEW.checkpoint_id::text)
       OR jsonb_typeof(review->'descendants') IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_EVIDENCE_INVALID',ERRCODE='23514';
    END IF;
    -- Check type before length/casts so malformed review bytes fail with the bounded diagnostic.
    IF jsonb_array_length(review->'descendants')>32
       OR length(review->>'inspectedAt') NOT BETWEEN 20 AND 40
       OR review->>'inspectedAt' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_EVIDENCE_INVALID',ERRCODE='23514';
    END IF;
    -- The application checks review freshness against its frozen clock immediately before transfer.
    -- SQL validates timestamp syntax, but does not introduce a second wall-clock authority.
    BEGIN
        PERFORM (review->>'inspectedAt')::timestamptz;
    EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_EVIDENCE_INVALID',ERRCODE='23514';
    END;
    RETURN NEW;
END $$;
CREATE TRIGGER content_fetch_recovery_handoff_insert BEFORE INSERT ON content_fetch_recovery_handoffs
    FOR EACH ROW EXECUTE FUNCTION guard_content_fetch_recovery_handoff();

-- This slice has no continuation, terminal transition, slot release or further handoff protocol.
-- Deferred validation permits the initial atomic transfer, never an observable unoccupied slot.
CREATE FUNCTION check_content_fetch_recovery_binding() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE affected_run uuid;
BEGIN
    -- Immutable enrollment, not handoff visibility, determines serialization. A competing
    -- transfer may not have inserted its handoff yet. Historical unmarked attempts in a
    -- marked run are also relevant: writing their stages would create mixed ancestry.
    IF TG_TABLE_NAME NOT IN ('coordinator','content_fetch_recovery_handoffs') THEN
        IF TG_TABLE_NAME='stage_invocations' THEN
            SELECT run_id INTO affected_run FROM attempts WHERE attempt_id=NEW.attempt_id;
            IF NOT FOUND THEN
                -- Do not mistake an unseen, uncommitted attempt for a legacy attempt.
                RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_ATTEMPT_REQUIRED',ERRCODE='23503';
            END IF;
        ELSE
            affected_run:=NEW.run_id;
        END IF;
        -- An old snapshot must not classify a newly enrolled run as legacy. Locking the
        -- run under snapshot isolation raises a serialization failure when enrollment or
        -- handoff changed it after that snapshot; READ COMMITTED keeps its lock-free legacy path.
        IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
            PERFORM 1 FROM runs WHERE run_id=affected_run FOR SHARE;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_RUN_REQUIRED',ERRCODE='23503';
            END IF;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM attempts WHERE run_id=affected_run AND
            (content_admission_policy IS NOT NULL OR content_recovery_policy IS NOT NULL)) THEN
            RETURN NULL;
        END IF;
    END IF;
    PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    IF EXISTS(SELECT 1 FROM content_fetch_recovery_handoffs h
        JOIN attempts a ON a.attempt_id=h.recovery_attempt_id
        JOIN runs r ON r.run_id=h.run_id
        CROSS JOIN coordinator c
        WHERE (affected_run IS NULL OR h.run_id=affected_run) AND
          (a.state<>'CLAIMED' OR a.kind<>'RECONCILE' OR a.finished_at IS NOT NULL
           OR a.content_recovery_policy IS DISTINCT FROM h.policy
           OR r.state<>'RECOVERY_REQUIRED' OR r.current_attempt_id IS DISTINCT FROM a.attempt_id
           OR r.result_artifact_id IS NOT NULL OR r.finished_at IS NOT NULL
           OR c.mode<>'RECOVERY_ONLY' OR c.active_import_id IS NOT NULL
           OR c.active_attempt_id IS DISTINCT FROM a.attempt_id OR c.owner_instance_id IS DISTINCT FROM a.instance_id
           OR c.epoch<>a.owner_epoch
           OR EXISTS(SELECT 1 FROM stage_invocations s JOIN attempts x USING(attempt_id) WHERE x.run_id=h.run_id)
           OR EXISTS(SELECT 1 FROM rss_deliveries WHERE run_id=h.run_id))) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_RETAINED',ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['content_fetch_recovery_handoffs','attempts','runs','coordinator','stage_invocations','rss_deliveries'] LOOP
        EXECUTE format('CREATE CONSTRAINT TRIGGER content_fetch_recovery_binding AFTER INSERT OR UPDATE ON %I DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_content_fetch_recovery_binding()',t);
    END LOOP;
END $$;
