-- Inactive terminal-only reconciliation. A receipt is evidence, never executable authority.
CREATE TABLE content_fetch_recovery_finalizations (
    finalization_id uuid PRIMARY KEY,
    handoff_id uuid NOT NULL UNIQUE REFERENCES content_fetch_recovery_handoffs ON DELETE RESTRICT,
    identity_version smallint NOT NULL CHECK(identity_version=1),
    policy varchar(64) NOT NULL CHECK(policy='ddip-content-fetch-terminal-recovery-v1'),
    run_id uuid NOT NULL UNIQUE REFERENCES runs ON DELETE RESTRICT,
    recovery_attempt_id uuid NOT NULL UNIQUE,
    target_bytes bytea NOT NULL CHECK(valid_object_bytes(target_bytes,8192)),
    target_sha256 sha256_hex NOT NULL CHECK(target_sha256=encode(sha256(target_bytes),'hex')),
    review_bytes bytea NOT NULL CHECK(valid_object_bytes(review_bytes,8192)),
    review_sha256 sha256_hex NOT NULL CHECK(review_sha256=encode(sha256(review_bytes),'hex')),
    snapshot_bytes bytea NOT NULL CHECK(valid_object_bytes(snapshot_bytes,65536)),
    snapshot_sha256 sha256_hex NOT NULL CHECK(snapshot_sha256=encode(sha256(snapshot_bytes),'hex')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(run_id,recovery_attempt_id) REFERENCES attempts(run_id,attempt_id) ON DELETE RESTRICT
);
CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON content_fetch_recovery_finalizations
    FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON content_fetch_recovery_finalizations
    FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation();

CREATE FUNCTION guard_content_fetch_recovery_finalization() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE h content_fetch_recovery_handoffs%ROWTYPE; c coordinator%ROWTYPE;
    s jsonb; t jsonb; review jsonb; expected jsonb; successor jsonb;
BEGIN
    IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_ISOLATION_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT * INTO c FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    SELECT * INTO h FROM content_fetch_recovery_handoffs WHERE handoff_id=NEW.handoff_id;
    PERFORM 1 FROM runs WHERE run_id=NEW.run_id FOR UPDATE;
    IF h.handoff_id IS NULL OR NEW.run_id<>h.run_id OR NEW.recovery_attempt_id<>h.recovery_attempt_id
       OR NEW.finalization_id IN (h.handoff_id,h.run_id,h.original_attempt_id,h.recovery_attempt_id,h.control_id)
       OR c.mode<>'READY' OR c.active_attempt_id IS NOT NULL OR c.owner_instance_id IS NOT NULL
       OR c.active_import_id IS NOT NULL OR c.heartbeat_at IS NOT NULL
       OR c.epoch<>h.original_epoch+1 OR c.row_version<>h.original_coordinator_version+2 THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_TERMINAL_BINDING_INVALID',ERRCODE='23514';
    END IF;
    s:=convert_from(NEW.snapshot_bytes,'UTF8')::jsonb;
    t:=convert_from(NEW.target_bytes,'UTF8')::jsonb;
    review:=convert_from(NEW.review_bytes,'UTF8')::jsonb;
    expected:=jsonb_build_object('identityVersion',1,'policy',NEW.policy,
        'finalizationId',NEW.finalization_id::text,'handoffId',h.handoff_id::text,
        'snapshotSha256',NEW.snapshot_sha256::text,'handoffSha256',h.ancestry_sha256::text);
    IF NOT native_reservation_json_matches(convert_from(NEW.target_bytes,'UTF8')::json,expected)
       OR NOT native_reservation_json_matches(convert_from(NEW.snapshot_bytes,'UTF8')::json,s)
       OR NOT s ?& ARRAY['coordinator','run','attempt','control','oldWorker','recovery','history','lineage','controls']
       OR s-ARRAY['coordinator','run','attempt','control','oldWorker','recovery','history','lineage','controls']<>'{}'::jsonb
       OR jsonb_typeof(s->'history') IS DISTINCT FROM 'array'
       OR jsonb_typeof(s->'controls') IS DISTINCT FROM 'array'
       OR s#>>'{coordinator,active_attempt_id}' IS DISTINCT FROM h.recovery_attempt_id::text
       OR s#>>'{coordinator,owner_instance_id}' IS DISTINCT FROM h.recovery_instance_id::text
       OR s#>>'{coordinator,mode}' IS DISTINCT FROM 'RECOVERY_ONLY'
       OR s#>>'{coordinator,epoch}' IS DISTINCT FROM (h.original_epoch+1)::text
       OR s#>>'{coordinator,row_version}' IS DISTINCT FROM (h.original_coordinator_version+1)::text
       OR s#>>'{run,run_id}' IS DISTINCT FROM h.run_id::text
       OR s#>>'{run,state}' IS DISTINCT FROM 'RECOVERY_REQUIRED'
       OR s#>>'{run,current_attempt_id}' IS DISTINCT FROM h.recovery_attempt_id::text
       OR s#>>'{run,row_version}' IS DISTINCT FROM (h.original_run_version+1)::text
       OR s#>>'{attempt,attempt_id}' IS DISTINCT FROM h.original_attempt_id::text
       OR s#>>'{attempt,state}' IS DISTINCT FROM 'INTERRUPTED'
       OR s#>>'{attempt,row_version}' IS DISTINCT FROM (h.original_attempt_version+1)::text
       OR s#>>'{control,control_id}' IS DISTINCT FROM h.control_id::text
       OR s#>>'{control,state}' IS DISTINCT FROM 'REQUESTED'
       OR s#>>'{control,row_version}' IS DISTINCT FROM h.control_version::text
       OR s#>>'{recovery,instance_id}' IS DISTINCT FROM h.recovery_instance_id::text
       OR s#>>'{recovery,status}' IS DISTINCT FROM 'RECOVERY_ONLY'
       OR s#>>'{recovery,row_version}' IS DISTINCT FROM h.recovery_worker_version::text
       OR s#>>'{oldWorker,instance_id}' IS DISTINCT FROM h.original_instance_id::text THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_TERMINAL_EVIDENCE_INVALID',ERRCODE='23514';
    END IF;
    IF jsonb_array_length(s->'history') NOT BETWEEN 2 AND 20
       OR jsonb_array_length(s->'controls') NOT BETWEEN 1 AND 128
       OR (SELECT count(*) FROM jsonb_array_elements(s->'history') a WHERE a->>'attempt_id'=h.recovery_attempt_id::text)<>1 THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_TERMINAL_EVIDENCE_INVALID',ERRCODE='23514';
    END IF;
    SELECT a INTO successor FROM jsonb_array_elements(s->'history') a WHERE a->>'attempt_id'=h.recovery_attempt_id::text;
    IF successor->>'state' IS DISTINCT FROM 'CLAIMED' OR successor->>'row_version' IS DISTINCT FROM '0'
       OR successor->>'kind' IS DISTINCT FROM 'RECONCILE'
       OR successor->>'instance_id' IS DISTINCT FROM h.recovery_instance_id::text
       OR successor->>'owner_epoch' IS DISTINCT FROM (h.original_epoch+1)::text
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
       OR review->'targetSha256' IS DISTINCT FROM to_jsonb(NEW.target_sha256::text)
       OR jsonb_typeof(review->'inspectedAt') IS DISTINCT FROM 'string'
       OR review->'reviewedCheckpoints' IS DISTINCT FROM jsonb_build_array(h.checkpoint_id::text)
       OR jsonb_typeof(review->'descendants') IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_TERMINAL_REVIEW_INVALID',ERRCODE='23514';
    END IF;
    IF jsonb_array_length(review->'descendants')>32
       OR length(review->>'inspectedAt') NOT BETWEEN 20 AND 40
       OR review->>'inspectedAt' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_TERMINAL_REVIEW_INVALID',ERRCODE='23514';
    END IF;
    BEGIN
        PERFORM (review->>'inspectedAt')::timestamptz;
    EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_TERMINAL_REVIEW_INVALID',ERRCODE='23514';
    END;
    -- Receipt insertion is last: all five immutable audit records must already correspond
    -- exactly to this disposition. A receipt-shaped row alone cannot release the slot.
    expected:=jsonb_build_object('identityVersion',1,'policy',NEW.policy,
        'finalizationId',NEW.finalization_id::text,'handoffId',h.handoff_id::text,
        'targetSha256',NEW.target_sha256::text,'checkpointId',h.checkpoint_id::text);
    IF (SELECT count(*) FROM audit_events e WHERE e.run_id=h.run_id
            AND e.safe_code='NATIVE_FETCH_INTERRUPTED')<>5
       OR EXISTS(SELECT 1 FROM (VALUES
            (1,'NATIVE_FETCH_TERMINAL_ATTEMPT','CLAIMED','SUCCEEDED',0::bigint),
            (2,'NATIVE_FETCH_TERMINAL_RUN','RECOVERY_REQUIRED','FAILED',h.original_run_version+1),
            (3,'NATIVE_FETCH_TERMINAL_CONTROL','REQUESTED','APPLIED',h.control_version),
            (4,'NATIVE_FETCH_TERMINAL_WORKER','RECOVERY_ONLY','STOPPED',h.recovery_worker_version),
            (5,'NATIVE_FETCH_TERMINAL_RELEASE','RECOVERY_ONLY','READY',h.original_coordinator_version+1)
        ) v(ordinal,action,before_state,after_state,before_version) WHERE NOT EXISTS(
            SELECT 1 FROM (SELECT event.*,row_number() OVER (ORDER BY event_seq) AS ordinal
                FROM audit_events event WHERE event.run_id=h.run_id
                    AND event.safe_code='NATIVE_FETCH_INTERRUPTED') e
            WHERE e.ordinal=v.ordinal AND e.actor_scope='local-operator'
                AND e.run_id=h.run_id AND e.attempt_id=h.recovery_attempt_id AND e.control_id=h.control_id
                AND e.action=v.action AND e.before_state=v.before_state AND e.after_state=v.after_state
                AND e.before_version=v.before_version AND e.after_version=v.before_version+1
                AND e.safe_code='NATIVE_FETCH_INTERRUPTED'
                AND native_reservation_json_matches(convert_from(e.details_bytes,'UTF8')::json,expected))) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_TERMINAL_AUDIT_INVALID',ERRCODE='23514';
    END IF;
    -- Freshness and trusted-local writer/process review are application checks, not SQL authority.
    RETURN NEW;
END $$;
CREATE TRIGGER content_fetch_recovery_finalization_insert BEFORE INSERT ON content_fetch_recovery_finalizations
    FOR EACH ROW EXECUTE FUNCTION guard_content_fetch_recovery_finalization();

-- Worker lifecycle checks locate their retained handoffs without scanning receipt bodies.
CREATE INDEX content_fetch_recovery_original_worker_idx ON content_fetch_recovery_handoffs(original_instance_id);
CREATE INDEX content_fetch_recovery_successor_worker_idx ON content_fetch_recovery_handoffs(recovery_instance_id);

-- Supersedes only V15's deferred occupied-state restriction. Pending handoffs remain occupied.
-- Terminal enrollment is permanent: no retry, new attempt, stage, delivery or further takeover.
CREATE OR REPLACE FUNCTION check_content_fetch_recovery_binding() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE affected_run uuid; affected_worker uuid;
BEGIN
    IF TG_TABLE_NAME='worker_instances' THEN
        affected_worker:=NEW.instance_id;
    ELSIF TG_TABLE_NAME IN ('content_fetch_recovery_handoffs','content_fetch_recovery_finalizations') THEN
        affected_run:=NEW.run_id;
    ELSIF TG_TABLE_NAME<>'coordinator' THEN
        IF TG_TABLE_NAME IN ('stage_invocations','content_operations','content_native_operations') THEN
            SELECT run_id INTO affected_run FROM attempts WHERE attempt_id=NEW.attempt_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_ATTEMPT_REQUIRED',ERRCODE='23503';
            END IF;
        ELSE
            affected_run:=NEW.run_id;
        END IF;
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
    -- Every relevant writer uses the same fence. Under snapshot isolation a changed coordinator
    -- aborts instead of allowing a stale snapshot to miss a newly inserted handoff/receipt.
    PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    IF EXISTS(SELECT 1 FROM content_fetch_recovery_handoffs h
        JOIN attempts a ON a.attempt_id=h.recovery_attempt_id
        JOIN runs r ON r.run_id=h.run_id
        CROSS JOIN coordinator c
        LEFT JOIN content_fetch_recovery_finalizations f ON f.handoff_id=h.handoff_id
        WHERE f.finalization_id IS NULL AND
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
    -- Admission caps history at 10,000 runs, but parsing every 64 KiB receipt snapshot
    -- on each later claim would still hold the global fence unnecessarily. Coordinator
    -- writes inspect only lightweight ownership/version columns. Row writers validate
    -- the exact affected run (or handoffs referencing the affected worker) below.
    IF TG_TABLE_NAME='coordinator' THEN
        IF EXISTS(SELECT 1 FROM content_fetch_recovery_finalizations f
            JOIN content_fetch_recovery_handoffs h USING(handoff_id)
            CROSS JOIN coordinator c
            WHERE c.active_attempt_id IN (h.original_attempt_id,h.recovery_attempt_id)
                OR c.owner_instance_id IN (h.original_instance_id,h.recovery_instance_id)
                OR c.epoch<h.original_epoch+1 OR c.row_version<h.original_coordinator_version+2) THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_TERMINAL_RETAINED',ERRCODE='23514';
        END IF;
        RETURN NULL;
    END IF;
    IF EXISTS(SELECT 1 FROM content_fetch_recovery_finalizations f
        JOIN content_fetch_recovery_handoffs h USING(handoff_id)
        JOIN attempts a ON a.attempt_id=h.recovery_attempt_id
        JOIN attempts predecessor ON predecessor.attempt_id=h.original_attempt_id
        JOIN runs r ON r.run_id=h.run_id
        JOIN controls ctl ON ctl.control_id=h.control_id
        JOIN worker_instances w ON w.instance_id=h.recovery_instance_id
        JOIN worker_instances ow ON ow.instance_id=h.original_instance_id
        CROSS JOIN coordinator c
        WHERE (h.run_id=affected_run OR
            (affected_worker IS NOT NULL AND affected_worker IN (h.original_instance_id,h.recovery_instance_id))) AND
          (f.run_id<>h.run_id OR f.recovery_attempt_id<>h.recovery_attempt_id
           OR a.kind<>'RECONCILE' OR a.state<>'SUCCEEDED' OR a.finished_at IS NULL
           OR a.outcome_code IS DISTINCT FROM 'RECONCILED_FAILED' OR a.row_version<>1
           OR a.content_recovery_policy IS DISTINCT FROM h.policy
           OR a.instance_id<>h.recovery_instance_id OR a.owner_epoch<>h.original_epoch+1
           OR a.content_admission_policy IS NOT NULL OR a.reused_checkpoint_id IS NOT NULL
           OR predecessor.state<>'INTERRUPTED' OR predecessor.outcome_code IS DISTINCT FROM 'INTERRUPTED'
           OR predecessor.row_version<>h.original_attempt_version+1 OR predecessor.finished_at IS NULL
           OR r.state<>'FAILED' OR r.outcome_code IS DISTINCT FROM 'NATIVE_FETCH_INTERRUPTED'
           OR r.current_attempt_id IS DISTINCT FROM a.attempt_id OR r.row_version<>h.original_run_version+2
           OR r.retry_eligible OR r.result_artifact_id IS NOT NULL OR r.counts_bytes IS NOT NULL OR r.finished_at IS NULL
           OR ctl.state<>'APPLIED' OR ctl.reason IS DISTINCT FROM 'NATIVE_FETCH_INTERRUPTED'
           OR ctl.row_version<>h.control_version+1 OR ctl.applied_at IS NULL
           OR ctl.type<>'RECONCILE' OR ctl.target_attempt_id IS DISTINCT FROM h.original_attempt_id
           OR w.status<>'STOPPED' OR w.row_version<>h.recovery_worker_version+1
           OR ow.row_version<>h.original_worker_version
           OR c.active_attempt_id IN (h.original_attempt_id,h.recovery_attempt_id)
           OR c.owner_instance_id IN (h.original_instance_id,h.recovery_instance_id)
           OR c.epoch<h.original_epoch+1 OR c.row_version<h.original_coordinator_version+2
           OR (SELECT count(*) FROM attempts x WHERE x.run_id=h.run_id)<>
               jsonb_array_length(convert_from(f.snapshot_bytes,'UTF8')::jsonb->'history')
           OR EXISTS(SELECT 1 FROM attempts x WHERE x.run_id=h.run_id AND NOT EXISTS(
               SELECT 1 FROM jsonb_array_elements(convert_from(f.snapshot_bytes,'UTF8')::jsonb->'history') prior
               WHERE prior->>'attempt_id'=x.attempt_id::text))
           OR (SELECT count(*) FROM controls x WHERE x.run_id=h.run_id)<>
               jsonb_array_length(convert_from(f.snapshot_bytes,'UTF8')::jsonb->'controls')
           OR EXISTS(SELECT 1 FROM controls x WHERE x.run_id=h.run_id AND NOT EXISTS(
               SELECT 1 FROM jsonb_array_elements(convert_from(f.snapshot_bytes,'UTF8')::jsonb->'controls') prior
               WHERE prior->>'controlId'=x.control_id::text))
           OR EXISTS(SELECT 1 FROM stage_invocations s JOIN attempts x USING(attempt_id) WHERE x.run_id=h.run_id)
           OR EXISTS(SELECT 1 FROM rss_deliveries WHERE run_id=h.run_id)
           OR EXISTS(SELECT 1 FROM content_operations o JOIN attempts x USING(attempt_id) WHERE x.run_id=h.run_id)
           OR EXISTS(SELECT 1 FROM content_native_operations o JOIN attempts x USING(attempt_id)
               WHERE x.run_id=h.run_id AND o.operation_id<>h.operation_id)
           OR EXISTS(SELECT 1 FROM controls WHERE run_id=h.run_id AND control_id<>h.control_id
               AND (state='REQUESTED' OR (type='CANCEL' AND state='APPLIED'))))) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_TERMINAL_RETAINED',ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['content_fetch_recovery_finalizations','controls','worker_instances','content_operations','content_native_operations'] LOOP
        EXECUTE format('CREATE CONSTRAINT TRIGGER content_fetch_recovery_binding AFTER INSERT OR UPDATE ON %I DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_content_fetch_recovery_binding()',t);
    END LOOP;
END $$;
