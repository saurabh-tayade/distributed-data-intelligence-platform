-- Additive, inactive strict launch identity. Historical FETCH v2 registrations stay unchanged.
CREATE TABLE content_fetch_launch_profiles (
    launch_profile_id varchar(128) PRIMARY KEY,
    configuration_bytes bytea NOT NULL CHECK(valid_object_bytes(configuration_bytes,65536)),
    configuration_sha256 sha256_hex NOT NULL CHECK(configuration_sha256=encode(sha256(configuration_bytes),'hex')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE content_native_launch_phases (
    operation_id uuid NOT NULL REFERENCES content_native_operations ON DELETE RESTRICT,
    phase varchar(32) NOT NULL CHECK(phase IN ('LAUNCH_BOUND','FILES_READY','START_AUTHORIZED','CHILD_RECORDED','RELEASE_AUTHORIZED','CANDIDATE_EXITED')),
    launch_profile_id varchar(128) REFERENCES content_fetch_launch_profiles ON DELETE RESTRICT,
    launch_profile_sha256 sha256_hex,
    child_instance_id uuid,
    child_pid bigint CHECK(child_pid>0),
    child_started_at timestamptz,
    bootstrap_sha256 sha256_hex,
    candidate_sha256 sha256_hex,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(operation_id,phase),
    CHECK((phase='LAUNCH_BOUND' AND launch_profile_id IS NOT NULL AND launch_profile_sha256 IS NOT NULL)
       OR (phase<>'LAUNCH_BOUND' AND launch_profile_id IS NULL AND launch_profile_sha256 IS NULL)),
    CHECK((phase='CHILD_RECORDED' AND child_instance_id IS NOT NULL AND child_pid IS NOT NULL AND child_started_at IS NOT NULL)
       OR (phase<>'CHILD_RECORDED' AND child_instance_id IS NULL AND child_pid IS NULL AND child_started_at IS NULL)),
    CHECK((phase='RELEASE_AUTHORIZED')=(bootstrap_sha256 IS NOT NULL)),
    CHECK((phase='CANDIDATE_EXITED')=(candidate_sha256 IS NOT NULL))
);
DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['content_fetch_launch_profiles','content_native_launch_phases'] LOOP
        EXECUTE format('CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation()',t);
        EXECUTE format('CREATE TRIGGER no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation()',t);
    END LOOP;
END $$;

CREATE FUNCTION guard_content_fetch_launch_profile() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE n jsonb; historical jsonb; raw bytea; digest text; manifest jsonb; expected jsonb; entry jsonb;
BEGIN
    n:=convert_from(NEW.configuration_bytes,'UTF8')::jsonb;
    SELECT configuration_bytes,configuration_sha256::text INTO raw,digest
        FROM execution_profiles WHERE profile_id=n->>'fetchRuntimeId';
    IF raw IS NULL THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    historical:=convert_from(raw,'UTF8')::jsonb;
    IF historical->'rssFetchRuntimeVersion' IS DISTINCT FROM '2'::jsonb
       OR historical->>'pipeline' IS DISTINCT FROM 'CONTENT_RSS_V1'
       OR historical->'manifest'->>'adapter' IS DISTINCT FROM 'platform.ingestion.managed.BoundedManagedRssFetch'
       OR historical->'testEndpoint' IS DISTINCT FROM 'true'::jsonb
       OR NOT native_reservation_json_matches(convert_from(raw,'UTF8')::json,historical)
       OR jsonb_typeof(historical->'manifest'->'javaRuntime') IS DISTINCT FROM 'string'
       OR jsonb_typeof(historical->'manifest'->'classpath') IS DISTINCT FROM 'array'
       OR jsonb_typeof(historical->'javaHome') IS DISTINCT FROM 'string'
       OR jsonb_typeof(historical->'runProfileId') IS DISTINCT FROM 'string'
       OR jsonb_typeof(historical->'runProfileSha256') IS DISTINCT FROM 'string'
       OR jsonb_typeof(historical->'controlBindingId') IS DISTINCT FROM 'string'
       OR jsonb_typeof(n->'deploymentSha256') IS DISTINCT FROM 'string'
       OR n->>'deploymentSha256' !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    IF jsonb_array_length(historical->'manifest'->'classpath') NOT BETWEEN 1 AND 128 THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    FOR entry IN SELECT value FROM jsonb_array_elements(historical->'manifest'->'classpath') LOOP
        IF jsonb_typeof(entry) IS DISTINCT FROM 'object' OR NOT entry ?& ARRAY['path','sha256']
           OR entry-ARRAY['path','sha256']<>'{}'::jsonb OR jsonb_typeof(entry->'path') IS DISTINCT FROM 'string'
           OR jsonb_typeof(entry->'sha256') IS DISTINCT FROM 'string' OR entry->>'sha256' !~ '^[0-9a-f]{64}$' THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
        END IF;
    END LOOP;
    manifest:=jsonb_build_object('adapter','platform.ingestion.managed.StrictManagedRssFetch',
        'javaRuntime',historical->'manifest'->'javaRuntime','classpath',historical->'manifest'->'classpath');
    expected:=jsonb_build_object('strictFetchLaunchVersion',1,'fetchRuntimeId',n->'fetchRuntimeId',
        'fetchRuntimeSha256',digest,'runProfileId',historical->'runProfileId','runProfileSha256',historical->'runProfileSha256',
        'controlBindingId',historical->'controlBindingId','javaHome',historical->'javaHome','manifest',manifest,
        'deploymentSha256',n->'deploymentSha256','testEndpoint',true);
    -- Java qualification additionally verifies canonical bytes and the canonical manifest hash.
    IF jsonb_typeof(n->'fetchRuntimeId') IS DISTINCT FROM 'string'
       OR NOT native_reservation_json_matches(convert_from(NEW.configuration_bytes,'UTF8')::json,expected) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_fetch_launch_profile_insert BEFORE INSERT ON content_fetch_launch_profiles
    FOR EACH ROW EXECUTE FUNCTION guard_content_fetch_launch_profile();

CREATE FUNCTION guard_content_native_launch_phase() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE
    c coordinator%ROWTYPE; r runs%ROWTYPE; a attempts%ROWTYPE; w worker_instances%ROWTYPE;
    op content_native_operations%ROWTYPE; admission jsonb; ready jsonb; previous text;
    launch jsonb; digest text; command jsonb; facts jsonb;
BEGIN
    IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_LAUNCH_ISOLATION_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT * INTO c FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    SELECT * INTO op FROM content_native_operations WHERE operation_id=NEW.operation_id;
    SELECT r0.* INTO r FROM runs r0 JOIN attempts a0 ON a0.run_id=r0.run_id
        WHERE a0.attempt_id=op.attempt_id FOR UPDATE OF r0;
    SELECT * INTO a FROM attempts WHERE attempt_id=op.attempt_id FOR UPDATE;
    SELECT * INTO w FROM worker_instances WHERE instance_id=a.instance_id;
    IF c.singleton_id IS NULL OR r.run_id IS NULL OR w.instance_id IS NULL OR a.attempt_id IS NULL
       OR a.content_admission_policy IS DISTINCT FROM 'ddip-content-narrow-v1'
       OR a.kind<>'EXECUTE' OR a.state<>'CLAIMED' OR a.attempt_no>19
       OR clock_timestamp()>=a.started_at+interval '2 hours'
       OR r.pipeline<>'CONTENT_RSS_V1' OR r.state<>'RUNNING' OR r.current_attempt_id IS DISTINCT FROM a.attempt_id
       OR c.mode<>'READY' OR c.active_import_id IS NOT NULL OR c.active_attempt_id IS DISTINCT FROM a.attempt_id
       OR c.owner_instance_id IS DISTINCT FROM a.instance_id OR c.epoch<>a.owner_epoch
       OR w.installation_id IS DISTINCT FROM c.installation_id OR w.status IS DISTINCT FROM 'READY'
       OR w.execution_profile_id IS DISTINCT FROM r.execution_profile_id
       OR EXISTS(SELECT 1 FROM controls WHERE run_id=a.run_id AND (state='REQUESTED' OR (type='CANCEL' AND state='APPLIED')))
       OR EXISTS(SELECT 1 FROM stage_invocations WHERE attempt_id=a.attempt_id) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_LAUNCH_OWNER_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT convert_from(admission_bytes,'UTF8')::jsonb INTO admission FROM content_attempt_admissions WHERE attempt_id=a.attempt_id;
    SELECT convert_from(receipt_bytes,'UTF8')::jsonb INTO ready FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='READY';
    IF admission IS NULL OR ready IS NULL
       OR admission->>'planKind' IS DISTINCT FROM 'ORIGINAL'
       OR admission->>'workerInstanceId' IS DISTINCT FROM a.instance_id::text
       OR admission->>'ownerEpoch' IS DISTINCT FROM a.owner_epoch::text
       OR admission->>'installationId' IS DISTINCT FROM c.installation_id::text THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_LAUNCH_READY_REQUIRED',ERRCODE='23514';
    END IF;
    previous:=CASE NEW.phase WHEN 'LAUNCH_BOUND' THEN NULL WHEN 'FILES_READY' THEN 'LAUNCH_BOUND'
        WHEN 'START_AUTHORIZED' THEN 'FILES_READY' WHEN 'CHILD_RECORDED' THEN 'START_AUTHORIZED'
        WHEN 'RELEASE_AUTHORIZED' THEN 'CHILD_RECORDED' WHEN 'CANDIDATE_EXITED' THEN 'RELEASE_AUTHORIZED' END;
    IF (previous IS NULL AND EXISTS(SELECT 1 FROM content_native_launch_phases WHERE operation_id=NEW.operation_id))
       OR (previous IS NOT NULL AND NOT EXISTS(SELECT 1 FROM content_native_launch_phases WHERE operation_id=NEW.operation_id AND phase=previous)) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_LAUNCH_PHASE_INVALID',ERRCODE='23514';
    END IF;
    IF NEW.phase='LAUNCH_BOUND' THEN
        SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO launch,digest
            FROM content_fetch_launch_profiles WHERE launch_profile_id=NEW.launch_profile_id;
        command:=convert_from(op.command_bytes,'UTF8')::jsonb;
        facts:=convert_from(op.bootstrap_bytes,'UTF8')::jsonb;
        IF launch IS NULL OR NEW.launch_profile_sha256::text IS DISTINCT FROM digest
           OR launch->>'fetchRuntimeSha256' IS DISTINCT FROM command->>'runtimeSha256'
           OR launch->>'fetchRuntimeSha256' IS DISTINCT FROM facts->'fetch'->>'runtimeSha256'
           OR launch->>'runProfileId' IS DISTINCT FROM r.execution_profile_id::text
           OR launch->>'runProfileSha256' IS DISTINCT FROM admission->>'profileSha256'
           OR launch->'manifest'->>'javaRuntime' IS DISTINCT FROM command->>'javaRuntime'
           OR command->'testEndpoint' IS DISTINCT FROM 'true'::jsonb THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_LAUNCH_BINDING_INVALID',ERRCODE='23514';
        END IF;
    END IF;
    -- CANDIDATE_EXITED is historical evidence only, never stage success or slot release.
    RETURN NEW;
END $$;
CREATE TRIGGER content_native_launch_phase_insert BEFORE INSERT ON content_native_launch_phases
    FOR EACH ROW EXECUTE FUNCTION guard_content_native_launch_phase();
