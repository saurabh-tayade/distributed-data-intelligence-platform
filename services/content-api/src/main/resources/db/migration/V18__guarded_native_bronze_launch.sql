-- Additive, inactive original-owner Bronze launch evidence. No completion or slot release.
CREATE TABLE content_bronze_launch_profiles (
    launch_profile_id varchar(128) PRIMARY KEY,
    configuration_bytes bytea NOT NULL CHECK(valid_object_bytes(configuration_bytes,65536)),
    configuration_sha256 sha256_hex NOT NULL CHECK(configuration_sha256=encode(sha256(configuration_bytes),'hex')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE content_bronze_launch_phases (
    operation_id uuid NOT NULL REFERENCES content_bronze_reservations ON DELETE RESTRICT,
    phase varchar(32) NOT NULL CHECK(phase IN ('LAUNCH_BOUND','FILES_READY','START_AUTHORIZED','CHILD_RECORDED','RELEASE_AUTHORIZED','CANDIDATE_EXITED')),
    launch_profile_id varchar(128) REFERENCES content_bronze_launch_profiles ON DELETE RESTRICT,
    launch_profile_sha256 sha256_hex,
    child_instance_id uuid,
    child_pid bigint CHECK(child_pid>0),
    child_started_at timestamptz,
    bootstrap_sha256 sha256_hex,
    candidate_sha256 sha256_hex,
    exit_code integer CHECK(exit_code IN (0,1)),
    storage_state varchar(16) CHECK(storage_state IN ('EXACT','ABSENT','PAYLOAD_ONLY','METADATA_ONLY','CONFLICT','UNAVAILABLE')),
    owner_coordinator_version bigint NOT NULL CHECK(owner_coordinator_version>=0),
    owner_worker_version bigint NOT NULL CHECK(owner_worker_version>=0),
    owner_run_version bigint NOT NULL CHECK(owner_run_version>=0),
    owner_attempt_version bigint NOT NULL CHECK(owner_attempt_version>=0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(operation_id,phase),
    CHECK((phase='LAUNCH_BOUND' AND launch_profile_id IS NOT NULL AND launch_profile_sha256 IS NOT NULL)
       OR (phase<>'LAUNCH_BOUND' AND launch_profile_id IS NULL AND launch_profile_sha256 IS NULL)),
    CHECK((phase='CHILD_RECORDED' AND child_instance_id IS NOT NULL AND child_pid IS NOT NULL AND child_started_at IS NOT NULL)
       OR (phase<>'CHILD_RECORDED' AND child_instance_id IS NULL AND child_pid IS NULL AND child_started_at IS NULL)),
    CHECK((phase='RELEASE_AUTHORIZED')=(bootstrap_sha256 IS NOT NULL)),
    CHECK((phase='CANDIDATE_EXITED')=(candidate_sha256 IS NOT NULL)),
    CHECK((phase='CANDIDATE_EXITED')=(exit_code IS NOT NULL)),
    CHECK((phase='CANDIDATE_EXITED')=(storage_state IS NOT NULL)),
    CHECK((exit_code=0)=(storage_state='EXACT'))
);
DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['content_bronze_launch_profiles','content_bronze_launch_phases'] LOOP
        EXECUTE format('CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation()',t);
        EXECUTE format('CREATE TRIGGER no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation()',t);
    END LOOP;
END $$;

CREATE FUNCTION guard_content_bronze_launch_profile() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE n jsonb; bronze jsonb; fetched jsonb; raw bytea; digest text; fetch_raw bytea;
    fetch_digest text; expected jsonb; manifest jsonb; entry jsonb;
BEGIN
    n:=convert_from(NEW.configuration_bytes,'UTF8')::jsonb;
    SELECT configuration_bytes,configuration_sha256::text INTO raw,digest
        FROM execution_profiles WHERE profile_id=n->>'bronzeRuntimeId';
    IF raw IS NULL THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    bronze:=convert_from(raw,'UTF8')::jsonb;
    SELECT configuration_bytes,configuration_sha256::text INTO fetch_raw,fetch_digest
        FROM execution_profiles WHERE profile_id=bronze->>'fetchRuntimeId';
    fetched:=convert_from(fetch_raw,'UTF8')::jsonb;
    IF bronze->'bronzeRuntimeVersion' IS DISTINCT FROM '2'::jsonb
       OR bronze->>'adapter' IS DISTINCT FROM 'platform.ingestion.managed.BoundedManagedBronze'
       OR NOT native_reservation_json_matches(convert_from(raw,'UTF8')::json,bronze)
       OR fetch_raw IS NULL OR fetch_digest IS DISTINCT FROM bronze->>'fetchRuntimeSha256'
       OR fetched->'rssFetchRuntimeVersion' IS DISTINCT FROM '2'::jsonb
       OR fetched->>'pipeline' IS DISTINCT FROM 'CONTENT_RSS_V1'
       OR fetched->'manifest'->>'adapter' IS DISTINCT FROM 'platform.ingestion.managed.BoundedManagedRssFetch'
       OR fetched->'testEndpoint' IS DISTINCT FROM 'true'::jsonb
       OR NOT native_reservation_json_matches(convert_from(fetch_raw,'UTF8')::json,fetched)
       OR fetched->>'runProfileId' IS DISTINCT FROM bronze->>'runProfileId'
       OR fetched->>'runProfileSha256' IS DISTINCT FROM bronze->>'runProfileSha256'
       OR fetched->>'controlBindingId' IS DISTINCT FROM bronze->>'controlBindingId'
       OR jsonb_typeof(fetched->'manifest'->'javaRuntime') IS DISTINCT FROM 'string'
       OR jsonb_typeof(fetched->'manifest'->'classpath') IS DISTINCT FROM 'array'
       OR jsonb_typeof(fetched->'javaHome') IS DISTINCT FROM 'string'
       OR jsonb_typeof(bronze->'runProfileId') IS DISTINCT FROM 'string'
       OR jsonb_typeof(bronze->'runProfileSha256') IS DISTINCT FROM 'string'
       OR bronze->>'runProfileSha256' !~ '^[0-9a-f]{64}$'
       OR jsonb_typeof(bronze->'controlBindingId') IS DISTINCT FROM 'string'
       OR jsonb_typeof(bronze->'storageBindingId') IS DISTINCT FROM 'string'
       OR jsonb_typeof(bronze->'storageBindingSha256') IS DISTINCT FROM 'string'
       OR bronze->>'storageBindingSha256' !~ '^[0-9a-f]{64}$'
       OR jsonb_typeof(n->'deploymentSha256') IS DISTINCT FROM 'string'
       OR n->>'deploymentSha256' !~ '^[0-9a-f]{64}$'
       OR jsonb_typeof(n->'strictRuntimeSha256') IS DISTINCT FROM 'string'
       OR n->>'strictRuntimeSha256' !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    IF jsonb_array_length(fetched->'manifest'->'classpath') NOT BETWEEN 1 AND 128 THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    FOR entry IN SELECT value FROM jsonb_array_elements(fetched->'manifest'->'classpath') LOOP
        IF jsonb_typeof(entry) IS DISTINCT FROM 'object' OR NOT entry ?& ARRAY['path','sha256']
           OR entry-ARRAY['path','sha256']<>'{}'::jsonb OR jsonb_typeof(entry->'path') IS DISTINCT FROM 'string'
           OR jsonb_typeof(entry->'sha256') IS DISTINCT FROM 'string' OR entry->>'sha256' !~ '^[0-9a-f]{64}$' THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
        END IF;
    END LOOP;
    manifest:=jsonb_build_object('adapter','platform.ingestion.managed.StrictManagedBronze',
        'javaRuntime',fetched->'manifest'->'javaRuntime','classpath',fetched->'manifest'->'classpath');
    expected:=jsonb_build_object('strictBronzeLaunchVersion',1,'bronzeRuntimeId',n->'bronzeRuntimeId',
        'bronzeRuntimeSha256',digest,'runProfileId',bronze->'runProfileId','runProfileSha256',bronze->'runProfileSha256',
        'controlBindingId',bronze->'controlBindingId','storageBindingId',bronze->'storageBindingId',
        'storageBindingSha256',bronze->'storageBindingSha256','javaHome',fetched->'javaHome','manifest',manifest,
        'deploymentSha256',n->'deploymentSha256','strictRuntimeSha256',n->'strictRuntimeSha256','testEndpoint',true);
    -- Java also checks canonical bytes, manifest digest and the exact strict protocol digest.
    -- strictRuntimeSha256 is deliberately separate from deploymentSha256.
    IF jsonb_typeof(n->'bronzeRuntimeId') IS DISTINCT FROM 'string'
       OR NOT native_reservation_json_matches(convert_from(NEW.configuration_bytes,'UTF8')::json,expected) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_bronze_launch_profile_insert BEFORE INSERT ON content_bronze_launch_profiles
    FOR EACH ROW EXECUTE FUNCTION guard_content_bronze_launch_profile();

CREATE FUNCTION guard_content_bronze_launch_phase() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE reservation content_bronze_reservations%ROWTYPE; a attempts%ROWTYPE; r runs%ROWTYPE;
    admission jsonb; ready jsonb; launch jsonb; digest text; command jsonb; facts jsonb; previous text;
BEGIN
    IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_ISOLATION_REQUIRED',ERRCODE='23514';
    END IF;
    PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    SELECT * INTO reservation FROM content_bronze_reservations WHERE operation_id=NEW.operation_id;
    IF reservation.operation_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_RESERVATION_REQUIRED',ERRCODE='23514';
    END IF;
    -- Exact current CAS versions are supplied on every phase, not copied from reservation time.
    -- Coordinator and worker heartbeat increments must not invalidate an otherwise live owner.
    PERFORM require_content_fetch_completion_owner(reservation.fetch_operation_id,NEW.owner_coordinator_version,
        NEW.owner_worker_version,NEW.owner_run_version,NEW.owner_attempt_version);
    SELECT * INTO a FROM attempts WHERE attempt_id=reservation.attempt_id;
    SELECT * INTO r FROM runs WHERE run_id=a.run_id;
    IF NEW.owner_coordinator_version<reservation.owner_coordinator_version
       OR NEW.owner_worker_version<reservation.owner_worker_version
       OR NEW.owner_run_version<>reservation.owner_run_version
       OR NEW.owner_attempt_version<>reservation.owner_attempt_version
       OR EXISTS(SELECT 1 FROM content_fetch_recovery_handoffs WHERE operation_id=reservation.fetch_operation_id)
       OR EXISTS(SELECT 1 FROM content_fetch_recovery_finalizations WHERE run_id=a.run_id) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_OWNER_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT convert_from(admission_bytes,'UTF8')::jsonb INTO admission
        FROM content_attempt_admissions WHERE attempt_id=a.attempt_id;
    SELECT convert_from(receipt_bytes,'UTF8')::jsonb INTO ready
        FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='READY';
    previous:=CASE NEW.phase WHEN 'LAUNCH_BOUND' THEN NULL WHEN 'FILES_READY' THEN 'LAUNCH_BOUND'
        WHEN 'START_AUTHORIZED' THEN 'FILES_READY' WHEN 'CHILD_RECORDED' THEN 'START_AUTHORIZED'
        WHEN 'RELEASE_AUTHORIZED' THEN 'CHILD_RECORDED' WHEN 'CANDIDATE_EXITED' THEN 'RELEASE_AUTHORIZED' END;
    IF (previous IS NULL AND EXISTS(SELECT 1 FROM content_bronze_launch_phases WHERE operation_id=NEW.operation_id))
       OR (previous IS NOT NULL AND NOT EXISTS(SELECT 1 FROM content_bronze_launch_phases WHERE operation_id=NEW.operation_id AND phase=previous)) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_PHASE_INVALID',ERRCODE='23514';
    END IF;
    IF NEW.phase='LAUNCH_BOUND' THEN
        SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO launch,digest
            FROM content_bronze_launch_profiles WHERE launch_profile_id=NEW.launch_profile_id;
        command:=convert_from(reservation.command_bytes,'UTF8')::jsonb;
        facts:=convert_from(reservation.reservation_bytes,'UTF8')::jsonb;
        IF launch IS NULL OR NEW.launch_profile_sha256::text IS DISTINCT FROM digest
           OR launch->>'bronzeRuntimeSha256' IS DISTINCT FROM command->>'runtimeSha256'
           OR launch->>'bronzeRuntimeSha256' IS DISTINCT FROM facts->>'bronzeRuntimeSha256'
           OR launch->>'runProfileId' IS DISTINCT FROM r.execution_profile_id::text
           OR launch->>'runProfileSha256' IS DISTINCT FROM admission->>'profileSha256'
           OR launch->>'runProfileSha256' IS DISTINCT FROM facts->>'profileSha256'
           OR launch->>'controlBindingId' IS DISTINCT FROM admission->'controlBinding'->>'bindingId'
           OR launch->>'storageBindingId' IS DISTINCT FROM command->'storage'->>'bindingId'
           OR launch->>'storageBindingSha256' IS DISTINCT FROM command->'storage'->>'bindingSha256'
           OR facts->>'ledgerSha256' IS DISTINCT FROM ready->>'ledgerSha256'
           OR facts->>'controlBindingSha256' IS DISTINCT FROM admission->>'controlBindingSha256'
           OR facts->>'selectionSha256' IS DISTINCT FROM r.selection_sha256::text THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_BINDING_INVALID',ERRCODE='23514';
        END IF;
    END IF;
    -- CANDIDATE_EXITED records evidence, including bounded failure. It cannot complete a
    -- checkpoint, change legacy stage/delivery state or release the coordinator slot.
    RETURN NEW;
END $$;
CREATE TRIGGER content_bronze_launch_phase_insert BEFORE INSERT ON content_bronze_launch_phases
    FOR EACH ROW EXECUTE FUNCTION guard_content_bronze_launch_phase();
