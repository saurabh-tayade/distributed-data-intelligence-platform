-- Additive inactive original-owner verification and Bronze checkpoint evidence only.
-- No legacy lifecycle, downstream authority, recovery, release or success transition.
CREATE TABLE content_bronze_verification_profiles (
    verification_profile_id varchar(128) PRIMARY KEY,
    configuration_bytes bytea NOT NULL CHECK(valid_object_bytes(configuration_bytes,65536)),
    configuration_sha256 sha256_hex NOT NULL CHECK(configuration_sha256=encode(sha256(configuration_bytes),'hex')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE content_bronze_verification_phases (
    operation_id uuid NOT NULL REFERENCES content_bronze_reservations ON DELETE RESTRICT,
    phase varchar(32) NOT NULL CHECK(phase IN ('VERIFY_BOUND','START_AUTHORIZED','CHILD_RECORDED','RELEASE_AUTHORIZED','OBSERVED')),
    verification_profile_id varchar(128) REFERENCES content_bronze_verification_profiles ON DELETE RESTRICT,
    verification_profile_sha256 sha256_hex,
    candidate_sha256 sha256_hex,
    child_instance_id uuid,
    child_pid bigint CHECK(child_pid>0),
    child_started_at timestamptz,
    bootstrap_sha256 sha256_hex,
    receipt_bytes bytea CHECK(valid_object_bytes(receipt_bytes,65536)),
    receipt_sha256 sha256_hex CHECK(receipt_sha256=encode(sha256(receipt_bytes),'hex')),
    verified_bytes bytea CHECK(valid_object_bytes(verified_bytes,65536)),
    verified_sha256 sha256_hex CHECK(verified_sha256=encode(sha256(verified_bytes),'hex')),
    owner_coordinator_version bigint NOT NULL CHECK(owner_coordinator_version>=0),
    owner_worker_version bigint NOT NULL CHECK(owner_worker_version>=0),
    owner_run_version bigint NOT NULL CHECK(owner_run_version>=0),
    owner_attempt_version bigint NOT NULL CHECK(owner_attempt_version>=0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(operation_id,phase),
    CHECK((phase='VERIFY_BOUND')=(verification_profile_id IS NOT NULL)),
    CHECK((phase='VERIFY_BOUND')=(verification_profile_sha256 IS NOT NULL)),
    CHECK((phase='VERIFY_BOUND')=(candidate_sha256 IS NOT NULL)),
    CHECK((phase='CHILD_RECORDED')=(child_instance_id IS NOT NULL)),
    CHECK((phase='CHILD_RECORDED')=(child_pid IS NOT NULL)),
    CHECK((phase='CHILD_RECORDED')=(child_started_at IS NOT NULL)),
    CHECK((phase='RELEASE_AUTHORIZED')=(bootstrap_sha256 IS NOT NULL)),
    CHECK((phase='OBSERVED')=(receipt_bytes IS NOT NULL)),
    CHECK((phase='OBSERVED')=(receipt_sha256 IS NOT NULL)),
    CHECK((phase='OBSERVED')=(verified_bytes IS NOT NULL)),
    CHECK((phase='OBSERVED')=(verified_sha256 IS NOT NULL))
);
CREATE TABLE content_bronze_checkpoints (
    operation_id uuid PRIMARY KEY REFERENCES content_bronze_reservations ON DELETE RESTRICT,
    checkpoint_id uuid NOT NULL UNIQUE,
    fetch_operation_id uuid NOT NULL UNIQUE REFERENCES content_fetch_checkpoints(operation_id) ON DELETE RESTRICT,
    candidate_sha256 sha256_hex NOT NULL,
    receipt_bytes bytea NOT NULL CHECK(valid_object_bytes(receipt_bytes,65536)),
    receipt_sha256 sha256_hex NOT NULL CHECK(receipt_sha256=encode(sha256(receipt_bytes),'hex')),
    verified_sha256 sha256_hex NOT NULL,
    verified_stamp_bytes bytea NOT NULL CHECK(valid_object_bytes(verified_stamp_bytes,65536)),
    verified_stamp_sha256 sha256_hex NOT NULL CHECK(verified_stamp_sha256=encode(sha256(verified_stamp_bytes),'hex')),
    owner_coordinator_version bigint NOT NULL CHECK(owner_coordinator_version>=0),
    owner_worker_version bigint NOT NULL CHECK(owner_worker_version>=0),
    owner_run_version bigint NOT NULL CHECK(owner_run_version>=0),
    owner_attempt_version bigint NOT NULL CHECK(owner_attempt_version>=0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['content_bronze_verification_profiles','content_bronze_verification_phases','content_bronze_checkpoints'] LOOP
        EXECUTE format('CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation()',t);
        EXECUTE format('CREATE TRIGGER no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation()',t);
    END LOOP;
END $$;

CREATE FUNCTION guard_content_bronze_verification_profile() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE n jsonb; bronze jsonb; fetched jsonb; raw bytea; digest text; fetch_raw bytea;
    fetch_digest text; expected jsonb; manifest jsonb; entry jsonb;
BEGIN
    n:=convert_from(NEW.configuration_bytes,'UTF8')::jsonb;
    SELECT configuration_bytes,configuration_sha256::text INTO raw,digest
        FROM execution_profiles WHERE profile_id=n->>'bronzeRuntimeId';
    IF raw IS NULL THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_PROFILE_INVALID',ERRCODE='23514';
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
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    IF jsonb_array_length(fetched->'manifest'->'classpath') NOT BETWEEN 1 AND 128 THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    FOR entry IN SELECT value FROM jsonb_array_elements(fetched->'manifest'->'classpath') LOOP
        IF jsonb_typeof(entry) IS DISTINCT FROM 'object' OR NOT entry ?& ARRAY['path','sha256']
           OR entry-ARRAY['path','sha256']<>'{}'::jsonb OR jsonb_typeof(entry->'path') IS DISTINCT FROM 'string'
           OR jsonb_typeof(entry->'sha256') IS DISTINCT FROM 'string' OR entry->>'sha256' !~ '^[0-9a-f]{64}$' THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_PROFILE_INVALID',ERRCODE='23514';
        END IF;
    END LOOP;
    manifest:=jsonb_build_object('adapter','platform.ingestion.managed.StrictBronzeVerifier',
        'javaRuntime',fetched->'manifest'->'javaRuntime','classpath',fetched->'manifest'->'classpath');
    expected:=jsonb_build_object('strictBronzeVerificationVersion',1,'bronzeRuntimeId',n->'bronzeRuntimeId',
        'bronzeRuntimeSha256',digest,'runProfileId',bronze->'runProfileId','runProfileSha256',bronze->'runProfileSha256',
        'controlBindingId',bronze->'controlBindingId','storageBindingId',bronze->'storageBindingId',
        'storageBindingSha256',bronze->'storageBindingSha256','javaHome',fetched->'javaHome','manifest',manifest,
        'deploymentSha256',n->'deploymentSha256','strictRuntimeSha256',n->'strictRuntimeSha256','testEndpoint',true);
    -- Java also checks canonical bytes, manifest digest and the exact strict protocol digest.
    -- strictRuntimeSha256 is deliberately separate from deploymentSha256.
    IF jsonb_typeof(n->'bronzeRuntimeId') IS DISTINCT FROM 'string'
       OR NOT native_reservation_json_matches(convert_from(NEW.configuration_bytes,'UTF8')::json,expected) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_bronze_verification_profile_insert BEFORE INSERT ON content_bronze_verification_profiles
    FOR EACH ROW EXECUTE FUNCTION guard_content_bronze_verification_profile();

CREATE FUNCTION require_content_bronze_checkpoint_owner(operation uuid,cv bigint,wv bigint,rv bigint,av bigint) RETURNS void
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE reservation content_bronze_reservations%ROWTYPE;
BEGIN
    IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_CHECKPOINT_ISOLATION_REQUIRED',ERRCODE='23514';
    END IF;
    PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    SELECT * INTO reservation FROM content_bronze_reservations WHERE operation_id=operation;
    IF reservation.operation_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_CHECKPOINT_RESERVATION_REQUIRED',ERRCODE='23514';
    END IF;
    PERFORM require_content_fetch_completion_owner(reservation.fetch_operation_id,cv,wv,rv,av);
    IF cv<reservation.owner_coordinator_version OR wv<reservation.owner_worker_version
       OR rv<>reservation.owner_run_version OR av<>reservation.owner_attempt_version
       OR EXISTS(SELECT 1 FROM content_fetch_recovery_handoffs WHERE operation_id=reservation.fetch_operation_id)
       OR EXISTS(SELECT 1 FROM content_fetch_recovery_finalizations f JOIN attempts a USING(run_id) WHERE a.attempt_id=reservation.attempt_id) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_CHECKPOINT_OWNER_REQUIRED',ERRCODE='23514';
    END IF;
END $$;

CREATE FUNCTION guard_content_bronze_verification_phase() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE reservation content_bronze_reservations%ROWTYPE; bound content_bronze_verification_phases%ROWTYPE;
    candidate content_bronze_launch_phases%ROWTYPE; fetched content_fetch_checkpoints%ROWTYPE;
    profile jsonb; launched jsonb; command jsonb; descriptor jsonb; receipt jsonb; expected jsonb; anchor jsonb;
    digest text; previous text; k text;
BEGIN
    PERFORM require_content_bronze_checkpoint_owner(NEW.operation_id,NEW.owner_coordinator_version,
        NEW.owner_worker_version,NEW.owner_run_version,NEW.owner_attempt_version);
    SELECT * INTO reservation FROM content_bronze_reservations WHERE operation_id=NEW.operation_id;
    SELECT * INTO candidate FROM content_bronze_launch_phases WHERE operation_id=NEW.operation_id AND phase='CANDIDATE_EXITED';
    IF candidate.operation_id IS NULL OR candidate.storage_state<>'EXACT' OR candidate.exit_code<>0 THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_CANDIDATE_REQUIRED',ERRCODE='23514';
    END IF;
    previous:=CASE NEW.phase WHEN 'VERIFY_BOUND' THEN NULL WHEN 'START_AUTHORIZED' THEN 'VERIFY_BOUND'
        WHEN 'CHILD_RECORDED' THEN 'START_AUTHORIZED' WHEN 'RELEASE_AUTHORIZED' THEN 'CHILD_RECORDED' WHEN 'OBSERVED' THEN 'RELEASE_AUTHORIZED' END;
    IF (previous IS NULL AND EXISTS(SELECT 1 FROM content_bronze_verification_phases WHERE operation_id=NEW.operation_id))
       OR (previous IS NOT NULL AND NOT EXISTS(SELECT 1 FROM content_bronze_verification_phases WHERE operation_id=NEW.operation_id AND phase=previous)) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_PHASE_INVALID',ERRCODE='23514';
    END IF;
    IF NEW.phase='VERIFY_BOUND' THEN
        SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO profile,digest
            FROM content_bronze_verification_profiles WHERE verification_profile_id=NEW.verification_profile_id;
        SELECT convert_from(p.configuration_bytes,'UTF8')::jsonb INTO launched FROM content_bronze_launch_profiles p
            JOIN content_bronze_launch_phases l USING(launch_profile_id) WHERE l.operation_id=NEW.operation_id AND l.phase='LAUNCH_BOUND';
        IF profile IS NULL OR launched IS NULL OR NEW.verification_profile_sha256::text IS DISTINCT FROM digest
           OR NEW.candidate_sha256 IS DISTINCT FROM candidate.candidate_sha256 THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_BINDING_INVALID',ERRCODE='23514';
        END IF;
        FOREACH k IN ARRAY ARRAY['bronzeRuntimeId','bronzeRuntimeSha256','runProfileId','runProfileSha256','controlBindingId','storageBindingId','storageBindingSha256','javaHome','testEndpoint'] LOOP
            IF profile->k IS DISTINCT FROM launched->k THEN
                RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_BINDING_INVALID',ERRCODE='23514';
            END IF;
        END LOOP;
    ELSIF NEW.phase='OBSERVED' THEN
        SELECT * INTO bound FROM content_bronze_verification_phases WHERE operation_id=NEW.operation_id AND phase='VERIFY_BOUND';
        SELECT * INTO fetched FROM content_fetch_checkpoints WHERE operation_id=reservation.fetch_operation_id;
        command:=convert_from(reservation.command_bytes,'UTF8')::jsonb;
        descriptor:=convert_from(fetched.journal_descriptor,'UTF8')::jsonb;
        receipt:=convert_from(NEW.receipt_bytes,'UTF8')::jsonb;
        expected:=jsonb_build_object('receiptVersion',1,'journalSha256',fetched.journal_sha256::text,
            'storageBindingId',command->'storage'->'bindingId','storageBindingSha256',command->'storage'->'bindingSha256',
            'bucket',command->'storage'->'configuration'->'bucket','expected',descriptor->'expected',
            'payloadContentType',receipt->'payloadContentType','metadataContentType','application/json');
        FOREACH k IN ARRAY ARRAY['runId','attemptId','sourceId','revision','capturedConfigSha256','ingestionId','ingestedAt','payloadByteLength','payloadSha256'] LOOP
            expected:=expected||jsonb_build_object(k,descriptor->k);
        END LOOP;
        IF receipt->'payloadContentType' IS DISTINCT FROM '"application/xml"'::jsonb
           AND receipt->'payloadContentType' IS DISTINCT FROM '"application/octet-stream"'::jsonb THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_RECEIPT_INVALID',ERRCODE='23514';
        END IF;
        anchor:=jsonb_build_object('bronzeCheckpointVersion',1,'operationId',NEW.operation_id::text,
            'fetchOperationId',reservation.fetch_operation_id::text,'checkpointId',reservation.checkpoint_id::text,
            'candidateSha256',bound.candidate_sha256::text,'verificationProfileSha256',bound.verification_profile_sha256::text,
            'receiptSha256',NEW.receipt_sha256::text,'receipt',expected);
        IF NOT native_reservation_json_matches(convert_from(NEW.receipt_bytes,'UTF8')::json,expected)
           OR NOT native_reservation_json_matches(convert_from(NEW.verified_bytes,'UTF8')::json,anchor) THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_RECEIPT_INVALID',ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_bronze_verification_phase_insert BEFORE INSERT ON content_bronze_verification_phases
    FOR EACH ROW EXECUTE FUNCTION guard_content_bronze_verification_phase();

CREATE FUNCTION guard_content_bronze_checkpoint() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE reservation content_bronze_reservations%ROWTYPE; observed content_bronze_verification_phases%ROWTYPE;
    bound content_bronze_verification_phases%ROWTYPE; stamp jsonb;
BEGIN
    PERFORM require_content_bronze_checkpoint_owner(NEW.operation_id,NEW.owner_coordinator_version,
        NEW.owner_worker_version,NEW.owner_run_version,NEW.owner_attempt_version);
    SELECT * INTO reservation FROM content_bronze_reservations WHERE operation_id=NEW.operation_id;
    SELECT * INTO observed FROM content_bronze_verification_phases WHERE operation_id=NEW.operation_id AND phase='OBSERVED';
    SELECT * INTO bound FROM content_bronze_verification_phases WHERE operation_id=NEW.operation_id AND phase='VERIFY_BOUND';
    stamp:=convert_from(NEW.verified_stamp_bytes,'UTF8')::jsonb;
    IF observed.operation_id IS NULL OR NEW.checkpoint_id IS DISTINCT FROM reservation.checkpoint_id
       OR NEW.fetch_operation_id IS DISTINCT FROM reservation.fetch_operation_id
       OR NEW.candidate_sha256 IS DISTINCT FROM bound.candidate_sha256
       OR NEW.receipt_bytes IS DISTINCT FROM observed.receipt_bytes OR NEW.receipt_sha256 IS DISTINCT FROM observed.receipt_sha256
       OR NEW.verified_sha256 IS DISTINCT FROM observed.verified_sha256
       OR NOT native_reservation_json_matches(convert_from(NEW.verified_stamp_bytes,'UTF8')::json,stamp)
       OR NOT stamp ?& ARRAY['identity','modified','size'] OR stamp-ARRAY['identity','modified','size']<>'{}'::jsonb
       OR jsonb_typeof(stamp->'identity') IS DISTINCT FROM 'string' OR stamp->>'identity' !~ '^[0-9]+:[0-9]+$'
       OR jsonb_typeof(stamp->'modified') IS DISTINCT FROM 'string' OR length(stamp->>'modified') NOT BETWEEN 1 AND 64
       OR stamp->'size' IS DISTINCT FROM to_jsonb(octet_length(observed.verified_bytes))
       OR stamp->>'size' IS DISTINCT FROM octet_length(observed.verified_bytes)::text THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_CHECKPOINT_INVALID',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_bronze_checkpoint_insert BEFORE INSERT ON content_bronze_checkpoints
    FOR EACH ROW EXECUTE FUNCTION guard_content_bronze_checkpoint();
