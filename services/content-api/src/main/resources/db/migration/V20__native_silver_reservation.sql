-- Inactive original-live-owner Silver reservation only. No launch, inspection or legacy conversion.
CREATE TABLE content_silver_reservations (
    operation_id uuid PRIMARY KEY,
    bronze_operation_id uuid NOT NULL UNIQUE REFERENCES content_bronze_checkpoints(operation_id) ON DELETE RESTRICT,
    attempt_id uuid NOT NULL UNIQUE REFERENCES content_attempt_admissions ON DELETE RESTRICT,
    checkpoint_id uuid NOT NULL UNIQUE,
    command_bytes bytea NOT NULL CHECK(valid_object_bytes(command_bytes,65536)),
    command_sha256 sha256_hex NOT NULL CHECK(command_sha256=encode(sha256(command_bytes),'hex')),
    reservation_bytes bytea NOT NULL CHECK(valid_object_bytes(reservation_bytes,65535)),
    reservation_sha256 sha256_hex NOT NULL CHECK(reservation_sha256=encode(sha256(reservation_bytes),'hex')),
    owner_coordinator_version bigint NOT NULL CHECK(owner_coordinator_version>=0),
    owner_worker_version bigint NOT NULL CHECK(owner_worker_version>=0),
    owner_run_version bigint NOT NULL CHECK(owner_run_version>=0),
    owner_attempt_version bigint NOT NULL CHECK(owner_attempt_version>=0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK(operation_id<>bronze_operation_id)
);
CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON content_silver_reservations
    FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON content_silver_reservations
    FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation();

CREATE FUNCTION guard_content_silver_reservation() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE op content_native_operations%ROWTYPE; fetched content_fetch_checkpoints%ROWTYPE;
    bronze_reservation content_bronze_reservations%ROWTYPE; checked content_bronze_checkpoints%ROWTYPE;
    observed content_bronze_verification_phases%ROWTYPE; bound content_bronze_verification_phases%ROWTYPE;
    a attempts%ROWTYPE; r runs%ROWTYPE; admission jsonb; ready jsonb; frozen jsonb; selected jsonb;
    profile jsonb; profile_sha text; runtime jsonb; runtime_id text; runtime_sha text; registered_sha text;
    raw bytea; family text; next_family text; fields text[]; content_sha text;
    bronze jsonb; bronze_sha text; bronze_id text; silver jsonb; silver_sha text; silver_id text;
    storage jsonb; control jsonb; temp jsonb; binding jsonb; role_name text; selected_binding_id text;
    bronze_command jsonb; descriptor jsonb; command jsonb; reservation jsonb; cp bytea; inspection_id uuid;
BEGIN
    -- This helper enforces READ COMMITTED and locks coordinator first, before all lookups.
    -- It also rejects stale/recovery ownership, cancellation, expiry, missing READY and legacy history.
    PERFORM require_content_bronze_checkpoint_owner(NEW.bronze_operation_id,NEW.owner_coordinator_version,
        NEW.owner_worker_version,NEW.owner_run_version,NEW.owner_attempt_version);
    SELECT * INTO bronze_reservation FROM content_bronze_reservations WHERE operation_id=NEW.bronze_operation_id;
    SELECT * INTO checked FROM content_bronze_checkpoints WHERE operation_id=NEW.bronze_operation_id;
    SELECT * INTO observed FROM content_bronze_verification_phases WHERE operation_id=NEW.bronze_operation_id AND phase='OBSERVED';
    SELECT * INTO bound FROM content_bronze_verification_phases WHERE operation_id=NEW.bronze_operation_id AND phase='VERIFY_BOUND';
    SELECT * INTO op FROM content_native_operations WHERE operation_id=bronze_reservation.fetch_operation_id;
    SELECT * INTO fetched FROM content_fetch_checkpoints WHERE operation_id=bronze_reservation.fetch_operation_id;
    SELECT * INTO a FROM attempts WHERE attempt_id=op.attempt_id;
    SELECT * INTO r FROM runs WHERE run_id=a.run_id;
    IF checked.operation_id IS NULL OR observed.operation_id IS NULL OR bound.operation_id IS NULL
       OR fetched.operation_id IS NULL OR op.operation_id IS NULL
       OR NEW.attempt_id IS DISTINCT FROM bronze_reservation.attempt_id OR NEW.attempt_id IS DISTINCT FROM op.attempt_id
       OR NEW.operation_id IN (bronze_reservation.operation_id,op.operation_id)
       OR checked.fetch_operation_id IS DISTINCT FROM op.operation_id
       OR checked.checkpoint_id IS DISTINCT FROM bronze_reservation.checkpoint_id
       OR checked.receipt_bytes IS DISTINCT FROM observed.receipt_bytes
       OR checked.receipt_sha256 IS DISTINCT FROM observed.receipt_sha256
       OR checked.verified_sha256 IS DISTINCT FROM observed.verified_sha256
       OR checked.candidate_sha256 IS DISTINCT FROM bound.candidate_sha256
       OR NEW.owner_coordinator_version<checked.owner_coordinator_version
       OR NEW.owner_worker_version<checked.owner_worker_version
       OR NEW.owner_run_version<>checked.owner_run_version
       OR NEW.owner_attempt_version<>checked.owner_attempt_version
       OR EXISTS(SELECT 1 FROM content_silver_reservations WHERE attempt_id=NEW.attempt_id) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_SILVER_BRONZE_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT convert_from(admission_bytes,'UTF8')::jsonb INTO admission FROM content_attempt_admissions WHERE attempt_id=a.attempt_id;
    SELECT convert_from(receipt_bytes,'UTF8')::jsonb INTO ready FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='READY';
    frozen:=convert_from(r.captured_selection,'UTF8')::jsonb; selected:=frozen->'sources'->0;
    SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO profile,profile_sha
        FROM execution_profiles WHERE profile_id=r.execution_profile_id;
    IF profile IS NULL OR profile_sha IS DISTINCT FROM admission->>'profileSha256'
       OR r.selection_sha256::text IS DISTINCT FROM admission->>'selectionSha256'
       OR frozen->'executionProfile'->>'profileId' IS DISTINCT FROM r.execution_profile_id::text
       OR frozen->'executionProfile'->>'configurationSha256' IS DISTINCT FROM profile_sha
       OR profile->'runProfileVersion' IS DISTINCT FROM '2'::jsonb
       OR profile->>'runProfileVersion' IS DISTINCT FROM '2'
       OR frozen->'executionProfile'->'configuration' IS DISTINCT FROM profile
       OR jsonb_typeof(frozen->'sources') IS DISTINCT FROM 'array' OR jsonb_array_length(frozen->'sources')<>1
       OR selected->>'sourceId' IS DISTINCT FROM admission->>'sourceId'
       OR NOT EXISTS(SELECT 1 FROM run_sources rs JOIN source_revisions sr USING(product_id,source_id,revision)
           WHERE rs.run_id=r.run_id AND rs.source_id=selected->>'sourceId' AND to_jsonb(rs.revision)=selected->'revision'
             AND sr.configuration_sha256::text=selected->>'configurationSha256'
             AND convert_from(sr.configuration_bytes,'UTF8')::jsonb=selected->'configuration') THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_SILVER_SOURCE_INVALID',ERRCODE='23514';
    END IF;
    -- The original command's registry chain is immutable, but independently traverse it rather
    -- than accepting any caller-supplied runtime digest as the lookup key.
    runtime_id:=profile->>'contentRuntimeId'; runtime_sha:=admission->>'runtimeSha256';
    FOREACH family IN ARRAY ARRAY['content','gold','silver','bronze','fetch'] LOOP
        SELECT configuration_bytes,configuration_sha256::text INTO raw,registered_sha FROM execution_profiles WHERE profile_id=runtime_id;
        IF raw IS NULL OR registered_sha IS DISTINCT FROM runtime_sha THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_SILVER_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        IF family='content' THEN content_sha:=registered_sha; END IF;
        runtime:=convert_from(raw,'UTF8')::jsonb;
        IF NOT native_reservation_json_matches(convert_from(raw,'UTF8')::json,runtime)
           OR runtime->(CASE WHEN family='fetch' THEN 'rssFetchRuntimeVersion' ELSE family||'RuntimeVersion' END) IS DISTINCT FROM '2'::jsonb
           OR runtime->>(CASE WHEN family='fetch' THEN 'rssFetchRuntimeVersion' ELSE family||'RuntimeVersion' END) IS DISTINCT FROM '2' THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_SILVER_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        fields:=CASE family
            WHEN 'content' THEN ARRAY['contentRuntimeVersion','goldRuntimeId','goldRuntimeSha256','referenceBindingId','contentBindingId','modules','seconds']
            WHEN 'gold' THEN ARRAY['goldRuntimeVersion','silverRuntimeId','silverRuntimeSha256','goldBindingId','modules','duckdb','seconds','python']
            WHEN 'silver' THEN ARRAY['silverRuntimeVersion','bronzeRuntimeId','bronzeRuntimeSha256','runProfileSha256','deploymentSha256','python','javaHome','processingRoot','modules','versions','outputBindingId','tempBindingId','seconds']
            WHEN 'bronze' THEN ARRAY['bronzeRuntimeVersion','adapter','fetchRuntimeId','fetchRuntimeSha256','runProfileId','runProfileSha256','deploymentSha256','controlBindingId','storageBindingId','storageBindingSha256']
            ELSE ARRAY['rssFetchRuntimeVersion','pipeline','runProfileId','runProfileSha256','deploymentSha256','javaHome','manifest','controlBindingId','testEndpoint','httpTimeoutMillis'] END;
        IF NOT runtime ?& fields OR runtime-fields<>'{}'::jsonb THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_SILVER_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        IF family IN ('silver','bronze','fetch') AND
           (runtime->>'runProfileSha256' IS DISTINCT FROM admission->>'profileSha256'
            OR runtime->>'deploymentSha256' IS DISTINCT FROM profile->'runtime'->>'deploymentSha256') THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_SILVER_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        IF family IN ('bronze','fetch') AND runtime->>'runProfileId' IS DISTINCT FROM r.execution_profile_id::text THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_SILVER_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        IF family='silver' THEN silver:=runtime;silver_sha:=registered_sha;silver_id:=runtime_id; END IF;
        IF family='bronze' THEN bronze:=runtime;bronze_sha:=registered_sha;bronze_id:=runtime_id; END IF;
        IF family<>'fetch' THEN
            next_family:=CASE family WHEN 'content' THEN 'gold' WHEN 'gold' THEN 'silver' WHEN 'silver' THEN 'bronze' ELSE 'fetch' END;
            runtime_id:=runtime->>(next_family||'RuntimeId');runtime_sha:=runtime->>(next_family||'RuntimeSha256');
        END IF;
    END LOOP;
    IF bronze->>'adapter' IS DISTINCT FROM 'platform.ingestion.managed.BoundedManagedBronze'
       OR bronze->>'controlBindingId' IS DISTINCT FROM admission->'controlBinding'->>'bindingId'
       OR bronze->>'fetchRuntimeSha256' IS DISTINCT FROM convert_from(op.command_bytes,'UTF8')::jsonb->>'runtimeSha256'
       OR bronze->>'storageBindingId' IS DISTINCT FROM selected->'configuration'->>'inputStorageProfileId'
       OR runtime->>'controlBindingId' IS DISTINCT FROM bronze->>'controlBindingId'
       OR runtime->>'pipeline' IS DISTINCT FROM 'CONTENT_RSS_V1'
       OR runtime->'manifest'->>'adapter' IS DISTINCT FROM 'platform.ingestion.managed.BoundedManagedRssFetch' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_SILVER_RUNTIME_INVALID',ERRCODE='23514';
    END IF;
    -- Independently resolve all three frozen Silver bindings against immutable registrations
    -- and Silver membership. The entire frozen objects, not caller-chosen subsets, are retained.
    FOREACH role_name IN ARRAY ARRAY['SILVER','TEMP','CONTROL'] LOOP
        selected_binding_id:=CASE role_name WHEN 'SILVER' THEN silver->>'outputBindingId'
            WHEN 'TEMP' THEN silver->>'tempBindingId' ELSE bronze->>'controlBindingId' END;
        SELECT value INTO binding FROM jsonb_array_elements(frozen->'storageBindings') WHERE value->>'bindingId'=selected_binding_id;
        IF binding IS NULL OR binding->>'role' IS DISTINCT FROM role_name
           OR (SELECT count(*) FROM jsonb_array_elements(frozen->'storageBindings') WHERE value->>'bindingId'=selected_binding_id)<>1
           OR NOT EXISTS(SELECT 1 FROM execution_profile_storage ps JOIN storage_bindings sb USING(binding_id)
               WHERE ps.profile_id=silver_id AND sb.binding_id=selected_binding_id
                 AND sb.installation_id=(admission->>'installationId')::uuid AND sb.role::text=role_name
                 AND sb.product::text=binding->>'productId'
                 AND sb.configuration_sha256::text=binding->>'configurationSha256'
                 AND native_reservation_json_matches(convert_from(sb.configuration_bytes,'UTF8')::json,binding->'configuration')) THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_SILVER_STORAGE_INVALID',ERRCODE='23514';
        END IF;
        IF role_name='SILVER' THEN storage:=binding;
        ELSIF role_name='TEMP' THEN temp:=binding;
        ELSE control:=binding;
        END IF;
    END LOOP;
    IF control->'configuration' IS DISTINCT FROM admission->'controlBinding'
       OR control->>'configurationSha256' IS DISTINCT FROM admission->>'controlBindingSha256' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_SILVER_STORAGE_INVALID',ERRCODE='23514';
    END IF;
    cp:=decode(md5('ddip-content-narrow-v1'||chr(10)||a.run_id::text||chr(10)||a.attempt_id::text||chr(10)||'SILVER:0'),'hex');
    cp:=set_byte(cp,6,(get_byte(cp,6)&15)|48);cp:=set_byte(cp,8,(get_byte(cp,8)&63)|128);
    IF NEW.checkpoint_id<>encode(cp,'hex')::uuid OR NEW.checkpoint_id IN (checked.checkpoint_id,fetched.checkpoint_id) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_SILVER_CHECKPOINT_INVALID',ERRCODE='23514';
    END IF;
    cp:=decode(md5('ddip-content-narrow-v1'||chr(10)||a.run_id::text||chr(10)||a.attempt_id::text||chr(10)||'INSPECT_BRONZE:0'),'hex');
    cp:=set_byte(cp,6,(get_byte(cp,6)&15)|48);cp:=set_byte(cp,8,(get_byte(cp,8)&63)|128);
    inspection_id:=encode(cp,'hex')::uuid;
    bronze_command:=convert_from(bronze_reservation.command_bytes,'UTF8')::jsonb;
    descriptor:=convert_from(fetched.journal_descriptor,'UTF8')::jsonb;
    -- Credential and verified stamps are retained evidence, never new SQL filesystem observations.
    command:=jsonb_build_object('silverReservationCommandVersion',1,'mode','SILVER_RESERVATION',
        'runId',a.run_id::text,'attemptId',a.attempt_id::text,'ownerEpoch',a.owner_epoch,
        'checkpointId',NEW.checkpoint_id::text,'nativeLaunchCompatible',false,
        'requiresInspection',jsonb_build_object('stage','INSPECT_BRONZE','itemOrdinal',0,'checkpointId',inspection_id::text),
        'bronze',jsonb_build_object('operationId',checked.operation_id::text,'checkpointId',checked.checkpoint_id::text,
            'commandSha256',bronze_reservation.command_sha256::text,'reservationSha256',bronze_reservation.reservation_sha256::text,
            'candidateSha256',checked.candidate_sha256::text,'receiptSha256',checked.receipt_sha256::text,
            'receipt',convert_from(checked.receipt_bytes,'UTF8')::jsonb,'verifiedSha256',checked.verified_sha256::text,
            'verified',convert_from(observed.verified_bytes,'UTF8')::jsonb,
            'verifiedStamp',convert_from(checked.verified_stamp_bytes,'UTF8')::jsonb,
            'verifiedStampSha256',checked.verified_stamp_sha256::text,'verificationProfileSha256',bound.verification_profile_sha256::text),
        'fetch',jsonb_build_object('operationId',op.operation_id::text,'checkpointId',fetched.checkpoint_id::text,
            'commandSha256',op.command_sha256::text,'descriptorSha256',fetched.journal_sha256::text,
            'referenceSha256',fetched.expected_sha256::text,'completionSha256',fetched.completion_sha256::text,
            'ingestionId',descriptor->'ingestionId','ingestedAt',descriptor->'ingestedAt'),
        'runtime',jsonb_build_object('silverRuntimeId',silver_id,'silverRuntimeSha256',silver_sha,
            'bronzeRuntimeId',bronze_id,'bronzeRuntimeSha256',bronze_sha,
            'fetchRuntimeId',runtime_id,'fetchRuntimeSha256',registered_sha,'registeredLedgerVersion',1),
        'bindings',jsonb_build_object('silver',storage,'temp',temp,'control',control),
        'credential',jsonb_build_object('sha256',bronze_command->'storage'->'credentialsSha256',
            'stamp',bronze_command->'storage'->'credentialsStamp'));
    reservation:=jsonb_build_object('reservationVersion',1,'mode','SILVER_RESERVATION',
        'operationId',NEW.operation_id::text,'bronzeOperationId',checked.operation_id::text,
        'bronzeCheckpointId',checked.checkpoint_id::text,'fetchOperationId',op.operation_id::text,
        'checkpointId',NEW.checkpoint_id::text,'commandSha256',NEW.command_sha256::text,
        'policy',admission->'policy','runId',a.run_id::text,'attemptId',a.attempt_id::text,
        'selectionSha256',r.selection_sha256::text,'profileSha256',admission->'profileSha256',
        'contentRuntimeSha256',admission->'runtimeSha256','silverRuntimeSha256',silver_sha,
        'ledgerSha256',ready->'ledgerSha256','controlBindingSha256',admission->'controlBindingSha256');
    IF admission IS NULL OR ready IS NULL
       OR NOT native_reservation_json_matches(convert_from(NEW.command_bytes,'UTF8')::json,command)
       OR NOT native_reservation_json_matches(convert_from(NEW.reservation_bytes,'UTF8')::json,reservation) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_SILVER_RESERVATION_INVALID',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_silver_reservation_insert BEFORE INSERT ON content_silver_reservations
    FOR EACH ROW EXECUTE FUNCTION guard_content_silver_reservation();

COMMENT ON TABLE content_silver_reservations IS
    'Immutable inactive Silver reservation. Does not satisfy INSPECT_BRONZE, authorize launch, release ownership or claim completion. Historical SILVER v2 remains ledger-v1 and is not native-launch-compatible.';
COMMENT ON COLUMN content_silver_reservations.command_bytes IS
    'Exact dependency-bound reservation only. Credential and verified-file stamps are retained evidence, not a credential read or filesystem observation by SQL.';
