-- Inactive original-live-owner successor reservation. No launch or legacy history conversion.
CREATE TABLE content_bronze_reservations (
    operation_id uuid PRIMARY KEY,
    fetch_operation_id uuid NOT NULL UNIQUE REFERENCES content_fetch_checkpoints(operation_id) ON DELETE RESTRICT,
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
    CHECK(operation_id<>fetch_operation_id)
);
CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON content_bronze_reservations
    FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON content_bronze_reservations
    FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation();

CREATE FUNCTION guard_content_bronze_reservation() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE op content_native_operations%ROWTYPE; fetched content_fetch_checkpoints%ROWTYPE;
    a attempts%ROWTYPE; r runs%ROWTYPE; admission jsonb; ready jsonb; frozen jsonb; selected jsonb;
    profile jsonb; profile_sha text; runtime jsonb; runtime_id text; runtime_sha text; registered_sha text;
    raw bytea; family text; next_family text; fields text[]; content_sha text; bronze jsonb; bronze_sha text; bronze_id text;
    storage jsonb; control jsonb; supplied jsonb; credentials jsonb; command jsonb; reservation jsonb; cp bytea;
BEGIN
    -- Existing helper locks coordinator first and rejects changed ownership, controls, deadline,
    -- non-original/recovery ownership, legacy history, missing READY and incomplete native launch.
    PERFORM require_content_fetch_completion_owner(NEW.fetch_operation_id,NEW.owner_coordinator_version,
        NEW.owner_worker_version,NEW.owner_run_version,NEW.owner_attempt_version);
    SELECT * INTO op FROM content_native_operations WHERE operation_id=NEW.fetch_operation_id;
    SELECT * INTO fetched FROM content_fetch_checkpoints WHERE operation_id=NEW.fetch_operation_id;
    SELECT * INTO a FROM attempts WHERE attempt_id=op.attempt_id;
    SELECT * INTO r FROM runs WHERE run_id=a.run_id;
    IF fetched.operation_id IS NULL OR NEW.attempt_id IS DISTINCT FROM op.attempt_id
       OR EXISTS(SELECT 1 FROM content_fetch_recovery_handoffs WHERE operation_id=NEW.fetch_operation_id)
       OR EXISTS(SELECT 1 FROM content_fetch_recovery_finalizations f JOIN content_fetch_recovery_handoffs h USING(handoff_id)
           WHERE h.operation_id=NEW.fetch_operation_id)
       OR EXISTS(SELECT 1 FROM content_bronze_reservations WHERE attempt_id=NEW.attempt_id) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_FETCH_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT convert_from(admission_bytes,'UTF8')::jsonb INTO admission FROM content_attempt_admissions WHERE attempt_id=a.attempt_id;
    SELECT convert_from(receipt_bytes,'UTF8')::jsonb INTO ready FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='READY';
    frozen:=convert_from(r.captured_selection,'UTF8')::jsonb; selected:=frozen->'sources'->0;
    SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO profile,profile_sha
        FROM execution_profiles WHERE profile_id=r.execution_profile_id;
    IF profile IS NULL OR profile_sha IS DISTINCT FROM admission->>'profileSha256'
       OR r.selection_sha256::text IS DISTINCT FROM admission->>'selectionSha256'
       OR frozen->'executionProfile'->'configuration' IS DISTINCT FROM profile
       OR jsonb_typeof(frozen->'sources') IS DISTINCT FROM 'array' OR jsonb_array_length(frozen->'sources')<>1
       OR selected->>'sourceId' IS DISTINCT FROM admission->>'sourceId'
       OR NOT EXISTS(SELECT 1 FROM run_sources rs JOIN source_revisions sr USING(product_id,source_id,revision)
           WHERE rs.run_id=r.run_id AND rs.source_id=selected->>'sourceId' AND to_jsonb(rs.revision)=selected->'revision'
             AND sr.configuration_sha256::text=selected->>'configurationSha256'
             AND convert_from(sr.configuration_bytes,'UTF8')::jsonb=selected->'configuration') THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_SOURCE_INVALID',ERRCODE='23514';
    END IF;
    -- The original command's registry chain is immutable, but independently traverse it rather
    -- than accepting the caller's Bronze runtime digest as the lookup key.
    runtime_id:=profile->>'contentRuntimeId'; runtime_sha:=admission->>'runtimeSha256';
    FOREACH family IN ARRAY ARRAY['content','gold','silver','bronze','fetch'] LOOP
        SELECT configuration_bytes,configuration_sha256::text INTO raw,registered_sha FROM execution_profiles WHERE profile_id=runtime_id;
        IF raw IS NULL OR registered_sha IS DISTINCT FROM runtime_sha THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        IF family='content' THEN content_sha:=registered_sha; END IF;
        runtime:=convert_from(raw,'UTF8')::jsonb;
        IF NOT native_reservation_json_matches(convert_from(raw,'UTF8')::json,runtime)
           OR runtime->(CASE WHEN family='fetch' THEN 'rssFetchRuntimeVersion' ELSE family||'RuntimeVersion' END) IS DISTINCT FROM '2'::jsonb THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        fields:=CASE family
            WHEN 'content' THEN ARRAY['contentRuntimeVersion','goldRuntimeId','goldRuntimeSha256','referenceBindingId','contentBindingId','modules','seconds']
            WHEN 'gold' THEN ARRAY['goldRuntimeVersion','silverRuntimeId','silverRuntimeSha256','goldBindingId','modules','duckdb','seconds','python']
            WHEN 'silver' THEN ARRAY['silverRuntimeVersion','bronzeRuntimeId','bronzeRuntimeSha256','runProfileSha256','deploymentSha256','python','javaHome','processingRoot','modules','versions','outputBindingId','tempBindingId','seconds']
            WHEN 'bronze' THEN ARRAY['bronzeRuntimeVersion','adapter','fetchRuntimeId','fetchRuntimeSha256','runProfileId','runProfileSha256','deploymentSha256','controlBindingId','storageBindingId','storageBindingSha256']
            ELSE ARRAY['rssFetchRuntimeVersion','pipeline','runProfileId','runProfileSha256','deploymentSha256','javaHome','manifest','controlBindingId','testEndpoint','httpTimeoutMillis'] END;
        IF NOT runtime ?& fields OR runtime-fields<>'{}'::jsonb THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        IF family IN ('silver','bronze','fetch') AND
           (runtime->>'runProfileSha256' IS DISTINCT FROM admission->>'profileSha256'
            OR runtime->>'deploymentSha256' IS DISTINCT FROM profile->'runtime'->>'deploymentSha256') THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        IF family IN ('bronze','fetch') AND runtime->>'runProfileId' IS DISTINCT FROM r.execution_profile_id::text THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
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
       OR runtime->>'controlBindingId' IS DISTINCT FROM bronze->>'controlBindingId' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_RUNTIME_INVALID',ERRCODE='23514';
    END IF;
    SELECT value INTO storage FROM jsonb_array_elements(frozen->'storageBindings')
        WHERE value->>'bindingId'=bronze->>'storageBindingId';
    SELECT value INTO control FROM jsonb_array_elements(frozen->'storageBindings')
        WHERE value->>'bindingId'=bronze->>'controlBindingId';
    IF storage IS NULL OR control IS NULL OR storage->>'role' IS DISTINCT FROM 'BRONZE'
       OR control->>'role' IS DISTINCT FROM 'CONTROL'
       OR storage->>'configurationSha256' IS DISTINCT FROM bronze->>'storageBindingSha256'
       OR control->'configuration' IS DISTINCT FROM admission->'controlBinding'
       OR control->>'configurationSha256' IS DISTINCT FROM admission->>'controlBindingSha256'
       OR NOT EXISTS(SELECT 1 FROM execution_profile_storage ps JOIN storage_bindings sb USING(binding_id)
           WHERE ps.profile_id=bronze_id AND sb.binding_id=storage->>'bindingId'
             AND sb.installation_id=(admission->>'installationId')::uuid AND sb.role='BRONZE'
             AND sb.product::text=storage->>'productId'
             AND sb.configuration_sha256::text=storage->>'configurationSha256'
             AND convert_from(sb.configuration_bytes,'UTF8')::jsonb=storage->'configuration')
       OR NOT EXISTS(SELECT 1 FROM execution_profile_storage ps JOIN storage_bindings sb USING(binding_id)
           WHERE ps.profile_id=bronze_id AND sb.binding_id=control->>'bindingId'
             AND sb.installation_id=(admission->>'installationId')::uuid AND sb.role='CONTROL'
             AND sb.product::text=control->>'productId'
             AND sb.configuration_sha256::text=control->>'configurationSha256'
             AND convert_from(sb.configuration_bytes,'UTF8')::jsonb=control->'configuration') THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_STORAGE_INVALID',ERRCODE='23514';
    END IF;
    cp:=decode(md5('ddip-content-narrow-v1'||chr(10)||a.run_id::text||chr(10)||a.attempt_id::text||chr(10)||'STORE_BRONZE:0'),'hex');
    cp:=set_byte(cp,6,(get_byte(cp,6)&15)|48);cp:=set_byte(cp,8,(get_byte(cp,8)&63)|128);
    IF NEW.checkpoint_id<>encode(cp,'hex')::uuid OR NEW.checkpoint_id=fetched.checkpoint_id THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_CHECKPOINT_INVALID',ERRCODE='23514';
    END IF;
    supplied:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
    credentials:=supplied->'storage'->'credentialsStamp';
    -- OS observations are asserted by the original owner and rechecked by Java outside SQL.
    -- SQL validates their exact bounded shape; no credential bytes are persisted here.
    IF jsonb_typeof(supplied->'storage'->'credentialsSha256') IS DISTINCT FROM 'string'
       OR supplied->'storage'->>'credentialsSha256' !~ '^[0-9a-f]{64}$'
       OR jsonb_typeof(credentials) IS DISTINCT FROM 'object'
       OR NOT credentials ?& ARRAY['identity','modified','size']
       OR credentials-ARRAY['identity','modified','size']<>'{}'::jsonb
       OR jsonb_typeof(credentials->'identity') IS DISTINCT FROM 'string' OR credentials->>'identity' !~ '^[0-9]+:[0-9]+$'
       OR jsonb_typeof(credentials->'modified') IS DISTINCT FROM 'string' OR length(credentials->>'modified') NOT BETWEEN 1 AND 64
       OR jsonb_typeof(credentials->'size') IS DISTINCT FROM 'number' OR credentials->>'size' !~ '^[0-9]+$'
       OR (credentials->>'size')::numeric NOT BETWEEN 1 AND 8192 THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_CREDENTIAL_OBSERVATION_INVALID',ERRCODE='23514';
    END IF;
    command:=jsonb_build_object('protocolVersion',1,'runId',a.run_id::text,'attemptId',a.attempt_id::text,
        'ownerEpoch',a.owner_epoch,'stage','STORE_BRONZE','itemOrdinal',0,'checkpointId',NEW.checkpoint_id::text,
        'mode','PUBLISH','runtimeSha256',bronze_sha,
        'journal',jsonb_build_object('directory',(admission->'controlBinding'->>'root')||'/'||fetched.journal_address::text,
            'commandSha256',op.command_sha256::text,'descriptorSha256',fetched.journal_sha256::text,
            'reference',convert_from(fetched.expected_evidence,'UTF8')::jsonb),
        'storage',jsonb_build_object('bindingId',storage->'bindingId','bindingSha256',storage->'configurationSha256',
            'configuration',storage->'configuration','credentialsSha256',supplied->'storage'->'credentialsSha256','credentialsStamp',credentials));
    reservation:=jsonb_build_object('reservationVersion',1,'mode','STORE_BRONZE_RESERVATION',
        'operationId',NEW.operation_id::text,'fetchOperationId',op.operation_id::text,'fetchCheckpointId',fetched.checkpoint_id::text,
        'fetchCommandSha256',op.command_sha256::text,'fetchJournalSha256',fetched.journal_sha256::text,
        'fetchReferenceSha256',fetched.expected_sha256::text,'fetchCompletionSha256',fetched.completion_sha256::text,
        'policy',admission->'policy','runId',a.run_id::text,'attemptId',a.attempt_id::text,'checkpointId',NEW.checkpoint_id::text,
        'commandSha256',NEW.command_sha256::text,'selectionSha256',r.selection_sha256::text,
        'profileSha256',profile_sha,'contentRuntimeSha256',content_sha,'bronzeRuntimeSha256',bronze_sha,
        'ledgerSha256',ready->'ledgerSha256','controlBindingSha256',admission->'controlBindingSha256');
    IF NOT native_reservation_json_matches(convert_from(NEW.command_bytes,'UTF8')::json,command)
       OR NOT native_reservation_json_matches(convert_from(NEW.reservation_bytes,'UTF8')::json,reservation) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_RESERVATION_INVALID',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_bronze_reservation_insert BEFORE INSERT ON content_bronze_reservations
    FOR EACH ROW EXECUTE FUNCTION guard_content_bronze_reservation();

-- Existing FETCH-only completion and terminal recovery cannot ignore a consumed successor.
-- These additive guards preserve historical readers and all immutable prior migrations.
CREATE FUNCTION guard_fetch_only_without_bronze() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE operation uuid;
BEGIN
    IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_EXCLUSION_ISOLATION_REQUIRED',ERRCODE='23514';
    END IF;
    PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    IF TG_TABLE_NAME='content_fetch_recovery_finalizations' THEN
        SELECT operation_id INTO operation FROM content_fetch_recovery_handoffs WHERE handoff_id=NEW.handoff_id;
    ELSE operation:=NEW.operation_id;
    END IF;
    IF EXISTS(SELECT 1 FROM content_bronze_reservations WHERE fetch_operation_id=operation) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_ONLY_BRONZE_RESERVED',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['content_fetch_completion_intents','content_fetch_checkpoints',
        'content_fetch_recovery_handoffs','content_fetch_recovery_finalizations'] LOOP
        EXECUTE format('CREATE TRIGGER fetch_only_without_bronze BEFORE INSERT ON %I FOR EACH ROW EXECUTE FUNCTION guard_fetch_only_without_bronze()',t);
    END LOOP;
END $$;

-- Recheck both enrollment directions at commit, including multi-step SQL transactions.
-- Do not reuse V16's table-name-specific binding trigger for this new table.
CREATE FUNCTION check_bronze_fetch_recovery_exclusion() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE operation uuid;
BEGIN
    IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_EXCLUSION_ISOLATION_REQUIRED',ERRCODE='23514';
    END IF;
    PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    IF TG_TABLE_NAME='content_bronze_reservations' THEN operation:=NEW.fetch_operation_id;
    ELSIF TG_TABLE_NAME='content_fetch_recovery_handoffs' THEN operation:=NEW.operation_id;
    ELSE
        SELECT operation_id INTO operation FROM content_fetch_recovery_handoffs WHERE handoff_id=NEW.handoff_id;
    END IF;
    IF EXISTS(SELECT 1 FROM content_bronze_reservations b JOIN content_fetch_recovery_handoffs h
        ON h.operation_id=b.fetch_operation_id WHERE b.fetch_operation_id=operation) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_FETCH_RECOVERY_EXCLUDED',ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['content_bronze_reservations','content_fetch_recovery_handoffs','content_fetch_recovery_finalizations'] LOOP
        EXECUTE format('CREATE CONSTRAINT TRIGGER bronze_fetch_recovery_exclusion AFTER INSERT OR UPDATE ON %I DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_bronze_fetch_recovery_exclusion()',t);
    END LOOP;
END $$;
