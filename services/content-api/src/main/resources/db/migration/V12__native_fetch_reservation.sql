-- Reservation facts only. No launch, phase, completion, or stage-history authority.
CREATE TABLE content_native_operations (
    operation_id uuid PRIMARY KEY,
    attempt_id uuid NOT NULL REFERENCES content_attempt_admissions ON DELETE RESTRICT,
    stage varchar(32) NOT NULL CHECK(stage='FETCH_SOURCE'),
    item_ordinal integer NOT NULL CHECK(item_ordinal=0),
    checkpoint_id uuid NOT NULL,
    command_bytes bytea NOT NULL CHECK(valid_object_bytes(command_bytes,65536)),
    command_sha256 sha256_hex NOT NULL CHECK(command_sha256=encode(sha256(command_bytes),'hex')),
    bootstrap_bytes bytea NOT NULL CHECK(valid_object_bytes(bootstrap_bytes,65535)),
    bootstrap_sha256 sha256_hex NOT NULL CHECK(bootstrap_sha256=encode(sha256(bootstrap_bytes),'hex')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(attempt_id,stage,item_ordinal)
);
CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON content_native_operations
    FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON content_native_operations
    FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation();

-- Compare the original JSON (not its lossy jsonb conversion) recursively. This rejects
-- duplicate/unknown keys and numeric/string substitutions at every depth. Java additionally
-- requires canonical encoding; hashes below always cover the exact original bytes.
CREATE FUNCTION native_reservation_json_matches(actual json, expected jsonb) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SET search_path=ddip_management,pg_catalog AS $$
DECLARE entry record; index_ integer:=0;
BEGIN
    IF actual IS NULL OR expected IS NULL OR json_typeof(actual) IS DISTINCT FROM jsonb_typeof(expected) THEN RETURN false; END IF;
    IF json_typeof(actual)='object' THEN
        IF (SELECT count(*) FROM json_each(actual))<>(SELECT count(*) FROM jsonb_each(expected))
           OR (SELECT count(*) FROM json_each(actual))<>(SELECT count(DISTINCT key) FROM json_each(actual)) THEN RETURN false; END IF;
        FOR entry IN SELECT * FROM json_each(actual) LOOP
            IF NOT expected ? entry.key OR NOT native_reservation_json_matches(entry.value,expected->entry.key) THEN RETURN false; END IF;
        END LOOP;
        RETURN true;
    ELSIF json_typeof(actual)='array' THEN
        IF json_array_length(actual)<>jsonb_array_length(expected) THEN RETURN false; END IF;
        FOR entry IN SELECT value FROM json_array_elements(actual) LOOP
            IF NOT native_reservation_json_matches(entry.value,expected->index_) THEN RETURN false; END IF;
            index_:=index_+1;
        END LOOP;
        RETURN true;
    ELSE
        RETURN (actual#>>'{}') IS NOT DISTINCT FROM (expected#>>'{}');
    END IF;
END $$;

CREATE FUNCTION guard_content_native_operation() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE
    c coordinator%ROWTYPE; r runs%ROWTYPE; a attempts%ROWTYPE; w worker_instances%ROWTYPE;
    admission jsonb; ready jsonb; frozen jsonb; selected jsonb; profile jsonb; runtime jsonb;
    runtime_id text; runtime_sha text; registered_sha text; raw bytea; family text; next_family text; fields text[];
    cp bytea; writer jsonb; expected_writer jsonb; frozen_identity jsonb; current_frozen_identity jsonb;
    profile_sha text; content_sha text; fetch_facts jsonb; command jsonb; bootstrap jsonb;
BEGIN
    -- Lock serialization needs fresh statement snapshots after a competing insert commits.
    -- Snapshot isolation can otherwise miss consumption in the other journal.
    IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_ISOLATION_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT * INTO c FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    SELECT r0.* INTO r FROM runs r0 JOIN attempts a0 ON a0.run_id=r0.run_id
        WHERE a0.attempt_id=NEW.attempt_id FOR UPDATE OF r0;
    SELECT * INTO a FROM attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
    SELECT * INTO w FROM worker_instances WHERE instance_id=a.instance_id;
    IF c.singleton_id IS NULL OR r.run_id IS NULL OR w.instance_id IS NULL OR a.attempt_id IS NULL OR a.content_admission_policy IS DISTINCT FROM 'ddip-content-narrow-v1'
       OR a.kind<>'EXECUTE' OR a.state<>'CLAIMED' OR a.attempt_no>19
       OR clock_timestamp()>=a.started_at+interval '2 hours'
       OR r.pipeline<>'CONTENT_RSS_V1' OR r.state<>'RUNNING' OR r.current_attempt_id IS DISTINCT FROM a.attempt_id
       OR c.mode<>'READY' OR c.active_import_id IS NOT NULL OR c.active_attempt_id IS DISTINCT FROM a.attempt_id
       OR c.owner_instance_id IS DISTINCT FROM a.instance_id OR c.epoch<>a.owner_epoch
       OR w.installation_id IS DISTINCT FROM c.installation_id OR w.status IS DISTINCT FROM 'READY'
       OR w.execution_profile_id IS DISTINCT FROM r.execution_profile_id
       OR EXISTS(SELECT 1 FROM controls WHERE run_id=a.run_id AND (state='REQUESTED' OR (type='CANCEL' AND state='APPLIED')))
       OR EXISTS(SELECT 1 FROM stage_invocations WHERE attempt_id=a.attempt_id) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_OWNER_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT convert_from(admission_bytes,'UTF8')::jsonb INTO admission FROM content_attempt_admissions WHERE attempt_id=a.attempt_id;
    SELECT convert_from(receipt_bytes,'UTF8')::jsonb INTO ready FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='READY';
    IF admission IS NULL OR ready IS NULL THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_READY_REQUIRED',ERRCODE='23514';
    END IF;

    IF EXISTS(SELECT 1 FROM content_operations WHERE attempt_id=a.attempt_id) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_PROBE_CONFLICT',ERRCODE='23514';
    END IF;
    cp:=decode(md5('ddip-content-narrow-v1'||chr(10)||a.run_id::text||chr(10)||a.attempt_id::text||chr(10)||'FETCH_SOURCE:0'),'hex');
    cp:=set_byte(cp,6,(get_byte(cp,6)&15)|48);cp:=set_byte(cp,8,(get_byte(cp,8)&63)|128);
    IF NEW.checkpoint_id<>encode(cp,'hex')::uuid THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_BINDING_INVALID',ERRCODE='23514';
    END IF;
    frozen:=convert_from(r.captured_selection,'UTF8')::jsonb;
    selected:=frozen->'sources'->0;
    SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO profile,profile_sha FROM execution_profiles
        WHERE profile_id=r.execution_profile_id;
    IF profile IS NULL OR profile_sha IS DISTINCT FROM admission->>'profileSha256'
       OR frozen->'executionProfile'->'configuration' IS DISTINCT FROM profile
       OR jsonb_typeof(frozen->'sources') IS DISTINCT FROM 'array'
       OR jsonb_array_length(frozen->'sources')<>1 OR selected->>'sourceId' IS DISTINCT FROM admission->>'sourceId'
       OR NOT EXISTS(SELECT 1 FROM run_sources rs JOIN source_revisions sr USING(product_id,source_id,revision)
           WHERE rs.run_id=r.run_id AND rs.source_id=selected->>'sourceId'
             AND to_jsonb(rs.revision)=selected->'revision' AND sr.configuration_sha256::text=selected->>'configurationSha256'
             AND convert_from(sr.configuration_bytes,'UTF8')::jsonb=selected->'configuration') THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_SOURCE_INVALID',ERRCODE='23514';
    END IF;
    IF jsonb_typeof(selected->'endpointSha256') IS DISTINCT FROM 'string'
       OR selected->>'endpointSha256' !~ '^[0-9a-f]{64}$'
       OR jsonb_typeof(selected->'endpoint'->'uri') IS DISTINCT FROM 'string'
       OR selected->'endpoint'->>'sourceId' IS DISTINCT FROM selected->>'sourceId'
       OR selected->'endpoint'->>'endpointId' IS DISTINCT FROM selected->'configuration'->>'feedEndpointId' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_SOURCE_INVALID',ERRCODE='23514';
    END IF;
    -- Never select a fetch registration using a caller-supplied command digest.
    runtime_id:=profile->>'contentRuntimeId'; runtime_sha:=admission->>'runtimeSha256';
    FOREACH family IN ARRAY ARRAY['content','gold','silver','bronze','fetch'] LOOP
        SELECT configuration_bytes,configuration_sha256::text INTO raw,registered_sha FROM execution_profiles WHERE profile_id=runtime_id;
        IF raw IS NULL OR registered_sha IS DISTINCT FROM runtime_sha THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        IF family='content' THEN content_sha:=registered_sha; END IF;
        runtime:=convert_from(raw,'UTF8')::jsonb;
        IF NOT native_reservation_json_matches(convert_from(raw,'UTF8')::json,runtime)
           OR runtime->(CASE WHEN family='fetch' THEN 'rssFetchRuntimeVersion' ELSE family||'RuntimeVersion' END) IS DISTINCT FROM '2'::jsonb THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        fields:=CASE family
            WHEN 'content' THEN ARRAY['contentRuntimeVersion','goldRuntimeId','goldRuntimeSha256','referenceBindingId','contentBindingId','modules','seconds']
            WHEN 'gold' THEN ARRAY['goldRuntimeVersion','silverRuntimeId','silverRuntimeSha256','goldBindingId','modules','duckdb','seconds','python']
            WHEN 'silver' THEN ARRAY['silverRuntimeVersion','bronzeRuntimeId','bronzeRuntimeSha256','runProfileSha256','deploymentSha256','python','javaHome','processingRoot','modules','versions','outputBindingId','tempBindingId','seconds']
            WHEN 'bronze' THEN ARRAY['bronzeRuntimeVersion','adapter','fetchRuntimeId','fetchRuntimeSha256','runProfileId','runProfileSha256','deploymentSha256','controlBindingId','storageBindingId','storageBindingSha256']
            ELSE ARRAY['rssFetchRuntimeVersion','pipeline','runProfileId','runProfileSha256','deploymentSha256','javaHome','manifest','controlBindingId','testEndpoint','httpTimeoutMillis'] END;
        IF NOT runtime ?& fields OR runtime-fields<>'{}'::jsonb THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        IF family IN ('silver','bronze','fetch') AND
           (runtime->>'runProfileSha256' IS DISTINCT FROM admission->>'profileSha256'
            OR runtime->>'deploymentSha256' IS DISTINCT FROM profile->'runtime'->>'deploymentSha256') THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        IF family IN ('bronze','fetch') AND runtime->>'runProfileId' IS DISTINCT FROM r.execution_profile_id::text THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_RUNTIME_INVALID',ERRCODE='23514';
        END IF;
        IF family<>'fetch' THEN
            next_family:=CASE family WHEN 'content' THEN 'gold' WHEN 'gold' THEN 'silver' WHEN 'silver' THEN 'bronze' ELSE 'fetch' END;
            runtime_id:=runtime->>(next_family||'RuntimeId');runtime_sha:=runtime->>(next_family||'RuntimeSha256');
        END IF;
    END LOOP;
    IF runtime->>'pipeline' IS DISTINCT FROM 'CONTENT_RSS_V1'
       OR runtime->'manifest'->>'adapter' IS DISTINCT FROM 'platform.ingestion.managed.BoundedManagedRssFetch'
       OR jsonb_typeof(runtime->'manifest'->'javaRuntime') IS DISTINCT FROM 'string'
       OR jsonb_typeof(runtime->'testEndpoint') IS DISTINCT FROM 'boolean'
       OR jsonb_typeof(runtime->'httpTimeoutMillis') IS DISTINCT FROM 'number'
       OR (runtime->>'httpTimeoutMillis') !~ '^[0-9]+$'
       OR (runtime->>'httpTimeoutMillis')::numeric NOT BETWEEN 1 AND 30000 THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_RUNTIME_INVALID',ERRCODE='23514';
    END IF;
    fetch_facts:=jsonb_build_object('revision',selected->'revision','capturedConfigSha256',selected->'configurationSha256',
        'endpointSha256',selected->'endpointSha256','feedUri',selected->'endpoint'->'uri','runtimeSha256',runtime_sha,
        'javaRuntime',runtime->'manifest'->'javaRuntime','testEndpoint',runtime->'testEndpoint','httpTimeoutMillis',runtime->'httpTimeoutMillis');
    command:=fetch_facts || jsonb_build_object('protocolVersion',1,'runId',a.run_id::text,'attemptId',a.attempt_id::text,
        'ownerEpoch',a.owner_epoch,'stage','FETCH_SOURCE','itemOrdinal',0,'checkpointId',NEW.checkpoint_id::text,'sourceId',admission->'sourceId');
    writer:=jsonb_build_object('installationId',c.installation_id::text,'workerInstanceId',a.instance_id::text,'epoch',a.owner_epoch::text,
        'runId',a.run_id::text,'attemptId',a.attempt_id::text,'stage','FETCH_SOURCE','ordinal',0,
        'checkpointId',NEW.checkpoint_id::text,'commandSha256',NEW.command_sha256::text);
    expected_writer:=writer || jsonb_build_object('installationId',admission->'installationId','workerInstanceId',admission->'workerInstanceId',
        'epoch',admission->'ownerEpoch','runId',admission->'runId','attemptId',admission->'attemptId');
    frozen_identity:=jsonb_build_object('selectionSha256',admission->'selectionSha256','profileSha256',admission->'profileSha256','runtimeSha256',admission->'runtimeSha256');
    current_frozen_identity:=jsonb_build_object('selectionSha256',r.selection_sha256::text,
        'profileSha256',profile_sha,'runtimeSha256',content_sha);
    IF current_frozen_identity IS DISTINCT FROM frozen_identity THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_BINDING_INVALID',ERRCODE='23514';
    END IF;
    bootstrap:=jsonb_build_object('reservationVersion',1,'mode','FETCH_SOURCE_RESERVATION','operationId',NEW.operation_id::text,
        'expected',jsonb_build_object('installationId',admission->'installationId','policy',admission->'policy','planKind','ORIGINAL',
            'runId',admission->'runId','originalAttemptId',admission->'attemptId','sourceId',admission->'sourceId',
            'controlRoot',admission->'controlBinding'->'root',
            'controlIdentity',jsonb_build_object('device',admission->'controlBinding'->'rootDevice','inode',admission->'controlBinding'->'rootInode'),
            'rootIdentity',jsonb_build_object('device',ready->'rootDevice','inode',ready->'rootInode'),
            'lockIdentity',jsonb_build_object('device',ready->'lockDevice','inode',ready->'lockInode'),
            'ledgerSha256',ready->'ledgerSha256','frozen',frozen_identity,'writer',expected_writer),
        'current',jsonb_build_object('policy',a.content_admission_policy,'frozen',current_frozen_identity,'writer',writer),'fetch',fetch_facts);
    IF NOT native_reservation_json_matches(convert_from(NEW.command_bytes,'UTF8')::json,command)
       OR NOT native_reservation_json_matches(convert_from(NEW.bootstrap_bytes,'UTF8')::json,bootstrap) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_BINDING_INVALID',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_native_operation_insert BEFORE INSERT ON content_native_operations
    FOR EACH ROW EXECUTE FUNCTION guard_content_native_operation();

-- Additive exclusion: leave V11's immutable migration and original guard unchanged.
CREATE FUNCTION guard_content_probe_native_exclusion() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
BEGIN
    IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_OPERATION_ISOLATION_REQUIRED',ERRCODE='23514';
    END IF;
    PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    IF EXISTS(SELECT 1 FROM content_native_operations WHERE attempt_id=NEW.attempt_id) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_OPERATION_NATIVE_CONFLICT',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_operation_native_exclusion BEFORE INSERT ON content_operations
    FOR EACH ROW EXECUTE FUNCTION guard_content_probe_native_exclusion();
