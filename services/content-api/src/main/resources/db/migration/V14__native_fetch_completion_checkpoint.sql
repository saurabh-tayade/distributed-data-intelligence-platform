-- Additive native checkpoint evidence. No legacy stage/delivery transition or continuation authority.
CREATE TABLE content_fetch_completion_intents (
    operation_id uuid PRIMARY KEY REFERENCES content_native_operations ON DELETE RESTRICT,
    candidate_sha256 sha256_hex NOT NULL,
    completion_bytes bytea NOT NULL CHECK(valid_object_bytes(completion_bytes,65536)),
    completion_sha256 sha256_hex NOT NULL CHECK(completion_sha256=encode(sha256(completion_bytes),'hex')),
    owner_coordinator_version bigint NOT NULL CHECK(owner_coordinator_version>=0),
    owner_worker_version bigint NOT NULL CHECK(owner_worker_version>=0),
    owner_run_version bigint NOT NULL CHECK(owner_run_version>=0),
    owner_attempt_version bigint NOT NULL CHECK(owner_attempt_version>=0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE content_fetch_checkpoints (
    operation_id uuid PRIMARY KEY REFERENCES content_fetch_completion_intents ON DELETE RESTRICT,
    checkpoint_id uuid NOT NULL UNIQUE,
    journal_address relative_address NOT NULL,
    journal_descriptor bytea NOT NULL CHECK(valid_object_bytes(journal_descriptor,65536)),
    journal_sha256 sha256_hex NOT NULL CHECK(journal_sha256=encode(sha256(journal_descriptor),'hex')),
    expected_evidence bytea NOT NULL CHECK(valid_object_bytes(expected_evidence,65536)),
    expected_sha256 sha256_hex NOT NULL CHECK(expected_sha256=encode(sha256(expected_evidence),'hex')),
    completion_sha256 sha256_hex NOT NULL,
    payload_length bigint NOT NULL CHECK(payload_length BETWEEN 0 AND 5242880),
    payload_sha256 sha256_hex NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['content_fetch_completion_intents','content_fetch_checkpoints'] LOOP
        EXECUTE format('CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation()',t);
        EXECUTE format('CREATE TRIGGER no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation()',t);
    END LOOP;
END $$;

CREATE FUNCTION require_content_fetch_completion_owner(operation uuid,cv bigint,wv bigint,rv bigint,av bigint) RETURNS void
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE c coordinator%ROWTYPE; r runs%ROWTYPE; a attempts%ROWTYPE; w worker_instances%ROWTYPE;
    op content_native_operations%ROWTYPE; admission jsonb;
BEGIN
    IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_COMPLETION_ISOLATION_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT * INTO c FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    SELECT * INTO op FROM content_native_operations WHERE operation_id=operation;
    SELECT r0.* INTO r FROM runs r0 JOIN attempts a0 USING(run_id) WHERE a0.attempt_id=op.attempt_id FOR UPDATE OF r0;
    SELECT * INTO a FROM attempts WHERE attempt_id=op.attempt_id FOR UPDATE;
    SELECT * INTO w FROM worker_instances WHERE instance_id=a.instance_id;
    IF c.singleton_id IS NULL OR r.run_id IS NULL OR a.attempt_id IS NULL OR w.instance_id IS NULL
       OR a.content_admission_policy IS DISTINCT FROM 'ddip-content-narrow-v1'
       OR a.kind<>'EXECUTE' OR a.state<>'CLAIMED' OR a.attempt_no>19
       OR clock_timestamp()>=a.started_at+interval '2 hours'
       OR r.pipeline<>'CONTENT_RSS_V1' OR r.state<>'RUNNING' OR r.current_attempt_id IS DISTINCT FROM a.attempt_id
       OR c.mode<>'READY' OR c.active_import_id IS NOT NULL OR c.active_attempt_id IS DISTINCT FROM a.attempt_id
       OR c.owner_instance_id IS DISTINCT FROM a.instance_id OR c.epoch<>a.owner_epoch
       OR w.installation_id IS DISTINCT FROM c.installation_id OR w.status IS DISTINCT FROM 'READY'
       OR w.execution_profile_id IS DISTINCT FROM r.execution_profile_id
       OR c.row_version<>cv OR w.row_version<>wv OR r.row_version<>rv OR a.row_version<>av
       OR EXISTS(SELECT 1 FROM controls WHERE run_id=a.run_id AND (state='REQUESTED' OR (type='CANCEL' AND state='APPLIED')))
       OR EXISTS(SELECT 1 FROM stage_invocations WHERE attempt_id=a.attempt_id)
       OR EXISTS(SELECT 1 FROM rss_deliveries WHERE run_id=a.run_id)
       OR EXISTS(SELECT 1 FROM content_operations WHERE attempt_id=a.attempt_id) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_COMPLETION_OWNER_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT convert_from(admission_bytes,'UTF8')::jsonb INTO admission FROM content_attempt_admissions WHERE attempt_id=a.attempt_id;
    IF admission IS NULL OR admission->>'planKind' IS DISTINCT FROM 'ORIGINAL'
       OR admission->>'workerInstanceId' IS DISTINCT FROM a.instance_id::text
       OR admission->>'ownerEpoch' IS DISTINCT FROM a.owner_epoch::text
       OR admission->>'installationId' IS DISTINCT FROM c.installation_id::text
       OR NOT EXISTS(SELECT 1 FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='READY')
       OR (SELECT count(*) FROM content_native_launch_phases WHERE operation_id=operation)<>6 THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_COMPLETION_READY_REQUIRED',ERRCODE='23514';
    END IF;
END $$;

-- Fixed marker fields only. The candidate digest is reconstructed from the five sorted entries,
-- in the same canonical JSON shape as the candidate inventory (integer and hex fields only).
CREATE FUNCTION guard_content_fetch_completion_intent() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE m jsonb; f jsonb; n text; expected_names text[]:=ARRAY['command.json','descriptor.json','metadata.json','payload.xml','result.json'];
    idx integer:=1; inventory text:='{'; op content_native_operations%ROWTYPE; digest text; maximum bigint;
BEGIN
    PERFORM require_content_fetch_completion_owner(NEW.operation_id,NEW.owner_coordinator_version,NEW.owner_worker_version,NEW.owner_run_version,NEW.owner_attempt_version);
    SELECT * INTO op FROM content_native_operations WHERE operation_id=NEW.operation_id;
    SELECT candidate_sha256::text INTO digest FROM content_native_launch_phases WHERE operation_id=NEW.operation_id AND phase='CANDIDATE_EXITED';
    m:=convert_from(NEW.completion_bytes,'UTF8')::jsonb;
    IF NEW.candidate_sha256::text IS DISTINCT FROM digest
       OR NOT native_reservation_json_matches(convert_from(NEW.completion_bytes,'UTF8')::json,m)
       OR NOT m ?& ARRAY['journalCompletionVersion','directoryIdentity','files']
       OR m-ARRAY['journalCompletionVersion','directoryIdentity','files']<>'{}'::jsonb
       OR m->'journalCompletionVersion' IS DISTINCT FROM '1'::jsonb OR m->>'journalCompletionVersion' IS DISTINCT FROM '1'
       OR jsonb_typeof(m->'directoryIdentity') IS DISTINCT FROM 'string' OR m->>'directoryIdentity' !~ '^[0-9]+:[0-9]+$'
       OR jsonb_typeof(m->'files') IS DISTINCT FROM 'array' OR jsonb_array_length(m->'files')<>5 THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_COMPLETION_INTENT_INVALID',ERRCODE='23514';
    END IF;
    FOR f IN SELECT value FROM jsonb_array_elements(m->'files') LOOP
        n:=expected_names[idx];maximum:=CASE n WHEN 'payload.xml' THEN 5242880 ELSE 65536 END;
        IF NOT f ?& ARRAY['name','byteLength','sha256','stamp'] OR f-ARRAY['name','byteLength','sha256','stamp']<>'{}'::jsonb
           OR f->'name' IS DISTINCT FROM to_jsonb(n)
           OR jsonb_typeof(f->'byteLength') IS DISTINCT FROM 'number' OR f->>'byteLength' !~ '^[0-9]+$'
           OR (f->>'byteLength')::numeric>maximum
           OR jsonb_typeof(f->'sha256') IS DISTINCT FROM 'string' OR f->>'sha256' !~ '^[0-9a-f]{64}$'
           OR jsonb_typeof(f->'stamp') IS DISTINCT FROM 'object'
           OR NOT f->'stamp' ?& ARRAY['identity','modified','size'] OR (f->'stamp')-ARRAY['identity','modified','size']<>'{}'::jsonb
           OR jsonb_typeof(f->'stamp'->'identity') IS DISTINCT FROM 'string' OR f->'stamp'->>'identity' !~ '^[0-9]+:[0-9]+$'
           OR jsonb_typeof(f->'stamp'->'modified') IS DISTINCT FROM 'string' OR length(f->'stamp'->>'modified') NOT BETWEEN 1 AND 64
           OR f->'stamp'->'size' IS DISTINCT FROM f->'byteLength' OR f->'stamp'->>'size' IS DISTINCT FROM f->>'byteLength'
           OR (n='command.json' AND (f->>'sha256' IS DISTINCT FROM op.command_sha256::text OR (f->>'byteLength')::integer<>octet_length(op.command_bytes))) THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_COMPLETION_MARKER_INVALID',ERRCODE='23514';
        END IF;
        IF idx>1 THEN inventory:=inventory||',';END IF;
        inventory:=inventory||'"'||n||'":{"bytes":'||(f->>'byteLength')||',"sha256":"'||(f->>'sha256')||'"}';idx:=idx+1;
    END LOOP;
    inventory:=inventory||'}';
    IF encode(sha256(convert_to(inventory,'UTF8')),'hex') IS DISTINCT FROM NEW.candidate_sha256::text THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_COMPLETION_CANDIDATE_INVALID',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER content_fetch_completion_intent_insert BEFORE INSERT ON content_fetch_completion_intents
    FOR EACH ROW EXECUTE FUNCTION guard_content_fetch_completion_intent();

CREATE FUNCTION guard_content_fetch_checkpoint() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE i content_fetch_completion_intents%ROWTYPE; op content_native_operations%ROWTYPE;
    c jsonb; d jsonb; e jsonb; f jsonb; m jsonb; k text; address text; bronze_prefix text;
BEGIN
    -- Coordinator first, before reading intent, so competing READ COMMITTED writers serialize.
    PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    SELECT * INTO i FROM content_fetch_completion_intents WHERE operation_id=NEW.operation_id;
    IF i.operation_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_COMPLETION_INTENT_REQUIRED',ERRCODE='23514';
    END IF;
    PERFORM require_content_fetch_completion_owner(NEW.operation_id,i.owner_coordinator_version,i.owner_worker_version,i.owner_run_version,i.owner_attempt_version);
    SELECT * INTO op FROM content_native_operations WHERE operation_id=NEW.operation_id;
    c:=convert_from(op.command_bytes,'UTF8')::jsonb;d:=convert_from(NEW.journal_descriptor,'UTF8')::jsonb;
    e:=convert_from(NEW.expected_evidence,'UTF8')::jsonb;m:=convert_from(i.completion_bytes,'UTF8')::jsonb;
    address:='runs/'||(c->>'runId')||'/attempts/'||op.attempt_id::text||'/fetch-source/'||(c->>'sourceId')||'/'||op.checkpoint_id::text;
    IF NEW.checkpoint_id IS DISTINCT FROM op.checkpoint_id OR NEW.journal_address::text IS DISTINCT FROM address
       OR NEW.completion_sha256 IS DISTINCT FROM i.completion_sha256
       OR NOT native_reservation_json_matches(convert_from(NEW.journal_descriptor,'UTF8')::json,d)
       OR NOT native_reservation_json_matches(convert_from(NEW.expected_evidence,'UTF8')::json,e)
       OR NOT d ?& ARRAY['journalVersion','commandSha256','runId','attemptId','ownerEpoch','stage','itemOrdinal','checkpointId','sourceId','revision','capturedConfigSha256','endpointSha256','runtimeSha256','ingestionId','ingestedAt','httpStatus','contentType','declaredContentLength','payloadByteLength','payloadSha256','expected']
       OR d-ARRAY['journalVersion','commandSha256','runId','attemptId','ownerEpoch','stage','itemOrdinal','checkpointId','sourceId','revision','capturedConfigSha256','endpointSha256','runtimeSha256','ingestionId','ingestedAt','httpStatus','contentType','declaredContentLength','payloadByteLength','payloadSha256','expected']<>'{}'::jsonb
       OR d->'journalVersion' IS DISTINCT FROM '1'::jsonb OR d->>'journalVersion' IS DISTINCT FROM '1' OR d->>'commandSha256' IS DISTINCT FROM op.command_sha256::text
       OR d->'payloadByteLength' IS DISTINCT FROM to_jsonb(NEW.payload_length) OR d->>'payloadByteLength' IS DISTINCT FROM NEW.payload_length::text OR d->>'payloadSha256' IS DISTINCT FROM NEW.payload_sha256::text
       OR jsonb_typeof(d->'httpStatus') IS DISTINCT FROM 'number' OR d->>'httpStatus' !~ '^[0-9]+$' OR (d->>'httpStatus')::numeric NOT BETWEEN 200 AND 299
       OR NOT e ?& ARRAY['completionSha256','completionStamp','bronze'] OR e-ARRAY['completionSha256','completionStamp','bronze']<>'{}'::jsonb
       OR e->'completionSha256' IS DISTINCT FROM to_jsonb(i.completion_sha256::text)
       OR NOT native_reservation_json_matches(convert_from(NEW.expected_evidence,'UTF8')::json->'bronze',d->'expected')
       OR jsonb_typeof(e->'completionStamp') IS DISTINCT FROM 'object'
       OR NOT e->'completionStamp' ?& ARRAY['identity','modified','size'] OR (e->'completionStamp')-ARRAY['identity','modified','size']<>'{}'::jsonb
       OR jsonb_typeof(e->'completionStamp'->'identity') IS DISTINCT FROM 'string' OR e->'completionStamp'->>'identity' !~ '^[0-9]+:[0-9]+$'
       OR jsonb_typeof(e->'completionStamp'->'modified') IS DISTINCT FROM 'string' OR length(e->'completionStamp'->>'modified') NOT BETWEEN 1 AND 64
       OR e->'completionStamp'->'size' IS DISTINCT FROM to_jsonb(octet_length(i.completion_bytes)) OR e->'completionStamp'->>'size' IS DISTINCT FROM octet_length(i.completion_bytes)::text THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_CHECKPOINT_INVALID',ERRCODE='23514';
    END IF;
    IF jsonb_typeof(d->'ingestionId') IS DISTINCT FROM 'string' OR d->>'ingestionId' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR jsonb_typeof(d->'ingestedAt') IS DISTINCT FROM 'string' OR d->>'ingestedAt' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$'
       OR jsonb_typeof(d->'contentType') NOT IN ('null','string')
       OR (d->'declaredContentLength'<>'null'::jsonb AND (jsonb_typeof(d->'declaredContentLength') IS DISTINCT FROM 'number' OR d->>'declaredContentLength' !~ '^[0-9]+$' OR (d->>'declaredContentLength')::numeric>9223372036854775807))
       OR jsonb_typeof(d->'expected') IS DISTINCT FROM 'object'
       OR NOT d->'expected' ?& ARRAY['payloadKey','metadataKey','metadataByteLength','metadataSha256']
       OR (d->'expected')-ARRAY['payloadKey','metadataKey','metadataByteLength','metadataSha256']<>'{}'::jsonb
       OR jsonb_typeof(d->'expected'->'metadataByteLength') IS DISTINCT FROM 'number' OR d->'expected'->>'metadataByteLength' !~ '^[0-9]+$'
       OR (d->'expected'->>'metadataByteLength')::numeric>65536
       OR jsonb_typeof(d->'expected'->'metadataSha256') IS DISTINCT FROM 'string' OR d->'expected'->>'metadataSha256' !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_CHECKPOINT_DESCRIPTOR_INVALID',ERRCODE='23514';
    END IF;
    -- Validate the timestamp, but do not round nanoseconds into the following UTC date.
    PERFORM (d->>'ingestedAt')::timestamptz;
    bronze_prefix:='bronze/rss/source_id='||rtrim(translate(replace(encode(convert_to(c->>'sourceId','UTF8'),'base64'),chr(10),''),'+/','-_'),'=')
        ||'/ingestion_date='||substring(d->>'ingestedAt' FROM 1 FOR 10)||'/ingestion_id='||(d->>'ingestionId')||'/';
    IF d->'expected'->'payloadKey' IS DISTINCT FROM to_jsonb(bronze_prefix||'payload.xml')
       OR d->'expected'->'metadataKey' IS DISTINCT FROM to_jsonb(bronze_prefix||'metadata.json') THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_CHECKPOINT_DESCRIPTOR_INVALID',ERRCODE='23514';
    END IF;
    FOREACH k IN ARRAY ARRAY['runId','attemptId','ownerEpoch','stage','itemOrdinal','checkpointId','sourceId','revision','capturedConfigSha256','endpointSha256','runtimeSha256'] LOOP
        IF d->k IS DISTINCT FROM c->k OR d->>k IS DISTINCT FROM c->>k THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_CHECKPOINT_IDENTITY_INVALID',ERRCODE='23514';
        END IF;
    END LOOP;
    FOR f IN SELECT value FROM jsonb_array_elements(m->'files') LOOP
        IF (f->>'name'='descriptor.json' AND (f->>'sha256' IS DISTINCT FROM NEW.journal_sha256::text OR f->'byteLength' IS DISTINCT FROM to_jsonb(octet_length(NEW.journal_descriptor))))
           OR (f->>'name'='payload.xml' AND (f->>'sha256' IS DISTINCT FROM NEW.payload_sha256::text OR f->'byteLength' IS DISTINCT FROM to_jsonb(NEW.payload_length)))
           OR (f->>'name'='metadata.json' AND (f->'sha256' IS DISTINCT FROM d->'expected'->'metadataSha256' OR f->'byteLength' IS DISTINCT FROM d->'expected'->'metadataByteLength')) THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_CHECKPOINT_EVIDENCE_INVALID',ERRCODE='23514';
        END IF;
    END LOOP;
    RETURN NEW;
END $$;
CREATE TRIGGER content_fetch_checkpoint_insert BEFORE INSERT ON content_fetch_checkpoints
    FOR EACH ROW EXECUTE FUNCTION guard_content_fetch_checkpoint();
