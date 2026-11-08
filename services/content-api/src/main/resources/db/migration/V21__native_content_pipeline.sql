-- Additive original-owner, test-only full-chain journal. No dispatcher or recovery authority.
CREATE TABLE content_pipeline_profiles (
 profile_id varchar(128) PRIMARY KEY,
 configuration_bytes bytea NOT NULL CHECK(valid_object_bytes(configuration_bytes,65536)),
 configuration_sha256 sha256_hex NOT NULL CHECK(configuration_sha256=encode(sha256(configuration_bytes),'hex')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE content_pipeline_operations (
 operation_id uuid PRIMARY KEY,
 attempt_id uuid NOT NULL REFERENCES content_attempt_admissions ON DELETE RESTRICT,
 bronze_operation_id uuid NOT NULL REFERENCES content_bronze_checkpoints(operation_id) ON DELETE RESTRICT,
 silver_reservation_id uuid NOT NULL REFERENCES content_silver_reservations(operation_id) ON DELETE RESTRICT,
 step_index integer NOT NULL CHECK(step_index BETWEEN 2 AND 16),
 stage varchar(32) NOT NULL, item_ordinal integer NOT NULL CHECK(item_ordinal BETWEEN 0 AND 3),
 checkpoint_id uuid NOT NULL, native_checkpoint_id uuid NOT NULL,
 predecessor_operation_id uuid NOT NULL,
 command_bytes bytea NOT NULL CHECK(valid_object_bytes(command_bytes,1048576)),
 command_sha256 sha256_hex NOT NULL CHECK(command_sha256=encode(sha256(command_bytes),'hex')),
 profile_id varchar(128) NOT NULL REFERENCES content_pipeline_profiles ON DELETE RESTRICT,
 profile_sha256 sha256_hex NOT NULL,
 owner_coordinator_version bigint NOT NULL CHECK(owner_coordinator_version>=0),
    owner_worker_version bigint NOT NULL CHECK(owner_worker_version>=0),
    owner_run_version bigint NOT NULL CHECK(owner_run_version>=0),
    owner_attempt_version bigint NOT NULL CHECK(owner_attempt_version>=0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(attempt_id,step_index), UNIQUE(attempt_id,stage,item_ordinal)
);
CREATE TABLE content_pipeline_phases (
 operation_id uuid NOT NULL REFERENCES content_pipeline_operations ON DELETE RESTRICT,
 phase varchar(32) NOT NULL CHECK(phase IN ('LAUNCH_BOUND','FILES_READY','START_AUTHORIZED','CHILD_RECORDED','RELEASE_AUTHORIZED','CANDIDATE_EXITED')),
 phase_bytes bytea NOT NULL CHECK(valid_object_bytes(phase_bytes,65536)),
 phase_sha256 sha256_hex NOT NULL CHECK(phase_sha256=encode(sha256(phase_bytes),'hex')),
 owner_coordinator_version bigint NOT NULL CHECK(owner_coordinator_version>=0),
    owner_worker_version bigint NOT NULL CHECK(owner_worker_version>=0),
    owner_run_version bigint NOT NULL CHECK(owner_run_version>=0),
    owner_attempt_version bigint NOT NULL CHECK(owner_attempt_version>=0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(operation_id,phase)
);
CREATE TABLE content_pipeline_checkpoints (
 operation_id uuid PRIMARY KEY REFERENCES content_pipeline_operations ON DELETE RESTRICT,
 proof_bytes bytea NOT NULL CHECK(valid_object_bytes(proof_bytes,3145728)),
 proof_sha256 sha256_hex NOT NULL CHECK(proof_sha256=encode(sha256(proof_bytes),'hex')),
 result_bytes bytea NOT NULL CHECK(valid_object_bytes(result_bytes,65536)),
 evidence_bytes bytea CHECK(valid_object_bytes(evidence_bytes,1048576)),
 selection_bytes bytea CHECK(valid_object_bytes(selection_bytes,1048576)),
 owner_coordinator_version bigint NOT NULL CHECK(owner_coordinator_version>=0),
    owner_worker_version bigint NOT NULL CHECK(owner_worker_version>=0),
    owner_run_version bigint NOT NULL CHECK(owner_run_version>=0),
    owner_attempt_version bigint NOT NULL CHECK(owner_attempt_version>=0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE content_pipeline_completions (
 attempt_id uuid PRIMARY KEY REFERENCES content_attempt_admissions ON DELETE RESTRICT,
 final_operation_id uuid NOT NULL UNIQUE REFERENCES content_pipeline_checkpoints(operation_id) ON DELETE RESTRICT,
 artifact_id uuid NOT NULL REFERENCES artifacts ON DELETE RESTRICT,
 proof_bytes bytea NOT NULL CHECK(valid_object_bytes(proof_bytes,65536)),
 proof_sha256 sha256_hex NOT NULL CHECK(proof_sha256=encode(sha256(proof_bytes),'hex')),
 owner_coordinator_version bigint NOT NULL CHECK(owner_coordinator_version>=0),
    owner_worker_version bigint NOT NULL CHECK(owner_worker_version>=0),
    owner_run_version bigint NOT NULL CHECK(owner_run_version>=0),
    owner_attempt_version bigint NOT NULL CHECK(owner_attempt_version>=0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['content_pipeline_profiles','content_pipeline_operations','content_pipeline_phases','content_pipeline_checkpoints','content_pipeline_completions'] LOOP
  EXECUTE format('CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation()',t);
  EXECUTE format('CREATE TRIGGER no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation()',t);
 END LOOP;
END $$;

-- Journal identities and native aliases deliberately differ; VERIFY_SILVER shares SILVER's UUID.
CREATE FUNCTION content_pipeline_slot(r uuid,a uuid,s text,n integer) RETURNS uuid
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=ddip_management,pg_catalog AS $$
DECLARE cp bytea; BEGIN
 cp:=decode(md5('ddip-content-narrow-v1'||chr(10)||r::text||chr(10)||a::text||chr(10)||
     CASE WHEN s='VERIFY_SILVER' THEN 'SILVER' ELSE s END||':'||n::text),'hex');
 cp:=set_byte(cp,6,(get_byte(cp,6)&15)|48); cp:=set_byte(cp,8,(get_byte(cp,8)&63)|128);
 RETURN encode(cp,'hex')::uuid;
END $$;
CREATE FUNCTION content_pipeline_step(i integer)
RETURNS TABLE(stage text,ordinal integer,family text,mode text,native_stage text,native_ordinal integer)
LANGUAGE sql IMMUTABLE STRICT AS $$
 SELECT stage,ordinal,family,mode,native_stage,native_ordinal FROM (VALUES
 (2,'INSPECT_BRONZE',0,'BRONZE','INSPECT','INSPECT_BRONZE',0),
 (3,'SILVER',0,'SILVER','PUBLISH','SILVER',0),
 (4,'VERIFY_SILVER',0,'SILVER','VERIFY','SILVER',0),
 (5,'RECHECK_SILVER',0,'SILVER','RECHECK','SILVER',0),
 (6,'INSPECT_BRONZE',1,'BRONZE','INSPECT','INSPECT_BRONZE',1),
 (7,'PREPARE_GOLD',0,'GOLD','PREPARE','PREPARE_GOLD',0),
 (8,'INSPECT_BRONZE',2,'BRONZE','INSPECT','INSPECT_BRONZE',2),
 (9,'PUBLISH_GOLD',0,'GOLD','PUBLISH','PUBLISH_GOLD',0),
 (10,'VERIFY_RESULT',0,'GOLD','VERIFY','PUBLISH_GOLD',0),
 (11,'CHECK_INPUTS',0,'GOLD','RECHECK','PUBLISH_GOLD',0),
 (12,'INSPECT_BRONZE',3,'BRONZE','INSPECT','INSPECT_BRONZE',3),
 (13,'CHECK_INPUTS',1,'CONTENT','PREPARE','CHECK_INPUTS',1),
 (14,'PUBLISH_CONTENT',0,'CONTENT','PUBLISH','PUBLISH_CONTENT',0),
 (15,'VERIFY_RESULT',1,'CONTENT','VERIFY','VERIFY_RESULT',1),
 (16,'CHECK_INPUTS',2,'CONTENT','RECHECK','VERIFY_RESULT',1)
 ) v(idx,stage,ordinal,family,mode,native_stage,native_ordinal) WHERE idx=i
$$;

CREATE FUNCTION guard_content_pipeline_profile() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE n jsonb; rp jsonb; runtime jsonb; digest text; k text; registered jsonb; fields text[];
BEGIN
 n:=convert_from(NEW.configuration_bytes,'UTF8')::jsonb;
 fields:=ARRAY['strictContentPipelineVersion','runProfileId','runProfileSha256','contentRuntimeId','contentRuntimeSha256',
 'controlBindingId','strictRuntimeSha256','python','processingRoot','javaHome','modules','versions',
 'fetchLaunchProfileId','bronzeLaunchProfileId','bronzeVerificationProfileId'];
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO rp,digest
 FROM execution_profiles WHERE profile_id=n->>'runProfileId';
 IF NOT native_reservation_json_matches(convert_from(NEW.configuration_bytes,'UTF8')::json,n)
 OR NOT n ?& fields OR n-fields<>'{}'::jsonb OR n->'strictContentPipelineVersion' IS DISTINCT FROM '1'::jsonb OR n->>'strictContentPipelineVersion' IS DISTINCT FROM '1'
 OR rp IS NULL OR n->>'runProfileSha256' IS DISTINCT FROM digest
 OR rp->>'contentRuntimeId' IS DISTINCT FROM n->>'contentRuntimeId' THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PROFILE_INVALID',ERRCODE='23514'; END IF;
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO runtime,digest
 FROM execution_profiles WHERE profile_id=n->>'contentRuntimeId';
 IF runtime IS NULL OR runtime->'contentRuntimeVersion' IS DISTINCT FROM '2'::jsonb
 OR n->>'contentRuntimeSha256' IS DISTINCT FROM digest
 OR n->>'strictRuntimeSha256' IS DISTINCT FROM '9fa9d9fbc31c8080ca4cf0f3c2629c2cfa3151c98777bb293f957c3eb1746592'
 OR jsonb_typeof(n->'modules') IS DISTINCT FROM 'object' OR n->'modules'='{}'::jsonb
 OR jsonb_typeof(n->'versions') IS DISTINCT FROM 'object' THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PROFILE_INVALID',ERRCODE='23514'; END IF;
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO registered FROM execution_profiles WHERE profile_id=runtime->>'goldRuntimeId';
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO registered FROM execution_profiles WHERE profile_id=registered->>'silverRuntimeId';
 IF registered IS NULL OR n->'python' IS DISTINCT FROM registered->'python'
 OR n->'processingRoot' IS DISTINCT FROM registered->'processingRoot' OR n->'javaHome' IS DISTINCT FROM registered->'javaHome'
 OR n->'versions' IS DISTINCT FROM jsonb_build_object('python','3.12.15','pyspark','4.0.2','py4j','0.10.9.9',
 'boto3','1.43.114','botocore','1.43.114','duckdb','1.4.5','java','21')
 OR (SELECT count(*) FROM jsonb_object_keys(n->'modules'))>128 THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PROFILE_RUNTIME_INVALID',ERRCODE='23514'; END IF;
 FOREACH k IN ARRAY ARRAY['runProfileId','runProfileSha256','contentRuntimeId','contentRuntimeSha256','controlBindingId',
 'strictRuntimeSha256','python','processingRoot','javaHome','fetchLaunchProfileId','bronzeLaunchProfileId','bronzeVerificationProfileId'] LOOP
  IF jsonb_typeof(n->k) IS DISTINCT FROM 'string' OR length(n->>k)=0 THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PROFILE_INVALID',ERRCODE='23514'; END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM jsonb_each(n->'modules') WHERE key !~ '^[A-Za-z0-9_]+[.]py$'
  OR jsonb_typeof(value)<>'string' OR value#>>'{}' !~ '^[0-9a-f]{64}$')
 OR NOT n->'modules' ?& ARRAY['strict_native_content.py','strict_native_runtime.py','strict_native_sink.py']
 OR NOT n->'versions' ?& ARRAY['python','pyspark','py4j','boto3','botocore','duckdb','java']
 OR (n->'versions')-ARRAY['python','pyspark','py4j','boto3','botocore','duckdb','java']<>'{}'::jsonb
 OR EXISTS(SELECT 1 FROM jsonb_each(n->'versions') WHERE jsonb_typeof(value)<>'string' OR length(value#>>'{}')=0) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PROFILE_INVALID',ERRCODE='23514'; END IF;
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO registered
 FROM content_fetch_launch_profiles WHERE launch_profile_id=n->>'fetchLaunchProfileId';
 IF registered IS NULL OR registered->>'runProfileId' IS DISTINCT FROM n->>'runProfileId'
 OR registered->>'runProfileSha256' IS DISTINCT FROM n->>'runProfileSha256'
 OR registered->>'controlBindingId' IS DISTINCT FROM n->>'controlBindingId'
 OR registered->'testEndpoint' IS DISTINCT FROM 'true'::jsonb THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PROFILE_INVALID',ERRCODE='23514'; END IF;
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO registered
 FROM content_bronze_launch_profiles WHERE launch_profile_id=n->>'bronzeLaunchProfileId';
 IF registered IS NULL OR registered->>'runProfileId' IS DISTINCT FROM n->>'runProfileId'
 OR registered->>'runProfileSha256' IS DISTINCT FROM n->>'runProfileSha256'
 OR registered->>'controlBindingId' IS DISTINCT FROM n->>'controlBindingId' THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PROFILE_INVALID',ERRCODE='23514'; END IF;
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO registered
 FROM content_bronze_verification_profiles WHERE verification_profile_id=n->>'bronzeVerificationProfileId';
 IF registered IS NULL OR registered->>'runProfileId' IS DISTINCT FROM n->>'runProfileId'
 OR registered->>'runProfileSha256' IS DISTINCT FROM n->>'runProfileSha256'
 OR registered->>'controlBindingId' IS DISTINCT FROM n->>'controlBindingId' THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PROFILE_INVALID',ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER content_pipeline_profile_insert BEFORE INSERT ON content_pipeline_profiles
 FOR EACH ROW EXECUTE FUNCTION guard_content_pipeline_profile();

CREATE FUNCTION guard_content_pipeline_operation() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE s record; a attempts%ROWTYPE; r runs%ROWTYPE; b content_bronze_reservations%ROWTYPE;
 silver content_silver_reservations%ROWTYPE; prior content_pipeline_operations%ROWTYPE;
 profile jsonb; digest text; n jsonb; admission jsonb; runtime jsonb; runtime_id text; runtime_sha text;
 family text; next_family text; selected jsonb; fields text[];
BEGIN
 PERFORM require_content_bronze_checkpoint_owner(NEW.bronze_operation_id,NEW.owner_coordinator_version,
 NEW.owner_worker_version,NEW.owner_run_version,NEW.owner_attempt_version);
 SELECT * INTO s FROM content_pipeline_step(NEW.step_index);
 SELECT * INTO b FROM content_bronze_reservations WHERE operation_id=NEW.bronze_operation_id;
 SELECT * INTO silver FROM content_silver_reservations WHERE operation_id=NEW.silver_reservation_id;
 SELECT * INTO a FROM attempts WHERE attempt_id=NEW.attempt_id;
 SELECT * INTO r FROM runs WHERE run_id=a.run_id;
 SELECT convert_from(admission_bytes,'UTF8')::jsonb INTO admission FROM content_attempt_admissions WHERE attempt_id=a.attempt_id;
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO profile,digest
 FROM content_pipeline_profiles WHERE profile_id=NEW.profile_id;
 SELECT * INTO prior FROM content_pipeline_operations WHERE attempt_id=NEW.attempt_id AND step_index=NEW.step_index-1;
 IF b.attempt_id IS DISTINCT FROM NEW.attempt_id OR silver.attempt_id IS DISTINCT FROM NEW.attempt_id
 OR silver.bronze_operation_id IS DISTINCT FROM NEW.bronze_operation_id
 OR s.stage IS DISTINCT FROM NEW.stage OR s.ordinal IS DISTINCT FROM NEW.item_ordinal
 OR NEW.checkpoint_id IS DISTINCT FROM content_pipeline_slot(r.run_id,a.attempt_id,s.stage,s.ordinal)
 OR NEW.native_checkpoint_id IS DISTINCT FROM content_pipeline_slot(r.run_id,a.attempt_id,s.native_stage,s.native_ordinal)
 OR profile IS NULL OR digest IS DISTINCT FROM NEW.profile_sha256::text
 OR profile->>'runProfileId' IS DISTINCT FROM r.execution_profile_id::text
 OR profile->>'runProfileSha256' IS DISTINCT FROM admission->>'profileSha256'
 OR profile->>'contentRuntimeSha256' IS DISTINCT FROM admission->>'runtimeSha256'
 OR profile->>'controlBindingId' IS DISTINCT FROM admission->'controlBinding'->>'bindingId'
 OR NEW.owner_coordinator_version<silver.owner_coordinator_version OR NEW.owner_worker_version<silver.owner_worker_version
 OR NEW.owner_run_version<>silver.owner_run_version OR NEW.owner_attempt_version<>silver.owner_attempt_version
 OR NEW.operation_id IN (NEW.bronze_operation_id,NEW.silver_reservation_id,b.fetch_operation_id)
 OR EXISTS(SELECT 1 FROM content_pipeline_completions WHERE attempt_id=NEW.attempt_id) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_OPERATION_INVALID',ERRCODE='23514'; END IF;
 IF NEW.step_index=2 THEN
  IF NEW.predecessor_operation_id<>NEW.bronze_operation_id OR prior.operation_id IS NOT NULL
  OR EXISTS(SELECT 1 FROM content_pipeline_operations WHERE attempt_id=NEW.attempt_id) THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PREDECESSOR_REQUIRED',ERRCODE='23514'; END IF;
 ELSE
  IF prior.operation_id IS NULL OR NEW.predecessor_operation_id<>prior.operation_id
  OR NEW.bronze_operation_id<>prior.bronze_operation_id OR NEW.silver_reservation_id<>prior.silver_reservation_id
  OR NEW.profile_id<>prior.profile_id OR NEW.profile_sha256<>prior.profile_sha256
  OR NOT EXISTS(SELECT 1 FROM content_pipeline_checkpoints WHERE operation_id=prior.operation_id)
  OR NEW.owner_coordinator_version<prior.owner_coordinator_version OR NEW.owner_worker_version<prior.owner_worker_version THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PREDECESSOR_REQUIRED',ERRCODE='23514'; END IF;
 END IF;
 -- Require the exact earlier launch registrations, not merely another profile with similar fields.
 IF NOT EXISTS(SELECT 1 FROM content_native_launch_phases WHERE operation_id=b.fetch_operation_id
   AND phase='LAUNCH_BOUND' AND launch_profile_id=profile->>'fetchLaunchProfileId')
 OR NOT EXISTS(SELECT 1 FROM content_bronze_launch_phases WHERE operation_id=b.operation_id
   AND phase='LAUNCH_BOUND' AND launch_profile_id=profile->>'bronzeLaunchProfileId')
 OR NOT EXISTS(SELECT 1 FROM content_bronze_verification_phases WHERE operation_id=b.operation_id
   AND phase='VERIFY_BOUND' AND verification_profile_id=profile->>'bronzeVerificationProfileId') THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PROFILE_BINDING_INVALID',ERRCODE='23514'; END IF;
 selected:=convert_from(r.captured_selection,'UTF8')::jsonb->'sources'->0;
 IF selected->>'sourceId' IS DISTINCT FROM admission->>'sourceId'
 OR NOT EXISTS(SELECT 1 FROM run_sources rs JOIN source_revisions sr USING(product_id,source_id,revision)
 WHERE rs.run_id=r.run_id AND rs.source_id=selected->>'sourceId' AND to_jsonb(rs.revision)=selected->'revision'
 AND sr.configuration_sha256::text=selected->>'configurationSha256'
 AND convert_from(sr.configuration_bytes,'UTF8')::jsonb=selected->'configuration') THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_SOURCE_INVALID',ERRCODE='23514'; END IF;
 runtime_id:=profile->>'contentRuntimeId'; runtime_sha:=profile->>'contentRuntimeSha256';
 FOREACH family IN ARRAY ARRAY['CONTENT','GOLD','SILVER','BRONZE'] LOOP
  SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO runtime,digest
   FROM execution_profiles WHERE profile_id=runtime_id;
  IF runtime IS NULL OR digest IS DISTINCT FROM runtime_sha THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_RUNTIME_INVALID',ERRCODE='23514'; END IF;
  EXIT WHEN family=s.family;
  next_family:=CASE family WHEN 'CONTENT' THEN 'gold' WHEN 'GOLD' THEN 'silver' ELSE 'bronze' END;
  runtime_id:=runtime->>(next_family||'RuntimeId'); runtime_sha:=runtime->>(next_family||'RuntimeSha256');
 END LOOP;
 n:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 fields:=ARRAY['protocolVersion','mode','runId','attemptId','ownerEpoch','checkpointId','runtimeSha256'];
 fields:=fields||CASE s.family WHEN 'BRONZE' THEN ARRAY['receipt','bronze']
 WHEN 'SILVER' THEN ARRAY['receipt','artifact','binding','bronze','runtime','expectedCapture']
 WHEN 'GOLD' THEN ARRAY['inputSha256','silverBindingId','inputs','silver','gold','runtime','prepared','verified','projectionContext']
 ELSE ARRAY['inputSha256','goldProof','reference','referenceBindingId','referenceBinding','content','runtime','prepared','verified','projectionContext'] END;
 IF NOT native_reservation_json_matches(convert_from(NEW.command_bytes,'UTF8')::json,n)
 OR NOT n ?& fields OR n-fields<>'{}'::jsonb
 OR n->'protocolVersion' IS DISTINCT FROM '2'::jsonb OR n->>'protocolVersion' IS DISTINCT FROM '2' OR n->>'mode' IS DISTINCT FROM s.mode
 OR n->>'runId' IS DISTINCT FROM a.run_id::text OR n->>'attemptId' IS DISTINCT FROM a.attempt_id::text
 OR n->'ownerEpoch' IS DISTINCT FROM to_jsonb(a.owner_epoch) OR n->>'ownerEpoch' IS DISTINCT FROM a.owner_epoch::text
 OR n->>'checkpointId' IS DISTINCT FROM NEW.native_checkpoint_id::text
 OR n->>'runtimeSha256' IS DISTINCT FROM runtime_sha
 OR (s.family IN ('BRONZE','SILVER') AND octet_length(NEW.command_bytes)>65536) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_COMMAND_INVALID',ERRCODE='23514'; END IF;
 IF s.family IN ('BRONZE','SILVER') AND n->'receipt' IS DISTINCT FROM
  (SELECT convert_from(receipt_bytes,'UTF8')::jsonb FROM content_bronze_checkpoints WHERE operation_id=b.operation_id) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_RECEIPT_INVALID',ERRCODE='23514'; END IF;
 IF s.family IN ('GOLD','CONTENT') AND
 (n->'projectionContext' IS DISTINCT FROM jsonb_build_object('projectionContextVersion',1,'policy','ddip-content-narrow-v1',
 'kind',s.family,'outputBindingId',runtime->>(CASE s.family WHEN 'GOLD' THEN 'goldBindingId' ELSE 'contentBindingId' END))) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PROJECTION_INVALID',ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER content_pipeline_operation_insert BEFORE INSERT ON content_pipeline_operations
 FOR EACH ROW EXECUTE FUNCTION guard_content_pipeline_operation();

CREATE FUNCTION require_content_pipeline_owner(op uuid,cv bigint,wv bigint,rv bigint,av bigint) RETURNS void
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE o content_pipeline_operations%ROWTYPE;
BEGIN
 IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_ISOLATION_REQUIRED',ERRCODE='23514'; END IF;
 PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
 SELECT * INTO o FROM content_pipeline_operations WHERE operation_id=op;
 IF o.operation_id IS NULL THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_OPERATION_REQUIRED',ERRCODE='23514'; END IF;
 PERFORM require_content_bronze_checkpoint_owner(o.bronze_operation_id,cv,wv,rv,av);
 IF cv<o.owner_coordinator_version OR wv<o.owner_worker_version
 OR rv<>o.owner_run_version OR av<>o.owner_attempt_version
 OR EXISTS(SELECT 1 FROM content_pipeline_completions WHERE attempt_id=o.attempt_id) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_OWNER_REQUIRED',ERRCODE='23514'; END IF;
END $$;
CREATE FUNCTION guard_content_pipeline_phase() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE o content_pipeline_operations%ROWTYPE; n jsonb; previous text; p content_pipeline_phases%ROWTYPE; expected jsonb;
BEGIN
 PERFORM require_content_pipeline_owner(NEW.operation_id,NEW.owner_coordinator_version,
 NEW.owner_worker_version,NEW.owner_run_version,NEW.owner_attempt_version);
 SELECT * INTO o FROM content_pipeline_operations WHERE operation_id=NEW.operation_id;
 previous:=CASE NEW.phase WHEN 'FILES_READY' THEN 'LAUNCH_BOUND' WHEN 'START_AUTHORIZED' THEN 'FILES_READY'
 WHEN 'CHILD_RECORDED' THEN 'START_AUTHORIZED' WHEN 'RELEASE_AUTHORIZED' THEN 'CHILD_RECORDED'
 WHEN 'CANDIDATE_EXITED' THEN 'RELEASE_AUTHORIZED' ELSE NULL END;
 SELECT * INTO p FROM content_pipeline_phases WHERE operation_id=NEW.operation_id AND phase=previous;
 IF (previous IS NULL AND EXISTS(SELECT 1 FROM content_pipeline_phases WHERE operation_id=NEW.operation_id))
 OR (previous IS NOT NULL AND (p.operation_id IS NULL OR NEW.owner_coordinator_version<p.owner_coordinator_version
 OR NEW.owner_worker_version<p.owner_worker_version))
 OR EXISTS(SELECT 1 FROM content_pipeline_checkpoints WHERE operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PHASE_ORDER_INVALID',ERRCODE='23514'; END IF;
 n:=convert_from(NEW.phase_bytes,'UTF8')::jsonb;
 expected:=jsonb_build_object('phaseVersion',1,'operationId',o.operation_id::text,'phase',NEW.phase,
 'profileSha256',o.profile_sha256::text,'commandSha256',o.command_sha256::text,
 'child',CASE WHEN NEW.phase='CHILD_RECORDED' THEN n->'child' ELSE 'null'::jsonb END,
 'bootstrapSha256',CASE WHEN NEW.phase='RELEASE_AUTHORIZED' THEN n->'bootstrapSha256' ELSE 'null'::jsonb END,
 'candidateSha256',CASE WHEN NEW.phase='CANDIDATE_EXITED' THEN n->'candidateSha256' ELSE 'null'::jsonb END);
 IF NOT native_reservation_json_matches(convert_from(NEW.phase_bytes,'UTF8')::json,expected) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PHASE_INVALID',ERRCODE='23514'; END IF;
 IF NEW.phase='CHILD_RECORDED' AND
 (jsonb_typeof(n->'child') IS DISTINCT FROM 'object'
 OR NOT n->'child' ?& ARRAY['instanceId','pid','startedAt'] OR (n->'child')-ARRAY['instanceId','pid','startedAt']<>'{}'::jsonb
 OR jsonb_typeof(n->'child'->'instanceId') IS DISTINCT FROM 'string'
 OR n->'child'->>'instanceId' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
 OR jsonb_typeof(n->'child'->'pid') IS DISTINCT FROM 'string' OR n->'child'->>'pid' !~ '^[1-9][0-9]{0,18}$'
 OR jsonb_typeof(n->'child'->'startedAt') IS DISTINCT FROM 'string' OR length(n->'child'->>'startedAt') NOT BETWEEN 1 AND 64) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_CHILD_INVALID',ERRCODE='23514'; END IF;
 IF (NEW.phase='RELEASE_AUTHORIZED' AND (jsonb_typeof(n->'bootstrapSha256') IS DISTINCT FROM 'string' OR n->>'bootstrapSha256' !~ '^[0-9a-f]{64}$'))
 OR (NEW.phase='CANDIDATE_EXITED' AND (jsonb_typeof(n->'candidateSha256') IS DISTINCT FROM 'string' OR n->>'candidateSha256' !~ '^[0-9a-f]{64}$')) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PHASE_DIGEST_INVALID',ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER content_pipeline_phase_insert BEFORE INSERT ON content_pipeline_phases
 FOR EACH ROW EXECUTE FUNCTION guard_content_pipeline_phase();

CREATE FUNCTION guard_content_pipeline_checkpoint() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE o content_pipeline_operations%ROWTYPE; candidate_phase content_pipeline_phases%ROWTYPE;
 s record; n jsonb; result jsonb; evidence jsonb; selection jsonb; expected jsonb; command jsonb; profile jsonb; fields text[];
BEGIN
 PERFORM require_content_pipeline_owner(NEW.operation_id,NEW.owner_coordinator_version,
 NEW.owner_worker_version,NEW.owner_run_version,NEW.owner_attempt_version);
 SELECT * INTO o FROM content_pipeline_operations WHERE operation_id=NEW.operation_id;
 SELECT * INTO s FROM content_pipeline_step(o.step_index);
 SELECT * INTO candidate_phase FROM content_pipeline_phases WHERE operation_id=NEW.operation_id AND phase='CANDIDATE_EXITED';
 IF candidate_phase.operation_id IS NULL OR (SELECT count(*) FROM content_pipeline_phases WHERE operation_id=NEW.operation_id)<>6
 OR NEW.owner_coordinator_version<candidate_phase.owner_coordinator_version OR NEW.owner_worker_version<candidate_phase.owner_worker_version THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_CANDIDATE_REQUIRED',ERRCODE='23514'; END IF;
 command:=convert_from(o.command_bytes,'UTF8')::jsonb;
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO profile FROM content_pipeline_profiles WHERE profile_id=o.profile_id;
 result:=convert_from(NEW.result_bytes,'UTF8')::jsonb;
 evidence:=convert_from(NEW.evidence_bytes,'UTF8')::jsonb;
 selection:=convert_from(NEW.selection_bytes,'UTF8')::jsonb;
 n:=convert_from(NEW.proof_bytes,'UTF8')::jsonb;
 expected:=jsonb_build_object('pipelineCheckpointVersion',1,'operationId',o.operation_id::text,'stepIndex',o.step_index,
 'commandSha256',o.command_sha256::text,'candidateSha256',convert_from(candidate_phase.phase_bytes,'UTF8')::jsonb->'candidateSha256',
 'result',result,'evidence',evidence,'selection',selection);
 fields:=ARRAY['protocolVersion','family','mode','runId','attemptId','ownerEpoch','checkpointId','nativeCheckpointId',
 'runtimeSha256','strictRuntimeSha256','commandSha256','candidateOnly','status','evidenceSha256','selectionSha256','cleanup','code'];
 IF NOT native_reservation_json_matches(convert_from(NEW.proof_bytes,'UTF8')::json,expected)
 OR NOT native_reservation_json_matches(convert_from(NEW.result_bytes,'UTF8')::json,result)
 OR NOT result ?& fields OR result-fields<>'{}'::jsonb
 OR result->'protocolVersion' IS DISTINCT FROM '2'::jsonb OR result->>'protocolVersion' IS DISTINCT FROM '2' OR result->'candidateOnly' IS DISTINCT FROM 'true'::jsonb
 OR result->>'family' IS DISTINCT FROM s.family OR result->>'mode' IS DISTINCT FROM s.mode
 OR result->'runId' IS DISTINCT FROM command->'runId' OR result->'attemptId' IS DISTINCT FROM command->'attemptId'
 OR result->'ownerEpoch' IS DISTINCT FROM command->'ownerEpoch' OR result->>'ownerEpoch' IS DISTINCT FROM command->>'ownerEpoch'
 OR result->>'checkpointId' IS DISTINCT FROM o.checkpoint_id::text
 OR result->>'nativeCheckpointId' IS DISTINCT FROM o.native_checkpoint_id::text
 OR result->'runtimeSha256' IS DISTINCT FROM command->'runtimeSha256'
 OR result->'strictRuntimeSha256' IS DISTINCT FROM profile->'strictRuntimeSha256'
 OR result->>'commandSha256' IS DISTINCT FROM o.command_sha256::text
 OR result->>'evidenceSha256' IS DISTINCT FROM encode(sha256(NEW.evidence_bytes),'hex')
 OR result->>'selectionSha256' IS DISTINCT FROM encode(sha256(NEW.selection_bytes),'hex')
 OR result->'code' IS DISTINCT FROM 'null'::jsonb
 OR result->>'status' IS DISTINCT FROM (CASE WHEN s.family='BRONZE' THEN 'VERIFIED'
 WHEN s.family='SILVER' AND s.mode='PUBLISH' THEN 'PROCESSED'
 WHEN s.mode='RECHECK' AND s.family<>'CONTENT' THEN 'UNCHANGED' WHEN s.mode='PREPARE' THEN 'PREPARED' ELSE 'VERIFIED' END)
 OR jsonb_typeof(result->'cleanup') IS DISTINCT FROM 'object'
 OR NOT result->'cleanup' ?& ARRAY['status','gatewayPid'] OR (result->'cleanup')-ARRAY['status','gatewayPid']<>'{}'::jsonb
 OR result->'cleanup'->>'status' IS NULL OR result->'cleanup'->>'status' NOT IN ('NOT_STARTED','CONFIRMED') THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_CHECKPOINT_INVALID',ERRCODE='23514'; END IF;
 IF (NEW.evidence_bytes IS NOT NULL AND NOT native_reservation_json_matches(convert_from(NEW.evidence_bytes,'UTF8')::json,evidence))
 OR (NEW.selection_bytes IS NOT NULL AND NOT native_reservation_json_matches(convert_from(NEW.selection_bytes,'UTF8')::json,selection))
 OR (s.family='BRONZE' AND (NEW.evidence_bytes IS NULL OR octet_length(NEW.evidence_bytes)>65536 OR NEW.selection_bytes IS NOT NULL))
 OR (s.family='SILVER' AND s.mode='PUBLISH' AND (NEW.evidence_bytes IS NOT NULL OR NEW.selection_bytes IS NOT NULL))
 OR (s.family='SILVER' AND s.mode='VERIFY' AND (NEW.evidence_bytes IS NULL OR octet_length(NEW.evidence_bytes)>61440))
 OR (s.family='SILVER' AND s.mode='VERIFY' AND (NEW.selection_bytes IS NULL OR octet_length(NEW.selection_bytes)>61440))
 OR (s.family='SILVER' AND s.mode='RECHECK' AND (NEW.selection_bytes IS NOT NULL OR NEW.evidence_bytes IS NOT NULL))
 OR (s.family IN ('GOLD','CONTENT') AND (NEW.evidence_bytes IS NULL OR NEW.selection_bytes IS NOT NULL)) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_EVIDENCE_INVALID',ERRCODE='23514'; END IF;
 IF (s.family='BRONZE' AND evidence IS DISTINCT FROM jsonb_build_object('status','VERIFIED','receipt',command->'receipt'))
 OR (s.mode='PREPARE' AND (evidence->>'status' IS DISTINCT FROM 'PREPARED' OR jsonb_typeof(evidence->'prepared') IS DISTINCT FROM 'object'))
 OR (s.family='GOLD' AND s.mode='RECHECK' AND evidence IS DISTINCT FROM jsonb_build_object('status','UNCHANGED'))
 OR (o.step_index IN (10,15,16) AND evidence IS DISTINCT FROM (SELECT convert_from(c.evidence_bytes,'UTF8')::jsonb
 FROM content_pipeline_operations p JOIN content_pipeline_checkpoints c USING(operation_id)
 WHERE p.attempt_id=o.attempt_id AND p.step_index=CASE o.step_index WHEN 10 THEN 9 WHEN 15 THEN 14 ELSE 15 END))
 OR (s.family IN ('GOLD','CONTENT') AND s.mode IN ('PUBLISH','VERIFY') AND
 (evidence->>'status' IS DISTINCT FROM 'VERIFIED' OR evidence->'prepared' IS DISTINCT FROM command->'prepared')) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_NATIVE_EVIDENCE_INVALID',ERRCODE='23514'; END IF;
 IF (s.family='BRONZE' OR (s.family IN ('SILVER','GOLD') AND s.mode='RECHECK') OR (s.family='CONTENT' AND s.mode='PREPARE')) THEN
  IF result->'cleanup' IS DISTINCT FROM jsonb_build_object('status','NOT_STARTED','gatewayPid',NULL) THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_CLEANUP_INVALID',ERRCODE='23514'; END IF;
 ELSE
  IF result->'cleanup'->>'status' IS DISTINCT FROM 'CONFIRMED'
   OR jsonb_typeof(result->'cleanup'->'gatewayPid') IS DISTINCT FROM 'string'
   OR result->'cleanup'->>'gatewayPid' !~ '^[1-9][0-9]{0,18}$' THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_CLEANUP_INVALID',ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER content_pipeline_checkpoint_insert BEFORE INSERT ON content_pipeline_checkpoints
 FOR EACH ROW EXECUTE FUNCTION guard_content_pipeline_checkpoint();

CREATE FUNCTION guard_content_pipeline_completion() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE o content_pipeline_operations%ROWTYPE; a attempts%ROWTYPE; r runs%ROWTYPE;
 b content_bronze_reservations%ROWTYPE; fetched content_fetch_checkpoints%ROWTYPE;
 bronze content_bronze_checkpoints%ROWTYPE; artifact artifacts%ROWTYPE; gold artifacts%ROWTYPE; silver artifacts%ROWTYPE;
 reference artifacts%ROWTYPE; n jsonb; expected jsonb; operations jsonb; verified jsonb; command jsonb;
 gold_command jsonb; silver_input jsonb; gold_evidence jsonb; artifact_evidence jsonb; ref jsonb; qualification jsonb;
BEGIN
 PERFORM require_content_pipeline_owner(NEW.final_operation_id,NEW.owner_coordinator_version,
 NEW.owner_worker_version,NEW.owner_run_version,NEW.owner_attempt_version);
 SELECT * INTO o FROM content_pipeline_operations WHERE operation_id=NEW.final_operation_id;
 SELECT * INTO a FROM attempts WHERE attempt_id=NEW.attempt_id;
 SELECT * INTO r FROM runs WHERE run_id=a.run_id;
 SELECT * INTO b FROM content_bronze_reservations WHERE operation_id=o.bronze_operation_id;
 SELECT * INTO fetched FROM content_fetch_checkpoints WHERE operation_id=b.fetch_operation_id;
 SELECT * INTO bronze FROM content_bronze_checkpoints WHERE operation_id=b.operation_id;
 SELECT * INTO artifact FROM artifacts WHERE artifact_id=NEW.artifact_id;
 IF o.attempt_id IS DISTINCT FROM NEW.attempt_id OR o.step_index<>16
 OR (SELECT count(*) FROM content_pipeline_operations WHERE attempt_id=NEW.attempt_id)<>15
 OR (SELECT count(*) FROM content_pipeline_checkpoints c JOIN content_pipeline_operations p USING(operation_id)
 WHERE p.attempt_id=NEW.attempt_id)<>15
 OR EXISTS(SELECT 1 FROM content_pipeline_operations p LEFT JOIN content_pipeline_operations previous
 ON previous.attempt_id=p.attempt_id AND previous.step_index=p.step_index-1
 WHERE p.attempt_id=NEW.attempt_id AND (p.profile_id<>o.profile_id OR p.profile_sha256<>o.profile_sha256
 OR p.bronze_operation_id<>o.bronze_operation_id OR p.silver_reservation_id<>o.silver_reservation_id
 OR p.predecessor_operation_id IS DISTINCT FROM CASE WHEN p.step_index=2 THEN o.bronze_operation_id ELSE previous.operation_id END)) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_COMPLETION_CHAIN_REQUIRED',ERRCODE='23514'; END IF;
 SELECT jsonb_agg(jsonb_build_object('operationId',p.operation_id::text,'stepIndex',p.step_index,
 'proofSha256',c.proof_sha256::text) ORDER BY p.step_index) INTO operations
 FROM content_pipeline_operations p JOIN content_pipeline_checkpoints c USING(operation_id) WHERE p.attempt_id=NEW.attempt_id;
 expected:=jsonb_build_object('pipelineCompletionVersion',1,'runId',a.run_id::text,'attemptId',a.attempt_id::text,
 'fetchOperationId',fetched.operation_id::text,'fetchCheckpointSha256',fetched.completion_sha256::text,
 'bronzeOperationId',bronze.operation_id::text,'bronzeVerifiedSha256',bronze.verified_sha256::text,
 'operations',operations,'artifactId',NEW.artifact_id::text,'artifactSha256',artifact.evidence_sha256::text);
 IF NOT native_reservation_json_matches(convert_from(NEW.proof_bytes,'UTF8')::json,expected) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_COMPLETION_PROOF_INVALID',ERRCODE='23514'; END IF;
 SELECT convert_from(c.evidence_bytes,'UTF8')::jsonb,convert_from(p.command_bytes,'UTF8')::jsonb INTO verified,command
 FROM content_pipeline_operations p JOIN content_pipeline_checkpoints c USING(operation_id)
 WHERE p.attempt_id=NEW.attempt_id AND p.step_index=15;
 -- Final recheck is bound to precisely the independent verification being published.
 IF convert_from(o.command_bytes,'UTF8')::jsonb->'verified' IS DISTINCT FROM verified THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_FINAL_RECHECK_INVALID',ERRCODE='23514'; END IF;
 artifact_evidence:=convert_from(artifact.evidence_bytes,'UTF8')::jsonb;
 SELECT * INTO gold FROM artifacts WHERE artifact_id=(command->'goldProof'->>'artifactId')::uuid;
 SELECT * INTO reference FROM artifacts WHERE artifact_id=(command->'reference'->>'artifactId')::uuid;
 gold_command:=command->'goldProof'->'command'; silver_input:=gold_command->'inputs'->0;
 SELECT * INTO silver FROM artifacts WHERE artifact_id=(silver_input->>'artifactId')::uuid;
 gold_evidence:=convert_from(gold.evidence_bytes,'UTF8')::jsonb;
 qualification:=convert_from(silver.evidence_bytes,'UTF8')::jsonb;
 ref:=command->'reference';
 expected:=jsonb_build_object('managedContentVersion',1,'kind','CONTENT','productId','content-intelligence',
 'contractVersion',1,'processorRevision',1,'nativeFingerprint',command->'prepared'->'fingerprint',
 'storageBindingId',artifact.storage_binding_id::text,
 'relativeAddress','v1/processor_revision=1/publication='||(command->'prepared'->>'fingerprint'),
 'verification',verified,'inputs',jsonb_build_array(
 jsonb_build_object('role','GOLD','artifactId',gold.artifact_id::text,'evidenceSha256',gold.evidence_sha256::text),
 jsonb_build_object('role','PUBLISHER_REFERENCE','artifactId',reference.artifact_id::text,'evidenceSha256',reference.evidence_sha256::text)));
 IF artifact.artifact_id IS NULL OR gold.artifact_id IS NULL OR silver.artifact_id IS NULL OR reference.artifact_id IS NULL
 OR artifact.kind<>'CONTENT' OR gold.kind<>'GOLD' OR silver.kind<>'SILVER' OR reference.kind<>'PUBLISHER_REFERENCE'
 OR artifact.product_id<>'content-intelligence' OR gold.product_id<>artifact.product_id OR silver.product_id<>artifact.product_id OR reference.product_id<>artifact.product_id
 OR artifact.contract_version<>1 OR artifact.processor_revision<>1 OR gold.contract_version<>1 OR gold.processor_revision<>1
 OR silver.contract_version<>1 OR silver.processor_revision<>1
 OR artifact.native_identity IS DISTINCT FROM command->'prepared'->>'fingerprint'
 OR artifact.relative_address::text IS DISTINCT FROM expected->>'relativeAddress'
 OR NOT native_reservation_json_matches(convert_from(artifact.evidence_bytes,'UTF8')::json,expected)
 OR gold_evidence IS DISTINCT FROM command->'goldProof'->'artifact'
 OR gold.evidence_sha256::text IS DISTINCT FROM command->'goldProof'->>'evidenceSha256'
 OR silver.evidence_sha256::text IS DISTINCT FROM silver_input->>'evidenceSha256'
 OR qualification IS DISTINCT FROM silver_input->'qualification'
 OR qualification->'qualificationVersion' IS DISTINCT FROM '1'::jsonb
 OR reference.evidence_sha256::text IS DISTINCT FROM ref->>'evidenceSha256'
 OR convert_from(reference.evidence_bytes,'UTF8')::jsonb IS DISTINCT FROM ref->'qualification'
 OR silver_input->>'originRunId' IS DISTINCT FROM a.run_id::text
 OR silver_input->'receipt' IS DISTINCT FROM convert_from(bronze.receipt_bytes,'UTF8')::jsonb
 OR jsonb_array_length(gold_command->'inputs')<>1 THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_ARTIFACT_INVALID',ERRCODE='23514'; END IF;
 expected:=jsonb_build_object('managedGoldVersion',1,'kind','GOLD','productId','content-intelligence',
 'contractVersion',1,'processorRevision',1,'nativeFingerprint',gold_command->'prepared'->'inputSetFingerprint',
 'storageBindingId',gold_command->'projectionContext'->'outputBindingId',
 'relativeAddress','v1/processor_revision=1/input_set='||(gold_command->'prepared'->>'inputSetFingerprint'),
 'verification',gold_evidence->'verification','inputs',jsonb_build_array(jsonb_build_object(
 'artifactId',silver.artifact_id::text,'evidenceSha256',silver.evidence_sha256::text)));
 IF NOT native_reservation_json_matches(convert_from(gold.evidence_bytes,'UTF8')::json,expected) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_GOLD_ARTIFACT_INVALID',ERRCODE='23514'; END IF;
 IF qualification->'verification' IS DISTINCT FROM (SELECT convert_from(c.evidence_bytes,'UTF8')::jsonb
 FROM content_pipeline_checkpoints c JOIN content_pipeline_operations p USING(operation_id) WHERE p.attempt_id=a.attempt_id AND p.step_index=4)
 OR qualification->'selection' IS DISTINCT FROM (SELECT convert_from(command_bytes,'UTF8')::jsonb->'artifact'
 FROM content_pipeline_operations WHERE attempt_id=a.attempt_id AND step_index=4)
 OR gold_evidence->'verification' IS DISTINCT FROM (SELECT convert_from(c.evidence_bytes,'UTF8')::jsonb
 FROM content_pipeline_checkpoints c JOIN content_pipeline_operations p USING(operation_id) WHERE p.attempt_id=a.attempt_id AND p.step_index=10)
 OR gold_command IS DISTINCT FROM (SELECT convert_from(command_bytes,'UTF8')::jsonb FROM content_pipeline_operations WHERE attempt_id=a.attempt_id AND step_index=9)
 OR NOT EXISTS(SELECT 1 FROM run_inputs WHERE run_id=a.run_id AND role='PUBLISHER_REFERENCE' AND ordinal=0
 AND artifact_id=reference.artifact_id AND convert_from(evidence_bytes,'UTF8')::jsonb=ref)
 OR artifact.storage_binding_id::text IS DISTINCT FROM command->'projectionContext'->>'outputBindingId'
 OR gold.storage_binding_id::text IS DISTINCT FROM gold_command->'projectionContext'->>'outputBindingId'
 OR gold.native_identity IS DISTINCT FROM gold_evidence->>'nativeFingerprint'
 OR gold.relative_address::text IS DISTINCT FROM gold_evidence->>'relativeAddress'
 OR silver.storage_binding_id::text IS DISTINCT FROM qualification->'selection'->>'storageBindingId'
 OR silver.relative_address::text IS DISTINCT FROM qualification->'selection'->>'relativeAddress'
 OR silver.native_identity IS DISTINCT FROM qualification->'verification'->'inventoryIdentity'->>'sha256'
 OR (SELECT count(*) FROM artifact_dependencies WHERE artifact_id=artifact.artifact_id)<>2
 OR NOT EXISTS(SELECT 1 FROM artifact_dependencies WHERE artifact_id=artifact.artifact_id AND role='GOLD' AND ordinal=0 AND dependency_artifact_id=gold.artifact_id)
 OR NOT EXISTS(SELECT 1 FROM artifact_dependencies WHERE artifact_id=artifact.artifact_id AND role='PUBLISHER_REFERENCE' AND ordinal=0 AND dependency_artifact_id=reference.artifact_id)
 OR EXISTS(SELECT 1 FROM artifact_dependencies WHERE artifact_id IN (silver.artifact_id,reference.artifact_id))
 OR (SELECT count(*) FROM artifact_dependencies WHERE artifact_id=gold.artifact_id)<>1
 OR NOT EXISTS(SELECT 1 FROM artifact_dependencies WHERE artifact_id=gold.artifact_id AND role='SILVER' AND ordinal=0 AND dependency_artifact_id=silver.artifact_id) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_DEPENDENCIES_INVALID',ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER content_pipeline_completion_insert BEFORE INSERT ON content_pipeline_completions
 FOR EACH ROW EXECUTE FUNCTION guard_content_pipeline_completion();

-- The only changed V10 branch: completion-backed original CLAIMED -> SUCCEEDED.
-- The completion insert has already fenced the live owner; commit validation below requires
-- all terminal rows and release in this same transaction. Existing recovery paths are untouched.
CREATE OR REPLACE FUNCTION guard_content_attempt_progress() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
BEGIN
 IF NEW.content_admission_policy IS NULL THEN RETURN NEW; END IF;
 PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
 IF TG_OP='INSERT' THEN
  IF NEW.state<>'CLAIMED' THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_INITIAL_STATE_INVALID',ERRCODE='23514'; END IF;
 ELSE
  IF NEW.state='RUNNING' AND NOT EXISTS(SELECT 1 FROM content_admission_milestones WHERE attempt_id=NEW.attempt_id AND phase='READY') THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_NOT_READY',ERRCODE='23514'; END IF;
  IF OLD.state='CLAIMED' AND NEW.state IN ('FAILED','CANCELLED','SUCCEEDED')
  AND EXISTS(SELECT 1 FROM content_admission_milestones WHERE attempt_id=NEW.attempt_id AND phase='PREPARING')
  AND NOT (NEW.state='SUCCEEDED' AND EXISTS(SELECT 1 FROM content_pipeline_completions c
    WHERE c.attempt_id=NEW.attempt_id AND c.owner_attempt_version=OLD.row_version
    AND NEW.row_version=OLD.row_version+1 AND NEW.finished_at IS NOT NULL)) THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_PREPARATION_RETAINED',ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION guard_content_pipeline_success() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE target uuid;
BEGIN
 IF TG_TABLE_NAME='attempts' THEN
  IF NEW.content_admission_policy IS NULL OR NEW.state<>'SUCCEEDED' THEN RETURN NEW; END IF;
  target:=NEW.attempt_id;
 ELSE
  IF NEW.state<>'SUCCEEDED' OR NOT EXISTS(SELECT 1 FROM attempts WHERE attempt_id=NEW.current_attempt_id AND content_admission_policy IS NOT NULL) THEN RETURN NEW; END IF;
  target:=NEW.current_attempt_id;
 END IF;
 PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
 IF NOT EXISTS(SELECT 1 FROM content_pipeline_completions WHERE attempt_id=target) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_COMPLETION_REQUIRED',ERRCODE='23514'; END IF;
 -- Keep table-specific record fields in a separate PL/pgSQL branch: SQL boolean
 -- short-circuiting does not defer field resolution for the attempts trigger record.
 IF TG_TABLE_NAME='runs' THEN
  IF NOT EXISTS(SELECT 1 FROM content_pipeline_completions WHERE attempt_id=target
   AND artifact_id=NEW.result_artifact_id AND NEW.row_version=owner_run_version+1) THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_RESULT_REQUIRED',ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER content_pipeline_success BEFORE UPDATE ON attempts FOR EACH ROW EXECUTE FUNCTION guard_content_pipeline_success();
CREATE TRIGGER content_pipeline_success BEFORE UPDATE ON runs FOR EACH ROW EXECUTE FUNCTION guard_content_pipeline_success();

CREATE FUNCTION check_content_pipeline_completion_commit() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE a attempts%ROWTYPE; r runs%ROWTYPE; c coordinator%ROWTYPE; counts jsonb; expected_counts jsonb;
BEGIN
 SELECT * INTO a FROM attempts WHERE attempt_id=NEW.attempt_id;
 SELECT * INTO r FROM runs WHERE run_id=a.run_id;
 SELECT * INTO c FROM coordinator WHERE singleton_id=1 FOR UPDATE;
 SELECT convert_from(evidence_bytes,'UTF8')::jsonb->'verification'->'completion'->'output_counts' INTO counts
 FROM artifacts WHERE artifact_id=NEW.artifact_id;
 expected_counts:=jsonb_build_object('articles',counts->>'content_catalog_v1','rows',counts->>'content_source_discovery_daily_v1');
 IF counts IS NULL OR jsonb_typeof(counts->'content_catalog_v1') IS DISTINCT FROM 'number'
 OR jsonb_typeof(counts->'content_source_discovery_daily_v1') IS DISTINCT FROM 'number'
 OR counts->>'content_catalog_v1' !~ '^(0|[1-9][0-9]*)$'
 OR counts->>'content_source_discovery_daily_v1' !~ '^(0|[1-9][0-9]*)$'
 OR r.counts_bytes IS NULL
 OR NOT native_reservation_json_matches(convert_from(r.counts_bytes,'UTF8')::json,expected_counts) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_SUMMARY_COUNTS_INVALID',ERRCODE='23514'; END IF;
 IF a.state<>'SUCCEEDED' OR a.finished_at IS NULL OR a.row_version<>NEW.owner_attempt_version+1
 OR a.kind<>'EXECUTE' OR a.content_admission_policy IS DISTINCT FROM 'ddip-content-narrow-v1'
 OR r.state<>'SUCCEEDED' OR r.finished_at IS NULL OR r.row_version<>NEW.owner_run_version+1
 OR r.result_artifact_id IS DISTINCT FROM NEW.artifact_id OR r.current_attempt_id IS DISTINCT FROM a.attempt_id
 OR r.retry_eligible OR c.active_attempt_id IS NOT NULL OR c.owner_instance_id IS NOT NULL
 OR c.row_version<>NEW.owner_coordinator_version+1 OR c.mode<>'READY' OR c.active_import_id IS NOT NULL
 OR c.epoch<>a.owner_epoch
 OR NOT EXISTS(SELECT 1 FROM worker_instances WHERE instance_id=a.instance_id AND row_version=NEW.owner_worker_version AND status='READY')
 OR EXISTS(SELECT 1 FROM stage_invocations WHERE attempt_id=a.attempt_id)
 OR EXISTS(SELECT 1 FROM rss_deliveries WHERE run_id=a.run_id)
 OR EXISTS(SELECT 1 FROM controls WHERE run_id=a.run_id AND (state='REQUESTED' OR (type='CANCEL' AND state='APPLIED'))) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_ATOMIC_COMPLETION_REQUIRED',ERRCODE='23514'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER content_pipeline_completion_commit AFTER INSERT ON content_pipeline_completions
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_content_pipeline_completion_commit();

COMMENT ON TABLE content_pipeline_completions IS
 'Original live-owner full-chain terminal journal; actual artifact dependencies and atomic success/release required. Historical reads are never continuation authority.';

CREATE FUNCTION content_pipeline_root(binding jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE STRICT AS $$
 SELECT jsonb_build_object('root',binding->'configuration'->'root','rootDevice',binding->'configuration'->'rootDevice',
 'rootInode',binding->'configuration'->'rootInode')
$$;
-- Separate additive guard closes native payload substitution without changing historical commands.
CREATE FUNCTION guard_content_pipeline_command_dependencies() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE n jsonb; frozen jsonb; s record; role_name text; binding jsonb; bindings jsonb:='{}'::jsonb;
 old_command jsonb; expected jsonb; receipt jsonb; address text; silver_runtime jsonb; content_runtime jsonb;
 runtime jsonb; run_uuid uuid; profile jsonb; selected jsonb; artifact artifacts%ROWTYPE; key text;
BEGIN
 PERFORM require_content_bronze_checkpoint_owner(NEW.bronze_operation_id,NEW.owner_coordinator_version,
 NEW.owner_worker_version,NEW.owner_run_version,NEW.owner_attempt_version);
 SELECT r.run_id,convert_from(r.captured_selection,'UTF8')::jsonb INTO run_uuid,frozen
 FROM runs r JOIN attempts a USING(run_id) WHERE a.attempt_id=NEW.attempt_id;
 SELECT * INTO s FROM content_pipeline_step(NEW.step_index);
 n:=convert_from(NEW.command_bytes,'UTF8')::jsonb;
 FOREACH role_name IN ARRAY ARRAY['BRONZE','SILVER','TEMP','GOLD','CONTENT','REFERENCE'] LOOP
  SELECT value INTO binding FROM jsonb_array_elements(frozen->'storageBindings') WHERE value->>'role'=role_name;
  IF binding IS NULL OR (SELECT count(*) FROM jsonb_array_elements(frozen->'storageBindings') WHERE value->>'role'=role_name)<>1
  OR NOT EXISTS(SELECT 1 FROM storage_bindings WHERE binding_id=binding->>'bindingId'
   AND configuration_sha256::text=binding->>'configurationSha256'
   AND convert_from(configuration_bytes,'UTF8')::jsonb=binding->'configuration') THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_FROZEN_BINDING_INVALID',ERRCODE='23514'; END IF;
  bindings:=bindings||jsonb_build_object(role_name,binding);
 END LOOP;
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO profile FROM content_pipeline_profiles WHERE profile_id=NEW.profile_id;
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO content_runtime FROM execution_profiles WHERE profile_id=profile->>'contentRuntimeId';
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO runtime FROM execution_profiles WHERE profile_id=content_runtime->>'goldRuntimeId';
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO silver_runtime FROM execution_profiles WHERE profile_id=runtime->>'silverRuntimeId';
 IF s.family<>'BRONZE' AND n->'runtime' IS DISTINCT FROM jsonb_build_object('javaHome',silver_runtime->'javaHome',
 'scratchRoot',bindings->'TEMP'->'configuration'->'root','scratchDevice',bindings->'TEMP'->'configuration'->'rootDevice',
 'scratchInode',bindings->'TEMP'->'configuration'->'rootInode') THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_NATIVE_RUNTIME_INVALID',ERRCODE='23514'; END IF;
 IF s.family IN ('BRONZE','SILVER') AND n->'bronze' IS DISTINCT FROM bindings->'BRONZE'->'configuration' THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_BRONZE_BINDING_INVALID',ERRCODE='23514'; END IF;
 IF s.family='SILVER' THEN
  receipt:=n->'receipt';key:=receipt->'expected'->>'metadataKey';
  address:='v1/'||encode(sha256(convert_to(receipt->>'bucket','UTF8')),'hex')||'/'||
   substr(key,length('bronze/rss/')+1,length(key)-length('bronze/rss/')-length('metadata.json'))||'complete.json';
  expected:=jsonb_build_object('kind','SILVER','productId','content-intelligence','storageBindingId',silver_runtime->'outputBindingId',
   'relativeAddress',address,'contractVersion',1,'processorRevision',1,'identity',jsonb_build_object(
   'sourceId',receipt->'sourceId','ingestionId',receipt->'ingestionId','bronzeStorageBindingId',receipt->'storageBindingId',
   'bronzeBucket',receipt->'bucket','bronzeMetadataKey',receipt->'expected'->'metadataKey','bronzePayloadKey',receipt->'expected'->'payloadKey'));
  IF n->'artifact' IS DISTINCT FROM expected OR n->'binding' IS DISTINCT FROM content_pipeline_root(bindings->'SILVER')
   OR (s.mode<>'RECHECK' AND n->'expectedCapture' IS DISTINCT FROM 'null'::jsonb)
   OR (s.mode='RECHECK' AND n->'expectedCapture' IS DISTINCT FROM (SELECT convert_from(c.evidence_bytes,'UTF8')::jsonb->'capture'
    FROM content_pipeline_operations p JOIN content_pipeline_checkpoints c USING(operation_id) WHERE p.attempt_id=NEW.attempt_id AND p.step_index=4)) THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_SILVER_INPUT_INVALID',ERRCODE='23514'; END IF;
 END IF;
 IF s.family='GOLD' THEN
  selected:=n->'inputs'->0;
  SELECT * INTO artifact FROM artifacts WHERE artifact_id=(selected->>'artifactId')::uuid;
  IF n->'silver' IS DISTINCT FROM content_pipeline_root(bindings->'SILVER')
  OR n->'gold' IS DISTINCT FROM content_pipeline_root(bindings->'GOLD')
  OR n->'silverBindingId' IS DISTINCT FROM silver_runtime->'outputBindingId'
  OR jsonb_typeof(n->'inputs') IS DISTINCT FROM 'array' OR jsonb_array_length(n->'inputs')<>1
  OR artifact.artifact_id IS NULL OR artifact.kind<>'SILVER'
  OR selected->>'evidenceSha256' IS DISTINCT FROM artifact.evidence_sha256::text
  OR selected->'qualification' IS DISTINCT FROM convert_from(artifact.evidence_bytes,'UTF8')::jsonb
  OR selected->>'originRunId' IS DISTINCT FROM run_uuid::text
  OR selected->'sourceId' IS DISTINCT FROM frozen->'sources'->0->'sourceId'
  OR selected->'receipt' IS DISTINCT FROM (SELECT convert_from(receipt_bytes,'UTF8')::jsonb FROM content_bronze_checkpoints WHERE operation_id=NEW.bronze_operation_id)
  OR selected->'bronze' IS DISTINCT FROM bindings->'BRONZE'->'configuration'
  OR selected->'credentialsSha256' IS DISTINCT FROM (SELECT convert_from(command_bytes,'UTF8')::jsonb->'storage'->'credentialsSha256' FROM content_bronze_reservations WHERE operation_id=NEW.bronze_operation_id) THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_GOLD_INPUT_INVALID',ERRCODE='23514'; END IF;
 END IF;
 IF s.family='CONTENT' THEN
  SELECT * INTO artifact FROM artifacts WHERE artifact_id=(n->'goldProof'->>'artifactId')::uuid;
  IF artifact.artifact_id IS NULL OR artifact.kind<>'GOLD'
  OR n->'goldProof'->>'evidenceSha256' IS DISTINCT FROM artifact.evidence_sha256::text
  OR n->'goldProof'->'artifact' IS DISTINCT FROM convert_from(artifact.evidence_bytes,'UTF8')::jsonb
  OR n->'goldProof'->'command' IS DISTINCT FROM (SELECT convert_from(command_bytes,'UTF8')::jsonb FROM content_pipeline_operations WHERE attempt_id=NEW.attempt_id AND step_index=9)
  OR n->'referenceBindingId' IS DISTINCT FROM content_runtime->'referenceBindingId'
  OR n->'referenceBinding' IS DISTINCT FROM content_pipeline_root(bindings->'REFERENCE')
  OR n->'content' IS DISTINCT FROM content_pipeline_root(bindings->'CONTENT')
  OR NOT EXISTS(SELECT 1 FROM run_inputs i JOIN artifacts a USING(artifact_id) WHERE i.run_id=run_uuid
   AND i.role='PUBLISHER_REFERENCE' AND i.ordinal=0 AND convert_from(i.evidence_bytes,'UTF8')::jsonb=n->'reference'
   AND a.evidence_sha256::text=n->'reference'->>'evidenceSha256' AND convert_from(a.evidence_bytes,'UTF8')::jsonb=n->'reference'->'qualification') THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_CONTENT_INPUT_INVALID',ERRCODE='23514'; END IF;
 END IF;
 IF s.family IN ('GOLD','CONTENT') THEN
  IF s.mode='PREPARE' THEN
   IF n->'prepared' IS DISTINCT FROM 'null'::jsonb OR n->'verified' IS DISTINCT FROM 'null'::jsonb THEN
    RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PREPARATION_INVALID',ERRCODE='23514'; END IF;
  ELSE
   SELECT convert_from(p.command_bytes,'UTF8')::jsonb,convert_from(c.evidence_bytes,'UTF8')::jsonb->'prepared'
   INTO old_command,expected FROM content_pipeline_operations p JOIN content_pipeline_checkpoints c USING(operation_id)
   WHERE p.attempt_id=NEW.attempt_id AND p.step_index=CASE s.family WHEN 'GOLD' THEN 7 ELSE 13 END;
   IF n-ARRAY['mode','checkpointId','prepared','verified'] IS DISTINCT FROM old_command-ARRAY['mode','checkpointId','prepared','verified']
   OR n->'prepared' IS DISTINCT FROM expected
   OR (s.mode<>'RECHECK' AND n->'verified' IS DISTINCT FROM 'null'::jsonb)
   OR (s.mode='RECHECK' AND n->'verified' IS DISTINCT FROM (SELECT convert_from(c.evidence_bytes,'UTF8')::jsonb
    FROM content_pipeline_operations p JOIN content_pipeline_checkpoints c USING(operation_id) WHERE p.attempt_id=NEW.attempt_id AND p.step_index=CASE s.family WHEN 'GOLD' THEN 10 ELSE 15 END)) THEN
    RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PREPARATION_INVALID',ERRCODE='23514'; END IF;
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER content_pipeline_payload_insert BEFORE INSERT ON content_pipeline_operations
 FOR EACH ROW EXECUTE FUNCTION guard_content_pipeline_command_dependencies();

-- A completed dependency graph cannot acquire an extra edge after validation, including later
-- in the same transaction. Evidence rows are already immutable under V5.
CREATE INDEX content_pipeline_completion_artifact_idx ON content_pipeline_completions(artifact_id);
CREATE INDEX artifact_dependencies_reverse_idx ON artifact_dependencies(dependency_artifact_id,artifact_id);

CREATE FUNCTION guard_content_pipeline_completed_dependencies() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
BEGIN
 PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
 -- Walk backwards from the proposed edge's source using reverse indexes. Completion
 -- validates the fixed two-level graph and its leaves, so no historical closure scan is needed.
 IF EXISTS(SELECT 1 FROM content_pipeline_completions WHERE artifact_id=NEW.artifact_id)
 OR EXISTS(SELECT 1 FROM artifact_dependencies d JOIN content_pipeline_completions c ON c.artifact_id=d.artifact_id
   WHERE d.dependency_artifact_id=NEW.artifact_id)
 OR EXISTS(SELECT 1 FROM artifact_dependencies leaf
   JOIN artifact_dependencies parent ON parent.dependency_artifact_id=leaf.artifact_id
   JOIN content_pipeline_completions c ON c.artifact_id=parent.artifact_id
   WHERE leaf.dependency_artifact_id=NEW.artifact_id) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_COMPLETED_DEPENDENCIES_IMMUTABLE',ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER content_pipeline_completed_dependencies BEFORE INSERT ON artifact_dependencies
 FOR EACH ROW EXECUTE FUNCTION guard_content_pipeline_completed_dependencies();

CREATE FUNCTION guard_content_pipeline_legacy_exclusion() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE target uuid;
BEGIN
 -- Preserve V10's immediate rejection of an absent/invisible attempt. This trigger
 -- sorts before content_stage_readiness and must not wait on its uncommitted owner.
 IF TG_TABLE_NAME='stage_invocations' THEN
  SELECT run_id INTO target FROM attempts WHERE attempt_id=NEW.attempt_id;
  IF NOT FOUND THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_ADMISSION_ATTEMPT_REQUIRED',ERRCODE='23503'; END IF;
 ELSE target:=NEW.run_id; END IF;
 -- Match V16's run-enrollment fast path: legacy-only runs must not take the
 -- global coordinator lock. Snapshot writers first fence the run so a concurrent
 -- native claim cannot remain invisible after changing that run's ownership.
 IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
  PERFORM 1 FROM runs WHERE run_id=target FOR SHARE;
  IF NOT FOUND THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_RUN_REQUIRED',ERRCODE='23503'; END IF;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM attempts WHERE run_id=target AND
   (content_admission_policy IS NOT NULL OR content_recovery_policy IS NOT NULL)) THEN
  RETURN NEW;
 END IF;
 -- Enrolled runs still serialize exclusion with pipeline reservation/ownership.
 PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
 IF EXISTS(SELECT 1 FROM content_pipeline_operations o JOIN attempts a USING(attempt_id) WHERE a.run_id=target) THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_LEGACY_HISTORY_FORBIDDEN',ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER content_pipeline_legacy_exclusion BEFORE INSERT ON stage_invocations
 FOR EACH ROW EXECUTE FUNCTION guard_content_pipeline_legacy_exclusion();
CREATE TRIGGER content_pipeline_legacy_exclusion BEFORE INSERT ON rss_deliveries
 FOR EACH ROW EXECUTE FUNCTION guard_content_pipeline_legacy_exclusion();
