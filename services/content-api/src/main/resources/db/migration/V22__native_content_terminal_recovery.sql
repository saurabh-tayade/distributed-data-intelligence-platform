-- Inactive, direct atomic safe-end. Receipts never confer execution authority.
ALTER TABLE attempts ADD COLUMN content_terminal_policy varchar(64)
 CHECK(content_terminal_policy IS NULL OR content_terminal_policy='ddip-content-original-safe-end-v1');
ALTER TABLE attempts ADD CONSTRAINT content_terminal_attempt_shape CHECK(content_terminal_policy IS NULL OR
 (kind='RECONCILE' AND state='SUCCEEDED' AND outcome_code='RECONCILED_FAILED' AND row_version=0
 AND started_at IS NOT NULL AND finished_at IS NOT NULL AND content_admission_policy IS NULL
 AND content_recovery_policy IS NULL AND reused_checkpoint_id IS NULL));
CREATE TABLE content_original_terminal_receipts (
 terminal_id uuid PRIMARY KEY, identity_version smallint NOT NULL CHECK(identity_version=1),
 policy varchar(64) NOT NULL CHECK(policy='ddip-content-original-safe-end-v1'),
 run_id uuid NOT NULL UNIQUE REFERENCES runs ON DELETE RESTRICT,
 control_id uuid NOT NULL UNIQUE REFERENCES controls ON DELETE RESTRICT,
 original_attempt_id uuid NOT NULL UNIQUE, terminal_attempt_id uuid NOT NULL UNIQUE,
 original_instance_id uuid NOT NULL REFERENCES worker_instances ON DELETE RESTRICT,
 terminal_instance_id uuid NOT NULL REFERENCES worker_instances ON DELETE RESTRICT,
 original_epoch bigint NOT NULL CHECK(original_epoch>0),
 original_coordinator_version bigint NOT NULL CHECK(original_coordinator_version>=0),
 original_run_version bigint NOT NULL CHECK(original_run_version>=0),
 original_attempt_version bigint NOT NULL CHECK(original_attempt_version>=0),
 original_worker_version bigint NOT NULL CHECK(original_worker_version>=0),
 terminal_worker_version bigint NOT NULL CHECK(terminal_worker_version>=0),
 control_version bigint NOT NULL CHECK(control_version>=0),
 target_bytes bytea NOT NULL CHECK(valid_object_bytes(target_bytes,8192)),
 target_sha256 sha256_hex NOT NULL CHECK(target_sha256=encode(sha256(target_bytes),'hex')),
 review_bytes bytea NOT NULL CHECK(valid_object_bytes(review_bytes,8192)),
 review_sha256 sha256_hex NOT NULL CHECK(review_sha256=encode(sha256(review_bytes),'hex')),
 snapshot_bytes bytea NOT NULL CHECK(valid_object_bytes(snapshot_bytes,65536)),
 snapshot_sha256 sha256_hex NOT NULL CHECK(snapshot_sha256=encode(sha256(snapshot_bytes),'hex')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(terminal_attempt_id,policy),
 FOREIGN KEY(run_id,original_attempt_id) REFERENCES attempts(run_id,attempt_id) ON DELETE RESTRICT,
 FOREIGN KEY(run_id,terminal_attempt_id) REFERENCES attempts(run_id,attempt_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
 CHECK(original_instance_id<>terminal_instance_id),
 CHECK(terminal_id<>original_attempt_id AND terminal_id<>terminal_attempt_id AND terminal_id<>control_id
 AND original_attempt_id<>terminal_attempt_id AND original_attempt_id<>control_id AND terminal_attempt_id<>control_id)
);
ALTER TABLE attempts ADD CONSTRAINT content_terminal_receipt_fk FOREIGN KEY(attempt_id,content_terminal_policy)
 REFERENCES content_original_terminal_receipts(terminal_attempt_id,policy) DEFERRABLE INITIALLY DEFERRED;
CREATE INDEX content_terminal_worker_idx ON content_original_terminal_receipts(terminal_instance_id);
CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON content_original_terminal_receipts
 FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON content_original_terminal_receipts
 FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation();

-- Compact exact byte commitments; text JSON conversion here is an explicitly versioned SQL
-- component encoding, not a claim that PostgreSQL emits the application's canonical JSON.
CREATE FUNCTION content_terminal_compact(n jsonb) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE k text; v jsonb; b bytea; result jsonb:='{}';
BEGIN
 FOR k,v IN SELECT * FROM jsonb_each(n) LOOP
  IF jsonb_typeof(v)='string' AND left(v#>>'{}',2)=chr(92)||'x' THEN
   b:=decode(substr(v#>>'{}',3),'hex');
   result:=result||jsonb_build_object(k,jsonb_build_object('byteLength',octet_length(b),'sha256',encode(sha256(b),'hex')));
  ELSE result:=result||jsonb_build_object(k,v); END IF;
 END LOOP;
 RETURN result;
END $$;

-- Exact SourceJson wire encoding for bounded terminal metadata. JSONB is used only
-- after duplicate-key validation; byte equality rejects whitespace, alternate escapes,
-- key order and fractional/exponent spellings that Java historical lookup would reject.
CREATE FUNCTION content_terminal_canonical(n jsonb, depth integer DEFAULT 0) RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=ddip_management,pg_catalog AS $$
DECLARE result text; entry record;
BEGIN
 IF depth>16 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_JSON_DEPTH' USING ERRCODE='23514'; END IF;
 CASE jsonb_typeof(n)
 WHEN 'object' THEN
  SELECT '{'||coalesce(string_agg(to_jsonb(key)::text||':'||content_terminal_canonical(value,depth+1),',' ORDER BY key COLLATE "C"),'')||'}' INTO result FROM jsonb_each(n);
 WHEN 'array' THEN
  SELECT '['||coalesce(string_agg(content_terminal_canonical(value,depth+1),',' ORDER BY ordinal),'')||']' INTO result FROM jsonb_array_elements(n) WITH ORDINALITY x(value,ordinal);
 WHEN 'number' THEN
  IF length(n::text)>20 OR n::text !~ '^-?(0|[1-9][0-9]*)$' THEN RAISE EXCEPTION 'CONTENT_TERMINAL_JSON_INTEGER_REQUIRED' USING ERRCODE='23514'; END IF;
  result:=n::text;
 ELSE result:=n::text;
 END CASE;
 RETURN result;
END $$;

CREATE FUNCTION content_terminal_components(a uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path=ddip_management,pg_catalog AS $$
DECLARE t text; predicate text; rows jsonb; result jsonb:='{}'; max_rows integer;
BEGIN
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_attempt_admissions x WHERE attempt_id=a LIMIT 2) q;
 IF jsonb_array_length(rows)>1 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_attempt_admissions',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_admission_milestones x WHERE attempt_id=a LIMIT 3) q;
 IF jsonb_array_length(rows)>2 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_admission_milestones',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_operations x WHERE attempt_id=a LIMIT 1) q;
 IF jsonb_array_length(rows)>0 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_operations',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_operation_phases x WHERE operation_id IN (SELECT operation_id FROM content_operations WHERE attempt_id=a) LIMIT 1) q;
 IF jsonb_array_length(rows)>0 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_operation_phases',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_native_operations x WHERE attempt_id=a LIMIT 2) q;
 IF jsonb_array_length(rows)>1 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_native_operations',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_native_launch_phases x WHERE operation_id IN (SELECT operation_id FROM content_native_operations WHERE attempt_id=a) LIMIT 7) q;
 IF jsonb_array_length(rows)>6 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_native_launch_phases',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_fetch_completion_intents x WHERE operation_id IN (SELECT operation_id FROM content_native_operations WHERE attempt_id=a) LIMIT 2) q;
 IF jsonb_array_length(rows)>1 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_fetch_completion_intents',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_fetch_checkpoints x WHERE operation_id IN (SELECT operation_id FROM content_native_operations WHERE attempt_id=a) LIMIT 2) q;
 IF jsonb_array_length(rows)>1 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_fetch_checkpoints',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_bronze_reservations x WHERE attempt_id=a LIMIT 2) q;
 IF jsonb_array_length(rows)>1 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_bronze_reservations',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_bronze_launch_phases x WHERE operation_id IN (SELECT operation_id FROM content_bronze_reservations WHERE attempt_id=a) LIMIT 7) q;
 IF jsonb_array_length(rows)>6 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_bronze_launch_phases',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_bronze_verification_phases x WHERE operation_id IN (SELECT operation_id FROM content_bronze_reservations WHERE attempt_id=a) LIMIT 8) q;
 IF jsonb_array_length(rows)>7 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_bronze_verification_phases',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_bronze_checkpoints x WHERE operation_id IN (SELECT operation_id FROM content_bronze_reservations WHERE attempt_id=a) LIMIT 2) q;
 IF jsonb_array_length(rows)>1 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_bronze_checkpoints',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_silver_reservations x WHERE attempt_id=a LIMIT 2) q;
 IF jsonb_array_length(rows)>1 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_silver_reservations',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_pipeline_operations x WHERE attempt_id=a LIMIT 16) q;
 IF jsonb_array_length(rows)>15 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_pipeline_operations',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_pipeline_phases x WHERE operation_id IN (SELECT operation_id FROM content_pipeline_operations WHERE attempt_id=a) LIMIT 91) q;
 IF jsonb_array_length(rows)>90 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_pipeline_phases',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_pipeline_checkpoints x WHERE operation_id IN (SELECT operation_id FROM content_pipeline_operations WHERE attempt_id=a) LIMIT 16) q;
 IF jsonb_array_length(rows)>15 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_pipeline_checkpoints',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM content_pipeline_completions x WHERE attempt_id=a LIMIT 1) q;
 IF jsonb_array_length(rows)>0 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('content_pipeline_completions',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM run_sources x WHERE run_id=(SELECT run_id FROM attempts WHERE attempt_id=a) LIMIT 129) q;
 IF jsonb_array_length(rows)>128 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('run_sources',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 SELECT coalesce(jsonb_agg(n ORDER BY n::text),'[]') INTO rows FROM (SELECT content_terminal_compact(to_jsonb(x)) n FROM run_inputs x WHERE run_id=(SELECT run_id FROM attempts WHERE attempt_id=a) LIMIT 129) q;
 IF jsonb_array_length(rows)>128 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=result||jsonb_build_object('run_inputs',jsonb_build_object('count',jsonb_array_length(rows),'byteLength',octet_length(convert_to(rows::text,'UTF8')),'sha256',encode(sha256(convert_to(rows::text,'UTF8')),'hex')));
 RETURN result;
END $$;

CREATE FUNCTION content_terminal_artifact_bindings(a uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path=ddip_management,pg_catalog AS $$
DECLARE r runs%ROWTYPE; bronze jsonb; frozen jsonb; cmd jsonb; verified jsonb; expected jsonb;
 b text; address text; family text; fp text; prereq integer; node artifacts%ROWTYPE; silver uuid;
 deps jsonb; result jsonb:='[]'; key text; ref record;
BEGIN
 SELECT x.* INTO r FROM runs x JOIN attempts y USING(run_id) WHERE y.attempt_id=a;
 frozen:=convert_from(r.captured_selection,'UTF8')::jsonb;
 FOR family IN SELECT unnest(ARRAY['SILVER','GOLD','CONTENT']) LOOP
  b:=NULL;address:=NULL;expected:=NULL;fp:=NULL;
  SELECT value->>'bindingId' INTO b FROM jsonb_array_elements(frozen->'storageBindings') WHERE value->>'role'=family;
  IF family='SILVER' THEN
   SELECT convert_from(c.receipt_bytes,'UTF8')::jsonb INTO bronze FROM content_bronze_checkpoints c
    JOIN content_bronze_reservations o USING(operation_id) WHERE o.attempt_id=a;
   IF bronze IS NOT NULL THEN
    key:=bronze->'expected'->>'metadataKey';
    address:='v1/'||encode(sha256(convert_to(bronze->>'bucket','UTF8')),'hex')||'/'||
      substr(key,length('bronze/rss/')+1,length(key)-length('bronze/rss/')-length('metadata.json'))||'complete.json';
   END IF;
   SELECT convert_from(o.command_bytes,'UTF8')::jsonb,convert_from(c.evidence_bytes,'UTF8')::jsonb INTO cmd,verified
    FROM content_pipeline_operations o JOIN content_pipeline_checkpoints c USING(operation_id) WHERE o.attempt_id=a AND step_index=4;
   expected:=jsonb_build_object('qualificationVersion',1,'selection',cmd->'artifact','verification',verified);
   fp:=verified->'inventoryIdentity'->>'sha256'; prereq:=5;
  ELSE
   SELECT convert_from(o.command_bytes,'UTF8')::jsonb,convert_from(c.evidence_bytes,'UTF8')::jsonb INTO cmd,verified
    FROM content_pipeline_operations o JOIN content_pipeline_checkpoints c USING(operation_id)
    WHERE o.attempt_id=a AND step_index=CASE family WHEN 'GOLD' THEN 7 ELSE 13 END;
   fp:=verified->'prepared'->>CASE family WHEN 'GOLD' THEN 'inputSetFingerprint' ELSE 'fingerprint' END;
   address:='v1/processor_revision=1/'||CASE family WHEN 'GOLD' THEN 'input_set=' ELSE 'publication=' END||fp;
   IF family='GOLD' THEN
    SELECT convert_from(o.command_bytes,'UTF8')::jsonb,convert_from(c.evidence_bytes,'UTF8')::jsonb INTO cmd,verified
     FROM content_pipeline_operations o JOIN content_pipeline_checkpoints c USING(operation_id) WHERE o.attempt_id=a AND step_index=10;
    expected:=jsonb_build_object('managedGoldVersion',1,'kind','GOLD','productId','content-intelligence',
     'contractVersion',1,'processorRevision',1,'nativeFingerprint',fp,'storageBindingId',b,'relativeAddress',address,
     'verification',verified,'inputs',jsonb_build_array(jsonb_build_object('artifactId',silver::text,
      'evidenceSha256',(SELECT evidence_sha256::text FROM artifacts WHERE artifact_id=silver)))); prereq:=11;
   END IF;
  END IF;
  IF address IS NULL THEN CONTINUE; END IF;
  IF b IS NULL THEN RAISE EXCEPTION 'CONTENT_TERMINAL_ARTIFACT_BINDING_INVALID' USING ERRCODE='23514'; END IF;
  SELECT * INTO node FROM artifacts WHERE storage_binding_id=b AND relative_address=address;
  deps:='[]';
  IF node.artifact_id IS NOT NULL THEN
   SELECT coalesce(jsonb_agg(to_jsonb(d) ORDER BY role,ordinal),'[]') INTO deps FROM (SELECT * FROM artifact_dependencies WHERE artifact_id=node.artifact_id ORDER BY role,ordinal LIMIT 3) d;
   IF family='CONTENT' OR NOT EXISTS(SELECT 1 FROM content_pipeline_operations o JOIN content_pipeline_checkpoints c USING(operation_id) WHERE o.attempt_id=a AND step_index=prereq)
    OR node.kind<>family OR node.product_id<>'content-intelligence' OR node.contract_version<>1 OR node.processor_revision<>1
    OR node.native_identity IS DISTINCT FROM fp
    OR NOT native_reservation_json_matches(convert_from(node.evidence_bytes,'UTF8')::json,expected)
    OR (family='SILVER' AND deps<>'[]')
    OR (family='GOLD' AND deps IS DISTINCT FROM jsonb_build_array(jsonb_build_object('artifact_id',node.artifact_id,'role','SILVER','ordinal',0,'dependency_artifact_id',silver))) THEN
    RAISE EXCEPTION 'CONTENT_TERMINAL_ARTIFACT_INVALID' USING ERRCODE='23514'; END IF;
  END IF;
  IF family='SILVER' THEN silver:=node.artifact_id; END IF;
  result:=result||jsonb_build_array(jsonb_build_object('kind',family,'bindingId',b,'relativeAddress',address,
   'artifact',CASE WHEN node.artifact_id IS NULL THEN NULL ELSE content_terminal_compact(to_jsonb(node)) END,'dependencies',deps));
 END LOOP;
 -- Only explicitly included input nodes, never their unrelated incoming consumers.
 FOR ref IN SELECT ar.* FROM run_inputs i JOIN artifacts ar USING(artifact_id) WHERE i.run_id=r.run_id ORDER BY i.role,i.ordinal LIMIT 129 LOOP
  SELECT coalesce(jsonb_agg(to_jsonb(d) ORDER BY role,ordinal),'[]') INTO deps FROM (SELECT * FROM artifact_dependencies WHERE artifact_id=ref.artifact_id LIMIT 1) d;
  IF deps<>'[]' THEN RAISE EXCEPTION 'CONTENT_TERMINAL_REFERENCE_DEPENDENCIES_INVALID' USING ERRCODE='23514'; END IF;
  result:=result||jsonb_build_array(jsonb_build_object('kind',ref.kind,'bindingId',ref.storage_binding_id,'relativeAddress',ref.relative_address,
    'artifact',content_terminal_compact(to_jsonb(ref)),'dependencies',deps));
 END LOOP;
 IF (SELECT count(*) FROM (SELECT 1 FROM run_inputs WHERE run_id=r.run_id LIMIT 129) x)>128 OR jsonb_array_length(result)>131 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_ARTIFACT_BOUND' USING ERRCODE='23514'; END IF;
 RETURN result;
END $$;

CREATE FUNCTION content_terminal_snapshot(r uuid,a uuid,k uuid,w uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path=ddip_management,pg_catalog AS $$
DECLARE result jsonb; history jsonb; controls_json jsonb;
BEGIN
 SELECT coalesce(jsonb_agg(content_terminal_compact(to_jsonb(x)) ORDER BY attempt_no),'[]') INTO history
  FROM (SELECT * FROM attempts WHERE run_id=r ORDER BY attempt_no LIMIT 21) x;
 SELECT coalesce(jsonb_agg(content_terminal_compact(to_jsonb(x)) ORDER BY control_id),'[]') INTO controls_json
  FROM (SELECT * FROM controls WHERE run_id=r ORDER BY control_id LIMIT 129) x;
 IF jsonb_array_length(history)>20 OR jsonb_array_length(controls_json)>128 THEN
  RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_BOUND' USING ERRCODE='23514'; END IF;
 result:=jsonb_build_object('snapshotVersion',1,
  'coordinator',(SELECT content_terminal_compact(to_jsonb(x)) FROM coordinator x WHERE singleton_id=1),
  'run',(SELECT content_terminal_compact(to_jsonb(x)) FROM runs x WHERE run_id=r),
  'attempt',(SELECT content_terminal_compact(to_jsonb(x)) FROM attempts x WHERE attempt_id=a AND run_id=r),
  'control',(SELECT content_terminal_compact(to_jsonb(x)) FROM controls x WHERE control_id=k AND run_id=r),
  'oldWorker',(SELECT content_terminal_compact(to_jsonb(x)) FROM worker_instances x JOIN attempts y ON y.instance_id=x.instance_id WHERE y.attempt_id=a),
  'terminalWorker',(SELECT content_terminal_compact(to_jsonb(x)) FROM worker_instances x WHERE instance_id=w),
  'history',history,'controls',controls_json,'components',content_terminal_components(a),'artifactBindings',content_terminal_artifact_bindings(a));
 IF octet_length(convert_to(result::text,'UTF8'))>65536 THEN RAISE EXCEPTION 'CONTENT_TERMINAL_SNAPSHOT_BOUND' USING ERRCODE='23514'; END IF;
 RETURN result;
END $$;

CREATE FUNCTION content_terminal_receipt_valid(id uuid) RETURNS boolean
LANGUAGE plpgsql STABLE SET search_path=ddip_management,pg_catalog AS $$
DECLARE h content_original_terminal_receipts%ROWTYPE; s jsonb; now_snapshot jsonb; expected jsonb; t jsonb;
 c coordinator%ROWTYPE; r runs%ROWTYPE; a attempts%ROWTYPE; z attempts%ROWTYPE; ctl controls%ROWTYPE;
 w worker_instances%ROWTYPE; ow worker_instances%ROWTYPE; prior jsonb; checkpoints jsonb; person jsonb; identities text[]:='{}'; ident text;
BEGIN
 SELECT * INTO h FROM content_original_terminal_receipts WHERE terminal_id=id;
 IF NOT FOUND THEN RETURN false; END IF;
 s:=convert_from(h.snapshot_bytes,'UTF8')::jsonb;
 IF h.snapshot_bytes IS DISTINCT FROM convert_to(content_terminal_canonical(s),'UTF8')
 OR h.target_bytes IS DISTINCT FROM convert_to(content_terminal_canonical(convert_from(h.target_bytes,'UTF8')::jsonb),'UTF8')
 OR h.review_bytes IS DISTINCT FROM convert_to(content_terminal_canonical(convert_from(h.review_bytes,'UTF8')::jsonb),'UTF8') THEN RETURN false; END IF;
 now_snapshot:=content_terminal_snapshot(h.run_id,h.original_attempt_id,h.control_id,h.terminal_instance_id);
 SELECT * INTO c FROM coordinator WHERE singleton_id=1;
 SELECT * INTO r FROM runs WHERE run_id=h.run_id;
 SELECT * INTO a FROM attempts WHERE attempt_id=h.original_attempt_id;
 SELECT * INTO z FROM attempts WHERE attempt_id=h.terminal_attempt_id;
 SELECT * INTO ctl FROM controls WHERE control_id=h.control_id;
 SELECT * INTO w FROM worker_instances WHERE instance_id=h.terminal_instance_id;
 SELECT * INTO ow FROM worker_instances WHERE instance_id=h.original_instance_id;
 IF NOT s ?& ARRAY['snapshotVersion','coordinator','run','attempt','control','oldWorker','terminalWorker','history','controls','components','artifactBindings']
 OR s->>'snapshotVersion' IS DISTINCT FROM '1'
 OR s-ARRAY['snapshotVersion','coordinator','run','attempt','control','oldWorker','terminalWorker','history','controls','components','artifactBindings']<>'{}'
 OR s->'components' IS DISTINCT FROM now_snapshot->'components' OR s->'artifactBindings' IS DISTINCT FROM now_snapshot->'artifactBindings'
 OR s->'oldWorker' IS DISTINCT FROM now_snapshot->'oldWorker'
 OR (s->'run')-ARRAY['state','current_stage','current_attempt_id','row_version','finished_at','outcome_code','retry_eligible','result_artifact_id','counts_bytes']
   IS DISTINCT FROM (now_snapshot->'run')-ARRAY['state','current_stage','current_attempt_id','row_version','finished_at','outcome_code','retry_eligible','result_artifact_id','counts_bytes']
 OR (s->'attempt')-ARRAY['state','row_version','finished_at','outcome_code'] IS DISTINCT FROM (now_snapshot->'attempt')-ARRAY['state','row_version','finished_at','outcome_code']
 OR (s->'control')-ARRAY['state','row_version','applied_at','reason'] IS DISTINCT FROM (now_snapshot->'control')-ARRAY['state','row_version','applied_at','reason']
 OR (s->'terminalWorker')-ARRAY['status','row_version'] IS DISTINCT FROM (now_snapshot->'terminalWorker')-ARRAY['status','row_version']
 OR s#>>'{coordinator,mode}' IS DISTINCT FROM 'READY' OR s#>>'{coordinator,active_import_id}' IS NOT NULL
 OR s#>>'{coordinator,active_attempt_id}' IS DISTINCT FROM h.original_attempt_id::text
 OR s#>>'{coordinator,owner_instance_id}' IS DISTINCT FROM h.original_instance_id::text
 OR s#>>'{coordinator,epoch}' IS DISTINCT FROM h.original_epoch::text
 OR s#>>'{coordinator,row_version}' IS DISTINCT FROM h.original_coordinator_version::text
 OR s#>>'{run,state}' IS DISTINCT FROM 'RUNNING' OR s#>>'{run,row_version}' IS DISTINCT FROM h.original_run_version::text
 OR s#>>'{run,current_attempt_id}' IS DISTINCT FROM h.original_attempt_id::text
 OR s#>>'{attempt,state}' IS DISTINCT FROM 'CLAIMED' OR s#>>'{attempt,row_version}' IS DISTINCT FROM h.original_attempt_version::text
 OR s#>>'{attempt,finished_at}' IS NOT NULL OR s#>>'{attempt,outcome_code}' IS NOT NULL
 OR s#>>'{control,state}' IS DISTINCT FROM 'REQUESTED' OR s#>>'{control,row_version}' IS DISTINCT FROM h.control_version::text
 OR s#>>'{oldWorker,status}' IS DISTINCT FROM 'READY' OR s#>>'{oldWorker,row_version}' IS DISTINCT FROM h.original_worker_version::text
 OR s#>>'{terminalWorker,status}' IS DISTINCT FROM 'RECOVERY_ONLY' OR s#>>'{terminalWorker,row_version}' IS DISTINCT FROM h.terminal_worker_version::text
 OR a.kind<>'EXECUTE' OR a.content_admission_policy IS DISTINCT FROM 'ddip-content-narrow-v1' OR a.content_recovery_policy IS NOT NULL OR a.content_terminal_policy IS NOT NULL
 OR a.instance_id<>h.original_instance_id OR a.owner_epoch<>h.original_epoch OR a.state<>'INTERRUPTED'
 OR a.outcome_code IS DISTINCT FROM 'INTERRUPTED' OR a.row_version<>h.original_attempt_version+1 OR a.finished_at IS NULL
 OR z.kind<>'RECONCILE' OR z.state<>'SUCCEEDED' OR z.outcome_code IS DISTINCT FROM 'RECONCILED_FAILED'
 OR z.content_terminal_policy IS DISTINCT FROM h.policy OR z.row_version<>0 OR z.finished_at IS NULL OR z.started_at IS NULL
 OR z.run_id<>h.run_id OR z.instance_id<>h.terminal_instance_id OR z.owner_epoch<>h.original_epoch+1
 OR z.attempt_no<>(SELECT max(attempt_no)+1 FROM attempts WHERE run_id=h.run_id AND attempt_id<>z.attempt_id) OR z.attempt_no>20
 OR r.pipeline<>'CONTENT_RSS_V1' OR r.state<>'FAILED' OR r.outcome_code IS DISTINCT FROM 'NATIVE_CONTENT_INTERRUPTED'
 OR r.row_version<>h.original_run_version+1 OR r.current_attempt_id IS DISTINCT FROM z.attempt_id OR r.current_stage IS NOT NULL
 OR r.result_artifact_id IS NOT NULL OR r.counts_bytes IS NOT NULL OR r.retry_eligible OR r.finished_at IS NULL
 OR ctl.type<>'RECONCILE' OR ctl.target_attempt_id IS DISTINCT FROM a.attempt_id OR ctl.state<>'APPLIED'
 OR ctl.quiescence_confirmed IS DISTINCT FROM true OR ctl.requester<>'local-operator' OR ctl.expected_version<>h.original_run_version
 OR ctl.reason IS DISTINCT FROM 'NATIVE_CONTENT_INTERRUPTED' OR ctl.row_version<>h.control_version+1 OR ctl.applied_at IS NULL
 OR w.status<>'STOPPED' OR w.row_version<>h.terminal_worker_version+1
 OR w.installation_id<>c.installation_id OR ow.installation_id<>c.installation_id
 OR w.execution_profile_id<>r.execution_profile_id OR ow.execution_profile_id<>r.execution_profile_id
 OR w.profile_sha256<>ow.profile_sha256 OR w.host_binding_sha256<>ow.host_binding_sha256
 OR c.epoch<h.original_epoch+1 OR c.row_version<h.original_coordinator_version+1
 OR c.active_attempt_id IN (h.original_attempt_id,h.terminal_attempt_id)
 OR c.owner_instance_id IN (h.original_instance_id,h.terminal_instance_id)
 OR NOT EXISTS(SELECT 1 FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='READY')
 OR EXISTS(SELECT 1 FROM content_fetch_recovery_handoffs WHERE run_id=h.run_id)
 OR EXISTS(SELECT 1 FROM attempts prior WHERE prior.run_id=h.run_id AND prior.attempt_id NOT IN(a.attempt_id,z.attempt_id)
 AND (prior.kind<>'EXECUTE' OR prior.state NOT IN('FAILED','CANCELLED') OR prior.content_admission_policy IS NOT NULL
 OR prior.content_recovery_policy IS NOT NULL OR prior.content_terminal_policy IS NOT NULL OR prior.reused_checkpoint_id IS NOT NULL))
 OR EXISTS(SELECT 1 FROM stage_invocations x JOIN attempts y USING(attempt_id) WHERE y.run_id=h.run_id)
 OR EXISTS(SELECT 1 FROM rss_deliveries WHERE run_id=h.run_id)
 OR EXISTS(SELECT 1 FROM controls WHERE run_id=h.run_id AND control_id<>h.control_id AND (state='REQUESTED' OR (type='CANCEL' AND state='APPLIED')))
 OR jsonb_array_length(s->'history') NOT BETWEEN 1 AND 19 OR jsonb_array_length(s->'controls') NOT BETWEEN 1 AND 128
 OR (SELECT count(DISTINCT x->>'attempt_id') FROM jsonb_array_elements(s->'history') x)<>jsonb_array_length(s->'history')
 OR (SELECT count(DISTINCT x->>'control_id') FROM jsonb_array_elements(s->'controls') x)<>jsonb_array_length(s->'controls')
 OR jsonb_array_length(now_snapshot->'history')<>jsonb_array_length(s->'history')+1
 OR jsonb_array_length(now_snapshot->'controls')<>jsonb_array_length(s->'controls') THEN RETURN false; END IF;
 FOR prior IN SELECT value FROM jsonb_array_elements(s->'history') LOOP
  IF prior->>'attempt_id'=h.original_attempt_id::text THEN
   IF prior IS DISTINCT FROM s->'attempt' THEN RETURN false; END IF;
  ELSIF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(now_snapshot->'history') x WHERE x=prior)
   OR prior->>'content_recovery_policy' IS NOT NULL OR prior->>'content_terminal_policy' IS NOT NULL THEN RETURN false;
  END IF;
 END LOOP;
 FOR prior IN SELECT value FROM jsonb_array_elements(s->'controls') LOOP
  IF prior->>'control_id'=h.control_id::text THEN
   IF prior IS DISTINCT FROM s->'control' THEN RETURN false; END IF;
  ELSIF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(now_snapshot->'controls') x WHERE x=prior) THEN RETURN false; END IF;
 END LOOP;
 expected:=jsonb_build_object('identityVersion',1,'policy',h.policy,'terminalId',h.terminal_id::text,'runId',h.run_id::text,
  'originalAttemptId',h.original_attempt_id::text,'terminalAttemptId',h.terminal_attempt_id::text,'controlId',h.control_id::text,'snapshotSha256',h.snapshot_sha256::text,'inventorySha256',convert_from(h.target_bytes,'UTF8')::jsonb->'inventorySha256','inspectionNonce',convert_from(h.target_bytes,'UTF8')::jsonb->'inspectionNonce');
 IF expected->>'inventorySha256' IS NULL OR expected->>'inventorySha256' !~ '^[0-9a-f]{64}$' THEN RETURN false; END IF;
 IF NOT native_reservation_json_matches(convert_from(h.target_bytes,'UTF8')::json,expected)
 OR NOT native_reservation_json_matches(convert_from(h.snapshot_bytes,'UTF8')::json,s) THEN RETURN false; END IF;
 t:=convert_from(h.review_bytes,'UTF8')::jsonb;
 IF NOT native_reservation_json_matches(convert_from(h.review_bytes,'UTF8')::json,t)
 OR t->>'targetSha256' IS DISTINCT FROM h.target_sha256::text OR t->'reviewVersion' IS DISTINCT FROM '1'::jsonb
 OR NOT t ?& ARRAY['nonce','inspectedAt','reviewedCheckpoints','descendants']
 OR t-ARRAY['targetSha256','inspectedAt','handlersStopped','unmanagedWritersStopped','orphanReviewComplete','reviewedCheckpoints','descendants','reviewVersion','nonce','sameHostAndProcessNamespace','descendantsReviewed','launchGapsReviewed','sparkGatewaysReviewed','remoteWritersStopped']<>'{}'
 OR jsonb_typeof(t->'reviewedCheckpoints') IS DISTINCT FROM 'array' OR jsonb_typeof(t->'descendants') IS DISTINCT FROM 'array'
 OR jsonb_array_length(t->'descendants')>32 OR jsonb_array_length(t->'reviewedCheckpoints')>512
 OR t->'handlersStopped' IS DISTINCT FROM 'true'::jsonb OR t->'unmanagedWritersStopped' IS DISTINCT FROM 'true'::jsonb
 OR t->'orphanReviewComplete' IS DISTINCT FROM 'true'::jsonb OR t->'sameHostAndProcessNamespace' IS DISTINCT FROM 'true'::jsonb
 OR t->'descendantsReviewed' IS DISTINCT FROM 'true'::jsonb OR t->'launchGapsReviewed' IS DISTINCT FROM 'true'::jsonb
 OR t->'sparkGatewaysReviewed' IS DISTINCT FROM 'true'::jsonb OR t->'remoteWritersStopped' IS DISTINCT FROM 'true'::jsonb THEN RETURN false; END IF;
 IF jsonb_typeof(t->'nonce') IS DISTINCT FROM 'string' OR jsonb_typeof(expected->'inspectionNonce') IS DISTINCT FROM 'string' OR t->>'nonce' IS DISTINCT FROM expected->>'inspectionNonce' THEN RETURN false; END IF;
 IF t->>'nonce' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
 OR t->>'inspectedAt' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$' THEN RETURN false; END IF;
 PERFORM (t->>'nonce')::uuid; PERFORM (t->>'inspectedAt')::timestamptz;
 SELECT coalesce(jsonb_agg(cp::text ORDER BY cp::text),'[]') INTO checkpoints FROM (
 SELECT checkpoint_id cp FROM content_native_operations WHERE attempt_id=h.original_attempt_id UNION
 SELECT checkpoint_id FROM content_bronze_reservations WHERE attempt_id=h.original_attempt_id UNION
 SELECT checkpoint_id FROM content_pipeline_operations WHERE attempt_id=h.original_attempt_id) x;
 IF t->'reviewedCheckpoints' IS DISTINCT FROM checkpoints THEN RETURN false; END IF;
 FOR person IN SELECT value FROM jsonb_array_elements(t->'descendants') LOOP
  IF NOT person ?& ARRAY['pid','startedAt'] OR person-ARRAY['pid','startedAt']<>'{}'
  OR jsonb_typeof(person->'pid') IS DISTINCT FROM 'string' OR jsonb_typeof(person->'startedAt') IS DISTINCT FROM 'string'
  OR person->>'pid' !~ '^[1-9][0-9]{0,18}$'
  OR person->>'startedAt' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$' THEN RETURN false; END IF;
  PERFORM (person->>'pid')::bigint;
  ident:=(person->>'pid')||':'||(regexp_replace(person->>'startedAt','(\.[0-9]{6})[0-9]+Z$','\1Z')::timestamptz)::text;
  IF ident=ANY(identities) THEN RETURN false; END IF; identities:=array_append(identities,ident);
 END LOOP;
 expected:=expected||jsonb_build_object('targetSha256',h.target_sha256::text);
 IF (SELECT count(*) FROM audit_events WHERE run_id=h.run_id AND safe_code='NATIVE_CONTENT_INTERRUPTED')<>6
 OR EXISTS(SELECT 1 FROM (VALUES
  (1,'NATIVE_CONTENT_TERMINAL_ORIGINAL','CLAIMED','INTERRUPTED',h.original_attempt_version,h.original_attempt_version+1),
  (2,'NATIVE_CONTENT_TERMINAL_ATTEMPT',NULL,'SUCCEEDED',NULL::bigint,0::bigint),
  (3,'NATIVE_CONTENT_TERMINAL_RUN','RUNNING','FAILED',h.original_run_version,h.original_run_version+1),
  (4,'NATIVE_CONTENT_TERMINAL_CONTROL','REQUESTED','APPLIED',h.control_version,h.control_version+1),
  (5,'NATIVE_CONTENT_TERMINAL_WORKER','RECOVERY_ONLY','STOPPED',h.terminal_worker_version,h.terminal_worker_version+1),
  (6,'NATIVE_CONTENT_TERMINAL_RELEASE','READY','READY',h.original_coordinator_version,h.original_coordinator_version+1)
 ) v(ordinal,action,before_state,after_state,before_version,after_version) WHERE NOT EXISTS(
  SELECT 1 FROM (SELECT e.*,row_number() OVER(ORDER BY event_seq) ordinal FROM audit_events e WHERE run_id=h.run_id AND safe_code='NATIVE_CONTENT_INTERRUPTED') e
  WHERE e.ordinal=v.ordinal AND e.action=v.action AND e.actor_scope='local-operator'
   AND e.attempt_id=h.terminal_attempt_id AND e.control_id=h.control_id
   AND e.before_state IS NOT DISTINCT FROM v.before_state AND e.after_state=v.after_state
   AND e.before_version IS NOT DISTINCT FROM v.before_version AND e.after_version=v.after_version
   AND e.details_bytes=convert_to(content_terminal_canonical(expected),'UTF8')
   AND native_reservation_json_matches(convert_from(e.details_bytes,'UTF8')::json,expected))) THEN RETURN false; END IF;
 RETURN true;
END $$;

CREATE FUNCTION content_terminal_review_fresh(raw bytea) RETURNS boolean
LANGUAGE plpgsql VOLATILE SET search_path=ddip_management,pg_catalog AS $$
DECLARE at_time timestamptz; moment timestamptz:=clock_timestamp(); value text;
BEGIN
 value:=convert_from(raw,'UTF8')::jsonb->>'inspectedAt';
 IF value IS NULL OR value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$' THEN RETURN false; END IF;
 at_time:=value::timestamptz;
 RETURN isfinite(at_time) AND at_time<=moment AND at_time>=moment-interval '60 seconds';
END $$;
-- Insert first. The receipt is only durable if the deferred exact disposition succeeds.
CREATE FUNCTION guard_content_terminal_receipt() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE c coordinator%ROWTYPE; actual jsonb;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'CONTENT_TERMINAL_ISOLATION_REQUIRED' USING ERRCODE='23514'; END IF;
 SELECT * INTO c FROM coordinator WHERE singleton_id=1 FOR UPDATE;
 IF NOT content_terminal_review_fresh(NEW.review_bytes) THEN RAISE EXCEPTION 'CONTENT_TERMINAL_REVIEW_EXPIRED' USING ERRCODE='23514'; END IF;
 actual:=content_terminal_snapshot(NEW.run_id,NEW.original_attempt_id,NEW.control_id,NEW.terminal_instance_id);
 IF NEW.snapshot_bytes IS DISTINCT FROM convert_to(content_terminal_canonical(actual),'UTF8')
 OR actual#>>'{run,state}' IS DISTINCT FROM 'RUNNING'
 OR actual#>>'{attempt,state}' IS DISTINCT FROM 'CLAIMED'
 OR actual#>>'{oldWorker,status}' IS DISTINCT FROM 'READY'
 OR actual#>>'{terminalWorker,status}' IS DISTINCT FROM 'RECOVERY_ONLY'
 OR actual#>>'{control,state}' IS DISTINCT FROM 'REQUESTED'
 OR c.mode<>'READY' OR c.active_attempt_id IS DISTINCT FROM NEW.original_attempt_id
 OR c.owner_instance_id IS DISTINCT FROM NEW.original_instance_id OR c.active_import_id IS NOT NULL
 OR c.epoch<>NEW.original_epoch OR c.row_version<>NEW.original_coordinator_version
 OR EXISTS(SELECT 1 FROM attempts WHERE attempt_id=NEW.terminal_attempt_id)
 OR EXISTS(SELECT 1 FROM audit_events WHERE run_id=NEW.run_id AND safe_code='NATIVE_CONTENT_INTERRUPTED') THEN
  RAISE EXCEPTION 'CONTENT_TERMINAL_INITIAL_SNAPSHOT_REQUIRED' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER content_terminal_receipt BEFORE INSERT ON content_original_terminal_receipts FOR EACH ROW EXECUTE FUNCTION guard_content_terminal_receipt();
CREATE FUNCTION check_content_terminal_receipt() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
BEGIN
 IF NOT content_terminal_review_fresh(NEW.review_bytes) OR NOT content_terminal_receipt_valid(NEW.terminal_id)
 OR NOT EXISTS(SELECT 1 FROM coordinator WHERE singleton_id=1 AND mode='READY' AND active_attempt_id IS NULL
  AND active_import_id IS NULL AND owner_instance_id IS NULL AND heartbeat_at IS NULL
  AND epoch=NEW.original_epoch+1 AND row_version=NEW.original_coordinator_version+1) THEN RAISE EXCEPTION 'CONTENT_TERMINAL_DISPOSITION_INVALID' USING ERRCODE='23514'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER content_terminal_receipt_commit AFTER INSERT ON content_original_terminal_receipts
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_content_terminal_receipt();

-- A new receipt permits only its own exact lifecycle writes in the inserting transaction.
-- Direct SQL cannot spoof the pre-state by flushing deferred OLD events before enrollment.
CREATE FUNCTION content_terminal_transaction_change(tab text,operation text,n jsonb,o jsonb) RETURNS boolean
LANGUAGE plpgsql STABLE SET search_path=ddip_management,pg_catalog AS $$
DECLARE h content_original_terminal_receipts%ROWTYPE; snap jsonb; key text;
 transaction_id xid:=((pg_current_xact_id()::text::numeric % 4294967296)::text)::xid;
BEGIN
 SELECT x.* INTO h FROM content_original_terminal_receipts x WHERE xmin=transaction_id AND
  (tab='coordinator' OR (tab='worker_instances' AND x.terminal_instance_id=(n->>'instance_id')::uuid)
   OR (tab IN ('runs','attempts','controls','audit_events') AND x.run_id=(n->>'run_id')::uuid));
 IF NOT FOUND THEN RETURN false; END IF;
 snap:=convert_from(h.snapshot_bytes,'UTF8')::jsonb;
 IF operation='INSERT' AND tab='attempts' THEN
  RETURN n->>'attempt_id'=h.terminal_attempt_id::text AND n->>'content_terminal_policy'=h.policy;
 ELSIF operation='INSERT' AND tab='audit_events' THEN
  RETURN n->>'attempt_id'=h.terminal_attempt_id::text AND n->>'control_id'=h.control_id::text
   AND n->>'safe_code'='NATIVE_CONTENT_INTERRUPTED' AND n->>'action'=ANY(ARRAY[
    'NATIVE_CONTENT_TERMINAL_ORIGINAL','NATIVE_CONTENT_TERMINAL_ATTEMPT','NATIVE_CONTENT_TERMINAL_RUN',
    'NATIVE_CONTENT_TERMINAL_CONTROL','NATIVE_CONTENT_TERMINAL_WORKER','NATIVE_CONTENT_TERMINAL_RELEASE']);
 ELSIF operation='UPDATE' THEN
  key:=CASE WHEN tab='coordinator' THEN 'coordinator' WHEN tab='runs' THEN 'run'
   WHEN tab='attempts' AND n->>'attempt_id'=h.original_attempt_id::text THEN 'attempt'
   WHEN tab='controls' AND n->>'control_id'=h.control_id::text THEN 'control'
   WHEN tab='worker_instances' THEN 'terminalWorker' ELSE NULL END;
  RETURN key IS NOT NULL AND content_terminal_compact(o)=snap->key;
 END IF;
 RETURN false;
END $$;

-- All writers share the coordinator fence BEFORE consulting receipt visibility. A changed
-- coordinator causes snapshot-isolated transactions to fail, not miss a concurrent enrollment.
CREATE FUNCTION guard_content_terminal_closed() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE n jsonb:=to_jsonb(NEW); r uuid; a uuid; op uuid;
BEGIN
 -- Preserve the existing legacy-stage fast path and invisible-attempt rejection. The
 -- run fence under snapshot isolation prevents a newly enrolled native owner being missed.
 IF TG_TABLE_NAME IN ('stage_invocations','stage_artifacts','rss_deliveries') THEN
  r:=(n->>'run_id')::uuid;
  IF r IS NULL THEN
   SELECT run_id INTO r FROM attempts WHERE attempt_id=(n->>'attempt_id')::uuid;
   IF NOT FOUND THEN RAISE EXCEPTION 'CONTENT_ADMISSION_ATTEMPT_REQUIRED' USING ERRCODE='23503'; END IF;
  END IF;
  IF current_setting('transaction_isolation')<>'read committed' THEN PERFORM 1 FROM runs WHERE run_id=r FOR SHARE; END IF;
  IF NOT EXISTS(SELECT 1 FROM attempts WHERE run_id=r AND (content_admission_policy IS NOT NULL OR content_recovery_policy IS NOT NULL OR content_terminal_policy IS NOT NULL)) THEN RETURN NEW; END IF;
 END IF;
 PERFORM 1 FROM coordinator WHERE singleton_id=1 FOR UPDATE;
 IF content_terminal_transaction_change(TG_TABLE_NAME,TG_OP,n,CASE WHEN TG_OP='UPDATE' THEN to_jsonb(OLD) ELSE NULL END) THEN RETURN NEW; END IF;
 IF TG_TABLE_NAME='coordinator' THEN
  IF EXISTS(SELECT 1 FROM content_original_terminal_receipts h WHERE
   NEW.active_attempt_id IN(h.original_attempt_id,h.terminal_attempt_id)
   OR NEW.owner_instance_id IN(h.original_instance_id,h.terminal_instance_id)
   OR NEW.epoch<h.original_epoch+1 OR NEW.row_version<h.original_coordinator_version+1) THEN
   RAISE EXCEPTION 'CONTENT_TERMINAL_OWNER_CLOSED' USING ERRCODE='23514'; END IF;
 ELSIF TG_TABLE_NAME='worker_instances' THEN
  IF EXISTS(SELECT 1 FROM content_original_terminal_receipts WHERE terminal_instance_id=NEW.instance_id OR original_instance_id=NEW.instance_id) THEN
   RAISE EXCEPTION 'CONTENT_TERMINAL_WORKER_CLOSED' USING ERRCODE='23514'; END IF;
 ELSIF TG_TABLE_NAME='artifacts' THEN
  IF EXISTS(SELECT 1 FROM content_original_terminal_receipts h,
   LATERAL jsonb_array_elements(convert_from(h.snapshot_bytes,'UTF8')::jsonb->'artifactBindings') b
   WHERE b->>'bindingId'=NEW.storage_binding_id::text AND b->>'relativeAddress'=NEW.relative_address::text) THEN
   RAISE EXCEPTION 'CONTENT_TERMINAL_ARTIFACT_CLOSED' USING ERRCODE='23514'; END IF;
 ELSIF TG_TABLE_NAME='artifact_dependencies' THEN
  IF EXISTS(SELECT 1 FROM content_original_terminal_receipts h,
   LATERAL jsonb_array_elements(convert_from(h.snapshot_bytes,'UTF8')::jsonb->'artifactBindings') b
   WHERE b#>>'{artifact,artifact_id}'=NEW.artifact_id::text) THEN
   RAISE EXCEPTION 'CONTENT_TERMINAL_DEPENDENCIES_CLOSED' USING ERRCODE='23514'; END IF;
 ELSE
  r:=(n->>'run_id')::uuid; a:=(n->>'attempt_id')::uuid; op:=(n->>'operation_id')::uuid;
  IF r IS NULL AND a IS NULL AND op IS NOT NULL THEN
   SELECT attempt_id INTO a FROM (
    SELECT operation_id,attempt_id FROM content_native_operations UNION ALL
    SELECT operation_id,attempt_id FROM content_bronze_reservations UNION ALL
    SELECT operation_id,attempt_id FROM content_pipeline_operations UNION ALL
    SELECT operation_id,attempt_id FROM content_operations) x WHERE operation_id=op LIMIT 1;
  END IF;
  IF r IS NULL THEN SELECT run_id INTO r FROM attempts WHERE attempt_id=a; END IF;
  IF EXISTS(SELECT 1 FROM content_original_terminal_receipts WHERE run_id=r) THEN
   RAISE EXCEPTION 'CONTENT_TERMINAL_HISTORY_CLOSED' USING ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END $$;
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['coordinator','worker_instances','runs','attempts','controls','stage_invocations','stage_artifacts','rss_deliveries',
 'run_inputs','run_sources','audit_events','content_attempt_admissions','content_admission_milestones','content_operations','content_operation_phases',
 'content_native_operations','content_native_launch_phases','content_fetch_completion_intents','content_fetch_checkpoints',
 'content_fetch_recovery_handoffs','content_fetch_recovery_finalizations','content_bronze_reservations','content_bronze_launch_phases',
 'content_bronze_verification_phases','content_bronze_checkpoints','content_silver_reservations','content_pipeline_operations',
 'content_pipeline_phases','content_pipeline_checkpoints','content_pipeline_completions','artifacts','artifact_dependencies'] LOOP
  EXECUTE format('CREATE TRIGGER aa_content_terminal_closed BEFORE INSERT OR UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION guard_content_terminal_closed()',t);
 END LOOP;
END $$;

-- Deferred transition checks prove the starting rows were actually observed, rather than trusting
-- caller-authored snapshot states. Every update must be the single exact transition in the receipt.
CREATE FUNCTION check_content_terminal_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE h content_original_terminal_receipts%ROWTYPE; key text; s jsonb;
BEGIN
 IF TG_TABLE_NAME='runs' AND to_jsonb(NEW)->>'outcome_code'='NATIVE_CONTENT_INTERRUPTED'
 AND NOT EXISTS(SELECT 1 FROM content_original_terminal_receipts WHERE run_id=(to_jsonb(NEW)->>'run_id')::uuid) THEN
  RAISE EXCEPTION 'CONTENT_TERMINAL_RECEIPT_REQUIRED' USING ERRCODE='23514'; END IF;
 IF TG_TABLE_NAME='coordinator' THEN
  IF OLD.active_attempt_id IS NOT NULL AND NEW.active_attempt_id IS NULL
  AND EXISTS(SELECT 1 FROM content_admission_milestones WHERE attempt_id=OLD.active_attempt_id AND phase='READY')
  AND NOT EXISTS(SELECT 1 FROM content_pipeline_completions WHERE attempt_id=OLD.active_attempt_id)
  AND NOT EXISTS(SELECT 1 FROM content_original_terminal_receipts WHERE original_attempt_id=OLD.active_attempt_id) THEN
   RAISE EXCEPTION 'CONTENT_TERMINAL_RECEIPT_REQUIRED' USING ERRCODE='23514'; END IF;
 END IF;
 FOR h IN SELECT * FROM content_original_terminal_receipts x WHERE
  (TG_TABLE_NAME='coordinator' AND x.original_coordinator_version=OLD.row_version)
  OR (TG_TABLE_NAME='runs' AND x.run_id=(to_jsonb(OLD)->>'run_id')::uuid)
  OR (TG_TABLE_NAME='attempts' AND x.original_attempt_id=(to_jsonb(OLD)->>'attempt_id')::uuid)
  OR (TG_TABLE_NAME='controls' AND x.control_id=(to_jsonb(OLD)->>'control_id')::uuid)
  OR (TG_TABLE_NAME='worker_instances' AND x.terminal_instance_id=(to_jsonb(OLD)->>'instance_id')::uuid)
 LOOP
  key:=CASE TG_TABLE_NAME WHEN 'coordinator' THEN 'coordinator' WHEN 'runs' THEN 'run' WHEN 'attempts' THEN 'attempt'
   WHEN 'controls' THEN 'control' WHEN 'worker_instances' THEN 'terminalWorker' END;
  s:=convert_from(h.snapshot_bytes,'UTF8')::jsonb;
  IF content_terminal_compact(to_jsonb(OLD)) IS DISTINCT FROM s->key OR NOT content_terminal_receipt_valid(h.terminal_id) THEN
   RAISE EXCEPTION 'CONTENT_TERMINAL_TRANSITION_INVALID' USING ERRCODE='23514'; END IF;
 END LOOP;
 RETURN NULL;
END $$;
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['coordinator','runs','attempts','controls','worker_instances'] LOOP
  EXECUTE format('CREATE CONSTRAINT TRIGGER content_terminal_transition AFTER UPDATE ON %I DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_content_terminal_transition()',t);
 END LOOP;
END $$;
COMMENT ON TABLE content_original_terminal_receipts IS 'Inactive original Content direct safe-end receipts. Historical evidence only; no handoff, retry, continuation, cleanup or dispatch authority.';
