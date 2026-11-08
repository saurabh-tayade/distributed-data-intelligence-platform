-- ADR 0046: immutable, separately identified public FETCH composition. No activation.
-- V1 branches retain the preceding guards. No historical rows or receipts are rewritten.
-- Canonical hashes below are new-v2 checks only. UTF-8 byte ordering matches SourceJson.
CREATE FUNCTION public_fetch_json_sha256(n jsonb) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path=ddip_management,pg_catalog AS $$
 SELECT encode(sha256(convert_to(content_terminal_canonical(n),'UTF8')),'hex')
$$;
CREATE FUNCTION public_fetch_network_policy() RETURNS jsonb
LANGUAGE sql IMMUTABLE SET search_path=ddip_management,pg_catalog AS $$
 SELECT jsonb_build_object('policyVersion',1,'policyId','ddip-public-feed-pinned-v1',
 'resolverProvider','platform.ingestion.managed.PublicFetchResolverProvider','maxResolvedAddresses',64,
 'addressPolicyVersion',1,'addressPolicySha256','85959d534b478bf1c8194e99f49a2c860f9fe9864978ed7118496913767cefcd',
 'addressFamily','IPV4_ONLY','httpVersion','HTTP_1_1','maxApplicationInvocations',1,
 'transportRetryPolicy','JDK_BOUNDED_PINNED_V1','redirects',false,'proxies',false)
$$;


-- Registration-only correspondence, with no filesystem or network readiness claim.
-- All rows are immutable; start from the actual RunProfile, never a caller's chain.
CREATE FUNCTION require_public_fetch_registration(fetch_id text, fetch_sha text) RETURNS jsonb
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE fetched jsonb; rp jsonb; raw bytea; digest text; rp_sha text; rp_id text;
 control storage_bindings%ROWTYPE; storage storage_bindings%ROWTYPE;
 runtime jsonb; runtime_id text; runtime_sha text; family text; next_family text;
 bronze jsonb; bronze_id text; bronze_sha text; silver jsonb; gold jsonb; content jsonb;
 chain jsonb:='{}'::jsonb; ids text[]:=ARRAY[]::text[]; item record; binding_id_ text;
BEGIN
 SELECT configuration_bytes,configuration_sha256::text INTO raw,digest FROM execution_profiles WHERE profile_id=fetch_id;
 fetched:=convert_from(raw,'UTF8')::jsonb;
 rp_id:=fetched->>'runProfileId';
 IF raw IS NULL OR digest IS DISTINCT FROM fetch_sha
  OR raw IS DISTINCT FROM convert_to(content_terminal_canonical(fetched),'UTF8') THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PUBLIC_REGISTRATION_FETCH_INVALID',ERRCODE='23514'; END IF;
 SELECT configuration_bytes,configuration_sha256::text INTO raw,rp_sha FROM execution_profiles WHERE profile_id=rp_id;
 rp:=convert_from(raw,'UTF8')::jsonb;
 IF raw IS NULL OR rp_sha IS DISTINCT FROM fetched->>'runProfileSha256'
  OR raw IS DISTINCT FROM convert_to(content_terminal_canonical(rp),'UTF8')
  OR rp->'runProfileVersion' IS DISTINCT FROM '2'::jsonb
  OR rp->>'pipeline' IS DISTINCT FROM 'CONTENT_RSS_V1'
  OR rp->'runtime'->>'deploymentSha256' IS DISTINCT FROM fetched->>'deploymentSha256' THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PUBLIC_REGISTRATION_RUN_PROFILE_INVALID',ERRCODE='23514'; END IF;
 SELECT b.* INTO control FROM execution_profile_storage p JOIN storage_bindings b USING(binding_id)
  WHERE p.profile_id=rp_id AND b.binding_id=fetched->>'controlBindingId' AND b.role='CONTROL';
 IF control.binding_id IS NULL OR control.configuration_bytes IS DISTINCT FROM
  convert_to(content_terminal_canonical(convert_from(control.configuration_bytes,'UTF8')::jsonb),'UTF8') THEN
  RAISE EXCEPTION USING MESSAGE='CONTENT_PUBLIC_REGISTRATION_CONTROL_INVALID',ERRCODE='23514'; END IF;
 runtime_id:=rp->>'contentRuntimeId';
 FOREACH family IN ARRAY ARRAY['content','gold','silver','bronze','fetch'] LOOP
  SELECT configuration_bytes,configuration_sha256::text INTO raw,digest FROM execution_profiles WHERE profile_id=runtime_id;
  runtime:=convert_from(raw,'UTF8')::jsonb;
  IF raw IS NULL OR runtime_id=ANY(ids)
   OR (family<>'content' AND digest IS DISTINCT FROM runtime_sha)
   OR raw IS DISTINCT FROM convert_to(content_terminal_canonical(runtime),'UTF8')
   OR runtime->(CASE WHEN family='fetch' THEN 'rssFetchRuntimeVersion' ELSE family||'RuntimeVersion' END) IS DISTINCT FROM '2'::jsonb
   OR NOT EXISTS(SELECT 1 FROM execution_profile_storage WHERE profile_id=runtime_id AND binding_id=control.binding_id) THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PUBLIC_REGISTRATION_CHAIN_INVALID',ERRCODE='23514'; END IF;
  ids:=array_append(ids,runtime_id);
  chain:=chain||jsonb_build_object(family,runtime_id);
  IF family IN ('silver','bronze','fetch') AND
   (runtime->>'runProfileSha256' IS DISTINCT FROM rp_sha
    OR runtime->>'deploymentSha256' IS DISTINCT FROM rp->'runtime'->>'deploymentSha256') THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PUBLIC_REGISTRATION_CHAIN_INVALID',ERRCODE='23514'; END IF;
  IF family IN ('bronze','fetch') AND
   (runtime->>'runProfileId' IS DISTINCT FROM rp_id OR runtime->>'controlBindingId' IS DISTINCT FROM control.binding_id::text) THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PUBLIC_REGISTRATION_CHAIN_INVALID',ERRCODE='23514'; END IF;
  CASE family
   WHEN 'content' THEN content:=runtime;
   WHEN 'gold' THEN gold:=runtime;
   WHEN 'silver' THEN silver:=runtime;
   WHEN 'bronze' THEN bronze:=runtime;bronze_id:=runtime_id;bronze_sha:=digest;
   ELSE
    IF runtime_id IS DISTINCT FROM fetch_id OR digest IS DISTINCT FROM fetch_sha THEN
     RAISE EXCEPTION USING MESSAGE='CONTENT_PUBLIC_REGISTRATION_CHAIN_INVALID',ERRCODE='23514'; END IF;
  END CASE;
  IF family<>'fetch' THEN
   next_family:=CASE family WHEN 'content' THEN 'gold' WHEN 'gold' THEN 'silver' WHEN 'silver' THEN 'bronze' ELSE 'fetch' END;
   runtime_id:=runtime->>(next_family||'RuntimeId');runtime_sha:=runtime->>(next_family||'RuntimeSha256');
  END IF;
 END LOOP;
 -- Match the registered family's storage use as well as RunProfile membership.
 FOR item IN SELECT * FROM (VALUES
  ('bronze',bronze->>'storageBindingId','BRONZE'),
  ('silver',silver->>'outputBindingId','SILVER'),('silver',silver->>'tempBindingId','TEMP'),
  ('gold',gold->>'goldBindingId','GOLD'),('gold',silver->>'outputBindingId','SILVER'),('gold',silver->>'tempBindingId','TEMP'),
  ('content',content->>'referenceBindingId','REFERENCE'),('content',content->>'contentBindingId','CONTENT'),
  ('content',gold->>'goldBindingId','GOLD'),('content',silver->>'tempBindingId','TEMP')
 ) x(family,binding_id,role) LOOP
  SELECT b.* INTO storage FROM storage_bindings b
   JOIN execution_profile_storage p ON p.binding_id=b.binding_id AND p.profile_id=rp_id
   JOIN execution_profile_storage q ON q.binding_id=b.binding_id AND q.profile_id=chain->>item.family
   WHERE b.binding_id=item.binding_id AND b.role=item.role AND b.product='content-intelligence'
    AND b.installation_id=control.installation_id;
  IF storage.binding_id IS NULL OR storage.configuration_bytes IS DISTINCT FROM
   convert_to(content_terminal_canonical(convert_from(storage.configuration_bytes,'UTF8')::jsonb),'UTF8')
   OR (item.family='bronze' AND storage.configuration_sha256::text IS DISTINCT FROM bronze->>'storageBindingSha256') THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PUBLIC_REGISTRATION_STORAGE_INVALID',ERRCODE='23514'; END IF;
 END LOOP;
 RETURN jsonb_build_object('bronzeRuntimeId',bronze_id,'bronzeRuntimeSha256',bronze_sha);
END $$;


CREATE OR REPLACE FUNCTION guard_content_fetch_launch_profile() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE n jsonb; historical jsonb; raw bytea; digest text; manifest jsonb; expected jsonb; entry jsonb;
    public_v2 boolean; provider jsonb;
BEGIN
    n:=convert_from(NEW.configuration_bytes,'UTF8')::jsonb;
    public_v2:=n->'strictFetchLaunchVersion'='2'::jsonb;
    SELECT configuration_bytes,configuration_sha256::text INTO raw,digest
        FROM execution_profiles WHERE profile_id=n->>'fetchRuntimeId';
    IF raw IS NULL THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    historical:=convert_from(raw,'UTF8')::jsonb;
    IF historical->'rssFetchRuntimeVersion' IS DISTINCT FROM '2'::jsonb
       OR historical->>'pipeline' IS DISTINCT FROM 'CONTENT_RSS_V1'
       OR historical->'manifest'->>'adapter' IS DISTINCT FROM 'platform.ingestion.managed.BoundedManagedRssFetch'
       OR historical->'testEndpoint' IS DISTINCT FROM (CASE WHEN public_v2 THEN 'false'::jsonb ELSE 'true'::jsonb END)
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
    IF public_v2 THEN
        PERFORM require_public_fetch_registration(n->>'fetchRuntimeId',digest);
        provider:=n->'manifest'->'classpath'->jsonb_array_length(historical->'manifest'->'classpath');
        IF jsonb_array_length(historical->'manifest'->'classpath')>=128
           OR jsonb_typeof(provider) IS DISTINCT FROM 'object'
           OR NOT provider ?& ARRAY['path','sha256'] OR provider-ARRAY['path','sha256']<>'{}'::jsonb
           OR jsonb_typeof(provider->'path') IS DISTINCT FROM 'string' OR provider->>'path' !~ '^/.*[.]jar$'
           OR provider->>'path' ~ '//|(^|/)[.]{1,2}(/|$)'
           OR jsonb_typeof(provider->'sha256') IS DISTINCT FROM 'string' OR provider->>'sha256' !~ '^[0-9a-f]{64}$'
           OR EXISTS(SELECT 1 FROM jsonb_array_elements(historical->'manifest'->'classpath') e
             WHERE e->>'path'=provider->>'path' OR e->>'path' !~ '^/.*[.]jar$'
               OR e->>'path' ~ '//|(^|/)[.]{1,2}(/|$)')
           OR (SELECT count(DISTINCT e->>'path') FROM jsonb_array_elements(historical->'manifest'->'classpath') e)
               <>jsonb_array_length(historical->'manifest'->'classpath') THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
        END IF;
        -- Filename is not provider authority. Live qualification checks the JAR service and bytes.
        manifest:=jsonb_build_object('adapter','platform.ingestion.managed.StrictPublicManagedRssFetch',
            'javaRuntime',historical->'manifest'->'javaRuntime',
            'classpath',(historical->'manifest'->'classpath')||jsonb_build_array(provider));
        expected:=expected||jsonb_build_object('strictFetchLaunchVersion',2,'testEndpoint',false,
            'manifest',manifest,'deploymentSha256',public_fetch_json_sha256(manifest),
            'networkPolicy',public_fetch_network_policy(),'networkPolicySha256',public_fetch_json_sha256(public_fetch_network_policy()));
    END IF;
    -- Java qualification additionally verifies canonical bytes and the canonical manifest hash.
    IF jsonb_typeof(n->'fetchRuntimeId') IS DISTINCT FROM 'string'
       OR (public_v2 AND NEW.configuration_bytes IS DISTINCT FROM convert_to(content_terminal_canonical(expected),'UTF8'))
       OR NOT native_reservation_json_matches(convert_from(NEW.configuration_bytes,'UTF8')::json,expected) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION guard_content_native_operation() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE
    c coordinator%ROWTYPE; r runs%ROWTYPE; a attempts%ROWTYPE; w worker_instances%ROWTYPE;
    admission jsonb; ready jsonb; frozen jsonb; selected jsonb; profile jsonb; runtime jsonb;
    runtime_id text; runtime_sha text; registered_sha text; raw bytea; family text; next_family text; fields text[];
    cp bytea; writer jsonb; expected_writer jsonb; frozen_identity jsonb; current_frozen_identity jsonb;
    profile_sha text; content_sha text; fetch_facts jsonb; command jsonb; bootstrap jsonb;
    supplied jsonb; launch jsonb; launch_sha text; pipeline_profile jsonb;
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
    supplied:=convert_from(NEW.bootstrap_bytes,'UTF8')::jsonb;
    IF supplied->'reservationVersion'='2'::jsonb THEN
        SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO pipeline_profile
          FROM content_pipeline_profiles WHERE convert_from(configuration_bytes,'UTF8')::jsonb->>'runProfileId'=r.execution_profile_id::text;
        IF (SELECT count(*) FROM content_pipeline_profiles
          WHERE convert_from(configuration_bytes,'UTF8')::jsonb->>'runProfileId'=r.execution_profile_id::text)<>1
          OR pipeline_profile->'strictContentPipelineVersion' IS DISTINCT FROM '2'::jsonb
          OR pipeline_profile->>'runProfileSha256' IS DISTINCT FROM profile_sha
          OR pipeline_profile->>'contentRuntimeSha256' IS DISTINCT FROM content_sha THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_PUBLIC_PIPELINE_INVALID',ERRCODE='23514';
        END IF;
        SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO launch,launch_sha
          FROM content_fetch_launch_profiles WHERE launch_profile_id=pipeline_profile->>'fetchLaunchProfileId';
        IF launch IS NULL OR pipeline_profile->>'fetchLaunchProfileSha256' IS DISTINCT FROM launch_sha
           OR launch->'strictFetchLaunchVersion' IS DISTINCT FROM '2'::jsonb
           OR launch->>'fetchRuntimeId' IS DISTINCT FROM runtime_id
           OR launch->>'fetchRuntimeSha256' IS DISTINCT FROM runtime_sha
           OR launch->>'runProfileId' IS DISTINCT FROM r.execution_profile_id::text
           OR launch->>'runProfileSha256' IS DISTINCT FROM profile_sha
           OR launch->>'controlBindingId' IS DISTINCT FROM admission->'controlBinding'->>'bindingId'
           OR runtime->'testEndpoint' IS DISTINCT FROM 'false'::jsonb
           OR selected->'endpoint' IS DISTINCT FROM jsonb_build_object('endpointId',selected->'configuration'->'feedEndpointId',
                'sourceId',selected->'sourceId','uri',selected->'endpoint'->'uri')
           OR public_fetch_json_sha256(selected->'endpoint') IS DISTINCT FROM selected->>'endpointSha256'
           OR NOT EXISTS(SELECT 1 FROM run_sources rs JOIN source_revisions sr USING(product_id,source_id,revision)
                WHERE rs.run_id=r.run_id AND rs.source_id=selected->>'sourceId'
                AND to_jsonb(rs.revision)=selected->'revision'
                AND sr.endpoint_id::text=selected->'endpoint'->>'endpointId'
                AND sr.endpoint_sha256::text=selected->>'endpointSha256') THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_PUBLIC_BINDING_INVALID',ERRCODE='23514';
        END IF;
        bootstrap:=bootstrap||jsonb_build_object('reservationVersion',2,'mode','PUBLIC_FETCH_SOURCE_RESERVATION',
            'publicFetch',jsonb_build_object('launchProfileId',pipeline_profile->'fetchLaunchProfileId',
              'launchProfileSha256',launch_sha,'networkPolicySha256',launch->'networkPolicySha256',
              'selectedEndpoint',selected->'endpoint','networkPolicy',launch->'networkPolicy'));
    END IF;
    IF NOT native_reservation_json_matches(convert_from(NEW.command_bytes,'UTF8')::json,command)
       OR NOT native_reservation_json_matches(convert_from(NEW.bootstrap_bytes,'UTF8')::json,bootstrap) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_OPERATION_BINDING_INVALID',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION guard_content_native_launch_phase() RETURNS trigger
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
           OR ((launch->'strictFetchLaunchVersion'='1'::jsonb AND facts->'reservationVersion'='1'::jsonb
                AND command->'testEndpoint'='true'::jsonb)
             OR (launch->'strictFetchLaunchVersion'='2'::jsonb AND facts->'reservationVersion'='2'::jsonb
                AND command->'testEndpoint'='false'::jsonb
                AND facts->'publicFetch'->>'launchProfileId'=NEW.launch_profile_id
                AND facts->'publicFetch'->>'launchProfileSha256'=digest
                AND facts->'publicFetch'->'networkPolicy'=launch->'networkPolicy'
                AND facts->'publicFetch'->'networkPolicySha256'=launch->'networkPolicySha256')) IS NOT TRUE THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_NATIVE_LAUNCH_BINDING_INVALID',ERRCODE='23514';
        END IF;
    END IF;
    -- CANDIDATE_EXITED is historical evidence only, never stage success or slot release.
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION guard_content_bronze_launch_profile() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE n jsonb; bronze jsonb; fetched jsonb; raw bytea; digest text; fetch_raw bytea;
    fetch_digest text; expected jsonb; manifest jsonb; entry jsonb;
    public_v2 boolean; public_launch jsonb; public_digest text; linked_bronze jsonb; storage jsonb; storage_digest text; port text;
BEGIN
    n:=convert_from(NEW.configuration_bytes,'UTF8')::jsonb;
    public_v2:=n->'strictBronzeLaunchVersion'='2'::jsonb;
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
       OR fetched->'testEndpoint' IS DISTINCT FROM (CASE WHEN public_v2 THEN 'false'::jsonb ELSE 'true'::jsonb END)
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
    IF public_v2 THEN
        SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO public_launch,public_digest
          FROM content_fetch_launch_profiles WHERE launch_profile_id=n->>'fetchLaunchProfileId';
        linked_bronze:=require_public_fetch_registration(bronze->>'fetchRuntimeId',fetch_digest);
        SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO storage,storage_digest
          FROM storage_bindings WHERE binding_id=bronze->>'storageBindingId';
        port:=substring(storage->>'endpoint' from '^http://(?:127[.]0[.]0[.]1|\[::1\]):([0-9]+)/*$');
        IF linked_bronze->>'bronzeRuntimeId' IS DISTINCT FROM n->>'bronzeRuntimeId'
           OR linked_bronze->>'bronzeRuntimeSha256' IS DISTINCT FROM digest
           OR public_launch IS NULL OR public_launch->'strictFetchLaunchVersion' IS DISTINCT FROM '2'::jsonb
           OR public_launch->>'fetchRuntimeId' IS DISTINCT FROM bronze->>'fetchRuntimeId'
           OR public_launch->>'fetchRuntimeSha256' IS DISTINCT FROM fetch_digest
           OR public_launch->>'runProfileId' IS DISTINCT FROM bronze->>'runProfileId'
           OR public_launch->>'runProfileSha256' IS DISTINCT FROM bronze->>'runProfileSha256'
           OR public_launch->>'controlBindingId' IS DISTINCT FROM bronze->>'controlBindingId'
           OR storage_digest IS DISTINCT FROM bronze->>'storageBindingSha256'
           OR storage->>'endpoint' !~ '^http://(127[.]0[.]0[.]1|\[::1\]):[0-9]+/?$'
           OR port IS NULL OR length(port)>5 OR port::numeric NOT BETWEEN 1 AND 65535 THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
        END IF;
        expected:=(expected-'testEndpoint')||jsonb_build_object('strictBronzeLaunchVersion',2,
          'sourceTestEndpoint',false,'storageEndpointPolicy','LITERAL_LOOPBACK_HTTP_V1',
          'fetchLaunchProfileId',n->'fetchLaunchProfileId','fetchLaunchProfileSha256',public_digest,
          'deploymentSha256',public_fetch_json_sha256(manifest),
          'strictRuntimeSha256',public_fetch_json_sha256(jsonb_build_object('strictBronzeRuntimeVersion',1,
              'adapter','platform.ingestion.managed.StrictManagedBronze','javaRuntime',manifest->'javaRuntime')));
    END IF;
    -- Java also checks canonical bytes, manifest digest and the exact strict protocol digest.
    -- strictRuntimeSha256 is deliberately separate from deploymentSha256.
    IF jsonb_typeof(n->'bronzeRuntimeId') IS DISTINCT FROM 'string'
       OR (public_v2 AND NEW.configuration_bytes IS DISTINCT FROM convert_to(content_terminal_canonical(expected),'UTF8'))
       OR NOT native_reservation_json_matches(convert_from(NEW.configuration_bytes,'UTF8')::json,expected) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION guard_content_bronze_verification_profile() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE n jsonb; bronze jsonb; fetched jsonb; raw bytea; digest text; fetch_raw bytea;
    fetch_digest text; expected jsonb; manifest jsonb; entry jsonb;
    public_v2 boolean; public_launch jsonb; public_digest text; linked_bronze jsonb; storage jsonb; storage_digest text; port text;
BEGIN
    n:=convert_from(NEW.configuration_bytes,'UTF8')::jsonb;
    public_v2:=n->'strictBronzeVerificationVersion'='2'::jsonb;
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
       OR fetched->'testEndpoint' IS DISTINCT FROM (CASE WHEN public_v2 THEN 'false'::jsonb ELSE 'true'::jsonb END)
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
    IF public_v2 THEN
        SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO public_launch,public_digest
          FROM content_fetch_launch_profiles WHERE launch_profile_id=n->>'fetchLaunchProfileId';
        linked_bronze:=require_public_fetch_registration(bronze->>'fetchRuntimeId',fetch_digest);
        SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO storage,storage_digest
          FROM storage_bindings WHERE binding_id=bronze->>'storageBindingId';
        port:=substring(storage->>'endpoint' from '^http://(?:127[.]0[.]0[.]1|\[::1\]):([0-9]+)/*$');
        IF linked_bronze->>'bronzeRuntimeId' IS DISTINCT FROM n->>'bronzeRuntimeId'
           OR linked_bronze->>'bronzeRuntimeSha256' IS DISTINCT FROM digest
           OR public_launch IS NULL OR public_launch->'strictFetchLaunchVersion' IS DISTINCT FROM '2'::jsonb
           OR public_launch->>'fetchRuntimeId' IS DISTINCT FROM bronze->>'fetchRuntimeId'
           OR public_launch->>'fetchRuntimeSha256' IS DISTINCT FROM fetch_digest
           OR public_launch->>'runProfileId' IS DISTINCT FROM bronze->>'runProfileId'
           OR public_launch->>'runProfileSha256' IS DISTINCT FROM bronze->>'runProfileSha256'
           OR public_launch->>'controlBindingId' IS DISTINCT FROM bronze->>'controlBindingId'
           OR storage_digest IS DISTINCT FROM bronze->>'storageBindingSha256'
           OR storage->>'endpoint' !~ '^http://(127[.]0[.]0[.]1|\[::1\]):[0-9]+/?$'
           OR port IS NULL OR length(port)>5 OR port::numeric NOT BETWEEN 1 AND 65535 THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_PROFILE_INVALID',ERRCODE='23514';
        END IF;
        expected:=(expected-'testEndpoint')||jsonb_build_object('strictBronzeVerificationVersion',2,
          'sourceTestEndpoint',false,'storageEndpointPolicy','LITERAL_LOOPBACK_HTTP_V1',
          'fetchLaunchProfileId',n->'fetchLaunchProfileId','fetchLaunchProfileSha256',public_digest,
          'deploymentSha256',public_fetch_json_sha256(manifest),
          'strictRuntimeSha256',public_fetch_json_sha256(jsonb_build_object('strictBronzeVerifierRuntimeVersion',1,
              'adapter','platform.ingestion.managed.StrictBronzeVerifier','javaRuntime',manifest->'javaRuntime')));
    END IF;
    -- Java also checks canonical bytes, manifest digest and the exact strict protocol digest.
    -- strictRuntimeSha256 is deliberately separate from deploymentSha256.
    IF jsonb_typeof(n->'bronzeRuntimeId') IS DISTINCT FROM 'string'
       OR (public_v2 AND NEW.configuration_bytes IS DISTINCT FROM convert_to(content_terminal_canonical(expected),'UTF8'))
       OR NOT native_reservation_json_matches(convert_from(NEW.configuration_bytes,'UTF8')::json,expected) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_PROFILE_INVALID',ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION guard_content_bronze_launch_phase() RETURNS trigger
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
    IF NEW.phase='LAUNCH_BOUND' AND launch->'strictBronzeLaunchVersion'='2'::jsonb THEN
        IF NOT EXISTS(SELECT 1 FROM content_native_launch_phases p
          WHERE p.operation_id=reservation.fetch_operation_id AND p.phase='LAUNCH_BOUND'
          AND p.launch_profile_id=launch->>'fetchLaunchProfileId'
          AND p.launch_profile_sha256::text=launch->>'fetchLaunchProfileSha256') THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_LAUNCH_PUBLIC_PREDECESSOR_INVALID',ERRCODE='23514';
        END IF;
    END IF;
    -- CANDIDATE_EXITED records evidence, including bounded failure. It cannot complete a
    -- checkpoint, change legacy stage/delivery state or release the coordinator slot.
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION guard_content_bronze_verification_phase() RETURNS trigger
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
        IF profile->'strictBronzeVerificationVersion' IS DISTINCT FROM launched->'strictBronzeLaunchVersion' THEN
            RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_BINDING_INVALID',ERRCODE='23514';
        END IF;
        IF profile->'strictBronzeVerificationVersion'='2'::jsonb THEN
            FOREACH k IN ARRAY ARRAY['sourceTestEndpoint','storageEndpointPolicy','fetchLaunchProfileId','fetchLaunchProfileSha256'] LOOP
                IF profile->k IS DISTINCT FROM launched->k THEN
                    RAISE EXCEPTION USING MESSAGE='CONTENT_BRONZE_VERIFICATION_BINDING_INVALID',ERRCODE='23514';
                END IF;
            END LOOP;
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

CREATE OR REPLACE FUNCTION guard_content_pipeline_profile() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE n jsonb; rp jsonb; runtime jsonb; digest text; k text; registered jsonb; fields text[];
    public_v2 boolean; fetch_profile jsonb; bronze_profile jsonb; verifier_profile jsonb; bronze_descriptor jsonb;
BEGIN
 n:=convert_from(NEW.configuration_bytes,'UTF8')::jsonb;
 public_v2:=n->'strictContentPipelineVersion'='2'::jsonb;
 fields:=ARRAY['strictContentPipelineVersion','runProfileId','runProfileSha256','contentRuntimeId','contentRuntimeSha256',
 'controlBindingId','strictRuntimeSha256','python','processingRoot','javaHome','modules','versions',
 'fetchLaunchProfileId','bronzeLaunchProfileId','bronzeVerificationProfileId'];
 IF public_v2 THEN fields:=fields||ARRAY['fetchLaunchProfileSha256','bronzeLaunchProfileSha256','bronzeVerificationProfileSha256']; END IF;
 SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO rp,digest
 FROM execution_profiles WHERE profile_id=n->>'runProfileId';
 IF NOT native_reservation_json_matches(convert_from(NEW.configuration_bytes,'UTF8')::json,n)
 OR NOT n ?& fields OR n-fields<>'{}'::jsonb OR n->'strictContentPipelineVersion' IS DISTINCT FROM (CASE WHEN public_v2 THEN '2'::jsonb ELSE '1'::jsonb END) OR n->>'strictContentPipelineVersion' IS DISTINCT FROM (CASE WHEN public_v2 THEN '2' ELSE '1' END)
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
 OR registered->'testEndpoint' IS DISTINCT FROM (CASE WHEN public_v2 THEN 'false'::jsonb ELSE 'true'::jsonb END) THEN
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
 IF public_v2 THEN
  IF NEW.configuration_bytes IS DISTINCT FROM convert_to(content_terminal_canonical(n),'UTF8') THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PUBLIC_COMPOSITION_INVALID',ERRCODE='23514'; END IF;
  SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO fetch_profile FROM content_fetch_launch_profiles
   WHERE launch_profile_id=n->>'fetchLaunchProfileId' AND configuration_sha256::text=n->>'fetchLaunchProfileSha256';
  SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO bronze_profile FROM content_bronze_launch_profiles
   WHERE launch_profile_id=n->>'bronzeLaunchProfileId' AND configuration_sha256::text=n->>'bronzeLaunchProfileSha256';
  SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO verifier_profile FROM content_bronze_verification_profiles
   WHERE verification_profile_id=n->>'bronzeVerificationProfileId' AND configuration_sha256::text=n->>'bronzeVerificationProfileSha256';
  SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO registered FROM execution_profiles
   WHERE profile_id=runtime->>'goldRuntimeId' AND configuration_sha256::text=runtime->>'goldRuntimeSha256';
  SELECT convert_from(configuration_bytes,'UTF8')::jsonb INTO registered FROM execution_profiles
   WHERE profile_id=registered->>'silverRuntimeId' AND configuration_sha256::text=registered->>'silverRuntimeSha256';
  SELECT convert_from(configuration_bytes,'UTF8')::jsonb,configuration_sha256::text INTO bronze_descriptor,digest
   FROM execution_profiles WHERE profile_id=registered->>'bronzeRuntimeId' AND configuration_sha256::text=registered->>'bronzeRuntimeSha256';
  IF fetch_profile IS NULL OR bronze_profile IS NULL OR verifier_profile IS NULL OR bronze_descriptor IS NULL
   OR fetch_profile->'strictFetchLaunchVersion' IS DISTINCT FROM '2'::jsonb
   OR bronze_profile->'strictBronzeLaunchVersion' IS DISTINCT FROM '2'::jsonb
   OR verifier_profile->'strictBronzeVerificationVersion' IS DISTINCT FROM '2'::jsonb
   OR bronze_profile->>'bronzeRuntimeId' IS DISTINCT FROM registered->>'bronzeRuntimeId'
   OR bronze_profile->>'bronzeRuntimeSha256' IS DISTINCT FROM digest
   OR verifier_profile->>'bronzeRuntimeId' IS DISTINCT FROM bronze_profile->>'bronzeRuntimeId'
   OR verifier_profile->>'bronzeRuntimeSha256' IS DISTINCT FROM digest
   OR fetch_profile->>'fetchRuntimeId' IS DISTINCT FROM bronze_descriptor->>'fetchRuntimeId'
   OR fetch_profile->>'fetchRuntimeSha256' IS DISTINCT FROM bronze_descriptor->>'fetchRuntimeSha256' THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PUBLIC_COMPOSITION_INVALID',ERRCODE='23514'; END IF;
  FOREACH k IN ARRAY ARRAY['fetchLaunchProfileId','fetchLaunchProfileSha256'] LOOP
   IF bronze_profile->k IS DISTINCT FROM n->k OR verifier_profile->k IS DISTINCT FROM n->k THEN
    RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PUBLIC_COMPOSITION_INVALID',ERRCODE='23514'; END IF;
  END LOOP;
  FOREACH k IN ARRAY ARRAY['sourceTestEndpoint','storageEndpointPolicy','storageBindingId','storageBindingSha256','javaHome'] LOOP
   IF bronze_profile->k IS DISTINCT FROM verifier_profile->k THEN
    RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PUBLIC_COMPOSITION_INVALID',ERRCODE='23514'; END IF;
  END LOOP;
 ELSE
  -- A historical pipeline must not acquire a new Bronze composition through a shared run profile.
  IF (SELECT convert_from(configuration_bytes,'UTF8')::jsonb->'strictBronzeLaunchVersion'
      FROM content_bronze_launch_profiles WHERE launch_profile_id=n->>'bronzeLaunchProfileId') IS DISTINCT FROM '1'::jsonb
   OR (SELECT convert_from(configuration_bytes,'UTF8')::jsonb->'strictBronzeVerificationVersion'
      FROM content_bronze_verification_profiles WHERE verification_profile_id=n->>'bronzeVerificationProfileId') IS DISTINCT FROM '1'::jsonb THEN
   RAISE EXCEPTION USING MESSAGE='CONTENT_PIPELINE_PROFILE_INVALID',ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION guard_content_pipeline_operation() RETURNS trigger
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
   AND phase='LAUNCH_BOUND' AND launch_profile_id=profile->>'fetchLaunchProfileId'
   AND (profile->'strictContentPipelineVersion'='1'::jsonb OR launch_profile_sha256::text=profile->>'fetchLaunchProfileSha256'))
 OR NOT EXISTS(SELECT 1 FROM content_bronze_launch_phases WHERE operation_id=b.operation_id
   AND phase='LAUNCH_BOUND' AND launch_profile_id=profile->>'bronzeLaunchProfileId'
   AND (profile->'strictContentPipelineVersion'='1'::jsonb OR launch_profile_sha256::text=profile->>'bronzeLaunchProfileSha256'))
 OR NOT EXISTS(SELECT 1 FROM content_bronze_verification_phases WHERE operation_id=b.operation_id
   AND phase='VERIFY_BOUND' AND verification_profile_id=profile->>'bronzeVerificationProfileId'
   AND (profile->'strictContentPipelineVersion'='1'::jsonb OR verification_profile_sha256::text=profile->>'bronzeVerificationProfileSha256')) THEN
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

CREATE OR REPLACE FUNCTION guard_content_fetch_recovery_handoff() RETURNS trigger
LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
DECLARE c coordinator%ROWTYPE; r runs%ROWTYPE; a attempts%ROWTYPE; successor attempts%ROWTYPE;
    ow worker_instances%ROWTYPE; rw worker_instances%ROWTYPE; ctl controls%ROWTYPE;
    op content_native_operations%ROWTYPE; cp content_fetch_checkpoints%ROWTYPE; ancestry jsonb; review jsonb; expected jsonb;
BEGIN
    IF current_setting('transaction_isolation') IS DISTINCT FROM 'read committed' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_ISOLATION_REQUIRED',ERRCODE='23514';
    END IF;
    SELECT * INTO c FROM coordinator WHERE singleton_id=1 FOR UPDATE;
    SELECT * INTO r FROM runs WHERE run_id=NEW.run_id FOR UPDATE;
    SELECT * INTO a FROM attempts WHERE attempt_id=NEW.original_attempt_id FOR UPDATE;
    SELECT * INTO successor FROM attempts WHERE attempt_id=NEW.recovery_attempt_id FOR UPDATE;
    SELECT * INTO ow FROM worker_instances WHERE instance_id=NEW.original_instance_id;
    SELECT * INTO rw FROM worker_instances WHERE instance_id=NEW.recovery_instance_id;
    SELECT * INTO ctl FROM controls WHERE control_id=NEW.control_id FOR UPDATE;
    SELECT * INTO op FROM content_native_operations WHERE operation_id=NEW.operation_id;
    SELECT * INTO cp FROM content_fetch_checkpoints WHERE operation_id=NEW.operation_id;
    IF c.singleton_id IS NULL OR r.run_id IS NULL OR a.attempt_id IS NULL OR successor.attempt_id IS NULL
       OR ow.instance_id IS NULL OR rw.instance_id IS NULL OR ctl.control_id IS NULL
       OR op.operation_id IS NULL OR cp.operation_id IS NULL
       OR a.run_id<>NEW.run_id OR a.kind<>'EXECUTE' OR a.state<>'INTERRUPTED'
       OR a.content_admission_policy IS DISTINCT FROM 'ddip-content-narrow-v1' OR a.content_recovery_policy IS NOT NULL
       OR a.instance_id<>NEW.original_instance_id OR a.owner_epoch<>NEW.original_epoch
       OR a.row_version<>NEW.original_attempt_version+1 OR a.finished_at IS NULL
       OR a.outcome_code IS DISTINCT FROM 'INTERRUPTED'
       OR successor.run_id<>NEW.run_id OR successor.kind<>'RECONCILE' OR successor.state<>'CLAIMED'
       OR successor.content_recovery_policy IS DISTINCT FROM NEW.policy
       OR successor.instance_id<>NEW.recovery_instance_id OR successor.owner_epoch<>NEW.original_epoch+1
       OR successor.row_version<>0 OR successor.finished_at IS NOT NULL OR successor.outcome_code IS NOT NULL
       OR successor.content_admission_policy IS NOT NULL OR successor.reused_checkpoint_id IS NOT NULL
       OR successor.attempt_no<>(SELECT max(attempt_no)+1 FROM attempts
           WHERE run_id=NEW.run_id AND attempt_id<>successor.attempt_id)
       OR r.pipeline<>'CONTENT_RSS_V1' OR r.state<>'RECOVERY_REQUIRED'
       OR r.outcome_code IS DISTINCT FROM 'RECOVERY_REQUIRED' OR r.retry_eligible
       OR r.current_attempt_id IS DISTINCT FROM successor.attempt_id OR r.row_version<>NEW.original_run_version+1
       OR r.result_artifact_id IS NOT NULL OR r.finished_at IS NOT NULL
       OR c.mode<>'RECOVERY_ONLY' OR c.active_import_id IS NOT NULL
       OR c.active_attempt_id IS DISTINCT FROM successor.attempt_id OR c.owner_instance_id IS DISTINCT FROM rw.instance_id
       OR c.epoch<>NEW.original_epoch+1 OR c.row_version<>NEW.original_coordinator_version+1
       OR ow.installation_id<>c.installation_id OR rw.installation_id<>c.installation_id
       OR ow.execution_profile_id<>r.execution_profile_id OR rw.execution_profile_id<>r.execution_profile_id
       OR ow.profile_sha256<>rw.profile_sha256 OR ow.host_binding_sha256<>rw.host_binding_sha256
       OR ow.row_version<>NEW.original_worker_version OR rw.row_version<>NEW.recovery_worker_version
       OR rw.status<>'RECOVERY_ONLY'
       OR ctl.run_id<>NEW.run_id OR ctl.type<>'RECONCILE' OR ctl.state<>'REQUESTED'
       OR ctl.target_attempt_id IS DISTINCT FROM a.attempt_id OR ctl.quiescence_confirmed IS DISTINCT FROM true
       OR ctl.row_version<>NEW.control_version OR ctl.expected_version<>NEW.original_run_version
       OR ctl.requester<>'local-operator'
       OR op.attempt_id<>a.attempt_id OR cp.checkpoint_id<>NEW.checkpoint_id OR op.checkpoint_id<>NEW.checkpoint_id
       OR (SELECT count(*) FROM content_native_launch_phases WHERE operation_id=NEW.operation_id)<>6
       OR NOT EXISTS(SELECT 1 FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='READY')
       OR EXISTS(SELECT 1 FROM attempts prior WHERE prior.run_id=NEW.run_id
           AND prior.attempt_id NOT IN (a.attempt_id,successor.attempt_id)
           AND (prior.kind<>'EXECUTE' OR prior.state NOT IN ('FAILED','CANCELLED')
               OR prior.content_admission_policy IS NOT NULL OR prior.content_recovery_policy IS NOT NULL
               OR prior.reused_checkpoint_id IS NOT NULL))
       OR EXISTS(SELECT 1 FROM stage_invocations s JOIN attempts x USING(attempt_id) WHERE x.run_id=NEW.run_id)
       OR EXISTS(SELECT 1 FROM rss_deliveries WHERE run_id=NEW.run_id)
       OR EXISTS(SELECT 1 FROM content_operations WHERE attempt_id=a.attempt_id)
       OR EXISTS(SELECT 1 FROM controls WHERE run_id=NEW.run_id AND control_id<>NEW.control_id
           AND (state='REQUESTED' OR (type='CANCEL' AND state='APPLIED'))) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_BINDING_INVALID',ERRCODE='23514';
    END IF;
    -- Entry only: old handoff/receipt reads and V16 finalization are unchanged.
    IF convert_from(op.bootstrap_bytes,'UTF8')::jsonb->'reservationVersion' IS DISTINCT FROM '1'::jsonb
       OR convert_from(op.bootstrap_bytes,'UTF8')::jsonb->>'mode' IS DISTINCT FROM 'FETCH_SOURCE_RESERVATION'
       OR convert_from(op.command_bytes,'UTF8')::jsonb->'testEndpoint' IS DISTINCT FROM 'true'::jsonb
       OR NOT EXISTS(SELECT 1 FROM content_native_launch_phases l JOIN content_fetch_launch_profiles p USING(launch_profile_id)
          WHERE l.operation_id=op.operation_id AND l.phase='LAUNCH_BOUND'
          AND l.launch_profile_sha256=p.configuration_sha256
          AND convert_from(p.configuration_bytes,'UTF8')::jsonb->'strictFetchLaunchVersion'='1'::jsonb
          AND convert_from(p.configuration_bytes,'UTF8')::jsonb->'testEndpoint'='true'::jsonb)
       OR EXISTS(SELECT 1 FROM content_pipeline_operations o JOIN content_pipeline_profiles p USING(profile_id)
          WHERE o.attempt_id=op.attempt_id AND convert_from(p.configuration_bytes,'UTF8')::jsonb->'strictContentPipelineVersion' IS DISTINCT FROM '1'::jsonb) THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_V1_REQUIRED',ERRCODE='23514';
    END IF;
    ancestry:=convert_from(NEW.ancestry_bytes,'UTF8')::jsonb;
    review:=convert_from(NEW.review_bytes,'UTF8')::jsonb;
    expected:=jsonb_build_object(
        'identityVersion',1,'policy',NEW.policy,'handoffId',NEW.handoff_id::text,'controlId',NEW.control_id::text,
        'runId',NEW.run_id::text,'originalAttemptId',NEW.original_attempt_id::text,
        'recoveryAttemptId',NEW.recovery_attempt_id::text,'operationId',NEW.operation_id::text,
        'checkpointId',NEW.checkpoint_id::text,
        'admissionSha256',(SELECT admission_sha256::text FROM content_attempt_admissions WHERE attempt_id=a.attempt_id),
        'readySha256',(SELECT receipt_sha256::text FROM content_admission_milestones WHERE attempt_id=a.attempt_id AND phase='READY'),
        'commandSha256',op.command_sha256::text,'bootstrapSha256',op.bootstrap_sha256::text,
        'candidateSha256',(SELECT candidate_sha256::text FROM content_native_launch_phases WHERE operation_id=NEW.operation_id AND phase='CANDIDATE_EXITED'),
        'completionSha256',cp.completion_sha256::text,'journalSha256',cp.journal_sha256::text,'expectedSha256',cp.expected_sha256::text,
        'snapshot',ancestry->'snapshot','snapshotSha256',ancestry->'snapshotSha256');
    IF NOT native_reservation_json_matches(convert_from(NEW.ancestry_bytes,'UTF8')::json,expected)
       OR jsonb_typeof(ancestry->'snapshot') IS DISTINCT FROM 'object'
       OR jsonb_typeof(ancestry->'snapshotSha256') IS DISTINCT FROM 'string'
       OR ancestry->>'snapshotSha256' !~ '^[0-9a-f]{64}$'
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
       OR review->'targetSha256' IS DISTINCT FROM to_jsonb(NEW.ancestry_sha256::text)
       OR jsonb_typeof(review->'inspectedAt') IS DISTINCT FROM 'string'
       OR review->'reviewedCheckpoints' IS DISTINCT FROM jsonb_build_array(NEW.checkpoint_id::text)
       OR jsonb_typeof(review->'descendants') IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_EVIDENCE_INVALID',ERRCODE='23514';
    END IF;
    -- Check type before length/casts so malformed review bytes fail with the bounded diagnostic.
    IF jsonb_array_length(review->'descendants')>32
       OR length(review->>'inspectedAt') NOT BETWEEN 20 AND 40
       OR review->>'inspectedAt' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$' THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_EVIDENCE_INVALID',ERRCODE='23514';
    END IF;
    -- The application checks review freshness against its frozen clock immediately before transfer.
    -- SQL validates timestamp syntax, but does not introduce a second wall-clock authority.
    BEGIN
        PERFORM (review->>'inspectedAt')::timestamptz;
    EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN
        RAISE EXCEPTION USING MESSAGE='CONTENT_FETCH_RECOVERY_EVIDENCE_INVALID',ERRCODE='23514';
    END;
    RETURN NEW;
END $$;
