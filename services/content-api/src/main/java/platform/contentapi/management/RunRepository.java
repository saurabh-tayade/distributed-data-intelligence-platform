package platform.contentapi.management;

import java.sql.*;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.*;
import platform.contentapi.importing.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.management.SourceFailure.Code.*;
import static platform.contentapi.management.SourceFailure.require;

/** Coordinator-first metadata admission only. Never opens an artifact or launches a process. */
public final class RunRepository {
    private static final DateTimeFormatter UTC=new DateTimeFormatterBuilder().appendInstant(6).toFormatter();
    private static final int MAX_EVIDENCE=16*1024*1024;
    private final JdbcTemplate jdbc;
    private final ManagementTransactions tx;
    private final UUID installation;
    private final ReviewedSourceCatalog catalog;
    public RunRepository(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,ReviewedSourceCatalog catalog) {
        this.jdbc=jdbc;this.tx=tx;this.installation=installation;this.catalog=catalog;
    }
    public JsonNode submit(RunCommands.Command c,String correlation) {
        return tx.required(() -> {
            var slots=jdbc.queryForList("SELECT * FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE");
            if(slots.size()!=1 || !installation.equals(slots.getFirst().get("installation_id")))throw new SourceFailure(MANAGEMENT_UNAVAILABLE);
            var existing=jdbc.queryForList("SELECT run_id,normalized_request,request_sha256 FROM ddip_management.runs WHERE product_id=? AND principal_scope='local-operator' AND idempotency_key=?",c.product(),c.key());
            if(!existing.isEmpty()) {
                var old=existing.getFirst();byte[] bytes=(byte[])old.get("normalized_request");
                if(!SourceJson.sha(bytes).equals(old.get("request_sha256")))throw new SourceFailure(STATE_CONFLICT);
                if(!Arrays.equals(bytes,c.normalized()))throw new SourceFailure(IDEMPOTENCY_CONFLICT);
                return detail((UUID)old.get("run_id"));
            }
            readiness(slots.getFirst());capacity();
            var profile=jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",c.profileId());
            require(profile.size()==1);var p=profile.getFirst();JsonNode config=RunProfile.validate(canonical((byte[])p.get("configuration_bytes"),(String)p.get("configuration_sha256")),c);
            var frozen=SourceJson.object().put("selectionVersion",1).put("pipeline",c.pipeline()).put("pipelineVersion",1).put("productId",c.product()).put("principalScope","local-operator").put("triggerType","OPERATOR");
            var runtime=frozen.putObject("executionProfile").put("profileId",c.profileId()).put("configurationSha256",(String)p.get("configuration_sha256"));runtime.set("configuration",config);
            var storage=storage(c);var bs=frozen.putArray("storageBindings");storage.values().forEach(bs::add);
            var sources=frozen.putArray("sources");var sourceConfigs=new HashMap<String,JsonNode>();
            for(var s:c.sources()) {
                var rows=jdbc.queryForList("SELECT s.*,r.configuration_bytes,r.configuration_sha256,r.endpoint_id,r.endpoint_sha256 FROM ddip_management.sources s JOIN ddip_management.source_revisions r ON (s.product_id,s.source_id,s.current_revision)=(r.product_id,r.source_id,r.revision) WHERE s.product_id=? AND s.source_id=? FOR UPDATE OF s",c.product(),s.id());
                require(rows.size()==1);var row=rows.getFirst();
                if(((Number)row.get("current_revision")).intValue()!=s.revision())throw new SourceFailure(STATE_CONFLICT);
                if(!Boolean.TRUE.equals(row.get("enabled")))throw new SourceFailure(SOURCE_DISABLED);
                var sc=SourceCommands.config(c.product(),canonical((byte[])row.get("configuration_bytes"),(String)row.get("configuration_sha256")));
                require(sc.get("profile").equals(config.get("sourceProfile")) && sc.get("profile").stringValue().equals(row.get("profile_family")));
                var approved=catalog.validate(c.product(),s.id(),sc);member(storage,approved.id(),c.content()?"BRONZE":"BATCH");
                var snapshot=sources.addObject().put("sourceId",s.id()).put("revision",s.revision()).put("capturedGateVersion",row.get("row_version").toString()).put("capturedEnabled",true).put("configurationSha256",(String)row.get("configuration_sha256"));snapshot.set("configuration",sc);
                if(c.content()) {
                    require(sc.get("feedEndpointId").stringValue().equals(String.valueOf(row.get("endpoint_id"))));
                    var endpoint=catalog.endpointEvidence(sc);String hash=SourceJson.sha(SourceJson.canonical(endpoint));
                    require(hash.equals(row.get("endpoint_sha256")));
                    snapshot.set("endpoint",endpoint);snapshot.put("endpointSha256",hash);
                } else {require(row.get("endpoint_id")==null && row.get("endpoint_sha256")==null);snapshot.putNull("endpoint");snapshot.putNull("endpointSha256");}
                sourceConfigs.put(s.id(),sc);
            }
            var inputs=frozen.putArray("inputs");Set<String> batchSources=new HashSet<>();long rawFiles=0,rawBytes=0,occurrences=0,silverBytes=0;
            long capturedSize=SourceJson.canonical(frozen).length;
            for(int i=0;i<c.artifacts().size()+1;i++) {
                boolean reference=i==c.artifacts().size();UUID id=reference?c.reference():c.artifacts().get(i);
                String kind=reference?(c.content()?"PUBLISHER_REFERENCE":"SERVICE_REFERENCE"):(c.content()?"SILVER":"RELIABILITY_BATCH_V2");
                var artifact=artifact(id,c.product(),kind,storage);var selection=artifact.get("qualification").get("selection");var verification=artifact.get("qualification").get("verification");
                if(!reference && !c.content()) {
                    var identity=selection.get("identity");String source=identity.get("sourceId").stringValue();JsonNode sc=sourceConfigs.get(source);require(sc!=null);
                    int revision=c.sources().stream().filter(s->s.id().equals(source)).findFirst().orElseThrow().revision();
                    require(identity.get("sourceRevision").intValue()==revision && identity.get("registeredProfileSha256").equals(sc.get("registeredProfileSha256")) && selection.get("storageBindingId").equals(sc.get("inputStorageProfileId")));
                    require(identity.get("inputFormat").stringValue().equals(config.get("sourceProfile").stringValue().equals("request-jsonl-v2")?"JSONL":"PARQUET"));batchSources.add(source);
                    for(var file:verification.get("native").get("inventory")){rawFiles++;rawBytes+=file.get("byte_length").longValue();occurrences+=file.get("record_count").longValue();}
                } else if(!reference) {
                    member(storage,selection.get("identity").get("bronzeStorageBindingId").stringValue(),"BRONZE");
                    for(var f:verification.get("capture").get("local").get("files"))if(Set.of("accepted","rejected").contains(f.get("dataset").stringValue()))silverBytes+=f.get("byteLength").longValue();
                }
                String role=reference?(c.content()?"PUBLISHER_REFERENCE":"SERVICE_REFERENCE"):(c.content()?"RETAINED_SILVER":"RELIABILITY_BATCH");
                artifact.put("role",role).put("ordinal",reference?0:i);inputs.add(artifact);
                capturedSize+=SourceJson.canonical(artifact).length+1;
                if(capturedSize>MAX_EVIDENCE)throw new SourceFailure(MANAGEMENT_CAPACITY_EXCEEDED);
            }
            require(c.content() || batchSources.equals(sourceConfigs.keySet()));
            if(rawFiles>128 || rawBytes>1024L*1024*1024 || occurrences>2000000 || silverBytes>1024L*1024*1024)throw new SourceFailure(MANAGEMENT_CAPACITY_EXCEEDED);
            byte[] captured=SourceJson.canonical(frozen);if(captured.length>MAX_EVIDENCE)throw new SourceFailure(MANAGEMENT_CAPACITY_EXCEEDED);
            UUID id=UUID.randomUUID();
            jdbc.update("INSERT INTO ddip_management.runs(run_id,product_id,pipeline,pipeline_version,principal_scope,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,state) VALUES (?,?,?,1,'local-operator',?,?,?,?,?,?,'QUEUED')",id,c.product(),c.pipeline(),c.key(),c.normalized(),SourceJson.sha(c.normalized()),captured,SourceJson.sha(captured),c.profileId());
            for(var s:sources)jdbc.update("INSERT INTO ddip_management.run_sources(run_id,product_id,source_id,revision,captured_gate_version,captured_enabled) VALUES (?,?,?,?,?,true)",id,c.product(),s.get("sourceId").stringValue(),s.get("revision").intValue(),Long.parseLong(s.get("capturedGateVersion").stringValue()));
            for(var input:inputs){byte[] bytes=SourceJson.canonical(input);jdbc.update("INSERT INTO ddip_management.run_inputs(run_id,role,ordinal,artifact_id,evidence_bytes,evidence_sha256) VALUES (?,?,?,?,?,?)",id,input.get("role").stringValue(),input.get("ordinal").intValue(),UUID.fromString(input.get("artifactId").stringValue()),bytes,SourceJson.sha(bytes));}
            var audit=SourceJson.object().put("protocolVersion",1).put("correlationId",correlation).put("requestSha256",SourceJson.sha(c.normalized())).put("selectionSha256",SourceJson.sha(captured));
            jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,action,after_state,after_version,safe_code,details_bytes) VALUES ('local-operator',?,'RUN_SUBMITTED','QUEUED',0,'ACCEPTED',?)",id,SourceJson.canonical(audit));
            return detail(id);
        });
    }
    private void readiness(Map<String,Object> slot) {
        if(!"READY".equals(slot.get("mode")) || slot.get("active_import_id")!=null)throw new SourceFailure(COORDINATOR_RECOVERY_REQUIRED);
        Boolean unresolved=jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.runs WHERE state='RECOVERY_REQUIRED') OR EXISTS(SELECT 1 FROM ddip_management.artifact_import_operations WHERE state='ACTIVE') OR " +
                "EXISTS(SELECT 1 FROM ddip_management.attempts a WHERE a.state IN ('CLAIMED','RUNNING') AND (a.kind='RECONCILE' OR a.attempt_id IS DISTINCT FROM ?::uuid)) OR " +
                "EXISTS(SELECT 1 FROM ddip_management.runs r WHERE r.state IN ('RUNNING','CANCEL_REQUESTED') AND r.current_attempt_id IS DISTINCT FROM ?::uuid)",Boolean.class,slot.get("active_attempt_id"),slot.get("active_attempt_id"));
        if(Boolean.TRUE.equals(unresolved))throw new SourceFailure(COORDINATOR_RECOVERY_REQUIRED);
        if(slot.get("active_attempt_id")!=null && !Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.attempts a JOIN ddip_management.runs r ON r.current_attempt_id=a.attempt_id JOIN ddip_management.worker_instances w ON w.instance_id=a.instance_id JOIN ddip_management.coordinator c ON c.active_attempt_id=a.attempt_id WHERE c.singleton_id=1 AND a.state IN ('CLAIMED','RUNNING') AND a.kind='EXECUTE' AND r.state IN ('RUNNING','CANCEL_REQUESTED') AND w.status='READY' AND c.heartbeat_at>=clock_timestamp()-interval '30 seconds')",Boolean.class)))throw new SourceFailure(COORDINATOR_RECOVERY_REQUIRED);
    }
    private void capacity() {
        long unfinished=jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class);
        if(unfinished>=20 || jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs",Long.class)>=10000)throw new SourceFailure(MANAGEMENT_CAPACITY_EXCEEDED);
        AuditCapacity.requireAvailable(jdbc,1,unfinished+1);
    }
    private Map<String,ObjectNode> storage(RunCommands.Command c) {
        var rows=jdbc.queryForList("SELECT b.* FROM ddip_management.storage_bindings b JOIN ddip_management.execution_profile_storage p USING(binding_id) WHERE p.profile_id=? ORDER BY b.binding_id COLLATE \"C\"",c.profileId());
        require(!rows.isEmpty() && rows.size()<=128);Map<String,ObjectNode> out=new LinkedHashMap<>();Set<String> roles=new HashSet<>();
        for(var b:rows){String role=(String)b.get("role");require(installation.equals(b.get("installation_id")) && (c.product().equals(b.get("product")) || role.equals("CONTROL")));
            var n=SourceJson.object().put("bindingId",(String)b.get("binding_id")).put("productId",(String)b.get("product")).put("role",role).put("configurationSha256",(String)b.get("configuration_sha256"));n.set("configuration",canonical((byte[])b.get("configuration_bytes"),(String)b.get("configuration_sha256")));out.put((String)b.get("binding_id"),n);roles.add(role);}
        require(roles.containsAll(c.content()?Set.of("BRONZE","SILVER","REFERENCE","GOLD","CONTENT","CONTROL","STAGING","TEMP"):Set.of("BATCH","REFERENCE","RELIABILITY","CONTROL","STAGING","TEMP")));return out;
    }
    private static void member(Map<String,ObjectNode> storage,String binding,String role){require(storage.containsKey(binding) && storage.get(binding).get("role").stringValue().equals(role));}
    private ObjectNode artifact(UUID id,String product,String kind,Map<String,ObjectNode> storage) {
        var rows=jdbc.queryForList("SELECT * FROM ddip_management.artifacts WHERE artifact_id=?",id);require(rows.size()==1);var row=rows.getFirst();require(product.equals(row.get("product_id")) && kind.equals(row.get("kind")));
        try {
            byte[] bytes=(byte[])row.get("evidence_bytes");var qualified=RegisteredImportEvidence.validate(bytes,(String)row.get("evidence_sha256"));var e=qualified.selection();
            require(e.kind().equals(kind) && e.product().equals(product) && e.bindingId().equals(row.get("storage_binding_id")) && e.address().equals(row.get("relative_address")) && e.nativeIdentity().equals(row.get("native_identity")) && Objects.equals(e.format(),row.get("native_format")) && e.contract()==((Number)row.get("contract_version")).intValue() && e.processor()==((Number)row.get("processor_revision")).intValue());
            member(storage,e.bindingId(),kind.equals("SILVER")?"SILVER":kind.equals("RELIABILITY_BATCH_V2")?"BATCH":"REFERENCE");
            require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifact_dependencies WHERE artifact_id=?",Long.class,id)==0); // Current import kinds are native leaves; descriptors are retained below.
            var n=SourceJson.object().put("artifactId",id.toString()).put("productId",product).put("kind",kind).put("contractVersion",e.contract()).put("processorRevision",e.processor()).put("storageBindingId",e.bindingId()).put("relativeAddress",e.address()).put("nativeIdentity",e.nativeIdentity()).put("evidenceSha256",(String)row.get("evidence_sha256"));
            n.put("verifiedAt",UTC.format(((Timestamp)row.get("verified_at")).toInstant()));n.set("qualification",qualified.evidence());return n;
        } catch(ImportFailure ex){throw new SourceFailure(STATE_CONFLICT);}
    }
    private static JsonNode canonical(byte[] bytes,String hash) {
        try{var n=SourceJson.parse(bytes);require(Arrays.equals(bytes,SourceJson.canonical(n)) && SourceJson.sha(bytes).equals(hash));return n;}
        catch(SourceFailure ex){throw new SourceFailure(STATE_CONFLICT);}
    }
    public JsonNode get(String id) {UUID uuid=UUID.fromString(SourceCommands.uuid(id));return tx.required(()->detail(uuid));}
    private ObjectNode detail(UUID id) {
        var rows=jdbc.query("SELECT * FROM ddip_management.runs WHERE run_id=?",(r,i)-> {
            var n=SourceJson.object().put("runId",id.toString()).put("productId",r.getString("product_id")).put("pipeline",r.getString("pipeline")).put("state",r.getString("state")).put("rowVersion",Long.toString(r.getLong("row_version"))).put("requestedAt",UTC.format(r.getObject("requested_at",OffsetDateTime.class).toInstant()));
            n.put("currentStage",r.getString("current_stage"));var end=r.getObject("finished_at",OffsetDateTime.class);n.put("finishedAt",end==null?null:UTC.format(end.toInstant()));n.put("outcomeCode",r.getString("outcome_code"));n.put("retryEligibility",r.getBoolean("retry_eligible"));
            if(r.getString("pipeline").equals("RELIABILITY_V2"))n.put("dataOrigin","synthetic");return n;
        },id);
        if(rows.isEmpty())throw new SourceFailure(NOT_FOUND);var n=rows.getFirst();var sources=n.putArray("sourceRevisions");
        jdbc.query("SELECT source_id,revision FROM ddip_management.run_sources WHERE run_id=? ORDER BY source_id COLLATE \"C\"",(org.springframework.jdbc.core.RowCallbackHandler)r->sources.addObject().put("sourceId",r.getString(1)).put("revision",r.getInt(2)),id);
        var inputs=n.putArray("inputArtifactIds");jdbc.query("SELECT artifact_id FROM ddip_management.run_inputs WHERE run_id=? ORDER BY role COLLATE \"C\",ordinal",(org.springframework.jdbc.core.RowCallbackHandler)r->inputs.add(r.getObject(1,UUID.class).toString()),id);return n;
    }
}
