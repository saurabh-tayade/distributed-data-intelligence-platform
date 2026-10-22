package platform.contentapi.management;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.importing.RegisteredImportEvidence;
import tools.jackson.databind.JsonNode;

/** Metadata-only worker preflight. No current catalog lookup, native reads or process execution.
 * Called under coordinator -> sorted sources -> run locks. Native consumption must reverify in C2/D.
 */
public final class FrozenRunSelection {
    private FrozenRunSelection() { }
    public static JsonNode canonical(byte[] raw,String digest,int maximum) {
        var n=SourceJson.parse(raw,maximum);
        check(SourceJson.sha(raw).equals(digest) && Arrays.equals(raw,SourceJson.canonical(n)));return n;
    }
    public static JsonNode profile(byte[] raw,String digest,String deployment) {
        var n=canonical(raw,digest,65536);RunProfile.validate(n,SourceJson.text(n.get("pipeline")));
        check(n.get("runtime").get("deploymentSha256").stringValue().equals(deployment));return n;
    }
    public static void validate(JdbcTemplate jdbc,UUID installation,Map<String,Object> run,List<Map<String,Object>> sources,
                                String profileId,String digest,String deployment) {
        check(profileId.equals(run.get("execution_profile_id")));
        var profiles=jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",profileId);
        check(profiles.size()==1 && digest.equals(profiles.getFirst().get("configuration_sha256")));
        var p=profile((byte[])profiles.getFirst().get("configuration_bytes"),digest,deployment);
        var f=canonical((byte[])run.get("captured_selection"),(String)run.get("selection_sha256"),16*1024*1024);
        SourceJson.fields(f,"selectionVersion","pipeline","pipelineVersion","productId","principalScope","triggerType","executionProfile","storageBindings","sources","inputs");
        check(f.get("selectionVersion").intValue()==1 && f.get("pipelineVersion").intValue()==1 && f.get("pipeline").stringValue().equals(run.get("pipeline")) && f.get("productId").stringValue().equals(run.get("product_id")) && f.get("principalScope").stringValue().equals("local-operator") && f.get("triggerType").stringValue().equals("OPERATOR"));
        var ep=f.get("executionProfile");SourceJson.fields(ep,"profileId","configurationSha256","configuration");
        check(ep.get("profileId").stringValue().equals(profileId) && ep.get("configurationSha256").stringValue().equals(digest) && ep.get("configuration").equals(p));
        var request=canonical((byte[])run.get("normalized_request"),(String)run.get("request_sha256"),65536);
        var command=RunCommands.parse(SourceJson.canonical(request),(String)run.get("idempotency_key"));
        check(Arrays.equals(command.normalized(),(byte[])run.get("normalized_request")));
        check(command.pipeline().equals(run.get("pipeline")) && command.product().equals(run.get("product_id")) && command.profileId().equals(profileId));
        var fs=f.get("sources");check(fs.isArray() && fs.size()==sources.size() && sources.size()==command.sources().size());
        for(int i=0;i<sources.size();i++) {
            var row=sources.get(i);var s=fs.get(i);SourceJson.fields(s,"sourceId","revision","capturedGateVersion","capturedEnabled","configurationSha256","configuration","endpoint","endpointSha256");
            String id=(String)row.get("source_id");int revision=((Number)row.get("revision")).intValue();
            check(id.equals(s.get("sourceId").stringValue()) && id.equals(command.sources().get(i).id()) && revision==command.sources().get(i).revision() && revision==s.get("revision").intValue());
            check(s.get("capturedEnabled").isBoolean() && s.get("capturedEnabled").booleanValue() && Boolean.TRUE.equals(row.get("captured_enabled")) && s.get("capturedGateVersion").stringValue().equals(row.get("captured_gate_version").toString()));
            var config=SourceCommands.config(command.product(),canonical((byte[])row.get("configuration_bytes"),(String)row.get("configuration_sha256"),65536));
            check(config.equals(s.get("configuration")) && s.get("configurationSha256").stringValue().equals(row.get("configuration_sha256")) && config.get("profile").equals(p.get("sourceProfile")) && config.get("profile").stringValue().equals(row.get("profile_family")));
            if(command.content())check(s.get("endpoint").isObject() && SourceJson.sha(SourceJson.canonical(s.get("endpoint"))).equals(row.get("endpoint_sha256")) && s.get("endpointSha256").stringValue().equals(row.get("endpoint_sha256")) && config.get("feedEndpointId").stringValue().equals(String.valueOf(row.get("endpoint_id"))));
            else check(s.get("endpoint").isNull() && s.get("endpointSha256").isNull());
        }
        var storage=jdbc.queryForList("SELECT b.binding_id,b.product,b.role,b.installation_id,b.configuration_bytes,b.configuration_sha256 FROM ddip_management.storage_bindings b JOIN ddip_management.execution_profile_storage p USING(binding_id) WHERE p.profile_id=? ORDER BY b.binding_id COLLATE \"C\"",profileId);
        var fb=f.get("storageBindings");check(fb.isArray() && fb.size()==storage.size() && !storage.isEmpty() && storage.size()<=128);
        Map<String,String> bindings=new HashMap<>();Set<String> roles=new HashSet<>();
        for(int i=0;i<storage.size();i++) {
            var b=storage.get(i);var n=fb.get(i);SourceJson.fields(n,"bindingId","productId","role","configurationSha256","configuration");
            check(installation.equals(b.get("installation_id")) && (command.product().equals(b.get("product")) || "CONTROL".equals(b.get("role"))) && n.get("bindingId").stringValue().equals(b.get("binding_id")) && n.get("productId").stringValue().equals(b.get("product")) && n.get("role").stringValue().equals(b.get("role")) && n.get("configurationSha256").stringValue().equals(b.get("configuration_sha256")) && n.get("configuration").equals(canonical((byte[])b.get("configuration_bytes"),(String)b.get("configuration_sha256"),65536)));
            bindings.put((String)b.get("binding_id"),(String)b.get("role"));roles.add((String)b.get("role"));
        }
        check(roles.containsAll(command.content()?Set.of("BRONZE","SILVER","REFERENCE","GOLD","CONTENT","CONTROL","STAGING","TEMP"):Set.of("BATCH","REFERENCE","RELIABILITY","CONTROL","STAGING","TEMP")));
        var inputs=jdbc.queryForList("SELECT i.role,i.ordinal,i.artifact_id,i.evidence_bytes,i.evidence_sha256,a.evidence_sha256 AS registered_sha256,a.product_id,a.kind,a.native_identity,a.storage_binding_id,a.relative_address,a.contract_version,a.processor_revision,a.native_format FROM ddip_management.run_inputs i JOIN ddip_management.artifacts a USING(artifact_id) WHERE i.run_id=? ORDER BY i.role COLLATE \"C\",i.ordinal",run.get("run_id"));
        check(inputs.size()==command.artifacts().size()+1 && f.get("inputs").isArray() && f.get("inputs").size()==inputs.size());
        Map<String,JsonNode> selected=new HashMap<>();for(var input:f.get("inputs"))check(selected.put(input.get("artifactId").stringValue(),input)==null);
        Set<UUID> requested=new HashSet<>(command.artifacts());requested.add(command.reference());check(requested.size()==inputs.size());
        Set<String> batchSources=new HashSet<>();
        check(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifact_dependencies d JOIN ddip_management.run_inputs i ON d.artifact_id=i.artifact_id WHERE i.run_id=?",Long.class,run.get("run_id"))==0);
        for(var input:inputs) {
            UUID id=(UUID)input.get("artifact_id");var n=selected.get(id.toString());check(n!=null && requested.contains(id));
            SourceJson.fields(n,"artifactId","productId","kind","contractVersion","processorRevision","storageBindingId","relativeAddress","nativeIdentity","evidenceSha256","verifiedAt","qualification","role","ordinal");
            check(n.equals(canonical((byte[])input.get("evidence_bytes"),(String)input.get("evidence_sha256"),16*1024*1024)) && n.get("role").stringValue().equals(input.get("role")) && n.get("ordinal").intValue()==((Number)input.get("ordinal")).intValue());
            byte[] evidence=SourceJson.canonical(n.get("qualification"));var e=RegisteredImportEvidence.validate(evidence,n.get("evidenceSha256").stringValue()).selection();
            boolean reference=id.equals(command.reference());String role=reference?(command.content()?"PUBLISHER_REFERENCE":"SERVICE_REFERENCE"):(command.content()?"RETAINED_SILVER":"RELIABILITY_BATCH");
            String kind=reference?role:command.content()?"SILVER":"RELIABILITY_BATCH_V2";
            check(role.equals(n.get("role").stringValue()) && e.kind().equals(kind) && n.get("ordinal").intValue()==(reference?0:command.artifacts().indexOf(id)));
            check(n.get("evidenceSha256").stringValue().equals(input.get("registered_sha256")) && e.nativeIdentity().equals(input.get("native_identity")) && e.product().equals(command.product()) && e.product().equals(input.get("product_id")) && e.kind().equals(input.get("kind")) && e.bindingId().equals(input.get("storage_binding_id")) && (reference?"REFERENCE":command.content()?"SILVER":"BATCH").equals(bindings.get(e.bindingId())) && e.address().equals(input.get("relative_address")) && e.contract()==((Number)input.get("contract_version")).intValue() && e.processor()==((Number)input.get("processor_revision")).intValue() && Objects.equals(e.format(),input.get("native_format")));
            check(n.get("productId").stringValue().equals(e.product()) && n.get("kind").stringValue().equals(e.kind()) && n.get("contractVersion").intValue()==e.contract() && n.get("processorRevision").intValue()==e.processor() && n.get("nativeIdentity").stringValue().equals(e.nativeIdentity()) && n.get("relativeAddress").stringValue().equals(e.address()) && n.get("storageBindingId").stringValue().equals(e.bindingId()));
            if(!reference && !command.content()) {
                var identity=e.identity();String source=identity.get("sourceId").stringValue();JsonNode captured=null;
                for(var s:fs)if(s.get("sourceId").stringValue().equals(source))captured=s;
                check(captured!=null);var config=captured.get("configuration");
                check(identity.get("sourceRevision").intValue()==captured.get("revision").intValue() && identity.get("registeredProfileSha256").equals(config.get("registeredProfileSha256")) && e.bindingId().equals(config.get("inputStorageProfileId").stringValue()) && e.format().equals(p.get("sourceProfile").stringValue().equals("request-jsonl-v2")?"JSONL":"PARQUET"));batchSources.add(source);
            } else if(!reference)check("BRONZE".equals(bindings.get(e.identity().get("bronzeStorageBindingId").stringValue())));
        }
        check(command.content() || batchSources.equals(command.sources().stream().map(RunCommands.Source::id).collect(java.util.stream.Collectors.toSet())));
    }
    private static void check(boolean valid){if(!valid)throw new IllegalArgumentException("FROZEN_SELECTION_INVALID");}
}
