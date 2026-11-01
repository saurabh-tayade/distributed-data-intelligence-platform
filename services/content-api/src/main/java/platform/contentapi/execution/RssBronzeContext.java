package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Immutable D1 + frozen storage/runtime resolution. Reading this context grants no write authority. */
final class RssBronzeContext {
    final UUID run,attempt,fetchCheckpoint;final int ordinal;final String source,state,namespace,journalSha,runtimeSha;
    final long deliveryVersion;final byte[] descriptor,reference,receipt,credentials;final JsonNode frozen,selected,storage;
    final RssJournalFiles journal;final RssFetchRuntime runtime;final Path credentialsPath;final JsonNode credentialsStamp;
    private RssBronzeContext(UUID run,String source,Map<String,Object> row,JsonNode frozen,JsonNode selected,JsonNode storage,
            String runtimeSha,RssJournalFiles journal,RssFetchRuntime runtime,byte[] credentials,JsonNode stamp) {
        this.run=run;this.source=source;attempt=(UUID)row.get("owning_attempt_id");fetchCheckpoint=(UUID)row.get("checkpoint_id");ordinal=((Number)row.get("item_ordinal")).intValue();
        state=(String)row.get("state");namespace=(String)row.get("journal_address");journalSha=(String)row.get("journal_sha256");deliveryVersion=((Number)row.get("row_version")).longValue();
        descriptor=((byte[])row.get("journal_descriptor")).clone();reference=((byte[])row.get("expected_evidence")).clone();receipt=row.get("receipt_bytes")==null?null:((byte[])row.get("receipt_bytes")).clone();
        this.frozen=frozen;this.selected=selected;this.storage=storage;this.runtimeSha=runtimeSha;this.journal=journal;this.runtime=runtime;this.credentials=credentials.clone();credentialsStamp=stamp;credentialsPath=Path.of(storage.get("configuration").get("credentialsFile").stringValue());
    }
    static RssBronzeContext load(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,String source,String runtimeId) {
        guard.checkHeld(guard.binding().installationId());
        try {
            var rows=tx.required(()->jdbc.queryForList("SELECT d.*,s.checkpoint_id,s.item_ordinal,s.command_sha256,s.result_sha256,r.captured_selection,r.selection_sha256 FROM ddip_management.rss_deliveries d JOIN ddip_management.stage_invocations s ON s.attempt_id=d.owning_attempt_id AND s.stage='FETCH_SOURCE' AND s.intended_identity=d.source_id AND s.intended_namespace=d.journal_address JOIN ddip_management.runs r ON r.run_id=d.run_id JOIN ddip_management.attempts a ON a.attempt_id=d.owning_attempt_id AND a.run_id=d.run_id JOIN ddip_management.worker_instances w ON w.instance_id=a.instance_id WHERE d.run_id=? AND d.source_id=? AND s.state='VERIFIED' AND w.installation_id=? AND w.host_binding_sha256=?",run,source,guard.binding().installationId(),guard.binding().digest()));
            need(rows.size()==1,INPUT_UNAVAILABLE);var row=rows.getFirst();
            byte[] descriptor=(byte[])row.get("journal_descriptor"),reference=(byte[])row.get("expected_evidence");
            need(SourceJson.sha(descriptor).equals(row.get("journal_sha256")) && SourceJson.sha(reference).equals(row.get("expected_sha256")),INPUT_CHANGED);
            var frozen=FrozenRunSelection.canonical((byte[])row.get("captured_selection"),(String)row.get("selection_sha256"),16*1024*1024);need(frozen.get("pipeline").stringValue().equals("CONTENT_RSS_V1"),RUNTIME_MISMATCH);
            int ordinal=((Number)row.get("item_ordinal")).intValue();need(ordinal>=0 && ordinal<frozen.get("sources").size(),INPUT_INVALID);var selected=frozen.get("sources").get(ordinal);
            var d=SourceJson.parse(descriptor);need(selected.get("sourceId").stringValue().equals(source) && selected.get("revision").equals(d.get("revision")) && selected.get("configurationSha256").equals(d.get("capturedConfigSha256")) && row.get("ingestion_id").toString().equals(d.get("ingestionId").stringValue()) && row.get("original_ingested_at_text").equals(d.get("ingestedAt").stringValue()),INPUT_CHANGED);
            need(((Number)row.get("revision")).intValue()==d.get("revision").intValue() && ((Number)row.get("payload_length")).longValue()==d.get("payloadByteLength").longValue() && row.get("payload_sha256").equals(d.get("payloadSha256").stringValue()),INPUT_CHANGED);
            if(row.get("state").equals("BRONZE_VERIFIED"))need(d.get("expected").get("metadataSha256").stringValue().equals(row.get("metadata_sha256")) && row.get("receipt_bytes")!=null,INPUT_CHANGED);
            else need(row.get("metadata_sha256")==null && row.get("receipt_bytes")==null,INPUT_CHANGED);
            if(ContentEvidenceProfile.bounded(frozen))ContentEvidenceBudget.require(guard,run,(UUID)row.get("owning_attempt_id"));
            var journal=new RssJournalFiles(guard,run,(UUID)row.get("owning_attempt_id"),(UUID)row.get("checkpoint_id"),source,(String)row.get("journal_address"),false);
            byte[] original=journal.read("command.json");need(SourceJson.sha(original).equals(row.get("command_sha256")),INPUT_CHANGED);
            var fetchCommand=SourceJson.parse(original);need(fetchCommand.get("endpointSha256").equals(selected.get("endpointSha256")) && fetchCommand.get("feedUri").equals(selected.get("endpoint").get("uri")),INPUT_CHANGED);
            var reopened=journal.reopen(original,(String)row.get("result_sha256"));need(Arrays.equals(reopened.descriptorBytes(),descriptor) && Arrays.equals(reopened.checkpointReference(),reference),INPUT_CHANGED);
            var profiles=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",runtimeId));need(profiles.size()==1,RUNTIME_MISMATCH);
            String digest=(String)profiles.getFirst().get("configuration_sha256");var c=FrozenRunSelection.canonical((byte[])profiles.getFirst().get("configuration_bytes"),digest,65536);
            SourceJson.fields(c,"bronzeRuntimeVersion","adapter","fetchRuntimeId","fetchRuntimeSha256","runProfileId","runProfileSha256","deploymentSha256","controlBindingId","storageBindingId","storageBindingSha256");
            need(c.get("bronzeRuntimeVersion").isIntegralNumber() && c.get("bronzeRuntimeVersion").canConvertToInt() && Set.of(1,2).contains(c.get("bronzeRuntimeVersion").intValue()) && c.get("adapter").stringValue().equals(c.get("bronzeRuntimeVersion").intValue()==2?"platform.ingestion.managed.BoundedManagedBronze":"platform.ingestion.managed.ManagedBronze") && c.get("controlBindingId").stringValue().equals(guard.binding().id()),RUNTIME_MISMATCH);
            ContentEvidenceProfile.runtime(jdbc,tx,frozen,"BRONZE",runtimeId,c.get("bronzeRuntimeVersion").intValue());
            var ep=frozen.get("executionProfile");String profile=ep.get("profileId").stringValue(),profileSha=ep.get("configurationSha256").stringValue(),deployment=ep.get("configuration").get("runtime").get("deploymentSha256").stringValue();
            need(profile.equals(c.get("runProfileId").stringValue()) && profileSha.equals(c.get("runProfileSha256").stringValue()) && deployment.equals(c.get("deploymentSha256").stringValue()),RUNTIME_MISMATCH);
            var runtime=RssFetchRuntime.load(jdbc,tx,guard,profile,profileSha,deployment,c.get("fetchRuntimeId").stringValue(),frozen);
            need(runtime.digest().equals(c.get("fetchRuntimeSha256").stringValue()) && runtime.digest().equals(SourceJson.parse(original).get("runtimeSha256").stringValue()),RUNTIME_MISMATCH);
            String binding=c.get("storageBindingId").stringValue();need(selected.get("configuration").get("inputStorageProfileId").stringValue().equals(binding),RUNTIME_MISMATCH);
            JsonNode storage=null;for(var entry:frozen.get("storageBindings"))if(entry.get("bindingId").stringValue().equals(binding))storage=entry;
            need(storage!=null && storage.get("role").stringValue().equals("BRONZE") && storage.get("configurationSha256").equals(c.get("storageBindingSha256")),RUNTIME_MISMATCH);
            var config=storage.get("configuration");need(SourceJson.sha(SourceJson.canonical(config)).equals(c.get("storageBindingSha256").stringValue()),RUNTIME_MISMATCH);
            SourceJson.fields(config,"importBronzeVersion","endpoint","region","bucket","credentialsFile");need(config.get("importBronzeVersion").isIntegralNumber() && config.get("importBronzeVersion").canConvertToInt() && config.get("importBronzeVersion").intValue()==1,RUNTIME_MISMATCH);
            var endpoint=java.net.URI.create(config.get("endpoint").stringValue());need("http".equals(endpoint.getScheme()) && Set.of("127.0.0.1","localhost","[::1]").contains(endpoint.getHost()) && endpoint.getPort()>0 && endpoint.getPort()<=65535 && endpoint.getUserInfo()==null && endpoint.getQuery()==null && endpoint.getFragment()==null && Set.of("","/").contains(endpoint.getPath()),RUNTIME_MISMATCH);
            need(config.get("region").stringValue().matches("[A-Za-z0-9_-]{1,128}") && !config.get("bucket").stringValue().isBlank(),RUNTIME_MISMATCH);
            for(String id:List.of(binding,guard.binding().id())){
                byte[] expected=id.equals(binding)?SourceJson.canonical(config):guard.binding().descriptor();String expectedSha=SourceJson.sha(expected);
                need(tx.required(()->jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.execution_profile_storage p JOIN ddip_management.storage_bindings b USING(binding_id) WHERE p.profile_id=? AND b.binding_id=? AND b.installation_id=? AND b.configuration_sha256=? AND b.configuration_bytes=?)",Boolean.class,runtimeId,id,guard.binding().installationId(),expectedSha,expected)),RUNTIME_MISMATCH);
            }
            Path secrets=Path.of(config.get("credentialsFile").stringValue());PreflightFiles.privateEntry(secrets,false);need(Files.getPosixFilePermissions(secrets).equals(PosixFilePermissions.fromString("rw-------")),RUNTIME_MISMATCH);
            byte[] secret=PreflightFiles.read(secrets,8192,true);var values=SourceJson.parse(secret);SourceJson.fields(values,"accessKey","secretKey");for(String key:List.of("accessKey","secretKey"))need(values.get(key).isString() && !values.get(key).stringValue().isBlank() && values.get(key).stringValue().getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=4096 && values.get(key).stringValue().codePoints().noneMatch(Character::isISOControl),RUNTIME_MISMATCH);
            return new RssBronzeContext(run,source,row,frozen,selected,storage,digest,journal,runtime,secret,stamp(secrets));
        }catch(PreflightFailure | ManagementFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    void check(){runtime.check();var observed=journal.reopen(journal.read("command.json"),SourceJson.parse(reference).get("completionSha256").stringValue());need(Arrays.equals(descriptor,observed.descriptorBytes()) && Arrays.equals(reference,observed.checkpointReference()),INPUT_CHANGED);need(Arrays.equals(credentials,PreflightFiles.read(credentialsPath,8192,true)) && Arrays.equals(SourceJson.canonical(credentialsStamp),SourceJson.canonical(stamp(credentialsPath))),INPUT_CHANGED);try{need(Files.getPosixFilePermissions(credentialsPath).equals(PosixFilePermissions.fromString("rw-------")),INPUT_CHANGED);}catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}}
    JsonNode journalCommand(){var n=SourceJson.object().put("directory",journal.directory.toString()).put("commandSha256",SourceJson.sha(journal.read("command.json"))).put("descriptorSha256",journalSha);n.set("reference",SourceJson.parse(reference));return n;}
    JsonNode storageCommand(){var n=SourceJson.object().put("bindingId",storage.get("bindingId").stringValue()).put("bindingSha256",storage.get("configurationSha256").stringValue()).put("credentialsSha256",SourceJson.sha(credentials));n.set("configuration",storage.get("configuration"));n.set("credentialsStamp",credentialsStamp);return n;}
    private static JsonNode stamp(Path path){try{var id=InstallationHostGuard.identity(path);return SourceJson.object().put("identity",id.device()+":"+id.inode()).put("modified",Files.getLastModifiedTime(path).toString()).put("size",Files.size(path));}catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}}
    @Override public String toString(){return "RssBronzeContext[redacted]";}
}
