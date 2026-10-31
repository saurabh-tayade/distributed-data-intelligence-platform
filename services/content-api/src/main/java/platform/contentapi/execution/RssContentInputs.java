package platform.contentapi.execution;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import platform.contentapi.importing.RegisteredImportEvidence;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Exact original reference and committed D4 result. Metadata checks complement native reopening. */
final class RssContentInputs {
    static JsonNode reference(JsonNode frozen){JsonNode found=null;for(var n:frozen.get("inputs"))if(n.get("role").stringValue().equals("PUBLISHER_REFERENCE")){need(found==null && n.get("ordinal").intValue()==0,INPUT_INVALID);found=n;}need(found!=null,INPUT_UNAVAILABLE);return found;}
    static void check(JdbcTemplate j,UUID run,UUID attempt,JsonNode frozen,JsonNode gold,JsonNode reference){
        attempt=ContentResumeLineage.dataAttempt(j,run,attempt);
        need(reference.equals(reference(frozen)),INPUT_CHANGED);
        var qualified=RegisteredImportEvidence.validate(SourceJson.canonical(reference.get("qualification")),reference.get("evidenceSha256").stringValue());need(qualified.selection().kind().equals("PUBLISHER_REFERENCE") && qualified.selection().contract()==1 && qualified.selection().processor()==1,INPUT_INVALID);
        var refs=j.queryForList("SELECT a.*,i.evidence_bytes AS captured_bytes FROM ddip_management.run_inputs i JOIN ddip_management.artifacts a USING(artifact_id) WHERE i.run_id=? AND i.role='PUBLISHER_REFERENCE' AND i.ordinal=0",run);need(refs.size()==1,INPUT_UNAVAILABLE);var r=refs.getFirst();
        need(reference.get("artifactId").stringValue().equals(r.get("artifact_id").toString()) && Arrays.equals((byte[])r.get("captured_bytes"),SourceJson.canonical(reference)) && Arrays.equals((byte[])r.get("evidence_bytes"),SourceJson.canonical(reference.get("qualification"))) && reference.get("evidenceSha256").stringValue().equals(r.get("evidence_sha256")) && RegisteredImportEvidence.matchesIdentity(qualified,r.get("native_identity")) && reference.get("storageBindingId").stringValue().equals(r.get("storage_binding_id")) && reference.get("relativeAddress").stringValue().equals(r.get("relative_address")) && "PUBLISHER_REFERENCE".equals(r.get("kind")) && "content-intelligence".equals(r.get("product_id")) && ((Number)r.get("contract_version")).intValue()==1 && ((Number)r.get("processor_revision")).intValue()==1 && r.get("verified_at")!=null,INPUT_CHANGED);
        var rows=j.queryForList("SELECT a.* FROM ddip_management.stage_invocations s JOIN ddip_management.stage_artifacts x USING(attempt_id,stage,item_ordinal) JOIN ddip_management.artifacts a USING(artifact_id) WHERE s.attempt_id=? AND s.stage='PUBLISH_GOLD' AND s.state='VERIFIED' AND x.role='RESULT'",attempt);need(rows.size()==1,INPUT_UNAVAILABLE);var a=rows.getFirst();var e=gold.get("artifact");
        need(gold.get("artifactId").stringValue().equals(a.get("artifact_id").toString()) && gold.get("evidenceSha256").stringValue().equals(a.get("evidence_sha256")) && Arrays.equals(SourceJson.canonical(e),(byte[])a.get("evidence_bytes")) && "GOLD".equals(a.get("kind")) && "content-intelligence".equals(a.get("product_id")) && ((Number)a.get("contract_version")).intValue()==1 && ((Number)a.get("processor_revision")).intValue()==1 && e.get("nativeFingerprint").stringValue().equals(a.get("native_identity")) && e.get("storageBindingId").stringValue().equals(a.get("storage_binding_id")) && e.get("relativeAddress").stringValue().equals(a.get("relative_address")) && a.get("verified_at")!=null,INPUT_CHANGED);
        RssGoldInputs.check(j,run,attempt,frozen,gold.get("command").get("inputs"));
        var links=j.queryForList("SELECT role,ordinal,dependency_artifact_id FROM ddip_management.artifact_dependencies WHERE artifact_id=? ORDER BY role,ordinal",UUID.fromString(gold.get("artifactId").stringValue()));var inputs=e.get("inputs");need(links.size()==inputs.size(),INPUT_CHANGED);for(int i=0;i<inputs.size();i++)need("SILVER".equals(links.get(i).get("role")) && ((Number)links.get(i).get("ordinal")).intValue()==i && links.get(i).get("dependency_artifact_id").toString().equals(inputs.get(i).get("artifactId").stringValue()),INPUT_CHANGED);
    }
    static JsonNode dependencies(JsonNode gold,JsonNode reference){var n=SourceJson.object().putArray("items");n.addObject().put("role","GOLD").put("artifactId",gold.get("artifactId").stringValue()).put("evidenceSha256",gold.get("evidenceSha256").stringValue());n.addObject().put("role","PUBLISHER_REFERENCE").put("artifactId",reference.get("artifactId").stringValue()).put("evidenceSha256",reference.get("evidenceSha256").stringValue());return n;}
}
