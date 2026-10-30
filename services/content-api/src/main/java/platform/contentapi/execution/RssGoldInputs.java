package platform.contentapi.execution;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Complete fresh D3 set plus exactly submitted retained D3 history. Never an artifact/latest scan. */
final class RssGoldInputs {
    static JsonNode ids(JdbcTemplate jdbc,UUID run,UUID attempt,JsonNode frozen){
        var stages=jdbc.queryForList("SELECT s.item_ordinal,s.state,a.artifact_id FROM ddip_management.stage_invocations s LEFT JOIN ddip_management.stage_artifacts a ON a.attempt_id=s.attempt_id AND a.stage=s.stage AND a.item_ordinal=s.item_ordinal AND a.role='RESULT' AND a.ordinal=0 WHERE s.attempt_id=? AND s.stage='SILVER' ORDER BY s.item_ordinal",attempt);
        need(stages.size()==frozen.get("sources").size(),INPUT_UNAVAILABLE);var result=SourceJson.object().putArray("items");Set<UUID> seen=new HashSet<>();
        for(int i=0;i<stages.size();i++) {var row=stages.get(i);need(((Number)row.get("item_ordinal")).intValue()==i && "VERIFIED".equals(row.get("state")) && row.get("artifact_id")!=null,INPUT_UNAVAILABLE);UUID id=(UUID)row.get("artifact_id");need(seen.add(id),INPUT_INVALID);result.addObject().put("artifactId",id.toString()).put("originRunId",run.toString()).put("sourceId",frozen.get("sources").get(i).get("sourceId").stringValue());}
        for(var input:frozen.get("inputs"))if(input.get("role").stringValue().equals("RETAINED_SILVER")){
            UUID id=UUID.fromString(input.get("artifactId").stringValue());need(seen.add(id),INPUT_INVALID);
            var rows=jdbc.queryForList("SELECT a.run_id,d.source_id FROM ddip_management.stage_artifacts x JOIN ddip_management.stage_invocations s USING(attempt_id,stage,item_ordinal) JOIN ddip_management.attempts a USING(attempt_id) JOIN ddip_management.rss_deliveries d ON d.owning_attempt_id=a.attempt_id AND d.ingestion_id::text=(convert_from(d.receipt_bytes,'UTF8')::jsonb->>'ingestionId') WHERE x.artifact_id=? AND x.stage='SILVER' AND x.role='RESULT' AND x.ordinal=0 AND s.state='VERIFIED' AND (convert_from(?, 'UTF8')::jsonb->'qualification'->'selection'->'identity'->>'sourceId')=d.source_id",id,SourceJson.canonical(input));
            need(rows.size()==1,INPUT_INVALID);var r=rows.getFirst();result.addObject().put("artifactId",id.toString()).put("originRunId",r.get("run_id").toString()).put("sourceId",(String)r.get("source_id"));
        }
        need(result.size()<=128,RESOURCE_LIMIT);return result;
    }
    static void check(JdbcTemplate jdbc,UUID run,UUID attempt,JsonNode frozen,JsonNode selected){
        var ids=ids(jdbc,run,attempt,frozen);need(ids.size()==selected.size(),INPUT_CHANGED);
        for(int i=0;i<ids.size();i++){var s=selected.get(i);for(String k:List.of("artifactId","originRunId","sourceId"))need(ids.get(i).get(k).equals(s.get(k)),INPUT_CHANGED);
            var rows=jdbc.queryForList("SELECT evidence_sha256,evidence_bytes,kind,storage_binding_id,relative_address,native_identity FROM ddip_management.artifacts WHERE artifact_id=?",UUID.fromString(s.get("artifactId").stringValue()));need(rows.size()==1,INPUT_UNAVAILABLE);var row=rows.getFirst();var q=s.get("qualification");
            need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.rss_deliveries WHERE run_id=? AND source_id=? AND state='BRONZE_VERIFIED' AND receipt_bytes=?",Integer.class,UUID.fromString(s.get("originRunId").stringValue()),s.get("sourceId").stringValue(),SourceJson.canonical(s.get("receipt")))==1,INPUT_CHANGED);
            need("SILVER".equals(row.get("kind")) && SourceJson.sha(SourceJson.canonical(q)).equals(row.get("evidence_sha256")) && Arrays.equals(SourceJson.canonical(q),(byte[])row.get("evidence_bytes")) && q.get("selection").get("storageBindingId").stringValue().equals(row.get("storage_binding_id")) && q.get("selection").get("relativeAddress").stringValue().equals(row.get("relative_address")) && q.get("verification").get("inventoryIdentity").get("sha256").stringValue().equals(row.get("native_identity")),INPUT_CHANGED);
        }
    }
}
