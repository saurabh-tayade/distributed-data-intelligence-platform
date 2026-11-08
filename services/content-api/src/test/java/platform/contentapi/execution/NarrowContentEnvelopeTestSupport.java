package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

/** Measurements of actual retained bytes. Never creates evidence or execution authority. */
public final class NarrowContentEnvelopeTestSupport {
    public static ObjectNode measure(JdbcTemplate jdbc, Path root, UUID run, UUID attempt, String source) throws Exception {
        var report=SourceJson.object().put("measurementVersion",1);
        var records=report.putArray("envelopes");
        var plan=ContentOperationPlan.original(root,run,attempt,source);
        long total=0;int entries=0;
        try(var walk=Files.walk(root)) {
            for(var path:walk.sorted().toList()) {
                entries++;
                if(Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))continue;
                assertThat(Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)).isTrue();
                byte[] raw=Files.readAllBytes(path);total+=raw.length;
                String name=path.getFileName().toString();
                int cap;
                if(name.equals(ContentEvidenceBudget.MANIFEST))cap=ContentEvidenceBudget.LEDGER;
                else if(name.equals(ContentEvidenceBudget.LOCK)){assertThat(raw).isEmpty();continue;}
                else {
                    var slots=plan.reservations().get(path.getParent());
                    assertThat(slots).as("reserved operation %s",root.relativize(path.getParent())).isNotNull();
                    String slot=name.endsWith(".preparing")?name.substring(0,name.length()-10):name;
                    assertThat(slots).containsKey(slot);cap=slots.get(slot);
                }
                assertThat(raw.length).as("file %s",name).isLessThanOrEqualTo(cap);
                if(name.endsWith(".json"))records.add(envelope("file/"+root.relativize(path),raw,cap,!name.equals("metadata.json")));
            }
        }
        assertThat(total).isLessThanOrEqualTo(ContentEvidenceBudget.BYTES);
        assertThat(entries).isLessThanOrEqualTo(221);
        report.put("privateBytes",total).put("privateEntries",entries)
                .put("reservedBytes",ContentEvidenceBudget.BYTES).put("reservedEntries",221);
        // These are the V21 complete record ceilings, not a sum of component allowances.
        for(var row:jdbc.queryForList("SELECT step_index,stage,command_bytes FROM ddip_management.content_pipeline_operations WHERE attempt_id=? ORDER BY step_index",attempt))
            records.add(envelope("sql/command/"+row.get("step_index"),(byte[])row.get("command_bytes"),ContentEvidenceBudget.slots((String)row.get("stage")).get("command.json")));
        for(var row:jdbc.queryForList("SELECT c.*,o.stage FROM ddip_management.content_pipeline_checkpoints c JOIN ddip_management.content_pipeline_operations o USING(operation_id) WHERE o.attempt_id=? ORDER BY o.step_index",attempt)) {
            for(String field:List.of("proof","result","evidence","selection"))if(row.get(field+"_bytes") instanceof byte[] raw)
                records.add(envelope("sql/checkpoint/"+row.get("operation_id")+"/"+field,raw,field.equals("proof")?3*1024*1024:row.get("stage").equals("VERIFY_SILVER") && Set.of("evidence","selection").contains(field)?NarrowContentProfile.SILVER_QUALIFICATION:ContentEvidenceBudget.slots((String)row.get("stage")).get(field+".json")));
        }
        for(var row:jdbc.queryForList("SELECT proof_bytes FROM ddip_management.content_pipeline_completions WHERE attempt_id=?",attempt))
            records.add(envelope("sql/completion",(byte[])row.get("proof_bytes"),65536));
        // Traverse only this completed run's exact dependency graph, not unrelated fixture artifacts.
        for(var row:jdbc.queryForList("WITH RECURSIVE ids(id) AS (SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=? AND result_artifact_id IS NOT NULL UNION SELECT d.dependency_artifact_id FROM ddip_management.artifact_dependencies d JOIN ids i ON d.artifact_id=i.id) SELECT a.artifact_id,a.kind,a.evidence_bytes FROM ddip_management.artifacts a JOIN ids ON a.artifact_id=ids.id ORDER BY a.kind",run)) {
            String kind=(String)row.get("kind");byte[] raw=(byte[])row.get("evidence_bytes");
            int cap=Set.of("GOLD","CONTENT").contains(kind)?1024*1024:kind.equals("SILVER")?NarrowContentProfile.SILVER_QUALIFICATION:PreflightFiles.LARGE;
            records.add(envelope("sql/artifact/"+kind+"/"+row.get("artifact_id"),raw,cap));
        }
        return report;
    }
    private static ObjectNode envelope(String address,byte[] raw,int cap) {
        return envelope(address,raw,cap,true);
    }
    private static ObjectNode envelope(String address,byte[] raw,int cap,boolean requireCanonical) {
        assertThat(raw.length).as(address).isLessThanOrEqualTo(cap);
        var parsed=SourceJson.parse(raw,cap);
        boolean canonical=Arrays.equals(SourceJson.canonical(parsed),raw);
        if(requireCanonical)assertThat(canonical).as("canonical bytes: %s",address).isTrue();
        // Native RSS metadata retains its original serializer order; it is not a canonical command.
        assertThat(SourceJson.canonical(SourceJson.parse(SourceJson.canonical(parsed),cap))).isEqualTo(SourceJson.canonical(parsed));
        int[] shape={0,0,0};shape(parsed,0,shape);
        return SourceJson.object().put("address",address).put("bytes",raw.length).put("cap",cap)
                .put("headroom",cap-raw.length).put("sha256",SourceJson.sha(raw)).put("canonicalWireBytes",canonical)
                .put("depth",shape[0]).put("stringUtf16",shape[1]).put("keyUtf16",shape[2]);
    }
    private static void shape(JsonNode n,int depth,int[] shape) {
        shape[0]=Math.max(shape[0],depth);
        if(n.isString())shape[1]=Math.max(shape[1],n.stringValue().length());
        if(n.isObject())for(String key:n.propertyNames())shape[2]=Math.max(shape[2],key.length());
        if(n.isContainer())for(var child:n)shape(child,depth+1,shape);
    }
    private NarrowContentEnvelopeTestSupport(){}
}
