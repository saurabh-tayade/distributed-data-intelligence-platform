package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import platform.contentapi.management.FrozenRunSelection;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.*;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/** Fixed validation candidate; production Content dispatch stays disabled. Legacy admission is unchanged. */
final class ContentEvidenceProfile {
    static final List<String> MODULES=List.of("bounded_evidence.py","bounded_content_producer.py");
    static boolean bounded(JsonNode frozen){return frozen.get("executionProfile").get("configuration").get("runProfileVersion").intValue()==2;}
    static void selection(JsonNode frozen){
        need(bounded(frozen) && frozen.get("pipeline").stringValue().equals("CONTENT_RSS_V1") && frozen.get("sources").size()==1
                && frozen.get("inputs").size()==1 && frozen.get("inputs").get(0).get("role").stringValue().equals("PUBLISHER_REFERENCE")
                && SourceJson.canonical(frozen.get("inputs").get(0)).length<=65536 && SourceJson.canonical(frozen).length<=256*1024,RESOURCE_LIMIT);
    }
    static JsonNode configuration(JdbcTemplate j,ManagementTransactions tx,String id){
        var rows=tx.required(()->j.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",id));need(rows.size()==1,RUNTIME_MISMATCH);
        return FrozenRunSelection.canonical((byte[])rows.getFirst().get("configuration_bytes"),(String)rows.getFirst().get("configuration_sha256"),65536);
    }
    static void runtime(JdbcTemplate j,ManagementTransactions tx,JsonNode frozen,String kind,String id,int version){
        boolean bounded=bounded(frozen);need(version==(bounded?2:1),RUNTIME_MISMATCH);if(!bounded)return;
        selection(frozen);String expected=frozen.get("executionProfile").get("configuration").get("contentRuntimeId").stringValue();
        if(!kind.equals("CONTENT"))expected=configuration(j,tx,expected).get("goldRuntimeId").stringValue();
        if(Set.of("SILVER","BRONZE","FETCH").contains(kind))expected=configuration(j,tx,expected).get("silverRuntimeId").stringValue();
        if(Set.of("BRONZE","FETCH").contains(kind))expected=configuration(j,tx,expected).get("bronzeRuntimeId").stringValue();
        if(kind.equals("FETCH"))expected=configuration(j,tx,expected).get("fetchRuntimeId").stringValue();
        need(expected.equals(id),RUNTIME_MISMATCH);
    }
    static void prepare(JdbcTemplate j,ManagementTransactions tx,InstallationHostGuard guard,WorkerRepository.Owner owner,JsonNode frozen,String fetchId){
        if(!bounded(frozen))return;
        selection(frozen);runtime(j,tx,frozen,"FETCH",fetchId,2);
        String id=frozen.get("executionProfile").get("configuration").get("contentRuntimeId").stringValue();
        var content=RssContentRuntime.load(j,tx,guard,owner.runId(),frozen,id);
        var bronze=configuration(j,tx,content.gold.silver.config.get("bronzeRuntimeId").stringValue());
        need(bronze.get("bronzeRuntimeVersion").intValue()==2 && bronze.get("adapter").stringValue().equals("platform.ingestion.managed.BoundedManagedBronze")
                && bronze.get("runProfileSha256").equals(frozen.get("executionProfile").get("configurationSha256"))
                && bronze.get("fetchRuntimeId").stringValue().equals(fetchId)
                && bronze.get("fetchRuntimeSha256").stringValue().equals(SourceJson.sha(SourceJson.canonical(configuration(j,tx,fetchId)))),RUNTIME_MISMATCH);
        ContentEvidenceBudget.initialize(guard,owner.runId(),owner.attemptId());
    }
    static List<String> modules(List<String> legacy,int version){var result=new ArrayList<>(legacy);if(version==2)result.addAll(MODULES);return List.copyOf(result);}
    static List<String> command(JsonNode silver,String kind,Path command){return List.of(silver.get("python").stringValue(),"-I","-B","-X","utf8",Path.of(silver.get("processingRoot").stringValue()).resolve("bounded_content_producer.py").toString(),kind,command.toString());}
    static void initialize(InstallationHostGuard guard,JsonNode frozen,UUID run,UUID attempt){if(bounded(frozen))ContentEvidenceBudget.initialize(guard,run,attempt);}
    private ContentEvidenceProfile(){}
}
