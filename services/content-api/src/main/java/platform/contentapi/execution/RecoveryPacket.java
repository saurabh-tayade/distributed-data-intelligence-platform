package platform.contentapi.execution;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** Strict reader for the existing C3b packet. Byte columns remain length/hash projections, never payloads. */
final class RecoveryPacket {
    private static final String[] COORDINATOR="installation_id mode epoch active_attempt_id owner_instance_id active_import_id heartbeat_at row_version".split(" ");
    private static final String[] RUN="run_id product_id pipeline pipeline_version idempotency_key normalized_request request_sha256 captured_selection selection_sha256 execution_profile_id state current_stage current_attempt_id row_version finished_at result_artifact_id outcome_code counts_bytes retry_eligible".split(" ");
    private static final String[] ATTEMPT="attempt_id run_id attempt_no kind instance_id owner_epoch state started_at finished_at outcome_code row_version".split(" ");
    private static final String[] CONTROL="control_id run_id type idempotency_key normalized_request request_sha256 expected_version target_attempt_id quiescence_confirmed state reason requester requested_at applied_at row_version".split(" ");
    private static final String[] WORKER="instance_id installation_id execution_profile_id profile_sha256 host_binding_sha256 pid process_started_at status heartbeat_at row_version".split(" ");
    private static final String[] STAGE="stage item_ordinal checkpoint_id state input_evidence_sha256 intended_identity intended_namespace command_address command_sha256 result_address result_sha256 child_pid child_started_at child_instance_id started_at finished_at outcome_code counts_bytes row_version".split(" ");
    private static final String[] HISTORY="attempt_id attempt_no kind instance_id owner_epoch state row_version".split(" ");
    private static final Set<String> IDS=Set.of("installation_id","active_attempt_id","owner_instance_id","active_import_id","run_id","current_attempt_id","result_artifact_id","attempt_id","instance_id","control_id","target_attempt_id","checkpoint_id","child_instance_id");
    private static final Set<String> NUMBERS=Set.of("epoch","row_version","pipeline_version","attempt_no","owner_epoch","expected_version","pid","item_ordinal","child_pid");
    private static final Set<String> TIMES=Set.of("heartbeat_at","finished_at","started_at","requested_at","applied_at","process_started_at","child_started_at");
    private static final Set<String> BYTES=Set.of("normalized_request","captured_selection","counts_bytes");
    final JsonNode descriptor;
    final List<Map<String,Object>> rows;
    final List<Map<String,Object>> stages,history;
    final RecoveryQuiescence.Review review;
    private RecoveryPacket(JsonNode descriptor,List<Map<String,Object>> rows,List<Map<String,Object>> stages,List<Map<String,Object>> history,RecoveryQuiescence.Review review){this.descriptor=descriptor;this.rows=rows;this.stages=stages;this.history=history;this.review=review;}
    static RecoveryPacket parse(byte[] raw,String address,String sha,InstallationHostGuard guard) {
        try {
            need(sha!=null && sha.matches("[0-9a-f]{64}") && SourceJson.sha(raw).equals(sha),EVIDENCE_CHANGED);
            var n=PreflightFiles.canonical(raw,RecoveryFiles.MAXIMUM);
            SourceJson.fields(n,"qualificationVersion","policy","installationId","controlId","runId","oldAttemptId","oldInstanceId","oldEpoch","recoveryInstanceId","hostBindingSha256","snapshotSha256","selectionSha256","runtimeSha256","qualifiedAt","qualificationId","oldCoordinatorVersion","oldRunVersion","oldAttemptVersion","controlVersion","runProfileId","runProfileSha256","originalMetadata","operatorReview","privateEvidence");
            need(n.get("qualificationVersion").isIntegralNumber() && n.get("qualificationVersion").canConvertToInt() && n.get("qualificationVersion").intValue()==1 && text(n,"policy").equals("E5_EXECUTION_QUIESCENCE_V1"),EVIDENCE_CHANGED);
            for(String key:List.of("installationId","controlId","runId","oldAttemptId","oldInstanceId","recoveryInstanceId","qualificationId"))uuid(text(n,key));
            for(String key:List.of("hostBindingSha256","snapshotSha256","selectionSha256","runtimeSha256","runProfileSha256"))need(text(n,key).matches("[0-9a-f]{64}"),EVIDENCE_CHANGED);
            for(String key:List.of("oldEpoch","oldCoordinatorVersion","oldRunVersion","oldAttemptVersion","controlVersion"))number(text(n,key));
            need(address.equals("recovery/"+text(n,"controlId")+"/"+text(n,"qualificationId")+"/qualification.json") && text(n,"installationId").equals(guard.binding().installationId().toString()) && text(n,"hostBindingSha256").equals(guard.binding().digest()),EVIDENCE_CHANGED);
            var original=array(n.get("originalMetadata"),8);need(original.size()==8 && SourceJson.sha(SourceJson.canonical(n.get("originalMetadata"))).equals(text(n,"snapshotSha256")),EVIDENCE_CHANGED);
            var rows=new ArrayList<Map<String,Object>>();String[][] schemas={COORDINATOR,RUN,ATTEMPT,CONTROL,WORKER,WORKER};
            for(int i=0;i<schemas.length;i++)rows.add(row(original.get(i),schemas[i]));
            var stages=rows(original.get(6),512,STAGE);var history=rows(original.get(7),20,HISTORY);
            String[][] mirrors={{"installationId","installation_id"},{"oldEpoch","epoch"},{"oldCoordinatorVersion","row_version"}};
            for(var pair:mirrors)equal(n,pair[0],rows.get(0),pair[1]);
            equal(n,"runId",rows.get(1),"run_id");equal(n,"oldRunVersion",rows.get(1),"row_version");equal(n,"selectionSha256",rows.get(1),"selection_sha256");equal(n,"runProfileId",rows.get(1),"execution_profile_id");
            equal(n,"oldAttemptId",rows.get(2),"attempt_id");equal(n,"oldAttemptVersion",rows.get(2),"row_version");equal(n,"oldEpoch",rows.get(2),"owner_epoch");equal(n,"runId",rows.get(2),"run_id");equal(n,"oldInstanceId",rows.get(2),"instance_id");
            equal(n,"controlId",rows.get(3),"control_id");equal(n,"controlVersion",rows.get(3),"row_version");equal(n,"runId",rows.get(3),"run_id");equal(n,"oldAttemptId",rows.get(3),"target_attempt_id");equal(n,"oldRunVersion",rows.get(3),"expected_version");
            equal(n,"oldAttemptId",rows.get(0),"active_attempt_id");equal(n,"oldAttemptId",rows.get(1),"current_attempt_id");equal(n,"oldInstanceId",rows.get(0),"owner_instance_id");equal(n,"oldInstanceId",rows.get(4),"instance_id");equal(n,"recoveryInstanceId",rows.get(5),"instance_id");
            for(int i:List.of(4,5)){equal(n,"installationId",rows.get(i),"installation_id");equal(n,"runProfileId",rows.get(i),"execution_profile_id");equal(n,"runProfileSha256",rows.get(i),"profile_sha256");equal(n,"hostBindingSha256",rows.get(i),"host_binding_sha256");}
            need(!text(n,"oldInstanceId").equals(text(n,"recoveryInstanceId")) && number(text(n,"oldEpoch"))>0 && number(text(n,"oldEpoch"))<Long.MAX_VALUE && "RECONCILE".equals(rows.get(3).get("type")) && "REQUESTED".equals(rows.get(3).get("state")) && Boolean.TRUE.equals(rows.get(3).get("quiescence_confirmed")) && "local-operator".equals(rows.get(3).get("requester")) && "RECOVERY_ONLY".equals(rows.get(5).get("status")),EVIDENCE_CHANGED);
            var review=review(n.get("operatorReview"));
            need(review.targetSha256().equals(text(n,"snapshotSha256")) && review.handlersStopped() && review.unmanagedWritersStopped() && review.orphanReviewComplete(),QUIESCENCE_REQUIRED);
            var unresolved=new HashSet<UUID>();for(var stage:stages)if(Set.of("INTENT","RUNNING","UNKNOWN").contains(stage.get("state")))unresolved.add((UUID)stage.get("checkpoint_id"));
            need(review.reviewedCheckpoints().equals(unresolved),QUIESCENCE_REQUIRED);
            var qualifiedAt=Instant.parse(text(n,"qualifiedAt"));need(qualifiedAt.toString().equals(text(n,"qualifiedAt")),EVIDENCE_CHANGED);
            RecoveryQuiescence.fresh(review.inspectedAt(),qualifiedAt);
            SourceJson.fields(n.get("privateEvidence"),"files");need(n.get("privateEvidence").get("files").isArray() && n.get("privateEvidence").get("files").size()==stages.size()*6,EVIDENCE_CHANGED);
            return new RecoveryPacket(n,rows,stages,history,review);
        }catch(RecoveryFailure ex){throw ex;}catch(RuntimeException ex){throw new RecoveryFailure(EVIDENCE_CHANGED);}
    }
    private static Map<String,Object> row(JsonNode n,String[] fields) {
        SourceJson.fields(n,fields);var map=new HashMap<String,Object>();
        for(String key:fields){var v=n.get(key);Object value;
            if(v.isNull())value=null;
            else if(BYTES.contains(key)){SourceJson.fields(v,"sha256","length");need(text(v,"sha256").matches("[0-9a-f]{64}") && number(text(v,"length"))<=PreflightFiles.LARGE,EVIDENCE_CHANGED);value=v;}
            else if(IDS.contains(key))value=uuid(SourceJson.text(v));
            else if(NUMBERS.contains(key))value=number(SourceJson.text(v));
            else if(TIMES.contains(key))value=Timestamp.from(Instant.parse(SourceJson.text(v)));
            else if(Set.of("quiescence_confirmed","retry_eligible").contains(key)){need(v.isBoolean(),EVIDENCE_CHANGED);value=v.booleanValue();}
            else value=SourceJson.text(v);
            map.put(key,value);
        }return map;
    }
    private static List<Map<String,Object>> rows(JsonNode n,int maximum,String[] fields){var result=new ArrayList<Map<String,Object>>();for(var v:array(n,maximum))result.add(row(v,fields));return result;}
    private static JsonNode array(JsonNode n,int maximum){need(n!=null && n.isArray() && n.size()<=maximum,EVIDENCE_CHANGED);return n;}
    private static void equal(JsonNode n,String field,Map<String,Object> row,String key){need(text(n,field).equals(Objects.toString(row.get(key),null)),EVIDENCE_CHANGED);}
    static String text(JsonNode n,String key){return SourceJson.text(n.get(key));}
    private static long number(String n){need(n.matches("0|[1-9][0-9]{0,18}"),EVIDENCE_CHANGED);return Long.parseLong(n);}
    private static UUID uuid(String n){UUID id=UUID.fromString(n);need(id.toString().equals(n),EVIDENCE_CHANGED);return id;}
    static RecoveryQuiescence.Review review(JsonNode n) {
        SourceJson.fields(n,"targetSha256","inspectedAt","handlersStopped","unmanagedWritersStopped","orphanReviewComplete","reviewedCheckpoints","descendants");
        need(n.get("reviewedCheckpoints").isArray() && n.get("descendants").isArray(),EVIDENCE_CHANGED);
        var checkpoints=new HashSet<UUID>();for(var v:n.get("reviewedCheckpoints"))need(checkpoints.add(uuid(SourceJson.text(v))),EVIDENCE_CHANGED);
        var descendants=new ArrayList<RecoveryQuiescence.Identity>();for(var v:n.get("descendants")){SourceJson.fields(v,"pid","startedAt");descendants.add(new RecoveryQuiescence.Identity(number(text(v,"pid")),Instant.parse(text(v,"startedAt"))));}
        for(String key:List.of("handlersStopped","unmanagedWritersStopped","orphanReviewComplete"))need(n.get(key).isBoolean(),EVIDENCE_CHANGED);
        var review=new RecoveryQuiescence.Review(text(n,"targetSha256"),Instant.parse(text(n,"inspectedAt")),n.get("handlersStopped").booleanValue(),n.get("unmanagedWritersStopped").booleanValue(),n.get("orphanReviewComplete").booleanValue(),checkpoints,descendants);
        need(review.json().equals(n),EVIDENCE_CHANGED);return review;
    }
    @Override public String toString(){return "RecoveryPacket[redacted]";}
}
