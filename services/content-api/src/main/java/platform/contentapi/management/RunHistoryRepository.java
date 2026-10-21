package platform.contentapi.management;

import java.sql.*;
import java.time.*;
import java.time.format.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.ManagementTransactions;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.management.SourceFailure.require;
import static platform.contentapi.management.RunInspectionValues.check;

/** Fixed, bounded projections of existing history. No coordinator lock, native I/O, private evidence reads, or writes. */
public final class RunHistoryRepository {
    private static final DateTimeFormatter UTC=new DateTimeFormatterBuilder().appendInstant(6).toFormatter();
    private static final String BASE_COLUMNS="r.run_id,r.submission_seq,r.product_id,r.pipeline,r.state,r.current_stage,r.row_version,r.requested_at,r.finished_at,r.outcome_code,r.retry_eligible,r.current_attempt_id,r.result_artifact_id,"+
            "a.kind AS result_kind,a.product_id AS result_product,a.native_identity AS result_fingerprint,a.contract_version AS result_contract,a.processor_revision AS result_processor,a.verified_at AS result_verified_at,"+
            "EXISTS(SELECT 1 FROM ddip_management.stage_artifacts sa JOIN ddip_management.stage_invocations si USING(attempt_id,stage,item_ordinal) JOIN ddip_management.attempts t ON t.attempt_id=sa.attempt_id " +
            "WHERE t.run_id=r.run_id AND si.stage='VERIFY_RESULT' AND si.state='VERIFIED' AND sa.artifact_id=r.result_artifact_id) AS verified_result";
    private static final String FROM=" FROM ddip_management.runs r LEFT JOIN ddip_management.artifacts a ON a.artifact_id=r.result_artifact_id ";
    private final JdbcTemplate jdbc;
    private final ManagementTransactions tx;
    private final RunPageCursor cursors;
    public RunHistoryRepository(JdbcTemplate jdbc,ManagementTransactions tx,SourceCursor signer){this(jdbc,tx,signer,Clock.systemUTC());}
    RunHistoryRepository(JdbcTemplate jdbc,ManagementTransactions tx,SourceCursor signer,Clock clock){this.jdbc=jdbc;this.tx=tx;this.cursors=new RunPageCursor(signer,clock);}
    private static String time(ResultSet r,String key)throws SQLException{var t=r.getObject(key,OffsetDateTime.class);return t==null?null:UTC.format(t.toInstant());}
    private ObjectNode summary(ResultSet r,int ignored)throws SQLException {
        var n=SourceJson.object().put("runId",r.getObject("run_id",UUID.class).toString()).put("productId",r.getString("product_id")).put("pipeline",r.getString("pipeline")).put("state",r.getString("state"))
                .put("currentStage",r.getString("current_stage")).put("rowVersion",Long.toString(r.getLong("row_version"))).put("requestedAt",time(r,"requested_at")).put("finishedAt",time(r,"finished_at")).put("outcomeCode",RunInspectionValues.code(r.getString("outcome_code"))).put("retryEligibility",r.getBoolean("retry_eligible"));
        if(r.getString("pipeline").equals("RELIABILITY_V2"))n.put("dataOrigin","synthetic");
        UUID result=r.getObject("result_artifact_id",UUID.class);
        if(result==null)n.putNull("result");
        else {
            boolean content=r.getString("pipeline").equals("CONTENT_RSS_V1");String fingerprint=r.getString("result_fingerprint");
            check(r.getString("state").equals("SUCCEEDED") && r.getBoolean("verified_result") && r.getString("product_id").equals(r.getString("result_product")) &&
                    (content?"CONTENT":"RELIABILITY_V2").equals(r.getString("result_kind")) && r.getInt("result_contract")== (content?1:2) && r.getInt("result_processor")== (content?1:2) && fingerprint!=null && fingerprint.matches("[0-9a-f]{64}"));
            n.putObject("result").put("artifactId",result.toString()).put("nativeFingerprint",fingerprint).put("productContractVersion",r.getInt("result_contract")).put("processorRevision",r.getInt("result_processor")).put("verificationStatus","VERIFIED_AT_EXECUTION").put("verifiedAt",time(r,"result_verified_at"));
        }
        return n;
    }
    public SourceRepository.Page runs(String product,String source,String state,int limit,String cursor) {
        limit(limit);if(product!=null)SourceCommands.product(product);if(source!=null)SourceCommands.source(source);require(state==null || RunInspectionValues.STATES.contains(state));
        var filters=SourceJson.object().put("endpoint","runs").put("order","submission_seq_desc").put("productId",product).put("sourceId",source).put("state",state);String scope=SourceJson.sha(SourceJson.canonical(filters));
        return tx.required(()->{
            var p=cursor==null?cursors.first(jdbc.queryForObject("SELECT coalesce(max(submission_seq),0) FROM ddip_management.runs",Long.class),true):cursors.decode(cursor,"runs",scope,true);
            String where=" WHERE r.submission_seq<=?";var args=new ArrayList<Object>();args.add(p.high());
            if(cursor!=null){where+=" AND r.submission_seq<?";args.add(p.after());}
            if(product!=null){where+=" AND r.product_id=?";args.add(product);}if(state!=null){where+=" AND r.state=?";args.add(state);}
            if(source!=null){where+=" AND EXISTS(SELECT 1 FROM ddip_management.run_sources rs WHERE rs.run_id=r.run_id AND rs.product_id=r.product_id AND rs.source_id=?)";args.add(source);}
            args.add(limit+1);
            var rows=jdbc.query("SELECT "+BASE_COLUMNS+FROM+where+" ORDER BY r.submission_seq DESC LIMIT ?",this::summaryWithSequence,args.toArray());
            return page(rows,limit,"runs",scope,p,"_sequence");
        });
    }
    private ObjectNode summaryWithSequence(ResultSet r,int i)throws SQLException{var n=summary(r,i);n.put("_sequence",Long.toString(r.getLong("submission_seq")));return n;}
    public ObjectNode get(String id){UUID run=id(id);return tx.required(()->detail(run));}
    ObjectNode detail(UUID run) {
        var rows=jdbc.query("SELECT "+BASE_COLUMNS+",r.counts_bytes,(SELECT count(*) FROM ddip_management.attempts t WHERE t.run_id=r.run_id) AS attempt_count,"+
                "c.control_id,c.type AS control_type,c.state AS control_state,c.reason AS control_reason,c.requested_at AS control_requested_at,c.applied_at AS control_applied_at,c.row_version AS control_version"+FROM+
                "LEFT JOIN LATERAL(SELECT control_id,type,state,reason,requested_at,applied_at,row_version FROM ddip_management.controls WHERE run_id=r.run_id ORDER BY requested_at DESC,control_id DESC LIMIT 1) c ON true WHERE r.run_id=?",(r,i)->{
            var n=summary(r,i);n.set("counts",RunInspectionValues.counts(r.getBytes("counts_bytes")));
            n.putObject("attempts").put("count",Long.toString(r.getLong("attempt_count"))).put("currentAttemptId",r.getObject("current_attempt_id",UUID.class)==null?null:r.getObject("current_attempt_id",UUID.class).toString());
            UUID control=r.getObject("control_id",UUID.class);if(control==null)n.putNull("latestControl");else n.putObject("latestControl").put("controlId",control.toString()).put("type",r.getString("control_type")).put("state",r.getString("control_state")).put("reason",RunInspectionValues.code(r.getString("control_reason"))).put("requestedAt",time(r,"control_requested_at")).put("appliedAt",time(r,"control_applied_at")).put("rowVersion",Long.toString(r.getLong("control_version")));
            return n;
        },run);
        if(rows.isEmpty())throw new SourceFailure(SourceFailure.Code.NOT_FOUND);var n=rows.getFirst();
        var sources=jdbc.query("SELECT source_id,revision FROM ddip_management.run_sources WHERE run_id=? ORDER BY source_id COLLATE \"C\" LIMIT 33",(r,i)->SourceJson.object().put("sourceId",r.getString(1)).put("revision",r.getInt(2)),run);
        check(sources.size()<=(n.get("pipeline").stringValue().equals("CONTENT_RSS_V1")?8:32));n.putArray("sourceRevisions").addAll(sources);
        var inputs=jdbc.query("SELECT artifact_id FROM ddip_management.run_inputs WHERE run_id=? ORDER BY role COLLATE \"C\",ordinal LIMIT 130",(r,i)->r.getObject(1,UUID.class).toString(),run);
        check(inputs.size()<=129);var a=n.putArray("inputArtifactIds");inputs.forEach(a::add);return n;
    }
    public SourceRepository.Page attempts(String id,int limit,String cursor) {
        UUID run=id(id);limit(limit);String scope=scope("attempts",run,"attempt_no_asc");
        return tx.required(()->{
            exists(run);var p=cursor==null?cursors.first(jdbc.queryForObject("SELECT coalesce(max(attempt_no),0) FROM ddip_management.attempts WHERE run_id=?",Long.class,run),false):cursors.decode(cursor,"attempts",scope,false);
            var rows=jdbc.query("SELECT t.attempt_id,t.attempt_no,t.kind,t.state,t.started_at,t.finished_at,t.outcome_code,(SELECT count(*) FROM ddip_management.stage_invocations s WHERE s.attempt_id=t.attempt_id) AS stage_count FROM ddip_management.attempts t WHERE t.run_id=? AND t.attempt_no>? AND t.attempt_no<=? ORDER BY t.attempt_no LIMIT ?",(r,i)->{
                return SourceJson.object().put("attemptId",r.getObject("attempt_id",UUID.class).toString()).put("attemptNo",r.getInt("attempt_no")).put("kind",r.getString("kind")).put("state",r.getString("state")).put("startedAt",time(r,"started_at")).put("finishedAt",time(r,"finished_at")).put("outcomeCode",RunInspectionValues.code(r.getString("outcome_code"))).put("stageCount",Long.toString(r.getLong("stage_count"))).put("_sequence",Long.toString(r.getInt("attempt_no")));
            },run,p.after(),p.high(),limit+1);return page(rows,limit,"attempts",scope,p,"_sequence");
        });
    }
    public SourceRepository.Page events(String id,int limit,String cursor) {
        UUID run=id(id);limit(limit);String scope=scope("events",run,"event_seq_asc");
        return tx.required(()->{
            exists(run);var p=cursor==null?cursors.first(jdbc.queryForObject("SELECT coalesce(max(event_seq),0) FROM ddip_management.audit_events WHERE run_id=?",Long.class,run),false):cursors.decode(cursor,"events",scope,false);
            var rows=jdbc.query("SELECT e.event_seq,e.time,e.action,e.before_state,e.after_state,e.before_version,e.after_version,e.safe_code,e.attempt_id,e.control_id,t.run_id AS attempt_run,c.run_id AS control_run FROM ddip_management.audit_events e LEFT JOIN ddip_management.attempts t ON t.attempt_id=e.attempt_id LEFT JOIN ddip_management.controls c ON c.control_id=e.control_id WHERE e.run_id=? AND e.event_seq>? AND e.event_seq<=? ORDER BY e.event_seq LIMIT ?",(r,i)->{
                var n=SourceJson.object().put("eventSeq",Long.toString(r.getLong("event_seq"))).put("timestamp",time(r,"time")).put("action",RunInspectionValues.action(r.getString("action"))).put("beforeState",RunInspectionValues.state(r.getString("before_state"))).put("afterState",RunInspectionValues.state(r.getString("after_state"))).put("code",RunInspectionValues.code(r.getString("safe_code")));
                for(String field:List.of("before","after")){Long v=r.getObject(field+"_version",Long.class);n.put(field+"Version",v==null?null:v.toString());}
                var context=n.putObject("context");for(String field:List.of("attempt","control")){UUID value=r.getObject(field+"_id",UUID.class);check(value==null || run.equals(r.getObject(field+"_run",UUID.class)));context.put(field+"Id",value==null?null:value.toString());}n.put("_sequence",n.get("eventSeq").stringValue());return n;
            },run,p.after(),p.high(),limit+1);return page(rows,limit,"events",scope,p,"_sequence");
        });
    }
    private SourceRepository.Page page(List<ObjectNode> rows,int limit,String kind,String scope,RunPageCursor.Position p,String sequence) {
        boolean more=rows.size()>limit;String next=more?cursors.next(kind,scope,p,Long.parseLong(rows.get(limit-1).get(sequence).stringValue())):null;
        var data=SourceJson.object().putArray("unused");for(var n:rows.subList(0,Math.min(limit,rows.size()))){n.remove(sequence);data.add(n);}return new SourceRepository.Page(data,limit,next);
    }
    private static UUID id(String text){return UUID.fromString(SourceCommands.uuid(text));}
    private static void limit(int value){require(value>=1 && value<=100);}
    private static String scope(String kind,UUID id,String order){return SourceJson.sha(SourceJson.canonical(SourceJson.object().put("endpoint",kind).put("runId",id.toString()).put("order",order)));}
    private void exists(UUID run){if(!Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.runs WHERE run_id=?)",Boolean.class,run)))throw new SourceFailure(SourceFailure.Code.NOT_FOUND);}
}
