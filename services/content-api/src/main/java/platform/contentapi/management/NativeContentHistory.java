package platform.contentapi.management;

import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.ManagementTransactions;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.management.SourceFailure.require;
import static platform.contentapi.management.RunInspectionValues.check;

/** Read-only native facts, not legacy stages or authority to resume. Never selects private proof bytes. */
final class NativeContentHistory {
    private static final java.time.format.DateTimeFormatter UTC=new DateTimeFormatterBuilder().appendInstant(6).toFormatter();
    private static final String PHASE_ORDER="CASE phase WHEN 'LAUNCH_BOUND' THEN 1 WHEN 'FILES_READY' THEN 2 WHEN 'START_AUTHORIZED' THEN 3 WHEN 'CHILD_RECORDED' THEN 4 WHEN 'RELEASE_AUTHORIZED' THEN 5 WHEN 'CANDIDATE_EXITED' THEN 6 END";
    private static final String OPERATIONS="""
        SELECT o.attempt_id,o.operation_id,0 AS step_index,o.stage,o.item_ordinal,o.checkpoint_id,o.checkpoint_id AS native_checkpoint_id,o.created_at,
          c.completion_sha256::text AS proof_sha256,c.created_at AS checkpoint_at,
          (SELECT count(*) FROM ddip_management.content_native_launch_phases p WHERE p.operation_id=o.operation_id) AS phase_count,
          (SELECT phase FROM ddip_management.content_native_launch_phases p WHERE p.operation_id=o.operation_id ORDER BY %s DESC LIMIT 1) AS last_phase
        FROM ddip_management.content_native_operations o LEFT JOIN ddip_management.content_fetch_checkpoints c USING(operation_id)
        WHERE o.attempt_id=?
        UNION ALL
        SELECT o.attempt_id,o.operation_id,1,'STORE_BRONZE',0,o.checkpoint_id,o.checkpoint_id,o.created_at,
          c.verified_sha256::text,c.created_at,
          (SELECT count(*) FROM ddip_management.content_bronze_launch_phases p WHERE p.operation_id=o.operation_id)+
          (SELECT count(*) FROM ddip_management.content_bronze_verification_phases p WHERE p.operation_id=o.operation_id),
          coalesce((SELECT CASE WHEN phase='VERIFY_BOUND' THEN 'VERIFICATION_BOUND' ELSE 'VERIFICATION_'||phase END FROM ddip_management.content_bronze_verification_phases p WHERE p.operation_id=o.operation_id
            ORDER BY CASE phase WHEN 'VERIFY_BOUND' THEN 1 WHEN 'START_AUTHORIZED' THEN 2 WHEN 'CHILD_RECORDED' THEN 3 WHEN 'RELEASE_AUTHORIZED' THEN 4 WHEN 'OBSERVED' THEN 5 END DESC LIMIT 1),
          (SELECT phase FROM ddip_management.content_bronze_launch_phases p WHERE p.operation_id=o.operation_id ORDER BY %s DESC LIMIT 1))
        FROM ddip_management.content_bronze_reservations o LEFT JOIN ddip_management.content_bronze_checkpoints c USING(operation_id)
        WHERE o.attempt_id=?
        UNION ALL
        SELECT o.attempt_id,o.operation_id,o.step_index,o.stage,o.item_ordinal,o.checkpoint_id,o.native_checkpoint_id,o.created_at,
          c.proof_sha256::text,c.created_at,
          (SELECT count(*) FROM ddip_management.content_pipeline_phases p WHERE p.operation_id=o.operation_id),
          (SELECT phase FROM ddip_management.content_pipeline_phases p WHERE p.operation_id=o.operation_id ORDER BY %s DESC LIMIT 1)
        FROM ddip_management.content_pipeline_operations o LEFT JOIN ddip_management.content_pipeline_checkpoints c USING(operation_id)
        WHERE o.attempt_id=?
        """.formatted(PHASE_ORDER,PHASE_ORDER,PHASE_ORDER);
    private static final List<String> STAGES=List.of("FETCH_SOURCE","STORE_BRONZE","INSPECT_BRONZE","SILVER","VERIFY_SILVER","RECHECK_SILVER","INSPECT_BRONZE","PREPARE_GOLD","INSPECT_BRONZE","PUBLISH_GOLD","VERIFY_RESULT","CHECK_INPUTS","INSPECT_BRONZE","CHECK_INPUTS","PUBLISH_CONTENT","VERIFY_RESULT","CHECK_INPUTS");
    private static final List<Integer> ORDINALS=List.of(0,0,0,0,0,0,1,0,2,0,0,0,3,1,0,1,2);
    private static final Set<String> PHASES=Set.of("LAUNCH_BOUND","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","CANDIDATE_EXITED","VERIFICATION_BOUND","VERIFICATION_START_AUTHORIZED","VERIFICATION_CHILD_RECORDED","VERIFICATION_RELEASE_AUTHORIZED","VERIFICATION_OBSERVED");
    private final JdbcTemplate jdbc;
    private final ManagementTransactions tx;
    private final RunPageCursor cursors;
    NativeContentHistory(JdbcTemplate jdbc,ManagementTransactions tx,SourceCursor signer,Clock clock){this.jdbc=jdbc;this.tx=tx;this.cursors=new RunPageCursor(signer,clock);}
    SourceRepository.Page operations(String runId,String attemptId,int limit,String cursor) {
        UUID run=UUID.fromString(SourceCommands.uuid(runId)),attempt=UUID.fromString(SourceCommands.uuid(attemptId));require(limit>=1 && limit<=100);
        String kind="native-operations",scope=SourceJson.sha(SourceJson.canonical(SourceJson.object().put("endpoint",kind).put("runId",runId).put("attemptId",attemptId).put("order","step_index_asc")));
        return tx.required(()->{
            if(!Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.attempts WHERE run_id=? AND attempt_id=?)",Boolean.class,run,attempt)))throw new SourceFailure(SourceFailure.Code.NOT_FOUND);
            var position=cursor==null?cursors.first(jdbc.queryForObject("SELECT coalesce(max(step_index+1),0) FROM ("+OPERATIONS+") native",Long.class,attempt,attempt,attempt),false):cursors.decode(cursor,kind,scope,false);
            check(position.high()<=17);
            var rows=jdbc.query("SELECT * FROM ("+OPERATIONS+") native WHERE step_index+1>? AND step_index+1<=? ORDER BY step_index LIMIT ?",this::map,attempt,attempt,attempt,position.after(),position.high(),Math.min(limit,17)+1);
            boolean more=rows.size()>limit;var data=SourceJson.object().putArray("unused");data.addAll(rows.subList(0,Math.min(limit,rows.size())));
            String next=more?cursors.next(kind,scope,position,rows.get(limit-1).get("stepIndex").intValue()+1L):null;
            return new SourceRepository.Page(data,limit,next);
        });
    }
    private ObjectNode map(ResultSet r,int ignored)throws SQLException {
        int step=r.getInt("step_index");long phases=r.getLong("phase_count");String proof=r.getString("proof_sha256"),phase=r.getString("last_phase");
        check(step>=0 && step<=16 && phases>=0 && phases<=(step==1?11:6) && (proof==null || proof.matches("[0-9a-f]{64}")));
        check(STAGES.get(step).equals(r.getString("stage")) && ORDINALS.get(step)==r.getInt("item_ordinal") && (phase==null || PHASES.contains(phase)));
        String state=proof!=null?"CHECKPOINTED":phase==null?"RESERVED":phase.equals("CANDIDATE_EXITED")?"CANDIDATE_EXITED":"IN_PROGRESS";
        return SourceJson.object().put("attemptId",r.getObject("attempt_id",UUID.class).toString()).put("operationId",r.getObject("operation_id",UUID.class).toString())
            .put("stepIndex",step).put("stage",r.getString("stage")).put("itemOrdinal",r.getInt("item_ordinal"))
            .put("checkpointId",r.getObject("checkpoint_id",UUID.class).toString()).put("nativeCheckpointId",r.getObject("native_checkpoint_id",UUID.class).toString())
            .put("state",state).put("phaseCount",Long.toString(phases)).put("lastPhase",phase).put("proofSha256",proof)
            .put("reservedAt",time(r,"created_at")).put("checkpointedAt",time(r,"checkpoint_at")).put("continuationAuthorized",false);
    }
    private static String time(ResultSet r,String name)throws SQLException{var value=r.getObject(name,OffsetDateTime.class);return value==null?null:UTC.format(value.toInstant());}
}
