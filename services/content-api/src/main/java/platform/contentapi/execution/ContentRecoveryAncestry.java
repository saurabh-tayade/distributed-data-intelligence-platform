package platform.contentapi.execution;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.RecoveryFailure.*;
import static platform.contentapi.execution.RecoveryFailure.Code.*;

/** Read-only inherited-prefix projection. Historical stage rows retain their actual attempt IDs. */
final class ContentRecoveryAncestry {
    private final JdbcTemplate j;private final ManagementTransactions tx;
    private final RecoveryQuiescence.Processes processes;private final Clock clock;
    ContentRecoveryAncestry(JdbcTemplate j,ManagementTransactions tx,RecoveryQuiescence.Processes processes,Clock clock){this.j=j;this.tx=tx;this.processes=processes;this.clock=clock;}

    JsonNode inspect(InstallationHostGuard g,UUID run,UUID attempt,RssContentRuntime runtime,RecoveryQuiescence.Review review){
        try{
            var plan=tx.required(()->ContentResumeLineage.plan(j,run,attempt));if(plan==null)return null;
            need(plan.get("hostBindingSha256").stringValue().equals(g.binding().digest()) && plan.get("runtimeSha256").stringValue().equals(runtime.digest),EVIDENCE_CHANGED);
            // The applied RESUME, original checkpoint policy and claimed plan must agree exactly.
            tx.required(()->{
                var queued=j.queryForList("SELECT e.details_bytes FROM ddip_management.audit_events e JOIN ddip_management.controls k USING(control_id,run_id) WHERE e.run_id=? AND e.control_id=? AND e.action='CONTENT_RESUME_QUEUED' AND k.type='RESUME' AND k.state='APPLIED' AND k.reason='RESUME_QUEUED'",run,UUID.fromString(plan.get("resumeControlId").stringValue()));
                need(queued.size()==1 && SourceJson.parse((byte[])queued.getFirst().get("details_bytes"),PreflightFiles.LARGE).equals(plan),EVIDENCE_CHANGED);
                var expected=ContentResumePlan.checkpoint(j,j.queryForMap("SELECT * FROM ddip_management.runs WHERE run_id=?",run),UUID.fromString(plan.get("recoveryAttemptId").stringValue()));
                for(String key:expected.propertyNames())need(expected.get(key).equals(plan.get(key)),EVIDENCE_CHANGED);
                // Current continuation contracts describe an original EXECUTE prefix, not recursive copying.
                need(ContentResumeLineage.plan(j,run,UUID.fromString(plan.get("originalAttemptId").stringValue()))==null,EVIDENCE_UNAVAILABLE);
                return null;
            });
            var evidence=new ContentResumeEvidence(j,tx,processes);evidence.retained(g,plan);
            need(SourceJson.sha(PreflightFiles.read(java.nio.file.Path.of(runtime.gold.silver.config.get("processingRoot").stringValue()).resolve(runtime.config.get("contentRuntimeVersion").intValue()==2?"bounded_content_producer.py":"content_resume_verifier.py"),1024*1024,false)).equals(plan.get("resumeBridgeSha256").stringValue()),EVIDENCE_CHANGED);
            var proof=evidence.privateProof(g,plan,runtime);
            need(SourceJson.sha(SourceJson.canonical(proof)).equals(plan.get("privateSha256").stringValue()),EVIDENCE_CHANGED);
            var anchors=tx.required(()->j.queryForList("SELECT details_bytes FROM ddip_management.audit_events WHERE attempt_id=? AND control_id=? AND action='RECOVERY_HANDOFF'",UUID.fromString(plan.get("recoveryAttemptId").stringValue()),UUID.fromString(plan.get("recoveryAttemptId").stringValue())));
            need(anchors.size()==1,EVIDENCE_CHANGED);var anchor=SourceJson.parse((byte[])anchors.getFirst().get("details_bytes"));
            var packet=RecoveryPacket.parse(RecoveryFiles.read(g,anchor.get("qualificationAddress").stringValue()),anchor.get("qualificationAddress").stringValue(),anchor.get("qualificationSha256").stringValue(),g);
            var identities=new ArrayList<RecoveryQuiescence.Identity>();var worker=packet.rows.get(4);
            identities.add(new RecoveryQuiescence.Identity(((Number)worker.get("pid")).longValue(),((Timestamp)worker.get("process_started_at")).toInstant()));
            for(var s:packet.stages)if(s.get("child_pid")!=null)identities.add(new RecoveryQuiescence.Identity(((Number)s.get("child_pid")).longValue(),((Timestamp)s.get("child_started_at")).toInstant()));
            var recoveryStages=tx.required(()->j.queryForList("SELECT child_pid,child_started_at FROM ddip_management.stage_invocations WHERE attempt_id=?",UUID.fromString(plan.get("recoveryAttemptId").stringValue())));
            for(var s:recoveryStages)if(s.get("child_pid")!=null)identities.add(new RecoveryQuiescence.Identity(((Number)s.get("child_pid")).longValue(),((Timestamp)s.get("child_started_at")).toInstant()));
            var verifierFiles=ContentRecoveryFiles.capture(g,run,UUID.fromString(plan.get("resumeControlId").stringValue()),List.of(),runtime.config.get("contentRuntimeVersion").intValue()==2);
            for(var f:verifierFiles.inventory().get("files"))if(!f.get("directory").booleanValue() && f.get("address").stringValue().endsWith("/child.json")){
                var child=PreflightFiles.canonical(RecoveryFiles.readPrivate(g,f.get("address").stringValue(),65536),65536);
                identities.add(new RecoveryQuiescence.Identity(Long.parseLong(child.get("pid").stringValue()),java.time.Instant.parse(child.get("startedAt").stringValue())));
            }
            identities.addAll(packet.review.descendants());
            if(review!=null){
                need(review.descendants().containsAll(packet.review.descendants()),QUIESCENCE_REQUIRED);
                RecoveryQuiescence.check(new RecoveryQuiescence.Review(review.targetSha256(),review.inspectedAt(),review.handlersStopped(),review.unmanagedWritersStopped(),review.orphanReviewComplete(),Set.of(),review.descendants()),review.targetSha256(),Set.of(),identities,processes,clock.instant());
            }
            var n=SourceJson.object();n.set("plan",plan);n.set("proof",proof);
            var ps=n.putArray("recordedProcesses");identities.stream().distinct().forEach(p->ps.add(RecoveryOperatorInspection.process(p)));
            n.set("previouslyReviewedDescendants",packet.review.json().get("descendants"));
            // Full rows, not successful-row imitations. The caller selects only the authorized prefix.
            n.set("stages",RecoveryHandoff.json(tx.required(()->j.queryForList("SELECT * FROM ddip_management.stage_invocations WHERE attempt_id=? ORDER BY stage COLLATE \"C\",item_ordinal",UUID.fromString(plan.get("originalAttemptId").stringValue())))));
            n.set("verification",PreflightFiles.canonical(RecoveryFiles.readPrivate(g,plan.get("verificationAddress").stringValue(),PreflightFiles.LARGE),PreflightFiles.LARGE));
            n.put("epoch",packet.descriptor.get("oldEpoch").stringValue());return n;
        }catch(RecoveryFailure ex){throw ex;}
        catch(SourceFailure ex){throw new RecoveryFailure(ex.code==SourceFailure.Code.RESUME_EVIDENCE_UNAVAILABLE?EVIDENCE_UNAVAILABLE:EVIDENCE_CHANGED);}
        catch(PreflightFailure ex){throw new RecoveryFailure(ex.code()==PreflightFailure.Code.INPUT_UNAVAILABLE?EVIDENCE_UNAVAILABLE:EVIDENCE_CHANGED);}
        catch(ManagementFailure ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}
        catch(RuntimeException ex){throw new RecoveryFailure(EVIDENCE_CHANGED);}
    }
    static boolean inherited(JsonNode plan,String stage){return !plan.get("boundary").stringValue().equals("BEFORE_FETCH") && (Set.of("FETCH_SOURCE","STORE_BRONZE","SILVER","PREPARE_GOLD","PUBLISH_GOLD").contains(stage) || plan.get("boundary").stringValue().equals("CONTENT_INPUTS") && stage.equals("CHECK_INPUTS"));}
}
