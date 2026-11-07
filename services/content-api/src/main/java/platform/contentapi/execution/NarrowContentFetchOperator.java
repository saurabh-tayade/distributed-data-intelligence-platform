package platform.contentapi.execution;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.importing.ImportBindings;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** Trusted host-local terminal-only CLI. No owner serialization, bootstrap, dispatch or restart authority. */
final class NarrowContentFetchOperator {
    private static final int MAXIMUM=16*1024;
    private final JdbcTemplate jdbc;
    private final ManagementTransactions tx;
    private final UUID installation;
    private final Environment env;
    NarrowContentFetchOperator(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Environment env){
        this.jdbc=jdbc;this.tx=tx;this.installation=installation;this.env=env;
    }
    int run(String action)throws Exception {
        var handoffs=new NarrowContentFetchRecoveryHandoff(jdbc,tx,installation);
        var terminals=new NarrowContentFetchRecoveryFinalization(jdbc,tx,installation);
        if(action.equals("native-fetch-inspect")){
            UUID handoff=uuid("ddip.recovery.handoff-id"),terminal=uuid("ddip.recovery.finalization-id");
            var h=handoffs.lookup(handoff);var t=terminals.lookup(terminal);
            boolean related=t.status()!=NarrowContentFetchRecoveryFinalization.Status.CONFIRMED_TERMINAL || handoff.equals(t.handoffId());
            emit(SourceJson.object().put("status","NATIVE_FETCH_INSPECTED").put("handoffId",handoff.toString())
                    .put("finalizationId",terminal.toString()).put("handoffStatus",h.status().name())
                    .put("terminalStatus",related?t.status().name():"UNRESOLVED").put("authorization",false));
            return related && t.status()==NarrowContentFetchRecoveryFinalization.Status.CONFIRMED_TERMINAL?0:2;
        }
        need(action.equals("native-fetch-end") && "true".equals(env.getProperty("ddip.recovery.same-host-process-namespace")),INVALID_REQUEST);
        UUID control=uuid("ddip.recovery.control-id");
        String profile=token("ddip.worker.execution-profile-id"),binding=token("ddip.worker.control-binding-id");
        String deployment=env.getProperty("ddip.worker.deployment-sha256","");
        need(deployment.matches("[0-9a-f]{64}") && Runtime.version().feature()==21,INVALID_REQUEST);
        UUID successor=UUID.randomUUID(),handoff=UUID.randomUUID(),terminal=UUID.randomUUID();
        var summary=SourceJson.object().put("controlId",control.toString()).put("handoffId",handoff.toString())
                .put("finalizationId",terminal.toString()).put("runState","FAILED").put("code","NATIVE_FETCH_INTERRUPTED")
                .put("retryEligible",false).put("restartCanAuthorize",false);
        // Printed before any handoff; these are inspection addresses, never bearer capabilities.
        emit(((ObjectNode)summary.deepCopy()).put("status","NATIVE_FETCH_PLAN")
                .put("warning","After handoff, timeout, refusal or process death can leave the slot occupied. Restart cannot finish it."));
        try(var guard=InstallationHostGuard.acquire(new ImportBindings(jdbc,tx,installation).control(binding,profile))){
            // A restarted command must not register a replacement for a transferred or terminal owner.
            need(tx.required(()->jdbc.queryForObject("SELECT count(*) FROM ddip_management.controls k WHERE k.control_id=? AND k.type='RECONCILE' AND k.state='REQUESTED' AND NOT EXISTS(SELECT 1 FROM ddip_management.content_fetch_recovery_handoffs h WHERE h.control_id=k.control_id)",Long.class,control))==1,OWNERSHIP_LOST);
            // Avoid a known incompatible terminal disposition before transferring ownership.
            need(!tx.required(()->jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.controls other JOIN ddip_management.controls chosen ON chosen.run_id=other.run_id WHERE chosen.control_id=? AND other.control_id<>chosen.control_id AND (other.state='REQUESTED' OR (other.type='CANCEL' AND other.state='APPLIED')))",Boolean.class,control)),EVIDENCE_UNAVAILABLE);
            String digest=tx.required(()->jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,profile));
            var worker=new WorkerRepository(jdbc,tx,installation).register(guard,profile,digest,deployment);
            var target=handoffs.inspect(guard,worker,control,successor,handoff);
            summary.put("runId",target.runId().toString()).put("originalAttemptId",target.originalAttemptId().toString()).put("recoveryAttemptId",successor.toString());
            var review=confirm("NATIVE_FETCH_HANDOFF",summary,target.digest(),target.checkpoint(),target.processes(),List.of());
            var qualified=handoffs.qualify(guard,target,review);
            NarrowContentFetchRecoveryHandoff.Result result;
            try{result=handoffs.transfer(guard,qualified);}
            catch(RuntimeException ex){
                emit(((ObjectNode)summary.deepCopy()).put("status","NATIVE_FETCH_OUTCOME").put("handoffStatus",handoffs.resolve(qualified).status().name()).put("authorization",false));
                throw ex; // A committed receipt cannot replace the lost first owner return.
            }
            need(result.receipt().status()==NarrowContentFetchRecoveryHandoff.Status.COMMITTED && result.owner()!=null,HANDOFF_UNRESOLVED);
            // Only this local variable retains the first returned opaque authority, under this same live guard.
            var owner=result.owner();
            var finalTarget=terminals.inspect(guard,owner,terminal);
            var finalReview=confirm("NATIVE_FETCH_TERMINAL",summary,finalTarget.digest(),finalTarget.checkpoint(),finalTarget.processes(),review.quiescence().descendants());
            var finalQualified=terminals.qualify(guard,finalTarget,finalReview);
            NarrowContentFetchRecoveryFinalization.Receipt receipt;
            try{receipt=terminals.finish(guard,finalQualified);}
            catch(RuntimeException ex){
                receipt=terminals.resolve(finalQualified);
                if(receipt.status()!=NarrowContentFetchRecoveryFinalization.Status.CONFIRMED_TERMINAL){
                    emit(((ObjectNode)summary.deepCopy()).put("status","NATIVE_FETCH_OUTCOME").put("terminalStatus",receipt.status().name()).put("authorization",false));
                    throw ex;
                }
            }
            need(receipt.status()==NarrowContentFetchRecoveryFinalization.Status.COMMITTED || receipt.status()==NarrowContentFetchRecoveryFinalization.Status.CONFIRMED_TERMINAL,HANDOFF_UNRESOLVED);
            emit(((ObjectNode)summary.deepCopy()).put("status","COMPLETED").put("runId",receipt.runId().toString()));
            return 0;
        }
    }
    private static NarrowContentFetchRecoveryHandoff.Review confirm(String phase,JsonNode summary,String digest,UUID checkpoint,
            List<RecoveryQuiescence.Identity> processes,List<RecoveryQuiescence.Identity> previous)throws Exception {
        Instant at=Instant.now();String nonce=UUID.randomUUID().toString();
        var prompt=SourceJson.object().put("status","CONFIRMATION_REQUIRED").put("phase",phase).put("challenge",nonce)
                .put("inspectedAt",at.toString()).put("targetSha256",digest).put("checkpointId",checkpoint.toString());
        prompt.set("summary",summary);
        var recorded=prompt.putArray("recordedProcesses");
        for(var process:processes){var p=RecoveryOperatorInspection.process(process);need(Set.of("ABSENT","REUSED").contains(p.get("state").stringValue()),PROCESS_UNRESOLVED);recorded.add(p);}
        var prior=prompt.putArray("previouslyReviewedDescendants");
        for(var p:previous)prior.addObject().put("pid",Long.toString(p.pid())).put("startedAt",p.startedAt().toString());
        String hash=SourceJson.sha(SourceJson.canonical(prompt));prompt.put("inspectionSha256",hash);emit(prompt);
        JsonNode answer=SourceJson.parse(readConfirmation(),MAXIMUM);
        SourceJson.fields(answer,"approval","challenge","inspectionSha256","targetSha256","checkpointId","recordedProcesses",
                "handlersStopped","unmanagedWritersStopped","orphanReviewComplete","descendantsReviewed","launchGapsReviewed",
                "sameHostProcessNamespace","reviewMethod","descendants");
        need(text(answer,"approval").equals("AUTHORIZE_"+phase) && text(answer,"challenge").equals(nonce)
                && text(answer,"inspectionSha256").equals(hash) && text(answer,"targetSha256").equals(digest)
                && text(answer,"checkpointId").equals(checkpoint.toString()) && answer.get("recordedProcesses").equals(recorded),QUIESCENCE_REQUIRED);
        for(String flag:List.of("handlersStopped","unmanagedWritersStopped","orphanReviewComplete","descendantsReviewed","launchGapsReviewed","sameHostProcessNamespace"))
            need(answer.get(flag).isBoolean() && answer.get(flag).booleanValue(),QUIESCENCE_REQUIRED);
        need(text(answer,"reviewMethod").equals("LOCAL_PROCESS_TREE_AND_WRITERS_REVIEWED"),QUIESCENCE_REQUIRED);
        need(answer.get("descendants").isArray() && answer.get("descendants").size()<=32,INVALID_REQUEST);
        var descendants=new ArrayList<RecoveryQuiescence.Identity>();
        for(var p:answer.get("descendants")){
            SourceJson.fields(p,"pid","startedAt");String pid=text(p,"pid");need(pid.matches("[1-9][0-9]{0,18}"),INVALID_REQUEST);
            descendants.add(new RecoveryQuiescence.Identity(Long.parseLong(pid),Instant.parse(text(p,"startedAt"))));
        }
        need(descendants.containsAll(previous),QUIESCENCE_REQUIRED);RecoveryQuiescence.fresh(at,Instant.now());
        return new NarrowContentFetchRecoveryHandoff.Review(new RecoveryQuiescence.Review(digest,at,true,true,true,Set.of(checkpoint),descendants),true,true);
    }
    /** No file/config approval path or automatic affirmative response; one bounded human response per challenge. */
    private static byte[] readConfirmation()throws Exception {
        var task=new FutureTask<byte[]>(()->{var bytes=new ByteArrayOutputStream();for(int i=0;i<=MAXIMUM;i++){
            int b=System.in.read();need(b!=-1,QUIESCENCE_REQUIRED);if(b=='\n')return bytes.toByteArray();bytes.write(b);
        }throw new RecoveryFailure(INVALID_REQUEST);});
        Thread.ofPlatform().daemon().name("native-fetch-review").start(task);
        try{return task.get(60,TimeUnit.SECONDS);}catch(TimeoutException ex){throw new RecoveryFailure(FRESH_QUALIFICATION_REQUIRED);}
        catch(ExecutionException ex){if(ex.getCause() instanceof RecoveryFailure failure)throw failure;throw new RecoveryFailure(INVALID_REQUEST);}
    }
    private UUID uuid(String key){String raw=env.getProperty(key,"");try{UUID id=UUID.fromString(raw);need(id.toString().equals(raw),INVALID_REQUEST);return id;}catch(IllegalArgumentException ex){throw new RecoveryFailure(INVALID_REQUEST);}}
    private String token(String key){String value=env.getProperty(key,"");need(value.matches("[A-Za-z0-9_-]{1,128}"),INVALID_REQUEST);return value;}
    private static String text(JsonNode n,String key){return SourceJson.text(n.get(key));}
    private static void emit(JsonNode n){byte[] bytes=SourceJson.canonical(n);need(bytes.length<=MAXIMUM,CAPACITY_EXCEEDED);System.out.println(new String(bytes,StandardCharsets.UTF_8));System.out.flush();}
}
