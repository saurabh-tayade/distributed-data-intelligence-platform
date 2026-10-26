package platform.contentapi.execution;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.boot.*;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.importing.ImportBindings;
import platform.contentapi.management.SourceJson;
import platform.contentapi.management.FrozenRunSelection;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** Trusted local operator only. No web endpoint, worker dispatcher, arbitrary command or resume token. */
@Configuration(proxyBeanMethods=false)
@Conditional(OperatorRecoveryConfiguration.Enabled.class)
public class OperatorRecoveryConfiguration {
    private static final int CONFIRMATION_MAXIMUM=128*1024;
    static final class Enabled implements Condition {
        public boolean matches(ConditionContext c,AnnotatedTypeMetadata m){return RuntimeRole.from(c.getEnvironment())==RuntimeRole.OPERATOR_RECOVERY;}
    }
    @Bean Command operatorRecoveryCommand(JdbcTemplate jdbc,ManagementTransactions tx,DatabaseSettings settings,Environment environment){return new Command(jdbc,tx,settings.installationId(),environment);}
    static final class Command implements ApplicationRunner,ExitCodeGenerator {
        private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;private final Environment env;
        private int exitCode=2;
        Command(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Environment env){this.jdbc=jdbc;this.tx=tx;this.installation=installation;this.env=env;}
        public void run(ApplicationArguments arguments) {
            try {
                UUID control=uuid(env.getProperty("ddip.recovery.control-id",""));
                String action=env.getProperty("ddip.recovery.action","");need(Set.of("inspect","recover").contains(action),INVALID_REQUEST);
                var inspector=new RecoveryOperatorInspection(jdbc,tx,installation);var summary=inspector.inspect(control);
                if(action.equals("inspect")){emit(summary.put("status","INSPECTED"));exitCode=0;return;}
                String profile=token("ddip.worker.execution-profile-id"),binding=token("ddip.worker.control-binding-id"),runtime=token("ddip.worker.runtime-profile-id");
                String deployment=env.getProperty("ddip.worker.deployment-sha256","");need(deployment.matches("[0-9a-f]{64}") && Runtime.version().feature()==21,INVALID_REQUEST);
                try(var guard=InstallationHostGuard.acquire(new ImportBindings(jdbc,tx,installation).control(binding,profile))) {
                    // Completion replay is facts only and registers no new process or execution attempt.
                    var finalizer=new RecoveryFinalization(jdbc,tx,new RecoveryHandoff(jdbc,tx,installation));
                    if(summary.get("controlState").stringValue().equals("APPLIED")){complete(historical(guard,control,finalizer));return;}
                    // An existing successor belongs to its original JVM. Never adopt it, even before launch.
                    need(tx.required(()->jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE attempt_id=?",Long.class,control))==0,FRESH_QUALIFICATION_REQUIRED);
                    need(summary.get("canQualify").booleanValue(),PROCESS_UNRESOLVED);
                    var p=tx.required(()->jdbc.queryForList("SELECT configuration_sha256,configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id=?",profile));need(p.size()==1,RUNTIME_MISMATCH);
                    String digest=(String)p.getFirst().get("configuration_sha256");FrozenRunSelection.profile((byte[])p.getFirst().get("configuration_bytes"),digest,deployment);
                    var registered=new WorkerRepository(jdbc,tx,installation).register(guard,profile,digest,deployment);
                    var handoffs=new RecoveryHandoff(jdbc,tx,installation);var target=handoffs.inspect(guard,registered,control);
                    var evidence=handoffs.reviewEvidence(guard,target,runtime);
                    var handoffSummary=inspector.inspect(control);need(handoffSummary.get("canQualify").booleanValue(),PROCESS_UNRESOLVED);
                    var review=confirm("HANDOFF",handoffSummary,evidence,target,null);
                    need(handoffs.reviewEvidence(guard,target,runtime).equals(evidence),EVIDENCE_CHANGED);
                    var q=handoffs.qualify(guard,target,review,runtime);
                    try {need(handoffs.transferReviewed(guard,q).owner()!=null,HANDOFF_UNRESOLVED);}
                    catch(ManagementFailure ex) {
                        // Only exact committed facts plus this same live guard/process can continue.
                        need(handoffs.resolve(q).status()==RecoveryHandoff.Status.CONFIRMED_HANDOFF,HANDOFF_UNRESOLVED);
                    }
                    var bootstrap=handoffs.bootstrap(guard,q.address(),q.sha256(),runtime);
                    var verifier=new RecoveryVerification(jdbc,tx,handoffs);
                    var observed=verifier.verify(guard,bootstrap,review); // separate exact live authorization
                    var facts=verifier.inspect(guard,control);boolean supported=RecoveryFinalization.supports(facts);
                    emit(SourceJson.object().put("status","OBSERVED").put("controlId",control.toString()).put("classification",observed.classification()).put("canQualifyFinalization",supported));need(supported,EVIDENCE_UNAVAILABLE);
                    var finalEvidence=(ObjectNode)handoffs.reviewEvidence(guard,target,runtime);
                    finalEvidence.put("classification",observed.classification()).put("verificationCheckpoint",observed.stage().checkpoint().toString()).put("ownerInstance",observed.stage().owner().worker().instanceId().toString()).put("ownerEpoch",Long.toString(observed.stage().owner().worker().epoch()));
                    if(observed.stage().child()!=null)finalEvidence.withArray("recordedProcesses").add(RecoveryOperatorInspection.process(new RecoveryQuiescence.Identity(observed.stage().child().pid(),observed.stage().child().startedAt())));
                    finalEvidence.put("verificationEvidenceSha256",SourceJson.sha(SourceJson.canonical(facts.evidence())));
                    var finalReview=confirm("FINALIZATION",inspector.inspect(control),finalEvidence,target,review);
                    finalizer=new RecoveryFinalization(jdbc,tx,handoffs);
                    var qualification=finalizer.qualify(guard,bootstrap,observed,finalReview);
                    RecoveryFinalization.Receipt receipt;
                    try {receipt=finalizer.finish(guard,qualification,finalReview);}
                    catch(ManagementFailure ex){receipt=finalizer.resolve(guard,control,qualification.checkpoint(),q.sha256());}
                    complete(receipt);
                }
            }catch(RecoveryFailure | ManagementFailure | ImportOwnershipFailure | WorkerFailure | PreflightFailure ex){exitCode=2;emit(SourceJson.object().put("status","RECOVERY_REQUIRED").put("code",ex.getMessage()));}
            catch(Exception ex){exitCode=2;emit(SourceJson.object().put("status","RECOVERY_REQUIRED").put("code","OPERATOR_FAILURE"));}
        }
        private RecoveryFinalization.Receipt historical(InstallationHostGuard guard,UUID control,RecoveryFinalization service) {
            var rows=tx.required(()->jdbc.queryForList("SELECT details_bytes FROM ddip_management.audit_events WHERE control_id=? AND attempt_id=? AND action='RECOVERY_FINALIZED' ORDER BY event_seq LIMIT 2",control,control));need(rows.size()==1,EVIDENCE_CHANGED);
            var d=PreflightFiles.canonical((byte[])rows.getFirst().get("details_bytes"),8192);
            return service.resolve(guard,control,uuid(SourceJson.text(d.get("checkpointId"))),SourceJson.text(d.get("qualificationSha256")));
        }
        private void complete(RecoveryFinalization.Receipt receipt) {
            need(receipt.status()==RecoveryFinalization.Status.CONFIRMED,EVIDENCE_UNAVAILABLE);
            emit(SourceJson.object().put("status","COMPLETED").put("runId",receipt.runId().toString()).put("controlId",receipt.controlId().toString()).put("runState",receipt.runState()).put("code",receipt.code()).put("retryEligible",receipt.retryEligible()));exitCode=0;
        }
        private String token(String key){String value=env.getProperty(key,"");need(value.matches("[A-Za-z0-9_-]{1,128}"),INVALID_REQUEST);return value;}
        public int getExitCode(){return exitCode;}
    }

    private static RecoveryQuiescence.Review confirm(String phase,JsonNode summary,JsonNode evidence,RecoveryHandoff.Target target,RecoveryQuiescence.Review previous)throws Exception {
        Instant at=Instant.now();String nonce=UUID.randomUUID().toString();
        var request=SourceJson.object().put("status","CONFIRMATION_REQUIRED").put("phase",phase).put("challenge",nonce).put("inspectedAt",at.toString());request.set("summary",summary);request.set("evidence",evidence);
        String sha=SourceJson.sha(SourceJson.canonical(request));request.put("inspectionSha256",sha);emit(request);
        JsonNode answer=SourceJson.parse(readConfirmation(),CONFIRMATION_MAXIMUM);
        SourceJson.fields(answer,"approval","challenge","inspectionSha256","recordedProcesses","reviewedCheckpoints","lineageCheckpoints","handlersStopped","unmanagedWritersStopped","orphanReviewComplete","reviewMethod","descendants");
        need(SourceJson.text(answer.get("approval")).equals("AUTHORIZE_"+phase) && SourceJson.text(answer.get("challenge")).equals(nonce) && SourceJson.text(answer.get("inspectionSha256")).equals(sha),QUIESCENCE_REQUIRED);
        need(answer.get("recordedProcesses").equals(evidence.get("recordedProcesses")) && answer.get("reviewedCheckpoints").equals(evidence.get("unresolvedCheckpoints")),QUIESCENCE_REQUIRED);
        need(answer.get("lineageCheckpoints").equals(evidence.get("lineageCheckpoints")),QUIESCENCE_REQUIRED);
        need(SourceJson.text(answer.get("reviewMethod")).equals("LOCAL_PROCESS_TREE_AND_WRITERS_REVIEWED"),QUIESCENCE_REQUIRED);
        for(String field:List.of("handlersStopped","unmanagedWritersStopped","orphanReviewComplete"))need(answer.get(field).isBoolean() && answer.get(field).booleanValue(),QUIESCENCE_REQUIRED);
        for(var p:evidence.get("recordedProcesses"))need(Set.of("ABSENT","REUSED").contains(p.get("state").stringValue()),PROCESS_UNRESOLVED);
        need(answer.get("descendants").isArray() && answer.get("descendants").size()<=32,INVALID_REQUEST);
        var descendants=new ArrayList<RecoveryQuiescence.Identity>();for(var p:answer.get("descendants")){SourceJson.fields(p,"pid","startedAt");String pid=SourceJson.text(p.get("pid"));need(pid.matches("[1-9][0-9]{0,18}"),INVALID_REQUEST);descendants.add(new RecoveryQuiescence.Identity(Long.parseLong(pid),Instant.parse(SourceJson.text(p.get("startedAt")))));}
        need(previous==null || descendants.containsAll(previous.descendants()),QUIESCENCE_REQUIRED);
        for(var p:evidence.get("previouslyReviewedDescendants"))need(descendants.contains(new RecoveryQuiescence.Identity(Long.parseLong(SourceJson.text(p.get("pid"))),Instant.parse(SourceJson.text(p.get("startedAt"))))),QUIESCENCE_REQUIRED);
        RecoveryQuiescence.fresh(at,Instant.now());
        return new RecoveryQuiescence.Review(target.digest(),at,true,true,true,target.unresolvedCheckpoints(),descendants);
    }
    /** One bounded UTF-8 JSON line; bounded wait does not refresh inspection or heartbeat authority. */
    private static byte[] readConfirmation()throws Exception {
        var task=new FutureTask<byte[]>(()->{var bytes=new ByteArrayOutputStream();for(int i=0;i<=CONFIRMATION_MAXIMUM;i++){int b=System.in.read();need(b!=-1,QUIESCENCE_REQUIRED);if(b=='\n')return bytes.toByteArray();bytes.write(b);}throw new RecoveryFailure(INVALID_REQUEST);});
        Thread.ofPlatform().daemon().name("recovery-confirmation").start(task);
        try{return task.get(60,TimeUnit.SECONDS);}catch(TimeoutException ex){throw new RecoveryFailure(FRESH_QUALIFICATION_REQUIRED);}
        catch(ExecutionException ex){if(ex.getCause() instanceof RecoveryFailure failure)throw failure;throw new RecoveryFailure(INVALID_REQUEST);}
    }
    private static UUID uuid(String value){try{UUID id=UUID.fromString(value);need(id.toString().equals(value),INVALID_REQUEST);return id;}catch(IllegalArgumentException ex){throw new RecoveryFailure(INVALID_REQUEST);}}
    private static void emit(JsonNode n){byte[] raw=SourceJson.canonical(n);need(raw.length<=1024*1024,CAPACITY_EXCEEDED);System.out.println(new String(raw,StandardCharsets.UTF_8));System.out.flush();}
}
