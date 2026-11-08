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

/** Separate exact-target Content safe-end command. Does not widen native-fetch or ordinary dispatch. */
final class NarrowContentTerminalOperator {
    private static final int MAXIMUM=16384;
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;private final Environment env;
    NarrowContentTerminalOperator(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Environment env){this.jdbc=jdbc;this.tx=tx;this.installation=installation;this.env=env;}
    int run(String action)throws Exception{
        var repository=new NarrowContentTerminal(jdbc,tx,installation);
        if(action.equals("native-content-inspect")){
            UUID id=uuid("ddip.recovery.terminal-id");var r=repository.lookup(id);
            emit(SourceJson.object().put("status","NATIVE_CONTENT_INSPECTED").put("terminalId",id.toString()).put("terminalStatus",r.status().name()).put("authorization",false));
            return r.status()==NarrowContentTerminal.Status.CONFIRMED_TERMINAL?0:2;
        }
        need(action.equals("native-content-end") && "true".equals(env.getProperty("ddip.recovery.same-host-process-namespace")),INVALID_REQUEST);
        UUID control=uuid("ddip.recovery.control-id"),terminal=UUID.randomUUID(),id=UUID.randomUUID();
        String profile=token("ddip.worker.execution-profile-id"),binding=token("ddip.worker.control-binding-id"),deployment=env.getProperty("ddip.worker.deployment-sha256","");
        need(deployment.matches("[0-9a-f]{64}") && Runtime.version().feature()==21,INVALID_REQUEST);
        var chosen=tx.required(()->jdbc.queryForList("SELECT run_id,target_attempt_id FROM ddip_management.controls WHERE control_id=? AND type='RECONCILE' AND state='REQUESTED'",control));
        need(chosen.size()==1 && chosen.getFirst().get("target_attempt_id")!=null,OWNERSHIP_LOST);
        UUID run=(UUID)chosen.getFirst().get("run_id"),original=(UUID)chosen.getFirst().get("target_attempt_id");
        var summary=SourceJson.object().put("terminalId",id.toString()).put("runId",run.toString()).put("originalAttemptId",original.toString()).put("terminalAttemptId",terminal.toString()).put("controlId",control.toString())
            .put("runState","FAILED").put("code",NarrowContentTerminal.CODE).put("retryEligible",false).put("authorization",false);
        emit(((ObjectNode)summary.deepCopy()).put("status","NATIVE_CONTENT_PLAN").put("warning","Retained native effects remain unverified. This ends the original run as failed; it does not resume, repair, clean up, or publish a final result."));
        try(var guard=InstallationHostGuard.acquire(new ImportBindings(jdbc,tx,installation).control(binding,profile))){
            String digest=tx.required(()->jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,profile));
            new WorkerRepository(jdbc,tx,installation).register(guard,profile,digest,deployment);
            var target=repository.inspect(guard,run,original,control,terminal,id);
            var q=repository.qualify(guard,target,confirm(summary,target));
            NarrowContentTerminal.Receipt result;
            try{result=repository.finish(guard,q);}catch(RuntimeException ex){result=repository.resolve(q);}
            boolean done=result.status()==NarrowContentTerminal.Status.COMMITTED || result.status()==NarrowContentTerminal.Status.CONFIRMED_TERMINAL;
            emit(((ObjectNode)summary.deepCopy()).put("status",done?"COMPLETED":"NATIVE_CONTENT_UNRESOLVED").put("terminalStatus",result.status().name()));return done?0:2;
        }
    }
    private static NarrowContentTerminal.Review confirm(JsonNode summary,NarrowContentTerminal.Target target)throws Exception{
        Instant at=Instant.now();String nonce=target.nonce().toString();
        var prompt=SourceJson.object().put("status","CONFIRMATION_REQUIRED").put("phase","NATIVE_CONTENT_TERMINAL").put("challenge",nonce).put("targetSha256",target.digest()).put("inspectedAt",at.toString());prompt.set("summary",summary);
        var checkpoints=prompt.putArray("reviewedCheckpoints");target.checkpoints().stream().map(UUID::toString).sorted().forEach(checkpoints::add);
        var recorded=prompt.putArray("recordedProcesses");for(var p:target.processes()){var observation=RecoveryOperatorInspection.process(p);need(Set.of("ABSENT","REUSED").contains(observation.get("state").stringValue()),PROCESS_UNRESOLVED);recorded.add(observation);}
        String inspection=SourceJson.sha(SourceJson.canonical(prompt));prompt.put("inspectionSha256",inspection);emit(prompt);
        var answer=SourceJson.parse(readConfirmation(),MAXIMUM);
        SourceJson.fields(answer,"approval","challenge","inspectionSha256","targetSha256","reviewedCheckpoints","recordedProcesses","handlersStopped","unmanagedWritersStopped","orphanReviewComplete","sameHostAndProcessNamespace","descendantsReviewed","launchGapsReviewed","sparkGatewaysReviewed","remoteWritersStopped","reviewMethod","descendants");
        need(text(answer,"approval").equals("AUTHORIZE_NATIVE_CONTENT_TERMINAL") && text(answer,"challenge").equals(nonce) && text(answer,"inspectionSha256").equals(inspection) && text(answer,"targetSha256").equals(target.digest()) && answer.get("reviewedCheckpoints").equals(checkpoints) && answer.get("recordedProcesses").equals(recorded),QUIESCENCE_REQUIRED);
        for(String key:List.of("handlersStopped","unmanagedWritersStopped","orphanReviewComplete","sameHostAndProcessNamespace","descendantsReviewed","launchGapsReviewed","sparkGatewaysReviewed","remoteWritersStopped"))need(answer.get(key).isBoolean() && answer.get(key).booleanValue(),QUIESCENCE_REQUIRED);
        need(text(answer,"reviewMethod").equals("LOCAL_PROCESS_TREE_AND_ALL_WRITERS_REVIEWED") && answer.get("descendants").isArray() && answer.get("descendants").size()<=32,QUIESCENCE_REQUIRED);
        var descendants=new ArrayList<RecoveryQuiescence.Identity>();for(var p:answer.get("descendants")){SourceJson.fields(p,"pid","startedAt");String pid=text(p,"pid");need(pid.matches("[1-9][0-9]{0,18}"),INVALID_REQUEST);descendants.add(new RecoveryQuiescence.Identity(Long.parseLong(pid),Instant.parse(text(p,"startedAt"))));}
        RecoveryQuiescence.fresh(at,Instant.now());return new NarrowContentTerminal.Review(new RecoveryQuiescence.Review(target.digest(),at,true,true,true,target.checkpoints(),descendants),target.nonce(),true,true,true,true,true);
    }
    private static byte[] readConfirmation()throws Exception{
        var task=new FutureTask<byte[]>(()->{var out=new ByteArrayOutputStream();for(int i=0;i<=MAXIMUM;i++){int b=System.in.read();need(b!=-1,QUIESCENCE_REQUIRED);if(b=='\n')return out.toByteArray();out.write(b);}throw new RecoveryFailure(INVALID_REQUEST);});
        Thread.ofPlatform().daemon().name("native-content-terminal-review").start(task);
        try{return task.get(60,TimeUnit.SECONDS);}catch(TimeoutException ex){throw new RecoveryFailure(FRESH_QUALIFICATION_REQUIRED);}catch(ExecutionException ex){throw new RecoveryFailure(INVALID_REQUEST);}
    }
    private UUID uuid(String key){String value=env.getProperty(key,"");try{UUID id=UUID.fromString(value);need(id.toString().equals(value),INVALID_REQUEST);return id;}catch(IllegalArgumentException ex){throw new RecoveryFailure(INVALID_REQUEST);}}
    private String token(String key){String value=env.getProperty(key,"");need(value.matches("[A-Za-z0-9_-]{1,128}"),INVALID_REQUEST);return value;}
    private static String text(JsonNode n,String key){return SourceJson.text(n.get(key));}
    private static void emit(JsonNode n){byte[] bytes=SourceJson.canonical(n);need(bytes.length<=MAXIMUM,CAPACITY_EXCEEDED);System.out.println(new String(bytes,StandardCharsets.UTF_8));System.out.flush();}
}
