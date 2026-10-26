package platform.contentapi.execution;

import java.time.Duration;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Internal D1 checkpoint, deliberately absent from worker capability/dispatch and HTTP APIs. */
final class RssFetchExecution {
    enum Boundary { INTENT, BEFORE_AUTHORIZATION, AFTER_CHILD, BEFORE_COMPLETION, COMPLETED, BEFORE_CHECKPOINT, ACKNOWLEDGED }
    @FunctionalInterface interface Fault {void at(Boundary point,WorkerRepository.FetchStage stage);}
    record Journaled(WorkerRepository.FetchStage stage,RssJournalFiles.Evidence evidence) { }
    static final class FetchFailure extends RuntimeException {
        FetchFailure(String code){super(code,null,false,false);}
    }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;
    private final ReadOnlyVerifierProcess process;private final Fault fault;
    RssFetchExecution(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){this(jdbc,tx,workers,new ReadOnlyVerifierProcess(),(p,s)->{});}
    RssFetchExecution(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,ReadOnlyVerifierProcess process,Fault fault){this.jdbc=jdbc;this.tx=tx;this.workers=workers;this.process=process;this.fault=fault;}
    private static final class Current {WorkerRepository.FetchStage stage;}
    Journaled fetch(InstallationHostGuard guard,WorkerRepository.Owner owner,int ordinal,String runtimeId) {
        var frozen=workers.frozenContent(guard,owner);need(ordinal>=0 && ordinal<frozen.get("sources").size(),INPUT_INVALID);
        var selected=frozen.get("sources").get(ordinal);var runtime=RssFetchRuntime.load(jdbc,tx,guard,owner,runtimeId,frozen);runtime.endpoint(selected);
        UUID checkpoint=UUID.randomUUID();
        var c=SourceJson.object().put("protocolVersion",1).put("runId",owner.runId().toString()).put("attemptId",owner.attemptId().toString())
                .put("ownerEpoch",owner.worker().epoch()).put("stage","FETCH_SOURCE").put("itemOrdinal",ordinal).put("checkpointId",checkpoint.toString())
                .put("sourceId",selected.get("sourceId").stringValue()).put("revision",selected.get("revision").intValue())
                .put("capturedConfigSha256",selected.get("configurationSha256").stringValue()).put("endpointSha256",selected.get("endpointSha256").stringValue())
                .put("feedUri",selected.get("endpoint").get("uri").stringValue()).put("runtimeSha256",runtime.digest())
                .put("javaRuntime",runtime.javaRuntime()).put("testEndpoint",runtime.testEndpoint()).put("httpTimeoutMillis",runtime.timeoutMillis());
        byte[] command=SourceJson.canonical(c);need(command.length<=65536,RESOURCE_LIMIT);
        var current=new Current();current.stage=workers.beginFetch(guard,owner,ordinal,checkpoint,selected.get("sourceId").stringValue(),selected.get("configurationSha256").stringValue(),SourceJson.sha(command));
        // A fault/unknown acknowledgment after this commit may NEVER cause a second fetch.
        fault.at(Boundary.INTENT,current.stage);
        try {
            var files=new RssJournalFiles(guard,current.stage,true);files.write("command.json",command);
            var result=process.run(runtime.command(files.directory.resolve("command.json")),runtime.environment(),files.directory,Duration.ofSeconds(45),new ReadOnlyVerifierProcess.Observer(){
                public void beforeStart(){fault.at(Boundary.BEFORE_AUTHORIZATION,current.stage);runtime.check();runtime.endpoint(selected);need(Arrays.equals(command,files.read("command.json")),INPUT_CHANGED);current.stage=workers.authorizeFetch(guard,current.stage);}
                public void started(InstallationHostGuard.ProcessIdentity child){current.stage=workers.recordFetchChild(guard,current.stage,child);}
                public void heartbeat(){runtime.check();current.stage=workers.heartbeatFetch(guard,current.stage);}
            });
            fault.at(Boundary.AFTER_CHILD,current.stage);workers.checkFetch(guard,current.stage);runtime.check();
            if(result.exitCode()==1 && result.descendants().isEmpty())throw new FetchFailure(files.failedRetrieval(command));
            need(result.exitCode()==0 && result.descendants().isEmpty(),RESULT_INVALID);files.validateCandidate(command);
            fault.at(Boundary.BEFORE_COMPLETION,current.stage);workers.checkFetch(guard,current.stage);
            var evidence=files.complete(command);fault.at(Boundary.COMPLETED,current.stage);
            workers.checkFetch(guard,current.stage);runtime.check();
            evidence=files.reopen(command,evidence.completionSha());fault.at(Boundary.BEFORE_CHECKPOINT,current.stage);
            var finalRead=files.reopen(command,evidence.completionSha());
            need(Arrays.equals(finalRead.checkpointReference(),evidence.checkpointReference()),INPUT_CHANGED);
            workers.checkpointFetch(guard,current.stage,evidence);fault.at(Boundary.ACKNOWLEDGED,current.stage);
            return new Journaled(current.stage,evidence);
        }catch(ManagementFailure | WorkerFailure ex){throw ex;} // Never refresh/retry after an uncertain DB outcome.
        catch(FetchFailure ex){workers.blockFetch(guard,current.stage,ex.getMessage());throw ex;}
        catch(PreflightFailure ex){workers.blockFetch(guard,current.stage);throw ex;}
        // Uncaught injected crash/Error preserves exactly the last durable boundary and all files.
    }
    /** Explicit read-only acknowledgment inspection. A complete orphan remains unadopted. */
    boolean checkpointPresent(InstallationHostGuard guard,Journaled observed) {
        var files=new RssJournalFiles(guard,observed.stage(),false);
        var exact=files.reopen(files.read("command.json"),observed.evidence().completionSha());
        need(Arrays.equals(exact.descriptorBytes(),observed.evidence().descriptorBytes()),INPUT_CHANGED);
        need(Arrays.equals(exact.checkpointReference(),observed.evidence().checkpointReference()),INPUT_CHANGED);
        return workers.fetchCheckpointPresent(observed.stage(),exact);
    }
    /** Database-backed read-only inspection after lost acknowledgment or process restart. No Owner
     * is reconstructed, no orphan is adopted, and the authorization-stopped channel remains stopped. */
    boolean committedJournalIntact(InstallationHostGuard guard,UUID run,String source) {
        guard.checkHeld(guard.binding().installationId());
        var rows=tx.required(()->jdbc.queryForList("SELECT d.journal_address,d.journal_descriptor,d.journal_sha256,d.expected_evidence,d.expected_sha256,d.owning_attempt_id,s.checkpoint_id,s.command_sha256,s.result_sha256 FROM ddip_management.rss_deliveries d JOIN ddip_management.stage_invocations s ON s.attempt_id=d.owning_attempt_id AND s.stage='FETCH_SOURCE' AND s.intended_identity=d.source_id AND s.intended_namespace=d.journal_address JOIN ddip_management.attempts a ON a.attempt_id=d.owning_attempt_id AND a.run_id=d.run_id JOIN ddip_management.worker_instances w ON w.instance_id=a.instance_id WHERE d.run_id=? AND d.source_id=? AND d.state='FETCH_JOURNALED' AND s.state='VERIFIED' AND w.installation_id=? AND w.host_binding_sha256=?",run,source,guard.binding().installationId(),guard.binding().digest()));
        if(rows.isEmpty())return false;need(rows.size()==1,INPUT_INVALID);var row=rows.getFirst();
        byte[] descriptor=(byte[])row.get("journal_descriptor"),reference=(byte[])row.get("expected_evidence");
        need(SourceJson.sha(descriptor).equals(row.get("journal_sha256")) && SourceJson.sha(reference).equals(row.get("expected_sha256")),INPUT_CHANGED);
        var files=new RssJournalFiles(guard,run,(UUID)row.get("owning_attempt_id"),(UUID)row.get("checkpoint_id"),source,(String)row.get("journal_address"),false);
        byte[] command=files.read("command.json");need(SourceJson.sha(command).equals(row.get("command_sha256")),INPUT_CHANGED);
        var reopened=files.reopen(command,(String)row.get("result_sha256"));
        need(Arrays.equals(reopened.descriptorBytes(),descriptor) && Arrays.equals(reopened.checkpointReference(),reference),INPUT_CHANGED);return true;
    }
}
