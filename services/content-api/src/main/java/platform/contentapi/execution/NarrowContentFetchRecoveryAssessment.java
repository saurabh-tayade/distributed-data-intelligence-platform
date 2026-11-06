package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.management.SourceJson;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;

/** Inactive read-only assessment. Historical evidence and current observations never convey authority. */
final class NarrowContentFetchRecoveryAssessment {
    enum Prefix { ENROLLED, PREPARING, READY, RESERVED, LAUNCH_BOUND, FILES_READY, START_AUTHORIZED,
        CHILD_RECORDED, RELEASE_AUTHORIZED, CANDIDATE_EXITED, COMPLETION_INTENT, CHECKPOINT }
    enum Integrity { NOT_INSPECTED_NO_CHECKPOINT, NOT_REQUESTED, VERIFIED_AT_INSPECTION, UNAVAILABLE_OR_CHANGED }
    enum Role { WORKER, CHILD }
    enum ReviewTarget { STOP_EXECUTION_HANDLERS, REVIEW_ALL_LAUNCH_GAPS_AND_ORPHANS, REVIEW_UNMANAGED_WRITERS,
        REVIEW_DESCENDANTS, UNRECORDED_CHILD_LAUNCH_GAP, RELEASE_OR_RESULT_GAP, INCOMPLETE_COMPLETION,
        SEPARATE_RECOVERY_AUTHORITY_REQUIRED }
    enum Boundary { BEFORE_REOPEN, JOURNAL_REOPENED }
    record DurableFacts(UUID run,UUID attempt,UUID operation,UUID checkpoint,Prefix prefix,List<String> admissionPhases,
                        List<String> launchPhases,String admissionSha256,String commandSha256,String candidateSha256,String completionSha256) {
        DurableFacts {admissionPhases=List.copyOf(admissionPhases);launchPhases=List.copyOf(launchPhases);}
        @Override public String toString(){return "NativeFetchDurableFacts["+prefix+"]";}
    }
    record ProcessFact(Role role,long pid,Instant startedAt,RecoveryQuiescence.State state) { }
    record Assessment(DurableFacts durable,Integrity integrity,List<ProcessFact> recordedProcesses,List<ReviewTarget> reviewTargets) {
        Assessment {recordedProcesses=List.copyOf(recordedProcesses);reviewTargets=List.copyOf(reviewTargets);}
        boolean authorization(){return false;}
        boolean quiescenceEstablished(){return false;}
        boolean nativeLaunchCompatible(){return false;}
        @Override public String toString(){return "NativeFetchRecoveryAssessment["+durable.prefix()+"; "+integrity+"; no authority]";}
    }
    private record Recorded(Role role,RecoveryQuiescence.Identity identity) { }
    /** Private, bounded SQL material. It never escapes as a packet, receipt or continuation. */
    private record Snapshot(DurableFacts facts,NarrowContentEvidenceContext.Expected ready,List<Recorded> processes,
                            byte[] command,byte[] marker,byte[] descriptor,byte[] reference) { }
    private record Stamp(InstallationHostGuard.Identity identity,String modified,long size) { }

    static Assessment inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt){
        return inspect(jdbc,tx,guard,run,attempt,null,b->{});
    }
    static Assessment inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt,
                              RecoveryQuiescence.Processes processes,Consumer<Boundary> fault){
        require(jdbc!=null && tx!=null && run!=null && attempt!=null && fault!=null
                && !TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        Snapshot snapshot=tx.required(()->snapshot(jdbc,tx,run,attempt));
        // No SQL transaction spans process or filesystem IO. No worker is registered or reconstructed.
        // Default OS observations are meaningful only on the independently recorded host.
        // The guard is locally acquired and unforgeable; it grants no execution authority here.
        RecoveryQuiescence.Processes observer=processes!=null?processes:
                guard!=null && snapshot.ready()!=null && guard.binding().equals(snapshot.ready().host())
                ?RecoveryQuiescence.LOCAL:id->RecoveryQuiescence.State.UNAVAILABLE;
        var observed=new ArrayList<ProcessFact>();
        for(var p:snapshot.processes()){
            RecoveryQuiescence.State state;
            try{state=observer.inspect(p.identity());if(state==null)state=RecoveryQuiescence.State.UNAVAILABLE;}
            catch(RuntimeException ex){state=RecoveryQuiescence.State.UNAVAILABLE;}
            observed.add(new ProcessFact(p.role(),p.identity().pid(),p.identity().startedAt(),state));
        }
        var review=new ArrayList<>(List.of(ReviewTarget.STOP_EXECUTION_HANDLERS,ReviewTarget.REVIEW_ALL_LAUNCH_GAPS_AND_ORPHANS,
                ReviewTarget.REVIEW_UNMANAGED_WRITERS,ReviewTarget.REVIEW_DESCENDANTS,ReviewTarget.SEPARATE_RECOVERY_AUTHORITY_REQUIRED));
        var phases=snapshot.facts().launchPhases();
        if(phases.contains("START_AUTHORIZED") && !phases.contains("CHILD_RECORDED"))review.add(ReviewTarget.UNRECORDED_CHILD_LAUNCH_GAP);
        if(phases.contains("RELEASE_AUTHORIZED") && !phases.contains("CANDIDATE_EXITED"))review.add(ReviewTarget.RELEASE_OR_RESULT_GAP);
        if(snapshot.facts().prefix()==Prefix.COMPLETION_INTENT)review.add(ReviewTarget.INCOMPLETE_COMPLETION);
        Integrity integrity=Integrity.NOT_INSPECTED_NO_CHECKPOINT;
        if(snapshot.facts().prefix()==Prefix.CHECKPOINT){
            integrity=Integrity.NOT_REQUESTED;
            if(guard!=null){
                try{reopen(snapshot,guard,fault);integrity=Integrity.VERIFIED_AT_INSPECTION;}
                catch(RuntimeException ex){integrity=Integrity.UNAVAILABLE_OR_CHANGED;}
            }
        }
        return new Assessment(snapshot.facts(),integrity,observed,review);
    }
    private static Snapshot snapshot(JdbcTemplate jdbc,ManagementTransactions tx,UUID run,UUID attempt){
        var admission=NarrowContentAdmission.inspectHistorical(jdbc,run,attempt);
        // Probe or mixed legacy-stage history is not an empty or adoptable native FETCH prefix.
        require(!jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.content_operations WHERE attempt_id=?) OR EXISTS(SELECT 1 FROM ddip_management.stage_invocations WHERE attempt_id=?)",Boolean.class,attempt,attempt),WORKER_NOT_READY);
        var worker=one(jdbc.queryForList("SELECT w.pid,w.process_started_at FROM ddip_management.attempts a JOIN ddip_management.worker_instances w USING(instance_id) WHERE a.attempt_id=? AND a.run_id=?",attempt,run));
        var processes=new ArrayList<Recorded>();
        processes.add(new Recorded(Role.WORKER,new RecoveryQuiescence.Identity(((Number)worker.get("pid")).longValue(),((java.sql.Timestamp)worker.get("process_started_at")).toInstant())));
        Prefix prefix=admission.phases().isEmpty()?Prefix.ENROLLED:Prefix.valueOf(admission.phases().getLast());
        var operations=jdbc.queryForList("SELECT operation_id FROM ddip_management.content_native_operations WHERE attempt_id=? LIMIT 2",UUID.class,attempt);
        require(operations.size()<=1,RUNTIME_MISMATCH);
        UUID operation=null,checkpoint=null;String commandSha=null,candidate=null,completion=null;List<String> phases=List.of();
        byte[] command=null,marker=null,descriptor=null,reference=null;
        if(!operations.isEmpty()){
            require(admission.ready()!=null,RUNTIME_MISMATCH);operation=operations.getFirst();
            var reservation=NarrowContentFetchReservation.inspect(jdbc,tx,operation);
            var launch=NarrowContentFetchLaunch.inspect(jdbc,tx,operation);
            require(reservation.attempt().equals(attempt) && launch.attempt().equals(attempt),RUNTIME_MISMATCH);
            checkpoint=reservation.checkpoint();commandSha=reservation.commandSha256();phases=launch.phases();candidate=launch.candidateSha256();
            prefix=phases.isEmpty()?Prefix.RESERVED:Prefix.valueOf(phases.getLast());
            if(launch.child()!=null)processes.add(new Recorded(Role.CHILD,new RecoveryQuiescence.Identity(launch.child().pid(),launch.child().startedAt())));
            var intents=jdbc.queryForList("SELECT completion_bytes,completion_sha256 FROM ddip_management.content_fetch_completion_intents WHERE operation_id=?",operation);
            // The existing SQL-only reader verifies exact V12-V14 correspondence, including intent without receipt.
            var receipt=NarrowContentFetchCompletion.inspect(jdbc,tx,operation);
            if(!intents.isEmpty()){
                var intent=one(intents);require(phases.equals(NarrowContentFetchLaunch.PHASES),RUNTIME_MISMATCH);
                prefix=Prefix.COMPLETION_INTENT;marker=(byte[])intent.get("completion_bytes");completion=(String)intent.get("completion_sha256");
            }
            if(receipt.isPresent()){
                require(marker!=null && receipt.get().run().equals(run) && receipt.get().attempt().equals(attempt)
                        && receipt.get().completionSha256().equals(completion),RUNTIME_MISMATCH);
                prefix=Prefix.CHECKPOINT;
                var payload=NarrowContentFetchReservation.reconstruct(jdbc,tx,admission.ready(),operation);command=payload.commandBytes();
                var row=one(jdbc.queryForList("SELECT journal_descriptor,expected_evidence FROM ddip_management.content_fetch_checkpoints WHERE operation_id=?",operation));
                descriptor=(byte[])row.get("journal_descriptor");reference=(byte[])row.get("expected_evidence");
            }
        }
        return new Snapshot(new DurableFacts(run,attempt,operation,checkpoint,prefix,admission.phases(),phases,
                admission.admissionSha256(),commandSha,candidate,completion),admission.ready(),List.copyOf(processes),command,marker,descriptor,reference);
    }
    private static void reopen(Snapshot s,InstallationHostGuard guard,Consumer<Boundary> fault){
        var ready=s.ready();var context=new NarrowContentEvidenceContext(ready);
        // Validate independently recorded host and ledger identities before constructing a file reader.
        context.check(guard,ready.frozen());
        Path directory=ContentOperationPlan.original(context.root(),ready.run(),ready.attempt(),ready.source()).directory("FETCH_SOURCE",0);
        ContentEvidenceBudget.inspectFetchRecovery(context,guard,directory);
        var before=stamps(context.root(),directory);
        var journal=new RssJournalFiles(guard,ready.run(),ready.attempt(),s.facts().checkpoint(),ready.source(),
                guard.binding().root().relativize(directory).toString(),false);
        fault.accept(Boundary.BEFORE_REOPEN);
        require(Arrays.equals(s.marker(),journal.read("complete.json")),RUNTIME_MISMATCH);
        require(NarrowContentFetchCompletion.markerCandidate(s.marker(),s.command()).equals(s.facts().candidateSha256()),RUNTIME_MISMATCH);
        NarrowContentFetchCandidateVerification.metadataSchema(journal.read("metadata.json"));
        PreflightFiles.canonical(journal.read("descriptor.json"),RssJournalFiles.SMALL);
        var evidence=journal.reopen(s.command(),s.facts().completionSha256());
        require(Arrays.equals(s.descriptor(),evidence.descriptorBytes()) && Arrays.equals(s.reference(),evidence.checkpointReference()),RUNTIME_MISMATCH);
        fault.accept(Boundary.JOURNAL_REOPENED);
        ContentEvidenceBudget.inspectFetchRecovery(context,guard,directory);
        require(before.equals(stamps(context.root(),directory)) && Arrays.equals(s.marker(),journal.read("complete.json")),RUNTIME_MISMATCH);
        // Repeat the bounded reopen after the final scope/identity check to avoid trusting a cached success.
        var finalEvidence=journal.reopen(s.command(),s.facts().completionSha256());
        require(Arrays.equals(s.descriptor(),finalEvidence.descriptorBytes()) && Arrays.equals(s.reference(),finalEvidence.checkpointReference())
                && before.equals(stamps(context.root(),directory)),RUNTIME_MISMATCH);
    }
    private static Map<Path,Stamp> stamps(Path root,Path directory){
        var paths=new ArrayList<>(List.of(root,root.resolve(ContentEvidenceBudget.MANIFEST),root.resolve(ContentEvidenceBudget.LOCK),
                directory.getParent().getParent(),directory.getParent(),directory));
        for(String name:RssJournalFiles.CANDIDATE)paths.add(directory.resolve(name));paths.add(directory.resolve("complete.json"));
        var result=new HashMap<Path,Stamp>();
        try{
            for(Path p:paths){var a=Files.readAttributes(p,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);PreflightFiles.privateEntry(p,a.isDirectory());
                result.put(p,new Stamp(InstallationHostGuard.identity(p),a.lastModifiedTime().toString(),a.size()));}
        }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
        return Map.copyOf(result);
    }
    private static Map<String,Object> one(List<Map<String,Object>> rows){require(rows.size()==1,RUNTIME_MISMATCH);return rows.getFirst();}
    private NarrowContentFetchRecoveryAssessment(){}
}
