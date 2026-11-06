package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;

/** Inactive, read-only observation of one exited FETCH candidate. Never a checkpoint or authority. */
final class NarrowContentFetchCandidateVerification {
    enum Boundary { FACTS_READ, INVENTORY_READ, JOURNAL_VALIDATED, RECHECKED }
    record Observation(UUID operation,UUID run,UUID attempt,UUID checkpoint,String candidateSha256,long payloadBytes) {
        boolean rawHttpJournalVerified(){return true;}
        boolean completed(){return false;}
        boolean nativeLaunchCompatible(){return false;}
        @Override public String toString(){return "FetchCandidateObservation[raw HTTP journal only; no continuation]";}
    }
    private record Facts(NarrowContentEvidenceContext.Expected ready,NarrowContentFetchReservation.Payload payload,
                         NarrowContentFetchLaunch.Facts launch) {
        boolean same(Facts other){return ready.equals(other.ready) && launch.equals(other.launch)
                && Arrays.equals(payload.commandBytes(),other.payload.commandBytes())
                && Arrays.equals(payload.bootstrapBytes(),other.payload.bootstrapBytes());}
    }
    private record Stamp(InstallationHostGuard.Identity identity,String modified,long size) { }
    private record Inventory(String digest,Map<Path,Stamp> stamps,long payloadBytes) {
        Inventory {stamps=Map.copyOf(stamps);}
    }
    static Observation inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID operation){
        return inspect(jdbc,tx,guard,operation,b->{});
    }
    static Observation inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID operation,Consumer<Boundary> fault){
        require(jdbc!=null && tx!=null && guard!=null && operation!=null && fault!=null,WORKER_CONFIGURATION_INVALID);
        require(!org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        Facts facts=facts(jdbc,tx,guard,operation);fault.accept(Boundary.FACTS_READ);
        var ready=facts.ready();var context=new NarrowContentEvidenceContext(ready);
        Path directory=ContentOperationPlan.original(context.root(),ready.run(),ready.attempt(),ready.source()).directory("FETCH_SOURCE",0);
        ContentEvidenceBudget.inspectFetch(context,guard,directory,true);
        Inventory before=inventory(context,guard,directory,facts);fault.accept(Boundary.INVENTORY_READ);
        String address=guard.binding().root().relativize(directory).toString();
        // The historical opener creates nothing. Validate native raw HTTP journal semantics,
        // without complete(), checkpoint references, XML parsing, runtime loading or process execution.
        var journal=new RssJournalFiles(guard,ready.run(),ready.attempt(),facts.payload().checkpoint(),ready.source(),address,false);
        metadataSchema(journal.read("metadata.json"));
        PreflightFiles.canonical(journal.read("descriptor.json"),RssJournalFiles.SMALL);
        journal.validateCandidate(facts.payload().commandBytes());fault.accept(Boundary.JOURNAL_VALIDATED);
        ContentEvidenceBudget.inspectFetch(context,guard,directory,true);
        require(before.equals(inventory(context,guard,directory,facts)),RUNTIME_MISMATCH);
        require(facts.same(facts(jdbc,tx,guard,operation)),RUNTIME_MISMATCH);
        // Final hook precedes a final filesystem recheck; no observation escapes a changed tree.
        fault.accept(Boundary.RECHECKED);
        require(facts.same(facts(jdbc,tx,guard,operation)),RUNTIME_MISMATCH);
        ContentEvidenceBudget.inspectFetch(context,guard,directory,true);
        require(before.equals(inventory(context,guard,directory,facts)),RUNTIME_MISMATCH);
        return new Observation(operation,ready.run(),ready.attempt(),facts.payload().checkpoint(),before.digest(),before.payloadBytes());
    }
    /** Native Bronze metadata preserves insertion order; parsing must not require sorted keys.
     * Full field/command/payload correspondence remains the independent journal validator's job. */
    static void metadataSchema(byte[] bytes){
        try{
            var version=SourceJson.parse(bytes,RssJournalFiles.SMALL).get("schemaVersion");
            require(version!=null && version.isIntegralNumber() && version.canConvertToInt() && version.intValue()==1,RUNTIME_MISMATCH);
        }catch(WorkerFailure ex){throw ex;}
        catch(RuntimeException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_INVALID);}
    }
    private static Facts facts(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID operation){
        var rows=tx.required(()->jdbc.queryForList("SELECT a.run_id,o.attempt_id FROM ddip_management.content_native_operations o JOIN ddip_management.attempts a USING(attempt_id) WHERE o.operation_id=?",operation));
        require(rows.size()==1,WORKER_NOT_READY);var row=rows.getFirst();
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,(UUID)row.get("run_id"),(UUID)row.get("attempt_id"));
        var registered=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);
        var payload=NarrowContentFetchReservation.reconstruct(jdbc,tx,ready,operation);
        var launch=NarrowContentFetchLaunch.inspect(jdbc,tx,operation);
        require(launch.phases().equals(NarrowContentFetchLaunch.PHASES) && launch.attempt().equals(ready.attempt())
                && launch.child()!=null && launch.candidateSha256()!=null && launch.candidateSha256().matches("[0-9a-f]{64}"),WORKER_NOT_READY);
        var configurations=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.content_fetch_launch_profiles WHERE launch_profile_id=?",launch.launchProfileId()));
        require(configurations.size()==1,RUNTIME_MISMATCH);var configuration=configurations.getFirst();
        byte[] bytes=(byte[])configuration.get("configuration_bytes");
        require(launch.launchProfileSha256().equals(configuration.get("configuration_sha256"))
                && launch.launchProfileSha256().equals(SourceJson.sha(bytes)),RUNTIME_MISMATCH);
        // Registered correspondence only: no requirement that an old installed runtime remains present.
        StrictFetchLaunchRuntime.validate(PreflightFiles.canonical(bytes,65536),registered,guard.binding());
        return new Facts(ready,payload,launch);
    }
    private static Inventory inventory(NarrowContentEvidenceContext context,InstallationHostGuard guard,Path directory,Facts facts){
        context.check(guard,context.frozen());
        var before=stamps(context.root(),directory);
        String digest=NarrowContentFetchLaunch.candidate(directory,facts.payload().commandBytes());
        require(digest.equals(facts.launch().candidateSha256()) && before.equals(stamps(context.root(),directory)),RUNTIME_MISMATCH);
        context.check(guard,context.frozen());
        return new Inventory(digest,before,before.get(directory.resolve("payload.xml")).size());
    }
    private static Map<Path,Stamp> stamps(Path root,Path directory){
        var paths=new ArrayList<>(List.of(root,root.resolve(ContentEvidenceBudget.MANIFEST),root.resolve(ContentEvidenceBudget.LOCK),
                directory.getParent().getParent(),directory.getParent(),directory));
        for(String name:new TreeSet<>(RssJournalFiles.CANDIDATE))paths.add(directory.resolve(name));
        var result=new HashMap<Path,Stamp>();
        try{
            for(Path path:paths){
                var attrs=Files.readAttributes(path,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
                PreflightFiles.privateEntry(path,attrs.isDirectory());
                result.put(path,new Stamp(InstallationHostGuard.identity(path),attrs.lastModifiedTime().toString(),attrs.size()));
            }
            return Map.copyOf(result);
        }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
    }
    private NarrowContentFetchCandidateVerification(){}
}
