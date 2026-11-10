package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** Fresh read-only proof only. No retry disposition, fake attempt, native writer or namespace repair. */
final class ReliabilityRetryEvidence {
    static final String POLICY="E5_RELIABILITY_RETRY_EVIDENCE_V1";
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final ReadOnlyVerifierProcess process;
    ReliabilityRetryEvidence(JdbcTemplate jdbc,ManagementTransactions tx){this(jdbc,tx,new ReadOnlyVerifierProcess());}
    ReliabilityRetryEvidence(JdbcTemplate jdbc,ManagementTransactions tx,ReadOnlyVerifierProcess process){this.jdbc=jdbc;this.tx=tx;this.process=process;}
    static final class Proof {
        private final UUID installation;private final ReliabilityRuntime runtime;private final JsonNode pack;
        private final String fingerprint;private final RecoveryNativeSeal seal;private final Retained retained;private final Instant verified;
        private Proof(UUID installation,ReliabilityRuntime runtime,JsonNode pack,String fingerprint,RecoveryNativeSeal seal,Retained retained,Instant verified){this.installation=installation;this.runtime=runtime;this.pack=pack.deepCopy();this.fingerprint=fingerprint;this.seal=seal;this.retained=retained;this.verified=verified;}
        String runtimeDigest(){return runtime.digest();} String fingerprint(){return fingerprint;}
        String address(){return retained.address;} String sha256(){return SourceJson.sha(retained.result);}
        String sealDigest(){return seal.digest();}
        void recheck(InstallationHostGuard guard){
            outside(guard,installation);RecoveryQuiescence.fresh(verified,Instant.now());retained.check();runtime.admission();
            need(seal.same(RecoveryNativeSeal.capture(runtime,pack,fingerprint)),EVIDENCE_CHANGED);
            absent(runtime,fingerprint);guard.checkHeld(installation);RecoveryQuiescence.fresh(verified,Instant.now());
        }
        @Override public String toString(){return "ReliabilityRetryProof[redacted]";}
    }
    private static final class Retained {
        final Path directory;final String address;final byte[] command,bundle,result;
        final InstallationHostGuard.Identity directoryId,commandId,bundleId,resultId;
        Retained(InstallationHostGuard guard,Path directory,byte[] command,byte[] bundle,byte[] result){this.directory=directory;this.address=guard.binding().root().relativize(directory).toString();this.command=command.clone();this.bundle=bundle.clone();this.result=result.clone();directoryId=InstallationHostGuard.identity(directory);commandId=InstallationHostGuard.identity(directory.resolve("command.json"));bundleId=InstallationHostGuard.identity(directory.resolve("selection.json"));resultId=InstallationHostGuard.identity(directory.resolve("result.json"));}
        void check(){PreflightFiles.privateEntry(directory,true);need(directoryId.equals(InstallationHostGuard.identity(directory)) && commandId.equals(InstallationHostGuard.identity(directory.resolve("command.json"))) && bundleId.equals(InstallationHostGuard.identity(directory.resolve("selection.json"))) && resultId.equals(InstallationHostGuard.identity(directory.resolve("result.json"))),EVIDENCE_CHANGED);need(Arrays.equals(command,PreflightFiles.read(directory.resolve("command.json"),PreflightFiles.SMALL,true)) && Arrays.equals(bundle,PreflightFiles.read(directory.resolve("selection.json"),PreflightFiles.LARGE,true)) && Arrays.equals(result,PreflightFiles.read(directory.resolve("result.json"),PreflightFiles.LARGE,true)),EVIDENCE_CHANGED);}
    }
    Proof verify(InstallationHostGuard guard,UUID run,UUID control,JsonNode frozen,String runtimeId){
        UUID installation=guard.binding().installationId();outside(guard,installation);
        var rows=tx.required(()->jdbc.queryForList("SELECT a.attempt_id,a.owner_epoch FROM ddip_management.runs r JOIN ddip_management.attempts a ON a.attempt_id=r.current_attempt_id AND a.run_id=r.run_id WHERE r.run_id=? AND r.pipeline='RELIABILITY_V2' AND r.state IN ('FAILED','CANCELLED') AND a.state IN ('FAILED','CANCELLED','SUCCEEDED')",run));
        need(rows.size()==1,INVALID_REQUEST);
        return verify(guard,run,control,(UUID)rows.getFirst().get("attempt_id"),frozen,runtimeId);
    }
    Proof verify(InstallationHostGuard guard,UUID run,UUID control,UUID priorAttempt,JsonNode frozen,String runtimeId){
        UUID installation=guard.binding().installationId();outside(guard,installation);
        var rows=tx.required(()->jdbc.queryForList("SELECT attempt_id,owner_epoch FROM ddip_management.attempts WHERE attempt_id=? AND run_id=? AND kind='RECONCILE' AND state='SUCCEEDED'",priorAttempt,run));
        need(rows.size()==1,INVALID_REQUEST);var prior=rows.getFirst();
        var runtime=ReliabilityRuntime.terminal(jdbc,tx,guard,runtimeId,frozen);runtime.admission();
        var bundle=SourceJson.object();bundle.set("capturedSelection",frozen);bundle.set("bindings",runtime.bindings());byte[] selected=SourceJson.canonical(bundle);
        var command=SourceJson.object().put("protocolVersion",1).put("runId",run.toString()).put("attemptId",prior.get("attempt_id").toString()).put("ownerEpoch",prior.get("owner_epoch").toString()).put("stage","CHECK_INPUTS").put("itemOrdinal",0).put("capturedConfigSha256",SourceJson.sha(SourceJson.canonical(frozen))).put("checkpointId",control.toString()).put("selection","selection.json").put("bundleSha256",SourceJson.sha(selected));command.set("runtime",runtime.json());byte[] raw=SourceJson.canonical(command);
        var first=probe(guard,control,runtime,raw,selected);var response=validate(first,command,frozen);String fingerprint=response.get("evidence").get("nativeDependencyFingerprint").stringValue();
        var pack=SourceJson.object();pack.set("bundle",bundle);var seal=RecoveryNativeSeal.capture(runtime,pack,fingerprint);absent(runtime,fingerprint);
        var second=probe(guard,control,runtime,raw,selected);need(response.equals(validate(second,command,frozen)),EVIDENCE_CHANGED);first.check();second.check();
        need(seal.same(RecoveryNativeSeal.capture(runtime,pack,fingerprint)),EVIDENCE_CHANGED);absent(runtime,fingerprint);
        var proof=new Proof(installation,runtime,pack,fingerprint,seal,second,Instant.now());proof.recheck(guard);return proof;
    }
    private Retained probe(InstallationHostGuard guard,UUID control,ReliabilityRuntime runtime,byte[] raw,byte[] selected){
        Path directory=directory(guard,control);write(directory.resolve("command.json"),raw);write(directory.resolve("selection.json"),selected);var once=new AtomicBoolean();
        byte[] bridge=bridge();String hash=SourceJson.sha(bridge);var child=new InstallationHostGuard.ProcessIdentity[1];
        var result=process.run(List.of(runtime.json().get("configuration").get("python").stringValue(),"-I","-B","-X","utf8","-c",new String(bridge,java.nio.charset.StandardCharsets.UTF_8),directory.resolve("command.json").toString(),hash),runtime.environment(),runtime.scratch(),Duration.ofSeconds(runtime.seconds()),new ReadOnlyVerifierProcess.Observer(){
            private void check(){guard.checkHeld(guard.binding().installationId());runtime.admission();need(Arrays.equals(raw,PreflightFiles.read(directory.resolve("command.json"),PreflightFiles.SMALL,true)) && Arrays.equals(selected,PreflightFiles.read(directory.resolve("selection.json"),PreflightFiles.LARGE,true)),EVIDENCE_CHANGED);}
            public void beforeStart(){need(once.compareAndSet(false,true),INVALID_REQUEST);check();}
            public void started(InstallationHostGuard.ProcessIdentity id){child[0]=id;}
            public void heartbeat(){check();}
        });
        need(result.exitCode()==0 && child[0]!=null && child[0].equals(result.child()) && result.descendants().isEmpty(),EVIDENCE_UNAVAILABLE);
        try{PreparedCommand.forceDirectory(directory);}catch(java.io.IOException ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}
        var retained=new Retained(guard,directory,raw,selected,PreflightFiles.read(directory.resolve("result.json"),PreflightFiles.LARGE,true));retained.check();return retained;
    }
    private static JsonNode validate(Retained retained,JsonNode command,JsonNode frozen){
        var response=PreflightFiles.canonical(retained.result,PreflightFiles.LARGE);SourceJson.fields(response,"policy","commandSha256","selectionSha256","bridgeSha256","classification","evidence");
        need(response.get("policy").stringValue().equals(POLICY) && response.get("commandSha256").stringValue().equals(SourceJson.sha(retained.command)) && response.get("selectionSha256").equals(command.get("capturedConfigSha256")) && response.get("bridgeSha256").stringValue().equals(SourceJson.sha(bridge())) && response.get("classification").stringValue().equals("OUTPUT_ABSENT"),EVIDENCE_CHANGED);
        var evidence=response.get("evidence");var result=SourceJson.object();result.set("counts",evidence.get("counts"));ReliabilityPreflight.validateEvidence(command,frozen,result,evidence);
        need(evidence.get("nativeDependencyFingerprint").stringValue().matches("[0-9a-f]{64}"),EVIDENCE_CHANGED);return response;
    }
    static void absent(ReliabilityRuntime runtime,String fingerprint){
        try{Path root=Path.of(runtime.bindings().get("output").get("root").stringValue());Path current=root;for(String part:List.of("v2","processor_revision=2","publication="+fingerprint)){current=current.resolve(part);try{var attributes=Files.readAttributes(current,java.nio.file.attribute.BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);need(!part.startsWith("publication=") && attributes.isDirectory() && !attributes.isSymbolicLink() && current.equals(current.toRealPath()),EVIDENCE_CHANGED);}catch(NoSuchFileException ignored){ }}}
        catch(java.io.IOException ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}
    }
    private static Path directory(InstallationHostGuard guard,UUID control){
        try{Path root=guard.binding().root().resolve("retry-"+control);try{Files.createDirectory(root,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));}catch(FileAlreadyExistsException ignored){}PreflightFiles.privateEntry(root,true);try(var entries=Files.list(root)){need(entries.limit(17).count()<16,CAPACITY_EXCEEDED);}PreparedCommand.forceDirectory(root.getParent());Path directory=Files.createTempDirectory(root,"probe-",PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));PreparedCommand.forceDirectory(root);return directory;}
        catch(java.io.IOException ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}
    }
    private static void write(Path path,byte[] bytes){PreparedCommand.writeOnce(path.getParent(),path.getFileName().toString(),bytes,PreflightFiles.LARGE);}

    static byte[] bridge(){try(var input=ReliabilityRetryEvidence.class.getResourceAsStream("/recovery/reliability_retry_verification.py")){need(input!=null,RUNTIME_MISMATCH);return input.readAllBytes();}catch(java.io.IOException ex){throw new RecoveryFailure(RUNTIME_MISMATCH);}}
    private static void outside(InstallationHostGuard guard,UUID installation){need(!TransactionSynchronizationManager.isActualTransactionActive(),INVALID_REQUEST);guard.checkHeld(installation);}
}
