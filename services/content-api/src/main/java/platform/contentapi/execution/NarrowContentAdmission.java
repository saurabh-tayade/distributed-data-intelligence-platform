package platform.contentapi.execution;

import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;

/** Inactive admission seam. Durable facts never grant an Owner token or stage authority. */
final class NarrowContentAdmission {
    enum Boundary { PREPARING_COMMITTED, FILES_PREPARED, BEFORE_READY, READY_COMMITTED }
    private final JdbcTemplate jdbc;
    private final ManagementTransactions tx;
    private final WorkerRepository workers;
    NarrowContentAdmission(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){
        this.jdbc=Objects.requireNonNull(jdbc);this.tx=Objects.requireNonNull(tx);this.workers=Objects.requireNonNull(workers);
        workers.requireAdmissionRepository(jdbc,tx);
    }
    NarrowContentEvidenceContext.Expected prepare(InstallationHostGuard guard,WorkerRepository.Owner owner){return prepare(guard,owner,b->{});}
    NarrowContentEvidenceContext.Expected prepare(InstallationHostGuard guard,WorkerRepository.Owner owner,Consumer<Boundary> fault){
        Objects.requireNonNull(fault);
        // Commit the tracked, single-use preparation intent BEFORE the first directory or file effect.
        var enrollment=workers.narrowAdmissionTransaction(guard,owner,()->{
            var n=readEnrollment(jdbc,guard.binding(),owner.runId(),owner.attemptId());
            require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_admission_milestones WHERE attempt_id=?",Long.class,owner.attemptId())==0,WORKER_NOT_READY);
            milestone(jdbc,owner.attemptId(),"PREPARING",receipt(n));return n;
        });
        fault.accept(Boundary.PREPARING_COMMITTED);
        var prepared=NarrowContentPreparation.prepare(guard,new NarrowContentPreparation.Specification(NarrowContentProfile.POLICY,
                owner.runId(),owner.attemptId(),enrollment.get("sourceId").stringValue(),guard.binding(),identities(enrollment)));
        fault.accept(Boundary.FILES_PREPARED);
        new NarrowContentEvidenceContext(prepared.expected()).inspect(guard,prepared.independentlySupplied());
        fault.accept(Boundary.BEFORE_READY);
        workers.narrowAdmissionTransaction(guard,owner,()->{
            var current=readEnrollment(jdbc,guard.binding(),owner.runId(),owner.attemptId());
            require(Arrays.equals(SourceJson.canonical(enrollment),SourceJson.canonical(current)),RUNTIME_MISMATCH);
            readMilestone(jdbc,owner.attemptId(),"PREPARING",current);
            var e=prepared.expected();var ready=receipt(current).put("rootDevice",Long.toString(e.rootIdentity().device()))
                    .put("rootInode",Long.toString(e.rootIdentity().inode())).put("lockDevice",Long.toString(e.lockIdentity().device()))
                    .put("lockInode",Long.toString(e.lockIdentity().inode())).put("ledgerSha256",e.ledgerSha256());
            milestone(jdbc,owner.attemptId(),"READY",ready);return null;
        });
        fault.accept(Boundary.READY_COMMITTED);
        return inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
    }
    /** Fresh-process, read-only reconstruction. Missing evidence fails before filesystem access. */
    static NarrowContentEvidenceContext.Expected inspectReady(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt){
        require(!org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        guard.checkHeld(guard.binding().installationId());
        var expected=tx.required(()->{
            var n=readEnrollment(jdbc,guard.binding(),run,attempt);readMilestone(jdbc,attempt,"PREPARING",n);
            var ready=readMilestone(jdbc,attempt,"READY",n);
            return new NarrowContentEvidenceContext.Expected(NarrowContentProfile.POLICY,run,attempt,NarrowContentEvidenceContext.PlanKind.ORIGINAL,
                    n.get("sourceId").stringValue(),guard.binding(),identity(ready,"root"),identity(ready,"lock"),identities(n),ready.get("ledgerSha256").stringValue());
        });
        new NarrowContentEvidenceContext(expected).inspect(guard,expected.frozen());return expected;
    }
    /** Called only inside the same claim transaction after the owner pointers have been written. */
    static void enroll(JdbcTemplate jdbc,InstallationHostGuard guard,UUID run,UUID attempt){
        byte[] bytes=SourceJson.canonical(independent(jdbc,guard.binding(),run,attempt));require(bytes.length<=65536,RUNTIME_MISMATCH);
        jdbc.update("INSERT INTO ddip_management.content_attempt_admissions(attempt_id,policy,admission_bytes,admission_sha256) VALUES (?,?,?,?)",attempt,NarrowContentProfile.POLICY,bytes,SourceJson.sha(bytes));
    }
    /** SQL-only immutable admission prefix. No installed runtime or filesystem access. */
    record Historical(List<String> phases,String admissionSha256,NarrowContentEvidenceContext.Expected ready) {
        Historical {phases=List.copyOf(phases);}
        @Override public String toString(){return "AdmissionHistory[detached]";}
    }
    static Historical inspectHistorical(JdbcTemplate jdbc,UUID run,UUID attempt){
        var row=one(jdbc.queryForList("SELECT admission_bytes,admission_sha256 FROM ddip_management.content_attempt_admissions WHERE attempt_id=?",attempt));
        var n=canonical((byte[])row.get("admission_bytes"),(String)row.get("admission_sha256"));
        var host=n.get("controlBinding");
        var binding=new InstallationHostGuard.Binding(SourceJson.text(host.get("bindingId")),UUID.fromString(SourceJson.text(host.get("installationId"))),
                java.nio.file.Path.of(SourceJson.text(host.get("root"))),identity(host,"root"),identity(host,"guard"),identity(host,"installation"));
        require(Arrays.equals(binding.descriptor(),SourceJson.canonical(host)),RUNTIME_MISMATCH);
        n=readEnrollment(jdbc,binding,run,attempt);
        var phases=jdbc.queryForList("SELECT phase FROM ddip_management.content_admission_milestones WHERE attempt_id=? ORDER BY CASE phase WHEN 'PREPARING' THEN 1 ELSE 2 END LIMIT 3",String.class,attempt);
        var ordered=List.of("PREPARING","READY");require(phases.size()<=2 && phases.equals(ordered.subList(0,phases.size())),RUNTIME_MISMATCH);
        if(!phases.isEmpty())readMilestone(jdbc,attempt,"PREPARING",n);
        NarrowContentEvidenceContext.Expected ready=null;
        if(phases.size()==2){
            var r=readMilestone(jdbc,attempt,"READY",n);
            ready=new NarrowContentEvidenceContext.Expected(NarrowContentProfile.POLICY,run,attempt,NarrowContentEvidenceContext.PlanKind.ORIGINAL,
                    n.get("sourceId").stringValue(),binding,identity(r,"root"),identity(r,"lock"),identities(n),r.get("ledgerSha256").stringValue());
        }
        return new Historical(phases,(String)row.get("admission_sha256"),ready);
    }
    static boolean hasHistory(JdbcTemplate jdbc,UUID run){
        return jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.attempts WHERE run_id=? AND content_admission_policy IS NOT NULL)",Boolean.class,run);
    }
    private static ObjectNode independent(JdbcTemplate jdbc,InstallationHostGuard.Binding binding,UUID run,UUID attempt){
        try{
            var row=one(jdbc.queryForList("SELECT a.content_admission_policy,a.kind,a.attempt_no,a.instance_id,a.owner_epoch,r.captured_selection,r.selection_sha256,r.execution_profile_id,r.pipeline,w.installation_id,w.profile_sha256,w.host_binding_sha256,w.execution_profile_id AS worker_profile,p.configuration_bytes,p.configuration_sha256 FROM ddip_management.attempts a JOIN ddip_management.runs r USING(run_id) JOIN ddip_management.worker_instances w USING(instance_id) JOIN ddip_management.execution_profiles p ON p.profile_id=r.execution_profile_id WHERE a.attempt_id=? AND a.run_id=?",attempt,run));
            require(NarrowContentProfile.POLICY.equals(row.get("content_admission_policy")) && "EXECUTE".equals(row.get("kind"))
                    && ((Number)row.get("attempt_no")).intValue()<=19 && "CONTENT_RSS_V1".equals(row.get("pipeline"))
                    && binding.installationId().equals(row.get("installation_id")) && binding.digest().equals(row.get("host_binding_sha256"))
                    && row.get("execution_profile_id").equals(row.get("worker_profile")) && row.get("configuration_sha256").equals(row.get("profile_sha256")),RUNTIME_MISMATCH);
            var frozen=FrozenRunSelection.canonical((byte[])row.get("captured_selection"),(String)row.get("selection_sha256"),256*1024);
            var profile=FrozenRunSelection.canonical((byte[])row.get("configuration_bytes"),(String)row.get("configuration_sha256"),65536);
            ContentEvidenceProfile.selection(frozen);
            require(profile.get("runProfileVersion").isIntegralNumber() && profile.get("runProfileVersion").canConvertToInt() && profile.get("runProfileVersion").intValue()==2
                    && frozen.get("executionProfile").get("profileId").stringValue().equals(row.get("execution_profile_id"))
                    && frozen.get("executionProfile").get("configurationSha256").stringValue().equals(row.get("configuration_sha256"))
                    && frozen.get("executionProfile").get("configuration").equals(profile),RUNTIME_MISMATCH);
            require(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.execution_profile_storage p JOIN ddip_management.storage_bindings b USING(binding_id) WHERE p.profile_id=? AND b.binding_id=? AND b.installation_id=? AND b.role='CONTROL' AND b.configuration_sha256=? AND b.configuration_bytes=?)",Boolean.class,row.get("execution_profile_id"),binding.id(),binding.installationId(),binding.digest(),binding.descriptor()),RUNTIME_MISMATCH);
            String runtimeId=SourceJson.text(profile.get("contentRuntimeId"));
            var runtime=one(jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",runtimeId));
            FrozenRunSelection.canonical((byte[])runtime.get("configuration_bytes"),(String)runtime.get("configuration_sha256"),65536);
            String source=SourceJson.text(frozen.get("sources").get(0).get("sourceId"));
            require(source.matches("[a-z0-9][a-z0-9_-]{0,127}"),RUNTIME_MISMATCH);
            var n=SourceJson.object().put("admissionVersion",1).put("policy",NarrowContentProfile.POLICY)
                    .put("installationId",binding.installationId().toString()).put("runId",run.toString()).put("attemptId",attempt.toString())
                    .put("workerInstanceId",row.get("instance_id").toString()).put("ownerEpoch",row.get("owner_epoch").toString())
                    .put("planKind","ORIGINAL").put("sourceId",source).put("controlBindingSha256",binding.digest())
                    .put("selectionSha256",(String)row.get("selection_sha256")).put("profileId",(String)row.get("execution_profile_id"))
                    .put("profileSha256",(String)row.get("configuration_sha256")).put("runtimeId",runtimeId)
                    .put("runtimeSha256",(String)runtime.get("configuration_sha256"));
            n.set("controlBinding",SourceJson.parse(binding.descriptor()));identities(n);return n;
        }catch(org.springframework.dao.DataAccessException|WorkerFailure ex){throw ex;}
        catch(RuntimeException ex){throw new WorkerFailure(RUNTIME_MISMATCH);}
    }
    private static ObjectNode readEnrollment(JdbcTemplate jdbc,InstallationHostGuard.Binding binding,UUID run,UUID attempt){
        var current=independent(jdbc,binding,run,attempt);
        var row=one(jdbc.queryForList("SELECT policy,admission_bytes,admission_sha256 FROM ddip_management.content_attempt_admissions WHERE attempt_id=?",attempt));
        require(NarrowContentProfile.POLICY.equals(row.get("policy")),RUNTIME_MISMATCH);
        var n=canonical((byte[])row.get("admission_bytes"),(String)row.get("admission_sha256"));
        require(Arrays.equals(SourceJson.canonical(n),SourceJson.canonical(current)),RUNTIME_MISMATCH);return current;
    }
    private static JsonNode readMilestone(JdbcTemplate jdbc,UUID attempt,String phase,JsonNode enrollment){
        var row=one(jdbc.queryForList("SELECT receipt_bytes,receipt_sha256 FROM ddip_management.content_admission_milestones WHERE attempt_id=? AND phase=?",attempt,phase));
        var n=canonical((byte[])row.get("receipt_bytes"),(String)row.get("receipt_sha256"));
        try{
            if(phase.equals("READY"))SourceJson.fields(n,"protocolVersion","admissionSha256","rootDevice","rootInode","lockDevice","lockInode","ledgerSha256");
            else SourceJson.fields(n,"protocolVersion","admissionSha256");
            require(n.get("protocolVersion").isIntegralNumber() && n.get("protocolVersion").canConvertToInt() && n.get("protocolVersion").intValue()==1
                    && SourceJson.sha(SourceJson.canonical(enrollment)).equals(SourceJson.text(n.get("admissionSha256"))),RUNTIME_MISMATCH);
            if(phase.equals("READY")){identity(n,"root");identity(n,"lock");require(SourceJson.text(n.get("ledgerSha256")).matches("[0-9a-f]{64}"),RUNTIME_MISMATCH);}
            return n;
        }catch(WorkerFailure ex){throw ex;}catch(RuntimeException ex){throw new WorkerFailure(RUNTIME_MISMATCH);}
    }
    private static JsonNode canonical(byte[] bytes,String sha){
        try{return FrozenRunSelection.canonical(bytes,sha,65536);}catch(RuntimeException ex){throw new WorkerFailure(RUNTIME_MISMATCH);}
    }
    private static ObjectNode receipt(JsonNode enrollment){return SourceJson.object().put("protocolVersion",1).put("admissionSha256",SourceJson.sha(SourceJson.canonical(enrollment)));}
    private static void milestone(JdbcTemplate jdbc,UUID attempt,String phase,JsonNode value){
        byte[] bytes=SourceJson.canonical(value);require(bytes.length<=65536,RUNTIME_MISMATCH);
        jdbc.update("INSERT INTO ddip_management.content_admission_milestones(attempt_id,phase,receipt_bytes,receipt_sha256) VALUES (?,?,?,?)",attempt,phase,bytes,SourceJson.sha(bytes));
    }
    private static NarrowContentEvidenceContext.FrozenIdentities identities(JsonNode n){return new NarrowContentEvidenceContext.FrozenIdentities(n.get("selectionSha256").stringValue(),n.get("profileSha256").stringValue(),n.get("runtimeSha256").stringValue());}
    private static InstallationHostGuard.Identity identity(JsonNode n,String prefix){return new InstallationHostGuard.Identity(decimal(n,prefix+"Device"),decimal(n,prefix+"Inode"));}
    private static long decimal(JsonNode n,String key){try{String value=SourceJson.text(n.get(key));long number=Long.parseLong(value);require(Long.toString(number).equals(value),RUNTIME_MISMATCH);return number;}catch(RuntimeException ex){throw new WorkerFailure(RUNTIME_MISMATCH);}}
    private static Map<String,Object> one(List<Map<String,Object>> rows){require(rows.size()==1,RUNTIME_MISMATCH);return rows.getFirst();}
}
