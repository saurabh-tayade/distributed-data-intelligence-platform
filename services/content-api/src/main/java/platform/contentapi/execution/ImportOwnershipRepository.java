package platform.contentapi.execution;

import java.sql.*;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.management.SourceJson;
import platform.contentapi.importing.*;
import static platform.contentapi.execution.ImportOwnershipFailure.Code.*;

/** Fenced maintenance ownership and atomic qualified-artifact finalization. No external I/O in SQL transactions. */
public final class ImportOwnershipRepository {
    public enum Kind { IMPORT, RECOVERY }
    public enum State { ACTIVE, SUCCEEDED, FAILED, INTERRUPTED }
    public enum Outcome { SAFE_COMPLETION, SAFE_FAILURE, RECOVERY_CLOSED }
    public record Token(UUID importId,UUID processInstanceId,long epoch,long operationVersion,long coordinatorVersion) { }
    public record Reservation(UUID importId,String profileId,String profileSha256,byte[] requestBytes) {
        public Reservation {
            ImportOwnershipFailure.require(importId!=null && profileId!=null && profileId.matches("[A-Za-z0-9_-]{1,128}") &&
                    profileSha256!=null && profileSha256.matches("[0-9a-f]{64}"),INVALID_REQUEST);
            requestBytes=canonical(requestBytes);
        }
        @Override public byte[] requestBytes() {return requestBytes.clone();}
        @Override public String toString() {return "ImportReservation[redacted]";}
    }
    public record Operation(UUID importId,Kind kind,UUID predecessorId,UUID installationId,UUID processInstanceId,long pid,Instant processStartedAt,
                            String profileId,String profileSha256,String hostBindingId,String hostBindingSha256,long epoch,State state,
                            long rowVersion,byte[] requestBytes,byte[] resultBytes,String outcomeCode) {
        public Operation {requestBytes=requestBytes.clone();resultBytes=resultBytes==null?null:resultBytes.clone();}
        @Override public byte[] requestBytes() {return requestBytes.clone();}
        @Override public byte[] resultBytes() {return resultBytes==null?null:resultBytes.clone();}
        @Override public String toString() {return "ImportOperation[redacted]";}
    }
    private record Slot(UUID importId,UUID attemptId,UUID installation,long epoch,long version,String mode) { }
    private final JdbcTemplate jdbc;
    private final ManagementTransactions transactions;
    private final UUID installation;
    public ImportOwnershipRepository(JdbcTemplate jdbc,ManagementTransactions transactions,UUID installation) {
        this.jdbc=jdbc;this.transactions=transactions;this.installation=installation;
    }
    private static byte[] canonical(byte[] bytes) {
        try {
            ImportOwnershipFailure.require(bytes!=null,INVALID_REQUEST);
            byte[] copy=bytes.clone();
            ImportOwnershipFailure.require(Arrays.equals(copy,SourceJson.canonical(SourceJson.parse(copy))),INVALID_REQUEST);
            return copy;
        } catch(RuntimeException ex) {throw new ImportOwnershipFailure(INVALID_REQUEST);}
    }
    private static void outsideTransaction() {
        ImportOwnershipFailure.require(!TransactionSynchronizationManager.isActualTransactionActive(),INVALID_REQUEST);
    }
    private static long nextEpoch(long epoch) {
        ImportOwnershipFailure.require(epoch>=0 && epoch<Long.MAX_VALUE,OWNERSHIP_STALE);return epoch+1;
    }
    private Slot lockSlot() {
        var rows=jdbc.query("SELECT * FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE",(r,n) ->
                new Slot(r.getObject("active_import_id",UUID.class),r.getObject("active_attempt_id",UUID.class),r.getObject("installation_id",UUID.class),
                        r.getLong("epoch"),r.getLong("row_version"),r.getString("mode")));
        if(rows.size()!=1 || !installation.equals(rows.getFirst().installation()))throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_INCOMPATIBLE);
        return rows.getFirst();
    }
    private Operation map(ResultSet r,int n) throws SQLException {
        return new Operation(r.getObject("import_id",UUID.class),Kind.valueOf(r.getString("kind")),r.getObject("predecessor_id",UUID.class),
                r.getObject("installation_id",UUID.class),r.getObject("process_instance_id",UUID.class),r.getLong("owner_pid"),
                r.getObject("owner_started_at",OffsetDateTime.class).toInstant(),r.getString("execution_profile_id"),r.getString("profile_sha256"),
                r.getString("host_binding_id"),r.getString("host_binding_sha256"),r.getLong("owner_epoch"),State.valueOf(r.getString("state")),
                r.getLong("row_version"),r.getBytes("request_bytes"),r.getBytes("result_bytes"),r.getString("outcome_code"));
    }
    private Operation operation(UUID id,boolean lock) {
        var rows=jdbc.query("SELECT * FROM ddip_management.artifact_import_operations WHERE import_id=?"+(lock?" FOR UPDATE":""),this::map,id);
        ImportOwnershipFailure.require(rows.size()==1,IMPORT_NOT_FOUND);return rows.getFirst();
    }
    /** Lookup resolves uncertainty only; neither a token nor SUCCEEDED permits relaunch/qualification. */
    public Optional<Operation> lookup(UUID id,byte[] exactRequest) {
        byte[] request=canonical(exactRequest);ImportOwnershipFailure.require(id!=null,INVALID_REQUEST);
        return transactions.required(() -> {
            var rows=jdbc.query("SELECT * FROM ddip_management.artifact_import_operations WHERE import_id=? AND installation_id=?",this::map,id,installation);
            if(rows.isEmpty())return Optional.empty();
            ImportOwnershipFailure.require(Arrays.equals(request,rows.getFirst().requestBytes()),INVALID_REQUEST);
            return Optional.of(rows.getFirst());
        });
    }
    private void binding(InstallationHostGuard guard,String profileId,String profileSha) {
        var b=guard.binding();
        var rows=jdbc.queryForList("SELECT b.configuration_bytes FROM ddip_management.storage_bindings b JOIN ddip_management.execution_profile_storage s USING(binding_id) " +
                "JOIN ddip_management.execution_profiles p ON p.profile_id=s.profile_id WHERE b.binding_id=? AND b.installation_id=? AND b.role='CONTROL' " +
                "AND b.configuration_sha256=? AND p.profile_id=? AND p.configuration_sha256=?",b.id(),installation,b.digest(),profileId,profileSha);
        ImportOwnershipFailure.require(rows.size()==1 && Arrays.equals((byte[])rows.getFirst().get("configuration_bytes"),b.descriptor()),HOST_BINDING_INVALID);
        ImportOwnershipFailure.require(!Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.worker_instances WHERE installation_id=? AND host_binding_sha256<>?)",Boolean.class,installation,b.digest())),HOST_BINDING_INVALID);
    }
    private void noExecution() {
        ImportOwnershipFailure.require(!Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.attempts WHERE state IN ('CLAIMED','RUNNING')) OR " +
                "EXISTS(SELECT 1 FROM ddip_management.runs WHERE state IN ('RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED'))",Boolean.class)),OWNERSHIP_STALE);
    }
    private void admissionAuditCapacity(long events) {
        long unfinished=jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class);
        ImportOwnershipFailure.require(platform.contentapi.management.AuditCapacity.available(jdbc,events,unfinished),MANAGEMENT_CAPACITY_EXCEEDED);
    }
    private void insert(InstallationHostGuard guard,Reservation r,Kind kind,UUID predecessor,long epoch) {
        var p=guard.process();
        jdbc.update("INSERT INTO ddip_management.artifact_import_operations(import_id,installation_id,kind,predecessor_id,quiescence_confirmed,process_instance_id,owner_pid,owner_started_at," +
                "execution_profile_id,profile_sha256,host_binding_id,host_binding_sha256,owner_epoch,request_bytes,request_sha256,state) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'ACTIVE')",
                r.importId(),installation,kind.name(),predecessor,kind==Kind.RECOVERY,p.instanceId(),p.pid(),OffsetDateTime.ofInstant(p.startedAt(),ZoneOffset.UTC),
                r.profileId(),r.profileSha256(),guard.binding().id(),guard.binding().digest(),epoch,r.requestBytes(),SourceJson.sha(r.requestBytes()));
    }
    private void audit(UUID id,String action,String before,String after,long beforeVersion,long afterVersion,String code,UUID related) {
        var details=SourceJson.object().put("protocolVersion",1);
        if(action.equals("IMPORT_FINISHED"))details.put("resultSha256",jdbc.queryForObject("SELECT result_sha256 FROM ddip_management.artifact_import_operations WHERE import_id=?",String.class,id));
        if(related!=null)details.put("relatedOperationId",related.toString());
        jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,import_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?)",
                id,action,before,after,beforeVersion,afterVersion,code,SourceJson.canonical(details));
    }
    public Token reserve(InstallationHostGuard guard,Reservation reservation) {
        outsideTransaction();guard.checkHeld(installation);
        Token token=transactions.required(() -> {
            Slot s=lockSlot();
            ImportOwnershipFailure.require(s.importId()==null && s.attemptId()==null && s.mode().equals("READY"),OWNERSHIP_STALE);
            noExecution();binding(guard,reservation.profileId(),reservation.profileSha256());admissionAuditCapacity(4);
            long epoch=nextEpoch(s.epoch());
            insert(guard,reservation,Kind.IMPORT,null,epoch);
            int changed=jdbc.update("UPDATE ddip_management.coordinator SET active_import_id=?,epoch=?,row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? " +
                    "AND active_import_id IS NULL AND active_attempt_id IS NULL AND epoch=? AND row_version=? AND mode='READY'",
                    reservation.importId(),epoch,installation,s.epoch(),s.version());
            ImportOwnershipFailure.require(changed==1,OWNERSHIP_STALE);
            audit(reservation.importId(),"IMPORT_RESERVED",null,"ACTIVE",0,0,"RESERVED",null);
            return new Token(reservation.importId(),guard.process().instanceId(),epoch,0,s.version()+1);
        });
        guard.checkHeld(installation);return token;
    }
    private Operation owned(InstallationHostGuard guard,Token token,Slot s) {
        ImportOwnershipFailure.require(token!=null && token.importId()!=null && guard.process().instanceId().equals(token.processInstanceId()) &&
                token.importId().equals(s.importId()) && token.epoch()==s.epoch() && token.coordinatorVersion()==s.version() && s.attemptId()==null,OWNERSHIP_STALE);
        Operation op=operation(token.importId(),true);
        ImportOwnershipFailure.require(op.state()==State.ACTIVE && op.rowVersion()==token.operationVersion() && op.epoch()==token.epoch() &&
                op.processInstanceId().equals(token.processInstanceId()) && op.hostBindingSha256().equals(guard.binding().digest()),OWNERSHIP_STALE);
        return op;
    }
    private void terminal(Operation op,State state,String code,byte[] result) {
        int changed=jdbc.update("UPDATE ddip_management.artifact_import_operations SET state=?,outcome_code=?,result_bytes=?,result_sha256=?,finished_at=clock_timestamp(),row_version=row_version+1 " +
                "WHERE import_id=? AND installation_id=? AND process_instance_id=? AND owner_epoch=? AND row_version=? AND state='ACTIVE'",
                state.name(),code,result,SourceJson.sha(result),op.importId(),installation,op.processInstanceId(),op.epoch(),op.rowVersion());
        ImportOwnershipFailure.require(changed==1,OWNERSHIP_STALE);
    }
    /** Caller must establish definitive cleanup, independently of timeout/exit/lock/DB liveness. */
    public void finish(InstallationHostGuard guard,Token token,Outcome outcome,byte[] resultBytes,boolean safeToRelease) {
        outsideTransaction();guard.checkHeld(installation);
        ImportOwnershipFailure.require(safeToRelease,QUIESCENCE_REQUIRED);
        ImportOwnershipFailure.require(outcome!=null,INVALID_REQUEST);byte[] result=canonical(resultBytes);
        transactions.required(() -> {
            Slot s=lockSlot();Operation op=owned(guard,token,s);
            ImportOwnershipFailure.require((op.kind()==Kind.RECOVERY)==(outcome==Outcome.RECOVERY_CLOSED),INVALID_REQUEST);
            complete(op,token,outcome,result);return null;
        });
    }
    private void complete(Operation op,Token token,Outcome outcome,byte[] result) {
        State state=outcome==Outcome.SAFE_FAILURE?State.FAILED:State.SUCCEEDED;
        terminal(op,state,outcome.name(),result);
        audit(op.importId(),"IMPORT_FINISHED","ACTIVE",state.name(),op.rowVersion(),op.rowVersion()+1,outcome.name(),null);
        int changed=jdbc.update("UPDATE ddip_management.coordinator SET active_import_id=NULL,row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? " +
                "AND active_import_id=? AND active_attempt_id IS NULL AND epoch=? AND row_version=?",installation,op.importId(),op.epoch(),token.coordinatorVersion());
        ImportOwnershipFailure.require(changed==1,OWNERSHIP_STALE);
    }
    /** Verification and child cleanup are complete before entry. Registry, result and audit commit together. */
    public byte[] finishVerified(InstallationHostGuard guard,Token token,List<QualifiedArtifact> artifacts) {
        outsideTransaction();guard.checkHeld(installation);
        ImportOwnershipFailure.require(artifacts!=null && !artifacts.isEmpty() && artifacts.size()<=ImportCatalog.MAX_ARTIFACTS,INVALID_REQUEST);
        return transactions.required(() -> {
            Operation op=owned(guard,token,lockSlot());ImportOwnershipFailure.require(op.kind()==Kind.IMPORT,INVALID_REQUEST);
            var result=SourceJson.object().put("protocolVersion",1).put("status","VERIFIED").put("operationId",op.importId().toString());
            var expected=ImportCatalog.parse(op.requestBytes());
            ImportOwnershipFailure.require(expected.operationId().equals(op.importId()) && expected.entries().size()==artifacts.size(),INVALID_REQUEST);
            var ids=result.putArray("artifactIds");var evidence=result.putArray("artifacts");
            for(int i=0;i<artifacts.size();i++) {
                var artifact=artifacts.get(i);
                ImportOwnershipFailure.require(op.importId().equals(artifact.operation()) && Arrays.equals(SourceJson.canonical(expected.entries().get(i).json()),SourceJson.canonical(artifact.selection().json())),INVALID_REQUEST);
                UUID id=register(artifact);ids.add(id.toString());evidence.addObject().put("artifactId",id.toString()).put("evidenceSha256",SourceJson.sha(artifact.evidence()));
            }
            byte[] bytes=canonical(SourceJson.canonical(result));complete(op,token,Outcome.SAFE_COMPLETION,bytes);return bytes;
        });
    }
    private UUID register(QualifiedArtifact qualified) {
        var e=qualified.selection();byte[] evidence=qualified.evidence();
        var rows=jdbc.queryForList("SELECT * FROM ddip_management.artifacts WHERE (product_id=? AND kind=? AND contract_version=? AND processor_revision=? AND native_format IS NOT DISTINCT FROM CAST(? AS varchar) AND native_identity=?) OR (storage_binding_id=? AND relative_address=?)",
                e.product(),e.kind(),e.contract(),e.processor(),e.format(),e.nativeIdentity(),e.bindingId(),e.address());
        if(rows.size()>1)throw new ImportFailure(ImportFailure.Code.IDENTITY_CONFLICT);
        if(!rows.isEmpty()) {
            var r=rows.getFirst();var id=(UUID)r.get("artifact_id");
            if(!Objects.equals(r.get("product_id"),e.product()) || !Objects.equals(r.get("kind"),e.kind()) ||
                    ((Number)r.get("contract_version")).intValue()!=e.contract() || ((Number)r.get("processor_revision")).intValue()!=e.processor() ||
                    !Objects.equals(r.get("native_format"),e.format()) || !Objects.equals(r.get("native_identity"),e.nativeIdentity()) ||
                    !Objects.equals(r.get("storage_binding_id"),e.bindingId()) || !Objects.equals(r.get("relative_address"),e.address()) ||
                    !Arrays.equals((byte[])r.get("evidence_bytes"),evidence) || Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.artifact_dependencies WHERE artifact_id=?)",Boolean.class,id)))
                throw new ImportFailure(ImportFailure.Code.IDENTITY_CONFLICT);
            return id;
        }
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at,native_format) VALUES (?,?,?,?,?,?,?,?,?,?,clock_timestamp(),?)",
                id,e.kind(),e.product(),e.contract(),e.processor(),e.nativeIdentity(),e.bindingId(),e.address(),evidence,SourceJson.sha(evidence),e.format());
        // Retained raw/reference files and Silver Bronze receipts are exact native descriptors, not fabricated registry artifacts.
        return id;
    }
    private static void requireQuiescence(Operation old,boolean confirmed) {
        ImportOwnershipFailure.require(confirmed,QUIESCENCE_REQUIRED);
        try {
            Optional<ProcessHandle> found=ProcessHandle.of(old.pid());
            if(found.isPresent() && found.get().isAlive()) {
                var start=found.get().info().startInstant();
                // A reused PID is not the old process; unavailable start evidence is uncertainty.
                ImportOwnershipFailure.require(start.isPresent() && !start.get().truncatedTo(ChronoUnit.MICROS).equals(old.processStartedAt()),OWNER_PROCESS_UNRESOLVED);
            }
        } catch(SecurityException | UnsupportedOperationException ex) {throw new ImportOwnershipFailure(OWNER_PROCESS_UNRESOLVED);}
        // This check cannot prove orphan-descendant quiescence. Explicit trusted inspection is mandatory.
    }
    /** Atomic occupied -> occupied handoff. No qualification/processing runs here. */
    public Token handoff(InstallationHostGuard guard,Token expectedOld,UUID recoveryId,boolean quiescenceConfirmed) {
        outsideTransaction();guard.checkHeld(installation);
        ImportOwnershipFailure.require(expectedOld!=null && expectedOld.importId()!=null && recoveryId!=null && !recoveryId.equals(expectedOld.importId()),INVALID_REQUEST);
        Operation observed=transactions.required(() -> operation(expectedOld.importId(),false));
        requireQuiescence(observed,quiescenceConfirmed); // External process inspection is outside SQL transactions.
        Token token=transactions.required(() -> {
            Slot s=lockSlot();Operation old=operation(expectedOld.importId(),true);
            ImportOwnershipFailure.require(old.state()==State.ACTIVE && old.importId().equals(s.importId()) && old.epoch()==s.epoch() &&
                    expectedOld.epoch()==s.epoch() && expectedOld.coordinatorVersion()==s.version() && expectedOld.operationVersion()==old.rowVersion() &&
                    expectedOld.processInstanceId().equals(old.processInstanceId()) && old.installationId().equals(installation) && old.hostBindingSha256().equals(guard.binding().digest()),OWNERSHIP_STALE);
            noExecution();binding(guard,old.profileId(),old.profileSha256());admissionAuditCapacity(2);
            byte[] result=SourceJson.canonical(SourceJson.object().put("protocolVersion",1).put("successorOperationId",recoveryId.toString()));
            terminal(old,State.INTERRUPTED,"RECOVERY_HANDOFF",result);
            long epoch=nextEpoch(s.epoch());
            insert(guard,new Reservation(recoveryId,old.profileId(),old.profileSha256(),old.requestBytes()),Kind.RECOVERY,old.importId(),epoch);
            int changed=jdbc.update("UPDATE ddip_management.coordinator SET active_import_id=?,epoch=?,row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? " +
                    "AND active_import_id=? AND active_attempt_id IS NULL AND epoch=? AND row_version=?",recoveryId,epoch,installation,old.importId(),old.epoch(),s.version());
            ImportOwnershipFailure.require(changed==1,OWNERSHIP_STALE);
            audit(old.importId(),"IMPORT_INTERRUPTED","ACTIVE","INTERRUPTED",old.rowVersion(),old.rowVersion()+1,"RECOVERY_HANDOFF",recoveryId);
            audit(recoveryId,"IMPORT_RECOVERY_RESERVED",null,"ACTIVE",0,0,"QUIESCENCE_CONFIRMED",old.importId());
            return new Token(recoveryId,guard.process().instanceId(),epoch,0,s.version()+1);
        });
        guard.checkHeld(installation);return token;
    }
}
