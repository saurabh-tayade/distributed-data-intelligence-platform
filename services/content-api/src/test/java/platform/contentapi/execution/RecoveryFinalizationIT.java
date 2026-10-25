package platform.contentapi.execution;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;

/** Real native qualification and atomic terminal disposition, never a recovery publisher. */
class RecoveryFinalizationIT extends RecoveryVerificationFixtures {
    RecoveryVerification.Observation observed;
    RecoveryFinalization finalizer(){return new RecoveryFinalization(jdbc,tx,handoffs);}
    RecoveryQuiescence.Review freshReview(){clock.now=Instant.now();return new RecoveryQuiescence.Review(review.targetSha256(),clock.instant(),true,true,true,review.reviewedCheckpoints(),review.descendants());}
    RecoveryFinalization.Qualified qualified(){handoff();observed=recoveryService().verify(guard,bootstrap,review);return finalizer().qualify(guard,bootstrap,observed,freshReview());}
    Map<String,Object> slot(){return jdbc.queryForMap("SELECT mode,epoch,row_version,active_attempt_id,owner_instance_id FROM ddip_management.coordinator");}
    void released(RecoveryFinalization.Receipt receipt,String state,String reason,boolean retry) {
        assertThat(receipt.status()).isEqualTo(RecoveryFinalization.Status.CONFIRMED);assertThat(receipt.runState()).isEqualTo(state);assertThat(receipt.code()).isEqualTo(reason);assertThat(receipt.retryEligible()).isEqualTo(retry);
        assertThat(state()).isEqualTo(state);var c=slot();assertThat(c.get("mode")).isEqualTo("READY");assertThat(c.get("active_attempt_id")).isNull();assertThat(c.get("owner_instance_id")).isNull();assertThat(c.get("epoch")).isEqualTo(2L);
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,control)).isEqualTo("SUCCEEDED");assertThat(jdbc.queryForObject("SELECT outcome_code FROM ddip_management.attempts WHERE attempt_id=?",String.class,control)).isEqualTo("RECONCILED_"+state);
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.controls WHERE control_id=?",String.class,control)).isEqualTo("APPLIED");assertThat(jdbc.queryForObject("SELECT reason FROM ddip_management.controls WHERE control_id=?",String.class,control)).isEqualTo(reason);assertThat(jdbc.queryForObject("SELECT status FROM ddip_management.worker_instances WHERE instance_id=?",String.class,recoveryOwner.worker().instanceId())).isEqualTo("STOPPED");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,owner.attemptId())).isEqualTo("INTERRUPTED");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE control_id=? AND action='RECOVERY_FINALIZED'",Long.class,control)).isEqualTo(1);
    }
    @ParameterizedTest @ValueSource(strings={"JSONL","PARQUET"}) void recoveredPublicationLinksExactArtifactWithoutRewriting(String fmt)throws Exception {
        if(fmt.equals("PARQUET"))parquet();var before=nativeSnapshot();var q=qualified();var c=slot();var receipt=finalizer().finish(guard,q,freshReview());released(receipt,"SUCCEEDED","RECOVERED_PUBLICATION",false);
        assertThat(nativeSnapshot()).isEqualTo(before);assertThat(slot().get("row_version")).isEqualTo(((Number)c.get("row_version")).longValue()+1);
        var result=jdbc.queryForMap("SELECT result_artifact_id,counts_bytes FROM ddip_management.runs WHERE run_id=?",run);assertThat(result.get("result_artifact_id")).isEqualTo(receipt.artifactId());var a=jdbc.queryForMap("SELECT evidence_bytes,native_identity,native_format FROM ddip_management.artifacts WHERE artifact_id=?",receipt.artifactId());assertThat(a.get("native_identity")).isEqualTo(fingerprint());assertThat(a.get("native_format")).isEqualTo(fmt);
        assertThat(jdbc.queryForList("SELECT role,ordinal,dependency_artifact_id AS artifact_id FROM ddip_management.artifact_dependencies WHERE artifact_id=? ORDER BY role,ordinal",receipt.artifactId())).isEqualTo(jdbc.queryForList("SELECT role,ordinal,artifact_id FROM ddip_management.run_inputs WHERE run_id=? ORDER BY role,ordinal",run));
        assertThat(jdbc.queryForObject("SELECT artifact_id FROM ddip_management.stage_artifacts WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=1",UUID.class,control)).isEqualTo(receipt.artifactId());
        long audits=jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class);assertThat(finalizer().finish(guard,q,freshReview())).isEqualTo(receipt);assertThat(finalizer().resolve(guard,control,q.checkpoint(),qualification.sha256())).isEqualTo(receipt);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class)).isEqualTo(audits);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"absent","incomplete","corrupt","invalidMarker","changedDependency","missingDependency"}) void knownFailureClosesRunButReconciliationSucceeds(String mode)throws Exception {
        String code;boolean retry=false;
        switch(mode){
            case "absent" -> {deleteTree(publication());code="PUBLICATION_ABSENT";retry=true;}
            case "incomplete" -> {Files.delete(publication().resolve("complete.json"));code="INCOMPLETE_OUTPUT";}
            case "invalidMarker" -> {Files.writeString(publication().resolve("complete.json"),"invalid");code="PUBLICATION_INVALID";}
            case "corrupt" -> {try(var paths=Files.walk(publication())){Files.writeString(paths.filter(v->v.toString().endsWith(".parquet")).findFirst().orElseThrow(),"bad bytes");}code="PUBLICATION_INVALID";}
            default -> {Path path=dependencyMarker();if(mode.equals("missingDependency"))Files.delete(path);else Files.writeString(path," ",StandardOpenOption.APPEND);code="DEPENDENCY_CHANGED";}
        }
        var before=nativeSnapshot();var q=qualified();var result=finalizer().finish(guard,q,freshReview());released(result,"FAILED",code,retry);assertThat(result.artifactId()).isNull();assertThat(nativeSnapshot()).isEqualTo(before);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='RELIABILITY_V2'",Long.class)).isZero();assertThat(finalizer().resolve(guard,control,q.checkpoint(),qualification.sha256())).isEqualTo(result);
    }
    Path dependencyMarker(){var input=originalPack.get("bundle").get("capturedSelection").get("inputs").get(0);String role=input.get("role").stringValue().equals("RELIABILITY_BATCH")?"batch":"reference";return Path.of(fixture.get("bundle").get("bindings").get(role).get("root").stringValue()).resolve(input.get("relativeAddress").stringValue()).resolve("complete.json");}
    void stillBlocked(){occupied();assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE action='RECOVERY_FINALIZED'",Long.class)).isZero();}
    @ParameterizedTest @ValueSource(strings={"output","dependency","evidence","provenance","root","identity","guard","stale","coordinator","stage","owner","cancel"}) void changedOrUnavailableFinalizationPreconditionsRetainOwnership(String mode)throws Exception {
        var q=qualified();var before=nativeSnapshot();var c=slot();Set<java.nio.file.attribute.PosixFilePermission> permissions=null;Path blocked=null;
        switch(mode){
            case "output","dependency" -> {blocked=mode.equals("output")?publication():dependencyMarker().getParent();permissions=Files.getPosixFilePermissions(blocked);Files.setPosixFilePermissions(blocked,Set.of());}
            case "evidence" -> Files.writeString(binding.root().resolve("runs/"+run+"/attempts/"+control+"/verify-result/"+q.checkpoint()+"/evidence.json")," ",StandardOpenOption.APPEND);
            case "provenance" -> {var stage=jdbc.queryForMap("SELECT checkpoint_id FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='CHECK_INPUTS'",owner.attemptId());Files.delete(binding.root().resolve("runs/"+run+"/attempts/"+owner.attemptId()+"/check-inputs/"+stage.get("checkpoint_id")+"/verified.json"));}
            case "root" -> {Path root=output(),saved=root.resolveSibling("retained-output");Files.move(root,saved);Files.createDirectory(root);}
            case "identity" -> {Path path=binding.root().resolve("runs/"+run+"/attempts/"+control+"/verify-result/"+q.checkpoint()+"/evidence.json");Path displaced=path.resolveSibling("displaced-evidence.json");Files.move(path,displaced);Files.copy(displaced,path,StandardCopyOption.COPY_ATTRIBUTES);}
            case "guard" -> guard.close();
            case "stale" -> clock.now=clock.now.plusSeconds(61);
            case "coordinator" -> jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");
            case "owner" -> jdbc.update("UPDATE ddip_management.worker_instances SET row_version=row_version+1 WHERE instance_id=?",recoveryOwner.worker().instanceId());
            case "stage" -> { // Terminal stage immutability itself must reject mutation.
                assertThatThrownBy(()->jdbc.update("UPDATE ddip_management.stage_invocations SET row_version=row_version+1 WHERE attempt_id=? AND item_ordinal=1",control)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
                Files.writeString(publication().resolve("complete.json")," ",StandardOpenOption.APPEND);
            }
            case "cancel" -> {byte[] raw=SourceJson.canonical(SourceJson.object().put("action","CANCEL"));jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL',?,?,?,2,'REQUESTED','local-operator')",UUID.randomUUID(),run,"cancel-fixture",raw,SourceJson.sha(raw));}
        }
        try {
            RecoveryQuiescence.Review current=mode.equals("stale")?new RecoveryQuiescence.Review(review.targetSha256(),clock.instant(),true,true,true,review.reviewedCheckpoints(),review.descendants()):freshReview();
            try {assertThat(finalizer().finish(guard,q,current).status()).isEqualTo(RecoveryFinalization.Status.UNRESOLVED);}catch(RecoveryFailure | ImportOwnershipFailure | PreflightFailure ex){assertThat(ex.getMessage()).doesNotContain(work.toString());}
        }finally{if(blocked!=null)Files.setPosixFilePermissions(blocked,permissions);if(mode.equals("guard"))guard=InstallationHostGuard.acquire(binding);}
        stillBlocked();assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=?",Long.class,control)).isEqualTo(2);
        if(!Set.of("root","stage","output","dependency").contains(mode))assertThat(nativeSnapshot()).isEqualTo(before);
        if(!mode.equals("coordinator"))assertThat(slot()).isEqualTo(c);
    }
    @ParameterizedTest @ValueSource(strings={"ARTIFACT","DEPENDENCY","LINK","ATTEMPT","RUN","CONTROL","WORKER","COORDINATOR","AUDIT"}) void everyFinalMutationRollsBackAtomically(String point)throws Exception {
        var q=qualified();var before=nativeSnapshot();var c=slot();long audits=jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class);
        var svc=new RecoveryFinalization(jdbc,tx,handoffs,recoveryService(),step->{if(step.name().equals(point))throw new RecoveryFailure(RecoveryFailure.Code.INVALID_REQUEST);});assertThatThrownBy(()->svc.finish(guard,q,freshReview())).hasMessage("INVALID_REQUEST");stillBlocked();assertThat(slot()).isEqualTo(c);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class)).isEqualTo(audits);assertThat(nativeSnapshot()).isEqualTo(before);
        var result=finalizer().finish(guard,q,freshReview());released(result,"SUCCEEDED","RECOVERED_PUBLICATION",false);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=?",Long.class,control)).isEqualTo(2);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void lostCommitAcknowledgmentResolvesExactFactsWithoutDuplicateEffects()throws Exception {
        var q=qualified();var before=nativeSnapshot();var armed=new AtomicBoolean();var lost=lostAck(armed);var ht=new RecoveryHandoff(lost,transactions(lost),INSTALL,clock,RecoveryQuiescence.LOCAL,step->{});var svc=new RecoveryFinalization(lost,transactions(lost),ht,new RecoveryVerification(lost,transactions(lost),ht),step->{if(step==RecoveryFinalization.Step.AUDIT)armed.set(true);});
        assertThatThrownBy(()->svc.finish(guard,q,freshReview())).hasMessage("MANAGEMENT_UNAVAILABLE");long audits=jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class);var c=slot();
        var result=finalizer().resolve(guard,control,q.checkpoint(),qualification.sha256());released(result,"SUCCEEDED","RECOVERED_PUBLICATION",false);assertThat(svc.finish(guard,q,freshReview())).isEqualTo(result);assertThat(finalizer().resolve(guard,control,UUID.randomUUID(),qualification.sha256()).status()).isEqualTo(RecoveryFinalization.Status.UNRESOLVED);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class)).isEqualTo(audits);assertThat(slot()).isEqualTo(c);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"artifact","audit"}) void persistenceFailureRollsBackDispositionAndRelease(String table)throws Exception {
        var q=qualified();var before=nativeSnapshot();var c=slot();String name=table.equals("artifact")?"artifacts":"audit_events";
        jdbc.execute("CREATE FUNCTION ddip_management.reject_finalization() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_TABLE_NAME='artifacts' OR NEW.action='RECOVERY_FINALIZED' THEN RAISE EXCEPTION 'test failure' USING ERRCODE='23514'; END IF; RETURN NEW; END $$");jdbc.execute("CREATE TRIGGER reject_finalization BEFORE INSERT ON ddip_management."+name+" FOR EACH ROW EXECUTE FUNCTION ddip_management.reject_finalization()");
        assertThatThrownBy(()->finalizer().finish(guard,q,freshReview())).isInstanceOf(ManagementFailure.class);stillBlocked();assertThat(slot()).isEqualTo(c);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"before","during","after"}) void databaseOutageNeverInfersAnOutcomeOrReleasesPendingOwner(String point)throws Exception {
        var q=qualified();var before=nativeSnapshot();var c=slot();RecoveryFinalization.Receipt completed=null;
        if(point.equals("after"))completed=finalizer().finish(guard,q,freshReview());
        try {
            if(!point.equals("during"))outage(true);
            if(point.equals("during")){var svc=new RecoveryFinalization(jdbc,tx,handoffs,recoveryService(),step->{if(step==RecoveryFinalization.Step.RUN)outage(true);});assertThatThrownBy(()->svc.finish(guard,q,freshReview())).hasMessage("MANAGEMENT_UNAVAILABLE");}
            else assertThat(finalizer().finish(guard,q,freshReview()).status()).isEqualTo(RecoveryFinalization.Status.UNRESOLVED);
            assertThat(finalizer().resolve(guard,control,q.checkpoint(),qualification.sha256()).status()).isEqualTo(RecoveryFinalization.Status.UNRESOLVED);
        }finally{outage(false);}
        if(completed==null){stillBlocked();assertThat(slot()).isEqualTo(c);completed=finalizer().finish(guard,q,freshReview());}
        assertThat(finalizer().resolve(guard,control,q.checkpoint(),qualification.sha256())).isEqualTo(completed);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void restartAndReplacementGuardResolveOnlyHistoricalCompletion()throws Exception {
        var q=qualified();var before=nativeSnapshot();var result=finalizer().finish(guard,q,freshReview());var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);String signal=System.getProperty("ddip.test.finalization-restart-signal");
        if(signal!=null){Files.writeString(Path.of(signal),"READY");long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);while(!Files.exists(Path.of(signal+".done")) && System.nanoTime()<end)java.util.concurrent.locks.LockSupport.parkNanos(10000000);assertThat(Files.exists(Path.of(signal+".done"))).isTrue();assertThat(jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class)).isAfter(started);}
        guard.close();guard=InstallationHostGuard.acquire(binding);assertThat(finalizer().resolve(guard,control,q.checkpoint(),qualification.sha256())).isEqualTo(result);assertThat(finalizer().finish(guard,q,freshReview())).isEqualTo(result);assertThatThrownBy(()->finalizer().qualify(guard,bootstrap,observed,freshReview())).isInstanceOfAny(RecoveryFailure.class,ImportOwnershipFailure.class);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"writer","child","review","expiredReview","stopping"}) void freshQuiescenceAndKnownCleanupAreMandatoryAtDisposition(String mode)throws Exception {
        var q=qualified();var before=nativeSnapshot();var c=slot();var current=freshReview();RecoveryHandoff ht=handoffs;
        if(mode.equals("review"))current=new RecoveryQuiescence.Review(review.targetSha256(),clock.instant(),true,false,true,review.reviewedCheckpoints(),review.descendants());
        if(mode.equals("expiredReview"))current=new RecoveryQuiescence.Review(review.targetSha256(),clock.instant().minusSeconds(61),true,true,true,review.reviewedCheckpoints(),review.descendants());
        if(Set.of("writer","child").contains(mode)){long pid=mode.equals("writer")?oldPid:observed.stage().child().pid();ht=new RecoveryHandoff(jdbc,tx,INSTALL,clock,id->id.pid()==pid?(mode.equals("writer")?RecoveryQuiescence.State.LIVE:RecoveryQuiescence.State.UNAVAILABLE):RecoveryQuiescence.LOCAL.inspect(id),step->{});}
        if(mode.equals("stopping"))jdbc.update("UPDATE ddip_management.worker_instances SET status='STOPPING',row_version=row_version+1 WHERE instance_id=?",recoveryOwner.worker().instanceId());
        var svc=new RecoveryFinalization(jdbc,tx,ht,new RecoveryVerification(jdbc,tx,ht),step->{});var attestation=current;
        assertThatThrownBy(()->svc.finish(guard,q,attestation)).isInstanceOf(RecoveryFailure.class);stillBlocked();assertThat(slot()).isEqualTo(c);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void guardLostDuringFinalTransactionRollsBackAllEffects()throws Exception {
        var q=qualified();var before=nativeSnapshot();var c=slot();var svc=new RecoveryFinalization(jdbc,tx,handoffs,recoveryService(),step->{if(step==RecoveryFinalization.Step.WORKER)guard.close();});
        assertThatThrownBy(()->svc.finish(guard,q,freshReview())).isInstanceOf(ImportOwnershipFailure.class);guard=InstallationHostGuard.acquire(binding);stillBlocked();assertThat(slot()).isEqualTo(c);assertThat(nativeSnapshot()).isEqualTo(before);assertThat(finalizer().resolve(guard,control,q.checkpoint(),qualification.sha256()).status()).isEqualTo(RecoveryFinalization.Status.UNRESOLVED);
    }
    @Test void unavailableCleanupCannotStartFinalQualification()throws Exception {
        handoff();var before=nativeSnapshot();var p=new ReadOnlyVerifierProcess(){@Override Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer o){o.beforeStart();o.started(new InstallationHostGuard.ProcessIdentity(UUID.randomUUID(),oldPid,oldStart));throw new PreflightFailure(PreflightFailure.Code.TERMINATION_UNCONFIRMED,false);}};
        var incomplete=recoveryService(p).verify(guard,bootstrap,review);assertThat(incomplete.stage().state()).isEqualTo("UNKNOWN");assertThatThrownBy(()->finalizer().qualify(guard,bootstrap,incomplete,freshReview())).hasMessage("EVIDENCE_UNAVAILABLE");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=?",Long.class,control)).isEqualTo(1);stillBlocked();assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void nativeInvalidCodeWithoutPositiveCorruptionEvidenceStaysBlocked()throws Exception {
        handoff();var before=nativeSnapshot();var p=new ReadOnlyVerifierProcess(){@Override Result run(List<String> command,Map<String,String> env,Path dir,Duration timeout,Observer o){var c=new ArrayList<>(command);c.set(6,c.get(6).replace("publication = native.inspect(inner, pack)","p.need(False, 'PUBLICATION_INVALID')"));return super.run(c,env,dir,timeout,o);}};
        var reader=recoveryService(p);var initial=reader.verify(guard,bootstrap,review);assertThat(initial.classification()).isEqualTo("OUTPUT_INVALID");var svc=new RecoveryFinalization(jdbc,tx,handoffs,reader,step->{});assertThatThrownBy(()->svc.qualify(guard,bootstrap,initial,freshReview())).hasMessage("EVIDENCE_UNAVAILABLE");stillBlocked();assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void changedPublicationAfterHistoricalVerificationCannotBeFinalized()throws Exception {
        handoff();observed=recoveryService().verify(guard,bootstrap,review);Files.writeString(publication().resolve("complete.json")," ",StandardOpenOption.APPEND);var before=nativeSnapshot();assertThatThrownBy(()->finalizer().qualify(guard,bootstrap,observed,freshReview())).hasMessage("EVIDENCE_CHANGED");stillBlocked();assertThat(nativeSnapshot()).isEqualTo(before);
    }
    UUID preRegisterPublication(boolean conflict){
        var e=recoveryService().inspect(guard,control).evidence().get("publication");byte[] raw=SourceJson.canonical(conflict?SourceJson.object().put("conflicting",true):e);UUID id=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at,native_format) VALUES (?,'RELIABILITY_V2','application-reliability',2,2,?,?,?,?,?,now(),?)",id,fingerprint(),e.get("storageBindingId").stringValue(),e.get("relativeAddress").stringValue(),raw,SourceJson.sha(raw),e.get("inputFormat").stringValue());for(var input:jdbc.queryForList("SELECT role,ordinal,artifact_id FROM ddip_management.run_inputs WHERE run_id=?",run))jdbc.update("INSERT INTO ddip_management.artifact_dependencies VALUES (?,?,?,?)",id,input.get("role"),input.get("ordinal"),input.get("artifact_id"));return id;
    }
    @ParameterizedTest @ValueSource(strings={"matching","conflicting"}) void artifactIdentityReusesOnlyExactImmutableEvidence(String mode)throws Exception {
        var q=qualified();var before=nativeSnapshot();boolean conflict=mode.equals("conflicting");UUID id=preRegisterPublication(conflict);var c=slot();
        if(conflict){assertThatThrownBy(()->finalizer().finish(guard,q,freshReview())).hasMessage("EVIDENCE_CHANGED");assertThat(state()).isEqualTo("RECOVERY_REQUIRED");assertThat(slot()).isEqualTo(c);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE action='RECOVERY_FINALIZED'",Long.class)).isZero();}
        else {var result=finalizer().finish(guard,q,freshReview());released(result,"SUCCEEDED","RECOVERED_PUBLICATION",false);assertThat(result.artifactId()).isEqualTo(id);}
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='RELIABILITY_V2'",Long.class)).isEqualTo(1);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void duplicateQualificationAndConcurrentFinalizersCannotDuplicateCompletion()throws Exception {
        var q=qualified();var before=nativeSnapshot();long audits=jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class);assertThatThrownBy(()->finalizer().qualify(guard,bootstrap,observed,freshReview())).hasMessage("OWNERSHIP_LOST");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class)).isEqualTo(audits);var current=freshReview();var gate=new CountDownLatch(1);var results=new ArrayList<Object>();
        try(var pool=Executors.newFixedThreadPool(2)){var tasks=new ArrayList<Future<Object>>();for(int i=0;i<2;i++)tasks.add(pool.submit(()->{gate.await();try{return finalizer().finish(guard,q,current);}catch(RecoveryFailure ex){return ex.getMessage();}}));gate.countDown();for(var task:tasks)results.add(task.get(15,TimeUnit.SECONDS));}
        assertThat(results.stream().filter(v->v instanceof RecoveryFinalization.Receipt r && r.status()==RecoveryFinalization.Status.CONFIRMED).count()).isGreaterThanOrEqualTo(1);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE action='RECOVERY_FINALIZED'",Long.class)).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='RELIABILITY_V2'",Long.class)).isEqualTo(1);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void unrelatedQueueNeedsConfirmedReleaseAndFreshWorkerRegistration()throws Exception {
        jdbc.update("UPDATE ddip_management.coordinator SET heartbeat_at=clock_timestamp(),row_version=row_version+1");var before=nativeSnapshot();byte[] request=jdbc.queryForObject("SELECT normalized_request FROM ddip_management.runs WHERE run_id=?",byte[].class,run);UUID queued=UUID.fromString(new RunRepository(jdbc,tx,INSTALL,PreflightCatalogFixture.from(SourceJson.parse(Files.readAllBytes(work.resolve("source-catalog.json"))))).submit(RunCommands.parse(request,"unrelated-queued"),UUID.randomUUID().toString()).get("runId").stringValue());var q=qualified();
        assertThatThrownBy(()->WorkerTestSupport.reliability(jdbc,tx,INSTALL).claim(guard,observed.stage().owner().worker(),UUID.randomUUID())).hasMessage("WORKER_NOT_READY");assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,queued)).isEqualTo("QUEUED");var receipt=finalizer().finish(guard,q,freshReview());var released=slot();
        assertThatThrownBy(()->WorkerTestSupport.reliability(jdbc,tx,INSTALL).claim(guard,observed.stage().owner().worker(),UUID.randomUUID())).isInstanceOf(WorkerFailure.class);guard.close();guard=InstallationHostGuard.acquire(binding);var w=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var registered=w.register(guard,PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());assertThat(registered.status()).isEqualTo("READY");var next=w.claim(guard,registered,UUID.randomUUID());assertThat(next.status()).isEqualTo("CLAIMED");assertThat(next.runId()).isEqualTo(queued);assertThat(slot().get("epoch")).isEqualTo(3L);
        var occupied=slot();assertThat(finalizer().finish(guard,q,freshReview())).isEqualTo(receipt);assertThat(slot()).isEqualTo(occupied);assertThat(occupied.get("row_version")).isNotEqualTo(released.get("row_version"));assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void knownChangedPublicationClosesFailedWithoutRepair()throws Exception {
        UUID cp=UUID.randomUUID();var f=new PreflightFiles(guard,run,owner.attemptId(),cp,"VERIFY_RESULT");var c=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","ownerEpoch","itemOrdinal","capturedConfigSha256","runtime"))c.set(key,originalPack.get("command").get(key));c.put("stage","VERIFY_RESULT").put("checkpointId",cp.toString()).put("retainedSha256",SourceJson.sha(SourceJson.canonical(originalPack))).put("nativeFingerprint",fingerprint());byte[] raw=SourceJson.canonical(c);f.write("command.json",raw,PreflightFiles.SMALL);f.write("retained.json",SourceJson.canonical(originalPack),PreflightFiles.LARGE);
        var frozen=originalPack.get("bundle").get("capturedSelection");var runtime=ReliabilityRuntime.load(jdbc,tx,guard,owner,RUNTIME,frozen);var child=new ReadOnlyVerifierProcess().run(runtime.command(f.directory.resolve("command.json"),true),runtime.environment(),runtime.scratch(),Duration.ofSeconds(30),new ReadOnlyVerifierProcess.Observer(){public void beforeStart(){}public void started(InstallationHostGuard.ProcessIdentity c){}public void heartbeat(){}});assertThat(child.exitCode()).isZero();byte[] e=f.read("evidence.json",PreflightFiles.LARGE);var anchor=SourceJson.object().put("evidenceSha256",SourceJson.sha(e)).put("commandSha256",SourceJson.sha(raw)).put("resultSha256",SourceJson.sha(f.read("result.json",PreflightFiles.SMALL))).put("cleanup","OBSERVED_CLEAN_EXIT");byte[] validated=SourceJson.canonical(anchor);f.write("verified.json",validated,PreflightFiles.SMALL);
        jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256,intended_identity,command_address,command_sha256,result_address,result_sha256,finished_at) VALUES (?,'VERIFY_RESULT',0,?,'VERIFIED',?,?,?,?,?,?,now())",owner.attemptId(),cp,SourceJson.sha(raw),fingerprint(),f.address+"/command.json",SourceJson.sha(raw),f.address+"/verified.json",SourceJson.sha(validated));
        Path marker=publication().resolve("complete.json");var complete=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(Files.readAllBytes(marker));complete.put("completed_at","2026-10-25T00:00:00.000000Z");Files.write(marker,SourceJson.canonical(complete));var before=nativeSnapshot();var q=qualified();var result=finalizer().finish(guard,q,freshReview());released(result,"FAILED","PUBLICATION_CHANGED",false);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"launch","cleanup"}) void ambiguousFinalQualificationCannotBeRelaunched(String mode)throws Exception {
        handoff();observed=recoveryService().verify(guard,bootstrap,review);var before=nativeSnapshot();var calls=new AtomicInteger();var p=new ReadOnlyVerifierProcess(){@Override Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer o){calls.incrementAndGet();o.beforeStart();if(mode.equals("launch"))throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_UNAVAILABLE);o.started(new InstallationHostGuard.ProcessIdentity(UUID.randomUUID(),oldPid,oldStart));throw new PreflightFailure(PreflightFailure.Code.TERMINATION_UNCONFIRMED,false);}};
        var svc=new RecoveryFinalization(jdbc,tx,handoffs,recoveryService(p),step->{});assertThatThrownBy(()->svc.qualify(guard,bootstrap,observed,freshReview())).isInstanceOfAny(ManagementFailure.class,RecoveryFailure.class);assertThatThrownBy(()->svc.qualify(guard,bootstrap,observed,freshReview())).hasMessage("OWNERSHIP_LOST");assertThat(calls.get()).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=?",Long.class,control)).isEqualTo(2);stillBlocked();assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void terminalRecoveryHistoryExposesOnlyAllowlistedSummaries()throws Exception {
        var q=qualified();var result=finalizer().finish(guard,q,freshReview());var history=new RunHistoryRepository(jdbc,tx,PreflightCatalogFixture.cursor());
        assertThat(history.get(run.toString()).get("outcomeCode").stringValue()).isEqualTo("RECOVERED_PUBLICATION");assertThat(history.attempts(run.toString(),100,null).data().size()).isEqualTo(2);var events=history.events(run.toString(),100,null);assertThat(events.data().toString()).contains("RECOVERY_FINALIZED").doesNotContain("nativeSealSha256",work.toString());assertThat(result.artifactId()).isNotNull();
    }
}
