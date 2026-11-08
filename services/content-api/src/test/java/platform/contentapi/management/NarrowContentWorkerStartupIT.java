package platform.contentapi.management;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;

/** Production startup qualification only; no ordinary dispatch or packaged API acceptance claim. */
class NarrowContentWorkerStartupIT extends NarrowContentPipelineFixtures {
    @BeforeEach @Override void pipelineProfile() { /* Registration is controlled before immutable insertion below. */ }
    @BeforeEach void startupProfile(TestInfo test)throws Exception {
        if(!test.getTestMethod().orElseThrow().getName().equals("missingStrictRegistrationFailsWithoutCreatingAuthority"))super.pipelineProfile();
    }
    void qualify(String strict,java.util.function.BooleanSupplier stopping) {
        NarrowContentWorkerStartupTestSupport.qualify(jdbc,tx,guard,CONTENT_PROFILE,digest(CONTENT_PROFILE),
                NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,CONTENT_PROFILE),strict,stopping);
    }
    void noAuthority() {
        for(String table:List.of("runs","attempts","worker_instances","content_attempt_admissions","content_admission_milestones",
                "content_native_operations","content_pipeline_operations","stage_invocations","rss_deliveries"))assertThat(count(table)).as(table).isZero();
        assertThat(requests).hasValue(0);assertThat(objectRequests).hasValue(0);
    }
    @Test void exactStartupIsReadOnlyAndNeverManufacturesRunBoundAuthority()throws Exception {
        guard=InstallationHostGuard.acquire(binding);var sql=metadata();var files=filesystem();
        qualify(PIPELINE_PROFILE,()->false);
        assertThat(metadata()).isEqualTo(sql);assertThat(filesystem()).isEqualTo(files);noAuthority();
    }
    @Test void missingStrictRegistrationFailsWithoutCreatingAuthority()throws Exception {
        guard=InstallationHostGuard.acquire(binding);var sql=metadata();var files=filesystem();
        assertThat(count("content_pipeline_profiles")).isZero();assertThatThrownBy(()->qualify(PIPELINE_PROFILE,()->false)).isInstanceOf(PreflightFailure.class);
        assertThat(metadata()).isEqualTo(sql);assertThat(filesystem()).isEqualTo(files);noAuthority();
    }
    @ParameterizedTest @ValueSource(strings={"wrong-id","duplicate","manifest-missing","manifest-changed","storage-identity","credentials-missing","credentials-malformed"})
    void startupMismatchFailsClosedBeforeAnyNativeEffect(String mismatch)throws Exception {
        String selected=PIPELINE_PROFILE;
        switch(mismatch) {
            case "wrong-id" -> selected="unregistered-strict-runtime";
            case "duplicate" -> jdbc.update("INSERT INTO ddip_management.content_pipeline_profiles(profile_id,configuration_bytes,configuration_sha256) SELECT 'duplicate',configuration_bytes,configuration_sha256 FROM ddip_management.content_pipeline_profiles WHERE profile_id=?",PIPELINE_PROFILE);
            case "manifest-missing" -> Files.delete(installedFetchJar);
            case "manifest-changed" -> Files.write(installedFetchJar,new byte[]{1},StandardOpenOption.APPEND);
            case "storage-identity" -> {Path root=roots.get(SILVER_BINDING);Files.move(root,root.resolveSibling(root.getFileName()+"-old"));Files.createDirectory(root);}
            case "credentials-missing" -> Files.delete(credentials);
            case "credentials-malformed" -> Files.writeString(credentials,"{}");
        }
        guard=InstallationHostGuard.acquire(binding);var sql=metadata();var files=filesystem();String strict=selected;
        assertThatThrownBy(()->qualify(strict,()->false)).isInstanceOf(PreflightFailure.class);
        assertThat(metadata()).isEqualTo(sql);assertThat(filesystem()).isEqualTo(files);noAuthority();
    }
    @Test void stopAlreadyRequestedPreventsStartupProbe()throws Exception {
        guard=InstallationHostGuard.acquire(binding);var sql=metadata();var files=filesystem();
        assertThatThrownBy(()->qualify(PIPELINE_PROFILE,()->true)).isInstanceOf(PreflightFailure.class).hasMessage("INTERRUPTED");
        assertThat(metadata()).isEqualTo(sql);assertThat(filesystem()).isEqualTo(files);noAuthority();
    }
    @Test void stopWithLiveStartupProbeCleansChildWithoutCreatingAuthority()throws Exception {
        guard=InstallationHostGuard.acquire(binding);var sql=metadata();var files=filesystem();var childPid=new AtomicLong();
        var failure=catchThrowable(()->qualify(PIPELINE_PROFILE,()->{
            ProcessHandle.current().children().filter(p->p.info().commandLine().orElse("").contains("strict_native_content"))
                    .findFirst().ifPresent(p->childPid.compareAndSet(0,p.pid()));
            return childPid.get()!=0;
        }));
        assertThat(childPid.get()).as("Observed actual startup Python probe").isPositive();
        assertThat(failure).isInstanceOf(PreflightFailure.class);
        assertThat(ProcessHandle.of(childPid.get()).map(ProcessHandle::isAlive).orElse(false)).isFalse();
        assertThat(metadata()).isEqualTo(sql);assertThat(filesystem()).isEqualTo(files);noAuthority();
    }
    @ParameterizedTest @ValueSource(strings={"delete","change","mode-loss"})
    void credentialDriftDuringActualProbeInvalidatesReadiness(String drift)throws Exception {
        guard=InstallationHostGuard.acquire(binding);var sql=metadata();var files=filesystem();var childPid=new AtomicLong();
        var failure=catchThrowable(()->qualify(PIPELINE_PROFILE,()->{
            var child=ProcessHandle.current().children().filter(p->p.info().commandLine().orElse("").contains("strict_native_content")).findFirst();
            if(child.isPresent() && childPid.compareAndSet(0,child.get().pid()))try{
                switch(drift){
                    case "delete" -> Files.delete(credentials);
                    case "change" -> Files.write(credentials,SourceJson.canonical(SourceJson.object().put("accessKey","fixture-access-replaced").put("secretKey","fixture-secret-replaced")));
                    case "mode-loss" -> Files.setPosixFilePermissions(credentials,java.nio.file.attribute.PosixFilePermissions.fromString("rw-r--r--"));
                }
            }catch(java.io.IOException ex){throw new java.io.UncheckedIOException(ex);}
            return false;
        }));
        assertThat(childPid.get()).as("Observed actual startup probe before credential mutation").isPositive();
        assertThat(failure).isInstanceOf(PreflightFailure.class);
        assertThat(ProcessHandle.of(childPid.get()).map(ProcessHandle::isAlive).orElse(false)).isFalse();
        assertThat(metadata()).isEqualTo(sql);assertThat(filesystem()).isEqualTo(files);noAuthority();
    }
}
