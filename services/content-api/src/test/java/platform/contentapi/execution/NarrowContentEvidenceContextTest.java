package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.execution.NarrowContentEvidenceContext.*;

class NarrowContentEvidenceContextTest {
    @TempDir Path temp;
    InstallationHostGuard guard;UUID run=UUID.randomUUID(),attempt=UUID.randomUUID();Path root;
    final FrozenIdentities frozen=new FrozenIdentities("a".repeat(64),"b".repeat(64),"c".repeat(64));
    @BeforeEach void setup()throws Exception {
        guard=InstallationHostGuard.acquire(OwnershipFixtures.host(temp.toRealPath()));
        ContentEvidenceBudget.initialize(guard,run,attempt);root=guard.binding().root().resolve("runs/"+run+"/attempts/"+attempt);
        ContentOperationPlan.original(root,run,attempt,"source").reserve();
    }
    @AfterEach void close(){guard.close();}
    byte[] ledger()throws Exception{return Files.readAllBytes(root.resolve(ContentEvidenceBudget.MANIFEST));}
    Expected expected()throws Exception {
        return new Expected(NarrowContentProfile.POLICY,run,attempt,PlanKind.ORIGINAL,"source",guard.binding(),
                InstallationHostGuard.identity(root),InstallationHostGuard.identity(root.resolve(ContentEvidenceBudget.LOCK)),frozen,SourceJson.sha(ledger()));
    }
    Expected alter(Expected e,String policy,UUID r,UUID a,PlanKind kind,String source,FrozenIdentities ids,String hash){
        return new Expected(policy,r,a,kind,source,e.host(),e.rootIdentity(),e.lockIdentity(),ids,hash);
    }
    @Test void narrowInspectionIsReadOnlyAndReturnsNoAuthority()throws Exception {
        var e=expected();var context=new NarrowContentEvidenceContext(e);byte[] before=ledger();
        var time=Files.getLastModifiedTime(root.resolve(ContentEvidenceBudget.MANIFEST));
        var total=context.inspect(guard,frozen);
        assertThat(total.reservedBytes()).isEqualTo(64L*1024*1024);assertThat(total.reservedEntries()).isEqualTo(221);
        assertThat(ledger()).isEqualTo(before);assertThat(Files.getLastModifiedTime(root.resolve(ContentEvidenceBudget.MANIFEST))).isEqualTo(time);
        assertThat(context.toString()).isEqualTo("NarrowContentEvidenceContext[redacted]");
        assertThat(e.toString()).isEqualTo("NarrowContentExpectation[redacted]");assertThat(frozen.toString()).isEqualTo("FrozenIdentities[redacted]");
    }
    @Test void independentlySuppliedFrozenIdentityTupleMustMatchEveryOpaqueComponent()throws Exception {
        var context=new NarrowContentEvidenceContext(expected());
        for(var supplied:List.of(new FrozenIdentities("d".repeat(64),frozen.profileSha256(),frozen.runtimeSha256()),
                new FrozenIdentities(frozen.selectionSha256(),"d".repeat(64),frozen.runtimeSha256()),
                new FrozenIdentities(frozen.selectionSha256(),frozen.profileSha256(),"d".repeat(64))))
            assertThatThrownBy(()->context.inspect(guard,supplied)).hasMessage("RUNTIME_MISMATCH");
        assertThatThrownBy(()->context.inspect(guard,null)).hasMessage("RUNTIME_MISMATCH");
    }
    @Test void policyAndContextFieldsHaveStrictShapesWithoutLearningThemFromDisk()throws Exception {
        var e=expected();
        for(String invalid:new String[]{"", "DDIP-CONTENT-NARROW-V1", "legacy", "ddip-content-narrow-v2"})
            assertThatThrownBy(()->alter(e,invalid,run,attempt,PlanKind.ORIGINAL,"source",frozen,e.ledgerSha256())).hasMessage("INPUT_INVALID");
        for(String hash:new String[]{"", "a".repeat(63), "a".repeat(65), "A".repeat(64), "private"}){
            assertThatThrownBy(()->new FrozenIdentities(hash,frozen.profileSha256(),frozen.runtimeSha256())).hasMessage("INPUT_INVALID");
            assertThatThrownBy(()->alter(e,e.policy(),run,attempt,e.kind(),e.source(),frozen,hash)).hasMessage("INPUT_INVALID");
        }
        for(String source:new String[]{"", "../other", "Upper", "x".repeat(129)})
            assertThatThrownBy(()->alter(e,e.policy(),run,attempt,PlanKind.ORIGINAL,source,frozen,e.ledgerSha256())).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->alter(e,e.policy(),run,attempt,PlanKind.RECOVERY,"source",frozen,e.ledgerSha256())).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->alter(e,e.policy(),null,attempt,e.kind(),e.source(),frozen,e.ledgerSha256())).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->new NarrowContentEvidenceContext(null)).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->ContentEvidenceBudget.inspect(null,guard,frozen)).hasMessage("INPUT_INVALID");
    }
    @Test void exactLedgerHashAndExpectedPlanAreBothRequired()throws Exception {
        var e=expected();
        assertThatThrownBy(()->new NarrowContentEvidenceContext(alter(e,e.policy(),run,attempt,e.kind(),e.source(),frozen,"d".repeat(64))).inspect(guard,frozen)).hasMessage("INPUT_CHANGED");
        assertThatThrownBy(()->new NarrowContentEvidenceContext(alter(e,e.policy(),run,attempt,PlanKind.RECOVERY,null,frozen,e.ledgerSha256())).inspect(guard,frozen)).hasMessage("INPUT_CHANGED");
        assertThatThrownBy(()->new NarrowContentEvidenceContext(alter(e,e.policy(),run,attempt,e.kind(),"other",frozen,e.ledgerSha256())).inspect(guard,frozen)).hasMessage("INPUT_CHANGED");
        var coherent=(ObjectNode)SourceJson.parse(ledger(),ContentEvidenceBudget.LEDGER);
        ContentOperationPlan.original(root,run,attempt,"other").apply(coherent);
        Files.write(root.resolve(ContentEvidenceBudget.MANIFEST),SourceJson.canonical(coherent));
        var updatedHash=alter(e,e.policy(),run,attempt,e.kind(),e.source(),frozen,SourceJson.sha(ledger()));
        assertThatThrownBy(()->new NarrowContentEvidenceContext(updatedHash).inspect(guard,frozen)).hasMessage("INPUT_CHANGED");
    }
    @Test void retainedLedgerPrefixIsRefusedWithoutChangingLegacyInspection()throws Exception {
        var context=new NarrowContentEvidenceContext(expected());byte[] before=ledger();
        Path pending=root.resolve(ContentEvidenceBudget.MANIFEST+".preparing");Files.writeString(pending,"{");
        Files.setPosixFilePermissions(pending,PosixFilePermissions.fromString("rw-------"));
        ContentEvidenceBudget.inspect(root);
        assertThatThrownBy(()->context.inspect(guard,frozen)).hasMessage("INPUT_CHANGED");
        assertThat(ledger()).isEqualTo(before);assertThat(Files.readString(pending)).isEqualTo("{");
    }
    @Test void rootAndLockIdentityAreIndependentOfCoherentLedgerContents()throws Exception {
        var e=expected();
        var wrongRoot=new Expected(e.policy(),run,attempt,e.kind(),e.source(),e.host(),new InstallationHostGuard.Identity(0,0),e.lockIdentity(),frozen,e.ledgerSha256());
        assertThatThrownBy(()->new NarrowContentEvidenceContext(wrongRoot).inspect(guard,frozen)).hasMessage("INPUT_CHANGED");
        var wrongLock=new Expected(e.policy(),run,attempt,e.kind(),e.source(),e.host(),e.rootIdentity(),new InstallationHostGuard.Identity(0,0),frozen,e.ledgerSha256());
        assertThatThrownBy(()->new NarrowContentEvidenceContext(wrongLock).inspect(guard,frozen)).hasMessage("INPUT_CHANGED");
        Path lock=root.resolve(ContentEvidenceBudget.LOCK),retained=temp.resolve("retained.lock");Files.move(lock,retained);
        Files.createFile(lock,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        assertThatThrownBy(()->new NarrowContentEvidenceContext(e).inspect(guard,frozen)).hasMessage("INPUT_CHANGED");
    }
    @Test void changedAncestorPermissionsAndAliasedMarkersAreRejected()throws Exception {
        var context=new NarrowContentEvidenceContext(expected());Path ancestor=root.getParent();
        Files.setPosixFilePermissions(ancestor,PosixFilePermissions.fromString("rwxr-x---"));
        assertThatThrownBy(()->context.inspect(guard,frozen)).hasMessage("INPUT_INVALID");
        Files.setPosixFilePermissions(ancestor,PosixFilePermissions.fromString("rwx------"));
        Path manifest=root.resolve(ContentEvidenceBudget.MANIFEST),retained=temp.resolve("retained.json");Files.move(manifest,retained);Files.createSymbolicLink(manifest,retained);
        assertThatThrownBy(()->context.inspect(guard,frozen)).hasMessage("INPUT_INVALID");
    }
    @Test void unexpectedTreeEntriesAndOversizedActualSlotsRemainRejected()throws Exception {
        var context=new NarrowContentEvidenceContext(expected());Path extra=root.resolve("unplanned");
        Files.createDirectory(extra,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        assertThatThrownBy(()->context.inspect(guard,frozen)).hasMessage("INPUT_CHANGED");Files.delete(extra);
        var plan=ContentOperationPlan.original(root,run,attempt,"source");Path operation=plan.directory("CHECK_INPUTS",0);
        Files.createDirectories(operation);Files.setPosixFilePermissions(operation.getParent(),PosixFilePermissions.fromString("rwx------"));Files.setPosixFilePermissions(operation,PosixFilePermissions.fromString("rwx------"));
        Path result=operation.resolve("result.json");Files.write(result,new byte[65537]);Files.setPosixFilePermissions(result,PosixFilePermissions.fromString("rw-------"));
        assertThatThrownBy(()->context.inspect(guard,frozen)).hasMessage("INPUT_CHANGED");
    }
    @Test void liveFetchInspectionAllowsOnlyItsExactActivePrefixAndNeverCompletion()throws Exception {
        var context=new NarrowContentEvidenceContext(expected());
        Path fetch=ContentOperationPlan.original(root,run,attempt,"source").directory("FETCH_SOURCE",0);
        ContentEvidenceBudget.inspectFetch(context,guard,fetch,false);
        for(Path directory:List.of(fetch.getParent().getParent(),fetch.getParent(),fetch)){
            Files.createDirectory(directory,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            ContentEvidenceBudget.inspectFetch(context,guard,fetch,false);
        }
        Path prefix=Files.createFile(fetch.resolve("result.json.preparing"),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        assertThatThrownBy(()->ContentEvidenceBudget.inspectFetch(context,guard,fetch,false)).hasMessage("INPUT_CHANGED");
        ContentEvidenceBudget.inspectFetch(context,guard,fetch,true);
        Files.delete(prefix);
        Files.createFile(fetch.resolve("complete.json"),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        assertThatThrownBy(()->ContentEvidenceBudget.inspectFetch(context,guard,fetch,true)).hasMessage("INPUT_CHANGED");
        Files.delete(fetch.resolve("complete.json"));
        Path foreign=Files.createDirectory(root.resolve("check-inputs"),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        assertThatThrownBy(()->ContentEvidenceBudget.inspectFetch(context,guard,fetch,true)).hasMessage("INPUT_CHANGED");
    }
    @Test void lostHostGuardNeverBecomesInspectionAuthority()throws Exception {
        var context=new NarrowContentEvidenceContext(expected());guard.close();
        assertThatThrownBy(()->context.inspect(guard,frozen)).hasMessage("INPUT_CHANGED");
    }
}
