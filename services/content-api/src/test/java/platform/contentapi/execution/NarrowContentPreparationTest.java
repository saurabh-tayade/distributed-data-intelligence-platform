package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.execution.NarrowContentPreparationFixtures.*;

class NarrowContentPreparationTest {
    @TempDir Path temp;

    @Test void preparesOnlyFixedLedgerAndRepeatedStrictReadsDoNotChangeEvidence()throws Exception{
        var host=OwnershipFixtures.host(temp.toRealPath());var spec=specification(host);
        try(var guard=InstallationHostGuard.acquire(host)){
            var events=new ArrayList<NarrowContentPreparation.Boundary>();
            var prepared=NarrowContentPreparation.prepare(guard,spec,(boundary,path)->events.add(boundary));
            var expected=prepared.expected();Path root=root(spec);
            assertThat(expected.run()).isEqualTo(spec.run());assertThat(expected.attempt()).isEqualTo(spec.attempt());
            assertThat(expected.source()).isEqualTo(spec.source());assertThat(expected.frozen()).isEqualTo(FROZEN);
            assertThat(prepared.independentlySupplied()).isEqualTo(FROZEN);
            assertThat(expected.ledgerSha256()).isEqualTo(SourceJson.sha(Files.readAllBytes(root.resolve(ContentEvidenceBudget.MANIFEST))));
            try(var paths=Files.walk(root)){assertThat(paths.count()).isEqualTo(3);}
            assertThat(events).containsAll(Arrays.asList(NarrowContentPreparation.Boundary.values()));
            assertThat(Collections.frequency(events,NarrowContentPreparation.Boundary.ANCESTOR_FORCED)).isEqualTo(5);
            var before=snapshot(host.root());
            for(int i=0;i<2;i++){
                var totals=new NarrowContentEvidenceContext(expected).inspect(guard,FROZEN);
                assertThat(totals.reservedBytes()).isEqualTo(ContentEvidenceBudget.BYTES);
                assertThat(totals.reservedEntries()).isEqualTo(221);
            }
            assertThatThrownBy(()->NarrowContentPreparation.prepare(guard,spec)).hasMessage("INPUT_CHANGED");
            assertThat(snapshot(host.root())).isEqualTo(before);
            assertThat(spec.toString()).isEqualTo("NarrowContentPreparationSpecification[redacted]");
            assertThat(prepared.toString()).isEqualTo("NarrowContentPrepared[redacted]");
        }
    }

    @ParameterizedTest @EnumSource(NarrowContentPreparation.Boundary.class)
    void everyInterruptedBoundaryRetainsAllCreatedEvidence(NarrowContentPreparation.Boundary stop)throws Exception{
        var host=OwnershipFixtures.host(temp.toRealPath());var spec=specification(host);
        var atFailure=new AtomicReference<Map<String,Entry>>();
        try(var guard=InstallationHostGuard.acquire(host)){
            assertThatThrownBy(()->NarrowContentPreparation.prepare(guard,spec,(boundary,path)->{
                if(boundary==stop){
                    try{atFailure.set(snapshot(host.root()));}catch(Exception ex){throw new AssertionError(ex);}
                    throw new PreflightFailure(PreflightFailure.Code.INTERRUPTED);
                }
            })).hasMessage("INTERRUPTED");
            assertThat(atFailure.get()).isNotNull();
            assertThat(snapshot(host.root())).isEqualTo(atFailure.get());
            if(Files.exists(root(spec),LinkOption.NOFOLLOW_LINKS)){
                assertThatThrownBy(()->NarrowContentPreparation.prepare(guard,spec)).hasMessage("INPUT_CHANGED");
                assertThat(snapshot(host.root())).isEqualTo(atFailure.get());
            }else{
                // The durable admission layer, not an absent filesystem marker, forbids restart retries.
                assertThat(stop).isIn(NarrowContentPreparation.Boundary.ANCESTOR_CREATED,NarrowContentPreparation.Boundary.ANCESTOR_PARENT_FORCED);
            }
        }
    }

    @ParameterizedTest @ValueSource(strings={"empty","legacy","narrow","file","symlink"})
    void neverAdoptsAnyExistingAttemptRoot(String kind)throws Exception{
        var host=OwnershipFixtures.host(temp.toRealPath());var spec=specification(host);Path root=root(spec);
        try(var guard=InstallationHostGuard.acquire(host)){
            if(kind.equals("legacy"))ContentEvidenceBudget.initialize(guard,spec.run(),spec.attempt());
            else if(kind.equals("narrow"))NarrowContentPreparation.prepare(guard,spec);
            else{
                Files.createDirectories(root.getParent());
                if(kind.equals("empty"))Files.createDirectory(root);
                else if(kind.equals("file"))Files.writeString(root,"retain this exact original");
                else Files.createSymbolicLink(root,Files.createDirectory(temp.resolve("unrelated")));
            }
            var before=snapshot(host.root());
            assertThatThrownBy(()->NarrowContentPreparation.prepare(guard,spec)).hasMessage("INPUT_CHANGED");
            assertThat(snapshot(host.root())).isEqualTo(before);
        }
    }

    @Test void validatesSpecificationAndHostBeforeCreatingAnyPath()throws Exception{
        var host=OwnershipFixtures.host(temp.toRealPath());var spec=specification(host);var before=snapshot(host.root());
        try(var guard=InstallationHostGuard.acquire(host)){
            for(String source:List.of("","../escape","UPPER","a".repeat(129)))
                assertThatThrownBy(()->new NarrowContentPreparation.Specification(spec.policy(),spec.run(),spec.attempt(),source,host,FROZEN)).hasMessage("INPUT_INVALID");
            assertThatThrownBy(()->new NarrowContentPreparation.Specification("legacy",spec.run(),spec.attempt(),spec.source(),host,FROZEN)).hasMessage("INPUT_INVALID");
            assertThatThrownBy(()->NarrowContentPreparation.prepare(guard,null)).hasMessage("INPUT_INVALID");
            var other=new InstallationHostGuard.Binding("other",host.installationId(),host.root(),host.rootIdentity(),host.guardIdentity(),host.installationIdentity());
            var wrong=new NarrowContentPreparation.Specification(spec.policy(),spec.run(),spec.attempt(),spec.source(),other,FROZEN);
            assertThatThrownBy(()->NarrowContentPreparation.prepare(guard,wrong)).hasMessage("RUNTIME_MISMATCH");
            assertThat(snapshot(host.root())).isEqualTo(before);
        }
    }

    @Test void existingAncestorsAreCheckedAndAllAreForced()throws Exception{
        var host=OwnershipFixtures.host(temp.toRealPath());var spec=specification(host);Path runs=host.root().resolve("runs");
        Files.createDirectory(runs,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        try(var guard=InstallationHostGuard.acquire(host)){
            var forced=new ArrayList<Path>();
            NarrowContentPreparation.prepare(guard,spec,(boundary,path)->{if(boundary==NarrowContentPreparation.Boundary.ANCESTOR_FORCED)forced.add(path);});
            assertThat(forced).containsExactly(root(spec),root(spec).getParent(),root(spec).getParent().getParent(),runs,host.root());
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void partialLedgerBytesAreRetainedWithoutRepair(boolean fixed)throws Exception{
        var host=OwnershipFixtures.host(temp.toRealPath());var spec=specification(host);
        var stop=fixed?NarrowContentPreparation.Boundary.FIXED_PREFIX_CREATED:NarrowContentPreparation.Boundary.INITIAL_PREFIX_CREATED;
        try(var guard=InstallationHostGuard.acquire(host)){
            assertThatThrownBy(()->NarrowContentPreparation.prepare(guard,spec,(boundary,root)->{
                if(boundary==stop){
                    try{Files.writeString(root.resolve(ContentEvidenceBudget.MANIFEST+".preparing"),"{\"version\":");}
                    catch(Exception ex){throw new AssertionError(ex);}
                    throw new PreflightFailure(PreflightFailure.Code.INTERRUPTED);
                }
            })).hasMessage("INTERRUPTED");
            assertThat(Files.readString(root(spec).resolve(ContentEvidenceBudget.MANIFEST+".preparing"))).isEqualTo("{\"version\":");
            var before=snapshot(host.root());
            assertThatThrownBy(()->NarrowContentPreparation.prepare(guard,spec)).hasMessage("INPUT_CHANGED");
            assertThat(snapshot(host.root())).isEqualTo(before);
            assertThat(Files.exists(root(spec).resolve(ContentEvidenceBudget.MANIFEST))).isEqualTo(fixed);
        }
    }

    @ParameterizedTest @ValueSource(strings={"permissions","symlink"})
    void unsafeExistingAncestorIsRefusedBeforeAnyAttemptPathIsCreated(String change)throws Exception{
        var host=OwnershipFixtures.host(temp.toRealPath());var spec=specification(host);Path runs=host.root().resolve("runs");
        if(change.equals("symlink"))Files.createSymbolicLink(runs,Files.createDirectory(temp.resolve("outside")));
        else Files.createDirectory(runs,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-x---")));
        var before=snapshot(host.root());
        try(var guard=InstallationHostGuard.acquire(host)){
            assertThatThrownBy(()->NarrowContentPreparation.prepare(guard,spec)).hasMessage("INPUT_INVALID");
            assertThat(snapshot(host.root())).isEqualTo(before);
            assertThat(Files.exists(root(spec),LinkOption.NOFOLLOW_LINKS)).isFalse();
        }
    }

    @Test void closedHostGuardIsRefusedWithoutPrivateDiagnosticsOrWrites()throws Exception{
        var host=OwnershipFixtures.host(temp.toRealPath());var spec=specification(host);var before=snapshot(host.root());
        var guard=InstallationHostGuard.acquire(host);guard.close();
        assertThatThrownBy(()->NarrowContentPreparation.prepare(guard,spec)).isInstanceOf(PreflightFailure.class).hasMessage("INPUT_CHANGED").hasNoCause();
        assertThat(snapshot(host.root())).isEqualTo(before);
    }

    @ParameterizedTest @ValueSource(strings={"ledger","lock","root","unexpected","prefix"})
    void finalIndependentInspectionRejectsChangedPreparedEvidenceWithoutRepair(String change)throws Exception{
        var host=OwnershipFixtures.host(temp.toRealPath());var spec=specification(host);
        var changed=new AtomicReference<Map<String,Entry>>();
        try(var guard=InstallationHostGuard.acquire(host)){
            assertThatThrownBy(()->NarrowContentPreparation.prepare(guard,spec,(boundary,root)->{
                if(boundary!=NarrowContentPreparation.Boundary.BEFORE_INSPECTION)return;
                try{
                    if(change.equals("ledger")){
                        var n=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(Files.readAllBytes(root.resolve(ContentEvidenceBudget.MANIFEST)),ContentEvidenceBudget.LEDGER);
                        ContentOperationPlan.original(root,spec.run(),spec.attempt(),"other-source").apply(n);
                        Files.write(root.resolve(ContentEvidenceBudget.MANIFEST),SourceJson.canonical(n));
                    }else if(change.equals("lock")){
                        Files.move(root.resolve(ContentEvidenceBudget.LOCK),temp.resolve("retained-lock"));
                        Files.createFile(root.resolve(ContentEvidenceBudget.LOCK),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
                    }else if(change.equals("root")){
                        Files.move(root,temp.resolve("retained-root"));
                        Files.createDirectory(root,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
                    }else if(change.equals("unexpected"))Files.createDirectory(root.resolve("unplanned"),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
                    else Files.createFile(root.resolve(ContentEvidenceBudget.MANIFEST+".preparing"),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
                    changed.set(snapshot(host.root()));
                }catch(Exception ex){throw new AssertionError(ex);}
            })).hasMessage("INPUT_CHANGED");
            assertThat(snapshot(host.root())).isEqualTo(changed.get());
        }
    }
}
