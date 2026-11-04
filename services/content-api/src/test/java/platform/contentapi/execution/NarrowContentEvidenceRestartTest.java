package platform.contentapi.execution;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.assertThat;

/** Fresh-JVM coverage of the inactive reader, not restart qualification of a production lane. */
class NarrowContentEvidenceRestartTest {
    @TempDir Path temp;

    private static final NarrowContentEvidenceContext.FrozenIdentities FROZEN=
            new NarrowContentEvidenceContext.FrozenIdentities("a".repeat(64),"b".repeat(64),"c".repeat(64));

    private record Fixture(InstallationHostGuard.Binding host,Path root,
                           NarrowContentEvidenceContext.Expected expected,
                           NarrowContentEvidenceContext.FrozenIdentities current,
                           ContentEvidenceBudget.Totals totals) {
        Fixture expected(NarrowContentEvidenceContext.Expected value) {
            return new Fixture(host,root,value,current,totals);
        }
        Fixture current(NarrowContentEvidenceContext.FrozenIdentities value) {
            return new Fixture(host,root,expected,value,totals);
        }
    }

    private Fixture fixture(NarrowContentEvidenceContext.PlanKind kind) throws Exception {
        var host=OwnershipFixtures.host(temp.toRealPath());
        var run=UUID.randomUUID();var attempt=UUID.randomUUID();
        Path root=host.root().resolve("runs/"+run+"/attempts/"+attempt);
        String source=kind==NarrowContentEvidenceContext.PlanKind.ORIGINAL?"source-one":null;
        try(var guard=InstallationHostGuard.acquire(host)) {
            ContentEvidenceBudget.initialize(guard,run,attempt);
            if(kind==NarrowContentEvidenceContext.PlanKind.ORIGINAL)
                ContentOperationPlan.original(root,run,attempt,source).reserve();
            else ContentOperationPlan.recovery(root,run,attempt).reserve();
            var expected=new NarrowContentEvidenceContext.Expected(NarrowContentProfile.POLICY,run,attempt,kind,
                    source,host,InstallationHostGuard.identity(root),
                    InstallationHostGuard.identity(root.resolve(ContentEvidenceBudget.LOCK)),FROZEN,
                    SourceJson.sha(Files.readAllBytes(root.resolve(ContentEvidenceBudget.MANIFEST))));
            // Expectations and totals are captured before any adversarial change, then the guard closes.
            return new Fixture(host,root,expected,FROZEN,ContentEvidenceBudget.totals(root));
        }
    }

    @ParameterizedTest @EnumSource(NarrowContentEvidenceContext.PlanKind.class)
    void cleanOriginalAndRecoveryAreInspectedWithoutInitializationInFreshJvm(
            NarrowContentEvidenceContext.PlanKind kind) throws Exception {
        var fixture=fixture(kind);var totals=fixture.totals();
        assertThat(inspect(fixture)).isEqualTo("OK "+totals.actualBytes()+" "+totals.actualEntries()+" "
                +totals.reservedBytes()+" "+totals.reservedEntries()+" "+totals.inventoryMaximum());
    }

    @ParameterizedTest @ValueSource(strings={"root","manifest","lock","both-markers"})
    void missingEvidenceIsRejectedAndNeverRecreatedInFreshJvm(String missing) throws Exception {
        var fixture=fixture(NarrowContentEvidenceContext.PlanKind.ORIGINAL);
        switch(missing) {
            case "root" -> {
                try(var tree=Files.walk(fixture.root())) {
                    for(Path path:tree.sorted(Comparator.reverseOrder()).toList())Files.delete(path);
                }
            }
            case "manifest" -> Files.delete(fixture.root().resolve(ContentEvidenceBudget.MANIFEST));
            case "lock" -> Files.delete(fixture.root().resolve(ContentEvidenceBudget.LOCK));
            default -> {
                Files.delete(fixture.root().resolve(ContentEvidenceBudget.MANIFEST));
                Files.delete(fixture.root().resolve(ContentEvidenceBudget.LOCK));
            }
        }
        assertThat(inspect(fixture)).matches("REJECTED (INPUT_CHANGED|INPUT_UNAVAILABLE)");
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void validLegacyDowngradeIsRejectedEvenWithCoherentlyUpdatedHash(boolean updateHash) throws Exception {
        var fixture=fixture(NarrowContentEvidenceContext.PlanKind.ORIGINAL);
        var ledger=ledger(fixture);ledger.put("version",1);
        ledger.remove("reservationPolicy");ledger.remove("planKind");ledger.remove("sourceId");
        writeLedger(fixture,ledger);
        // Establish that the self-described legacy reader accepts this otherwise valid v1 ledger.
        assertThat(ContentEvidenceBudget.inspect(fixture.root()).reservedBytes()).isEqualTo(fixture.totals().reservedBytes());
        if(updateHash)fixture=updatedHash(fixture);
        assertThat(inspect(fixture)).isEqualTo("REJECTED INPUT_CHANGED");
    }

    @ParameterizedTest @CsvSource({"source,false","source,true","plan,false","plan,true"})
    void coherentAlternateSourceOrPlanCannotReplaceIndependentExpectation(String change,boolean updateHash) throws Exception {
        var fixture=fixture(NarrowContentEvidenceContext.PlanKind.ORIGINAL);
        var expected=fixture.expected();var ledger=ledger(fixture);
        var alternate=change.equals("source")
                ?ContentOperationPlan.original(fixture.root(),expected.run(),expected.attempt(),"source-two")
                :ContentOperationPlan.recovery(fixture.root(),expected.run(),expected.attempt());
        alternate.apply(ledger);writeLedger(fixture,ledger);
        // The altered reservation grammar is internally coherent, not merely malformed JSON.
        ContentEvidenceBudget.inspect(fixture.root());
        if(updateHash)fixture=updatedHash(fixture);
        assertThat(inspect(fixture)).isEqualTo("REJECTED INPUT_CHANGED");
    }

    @Test void substitutedBindingIsRejectedEvenWhenFilesystemIdentitiesMatch() throws Exception {
        var fixture=fixture(NarrowContentEvidenceContext.PlanKind.ORIGINAL);var host=fixture.host();
        var alternate=new InstallationHostGuard.Binding("different-binding",host.installationId(),host.root(),
                host.rootIdentity(),host.guardIdentity(),host.installationIdentity());
        var e=fixture.expected();
        fixture=fixture.expected(new NarrowContentEvidenceContext.Expected(e.policy(),e.run(),e.attempt(),e.kind(),
                e.source(),alternate,e.rootIdentity(),e.lockIdentity(),e.frozen(),e.ledgerSha256()));
        assertThat(inspect(fixture)).isEqualTo("REJECTED RUNTIME_MISMATCH");
    }

    @Test void replacementRootWithCoherentLedgerIdentityAndHashIsRejectedInFreshJvm() throws Exception {
        var fixture=fixture(NarrowContentEvidenceContext.PlanKind.ORIGINAL);var ledger=ledger(fixture);
        Path retained=Files.move(fixture.root(),temp.resolve("retained-attempt"));
        Files.createDirectory(fixture.root(),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        for(String marker:new String[]{ContentEvidenceBudget.MANIFEST,ContentEvidenceBudget.LOCK})
            Files.copy(retained.resolve(marker),fixture.root().resolve(marker),StandardCopyOption.COPY_ATTRIBUTES);
        var replacement=InstallationHostGuard.identity(fixture.root());
        assertThat(replacement).isNotEqualTo(fixture.expected().rootIdentity());
        ledger.put("rootDevice",Long.toString(replacement.device())).put("rootInode",Long.toString(replacement.inode()));
        writeLedger(fixture,ledger);ContentEvidenceBudget.inspect(fixture.root());
        // Updating only the digest cannot substitute the independently captured filesystem identity.
        assertThat(inspect(updatedHash(fixture))).isEqualTo("REJECTED INPUT_CHANGED");
    }

    @Test void replacementLockWithIdenticalBytesIsRejectedInFreshJvm() throws Exception {
        var fixture=fixture(NarrowContentEvidenceContext.PlanKind.RECOVERY);
        Path lock=fixture.root().resolve(ContentEvidenceBudget.LOCK);
        Path retained=Files.move(lock,temp.resolve("retained-lock"));
        Files.copy(retained,lock,StandardCopyOption.COPY_ATTRIBUTES);
        assertThat(InstallationHostGuard.identity(lock)).isNotEqualTo(fixture.expected().lockIdentity());
        assertThat(inspect(fixture)).isEqualTo("REJECTED INPUT_CHANGED");
    }

    @ParameterizedTest @ValueSource(strings={"selection","profile","runtime"})
    void eachOpaqueFrozenIdentityMustMatchTheIndependentCallerValue(String changed) throws Exception {
        var fixture=fixture(NarrowContentEvidenceContext.PlanKind.ORIGINAL);String other="d".repeat(64);
        var current=new NarrowContentEvidenceContext.FrozenIdentities(
                changed.equals("selection")?other:FROZEN.selectionSha256(),
                changed.equals("profile")?other:FROZEN.profileSha256(),
                changed.equals("runtime")?other:FROZEN.runtimeSha256());
        assertThat(inspect(fixture.current(current))).isEqualTo("REJECTED RUNTIME_MISMATCH");
    }

    private static ObjectNode ledger(Fixture fixture) throws Exception {
        return (ObjectNode)SourceJson.parse(Files.readAllBytes(fixture.root().resolve(ContentEvidenceBudget.MANIFEST)),ContentEvidenceBudget.LEDGER);
    }

    private static void writeLedger(Fixture fixture,ObjectNode ledger) throws Exception {
        Files.write(fixture.root().resolve(ContentEvidenceBudget.MANIFEST),SourceJson.canonical(ledger));
    }

    private static Fixture updatedHash(Fixture fixture) throws Exception {
        var e=fixture.expected();
        return fixture.expected(new NarrowContentEvidenceContext.Expected(e.policy(),e.run(),e.attempt(),e.kind(),e.source(),
                e.host(),e.rootIdentity(),e.lockIdentity(),e.frozen(),
                SourceJson.sha(Files.readAllBytes(fixture.root().resolve(ContentEvidenceBudget.MANIFEST)))));
    }

    private String inspect(Fixture fixture) throws Exception {
        // These files are fixture-only process arguments, outside the evidence tree. They do not
        // select a production persistence schema or establish an authority for opaque identities.
        Path actualHost=Files.write(temp.resolve("actual-host.json"),fixture.host().descriptor());
        Path expectedHost=Files.write(temp.resolve("expected-host.json"),fixture.expected().host().descriptor());
        Path expected=Files.write(temp.resolve("test-expectation.json"),arguments(fixture));
        Path output=temp.resolve("child-output.txt");
        String classpath=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));
        var before=snapshot(fixture.host().root());
        var child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",classpath,
                NarrowContentEvidenceTestChild.class.getName(),actualHost.toString(),expectedHost.toString(),expected.toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertThat(child.pid()).isNotEqualTo(ProcessHandle.current().pid());
            assertThat(child.waitFor(20,TimeUnit.SECONDS)).as("read-only child must finish within 20 seconds").isTrue();
            String text=Files.readString(output);
            assertThat(child.exitValue()).withFailMessage("Child failed: %s",text).isZero();
            var lines=text.lines().toList();
            assertThat(lines).withFailMessage("Unexpected child output: %s",text).hasSize(2);
            assertThat(lines.getFirst()).isEqualTo("PID "+child.pid());
            return lines.get(1);
        } finally {
            if(child.isAlive()) {
                child.descendants().forEach(ProcessHandle::destroyForcibly);
                child.destroyForcibly();
                assertThat(child.waitFor(5,TimeUnit.SECONDS)).as("timed-out child cleanup").isTrue();
            }
            assertThat(snapshot(fixture.host().root())).as("inspection must preserve all host-tree entries, bytes, identities and mtimes")
                    .isEqualTo(before);
        }
    }

    private static byte[] arguments(Fixture fixture) {
        var e=fixture.expected();
        var n=SourceJson.object().put("policy",e.policy()).put("run",e.run().toString()).put("attempt",e.attempt().toString())
                .put("kind",e.kind().name()).put("ledgerSha256",e.ledgerSha256());
        if(e.source()==null)n.putNull("source");else n.put("source",e.source());
        n.put("rootDevice",Long.toString(e.rootIdentity().device())).put("rootInode",Long.toString(e.rootIdentity().inode()))
                .put("lockDevice",Long.toString(e.lockIdentity().device())).put("lockInode",Long.toString(e.lockIdentity().inode()));
        frozen(n.putObject("expectedFrozen"),e.frozen());frozen(n.putObject("currentFrozen"),fixture.current());
        return SourceJson.canonical(n);
    }

    private static void frozen(ObjectNode node,NarrowContentEvidenceContext.FrozenIdentities identities) {
        node.put("selection",identities.selectionSha256()).put("profile",identities.profileSha256()).put("runtime",identities.runtimeSha256());
    }

    private record Entry(boolean directory,FileTime modified,InstallationHostGuard.Identity identity,
                         Set<PosixFilePermission> permissions,String bytes) { }

    private static Map<String,Entry> snapshot(Path root) throws Exception {
        var result=new TreeMap<String,Entry>();
        try(var tree=Files.walk(root)) {
            for(Path path:tree.toList()) {
                var attributes=Files.readAttributes(path,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
                result.put(root.relativize(path).toString(),new Entry(attributes.isDirectory(),attributes.lastModifiedTime(),
                        InstallationHostGuard.identity(path),Files.getPosixFilePermissions(path,LinkOption.NOFOLLOW_LINKS),
                        attributes.isDirectory()?null:Base64.getEncoder().encodeToString(Files.readAllBytes(path))));
            }
        }
        return result;
    }
}
