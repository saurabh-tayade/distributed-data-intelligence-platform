package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

class ContentOperationPlanTest {
    @TempDir Path temp;
    InstallationHostGuard guard;UUID run=UUID.randomUUID(),attempt=UUID.randomUUID();Path root;
    @BeforeEach void setup()throws Exception {
        guard=InstallationHostGuard.acquire(OwnershipFixtures.host(temp.toRealPath()));
        ContentEvidenceBudget.initialize(guard,run,attempt);root=guard.binding().root().resolve("runs/"+run+"/attempts/"+attempt);
    }
    @AfterEach void close(){guard.close();}
    ContentOperationPlan original(){return ContentOperationPlan.original(root,run,attempt,"x".repeat(128));}
    byte[] ledger()throws Exception{return Files.readAllBytes(root.resolve(ContentEvidenceBudget.MANIFEST));}
    @Test void entireCoreAndInspectionPairAreReservedBeforeAnyOperationDirectoryExists()throws Exception {
        var plan=original();plan.reserve();var totals=ContentEvidenceBudget.totals(root);
        assertThat(totals.reservedBytes()).isEqualTo(64L*1024*1024);
        assertThat(totals.reservedEntries()).isEqualTo(221);
        assertThat(totals.inventoryMaximum()).isLessThan(100*1024);
        assertThat(plan.reservations()).hasSize(19);
        for(Path directory:plan.reservations().keySet())assertThat(directory).doesNotExist();
        var recorded=SourceJson.parse(ledger(),ContentEvidenceBudget.LEDGER);
        assertThat(recorded.get("version").intValue()).isEqualTo(2);
        assertThat(recorded.get("reservationPolicy").stringValue()).isEqualTo(NarrowContentProfile.POLICY);
    }
    @Test void exactReservedOperationCanWriteButNeverOverwriteOrAddEvenEmptyDirectory()throws Exception {
        var plan=original();plan.reserve();Path path=plan.directory("CHECK_INPUTS",0);
        UUID checkpoint=UUID.fromString(path.getFileName().toString());var files=new PreflightFiles(guard,run,attempt,checkpoint,"CHECK_INPUTS");
        files.write("command.json",new byte[]{'{','}'},65536);byte[] before=ledger();
        assertThatThrownBy(()->files.write("command.json",new byte[]{'{','}'},65536)).isInstanceOf(PreflightFailure.class);
        Path unexpected=root.resolve("check-inputs/"+UUID.randomUUID());
        assertThatThrownBy(()->ContentEvidenceBudget.reserve(unexpected,Map.of())).hasMessage("RESOURCE_LIMIT");
        assertThatThrownBy(()->ContentEvidenceBudget.reserve(path,Map.of("unplanned.json",1))).hasMessage("RESOURCE_LIMIT");
        assertThat(unexpected).doesNotExist();assertThat(ledger()).isEqualTo(before);
        assertThat(Files.readAllBytes(files.directory.resolve("command.json"))).isEqualTo(new byte[]{'{','}'});
    }
    @Test void recreationReopensSamePlanWithoutChangingSealedLedgerOrAllocatingNewIdentities()throws Exception {
        var plan=original();plan.reserve();byte[] before=ledger();var modified=Files.getLastModifiedTime(root.resolve(ContentEvidenceBudget.MANIFEST));
        var binding=guard.binding();guard.close();guard=InstallationHostGuard.acquire(binding);
        ContentEvidenceBudget.initialize(guard,run,attempt);var reopened=original();reopened.reserve();
        assertThat(reopened.reservations()).isEqualTo(plan.reservations());assertThat(ledger()).isEqualTo(before);
        assertThat(Files.getLastModifiedTime(root.resolve(ContentEvidenceBudget.MANIFEST))).isEqualTo(modified);
        assertThat(plan.directory("SILVER",0).getFileName()).isEqualTo(plan.directory("VERIFY_SILVER",0).getFileName());
    }
    @Test void recoveryPoolIsFixedAndCannotBeExpandedOrConvertedToOriginal()throws Exception {
        var plan=ContentOperationPlan.recovery(root,run,attempt);plan.reserve();byte[] before=ledger();
        assertThat(ContentEvidenceBudget.totals(root).reservedBytes()).isEqualTo(60_686_336L);
        assertThat(ContentEvidenceBudget.totals(root).reservedEntries()).isEqualTo(122);
        assertThat(plan.reservations()).hasSize(9);
        assertThatThrownBy(()->plan.directory("VERIFY_RESULT",9)).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->original().reserve()).hasMessage("INPUT_CHANGED");assertThat(ledger()).isEqualTo(before);
    }
    @Test void legacyReservationsCannotBeRelabelledAsTheFixedProfile()throws Exception {
        ContentEvidenceBudget.reserve(root.resolve("check-inputs/"+UUID.randomUUID()),ContentEvidenceBudget.slots("CHECK_INPUTS"));
        byte[] before=ledger();assertThatThrownBy(()->original().reserve()).hasMessage("INPUT_CHANGED");assertThat(ledger()).isEqualTo(before);
    }
    @Test void interruptedLedgerRetainsPrefixAndRefusesPlanAndConsumption()throws Exception {
        var plan=original();plan.reserve();byte[] before=ledger();Path pending=root.resolve(ContentEvidenceBudget.MANIFEST+".preparing");
        Files.writeString(pending,"{");Files.setPosixFilePermissions(pending,PosixFilePermissions.fromString("rw-------"));
        assertThatThrownBy(plan::reserve).hasMessage("INPUT_CHANGED");
        assertThatThrownBy(()->ContentEvidenceBudget.reserve(plan.directory("CHECK_INPUTS",0),ContentEvidenceBudget.slots("CHECK_INPUTS"))).hasMessage("INPUT_CHANGED");
        assertThat(ledger()).isEqualTo(before);assertThat(Files.readString(pending)).isEqualTo("{");
    }
    @Test void legacyReaderHasNoExternalPolicyBindingToDetectAValidSchemaDowngrade()throws Exception {
        // Characterize an intentionally OPEN integration gate, not an authorized mutation:
        // a future frozen runtime policy must require v2 independently of this mutable file.
        original().reserve();var legacy=(ObjectNode)SourceJson.parse(ledger(),ContentEvidenceBudget.LEDGER);
        legacy.put("version",1);legacy.remove("reservationPolicy");legacy.remove("planKind");legacy.remove("sourceId");
        Files.write(root.resolve(ContentEvidenceBudget.MANIFEST),SourceJson.canonical(legacy));
        ContentEvidenceBudget.inspect(root);
        ContentEvidenceBudget.reserve(root.resolve("legacy-inspection/"+UUID.randomUUID()),Map.of());
        assertThat(ContentEvidenceBudget.totals(root).reservedEntries()).isEqualTo(223);
    }
    @Test void modifiedPlanGrammarAndMissingFilesCannotBeAdopted()throws Exception {
        original().reserve();byte[] before=ledger();
        for(String field:new String[]{"version","policy","capacity","directory"}){
            var n=(ObjectNode)SourceJson.parse(before,ContentEvidenceBudget.LEDGER);
            switch(field){
                case "version"->n.put("version",4294967298L);
                case "policy"->n.put("reservationPolicy","unknown");
                case "capacity"->((ObjectNode)n.get("files")).put(n.get("files").propertyNames().iterator().next(),1);
                default->n.withArray("directories").add("unplanned");
            }
            Files.write(root.resolve(ContentEvidenceBudget.MANIFEST),SourceJson.canonical(n));
            assertThatThrownBy(()->ContentEvidenceBudget.inspect(root)).isInstanceOf(PreflightFailure.class);
        }
        Files.write(root.resolve(ContentEvidenceBudget.MANIFEST),before);
        Files.delete(root.resolve(ContentEvidenceBudget.MANIFEST));assertThatThrownBy(()->original().reserve()).isInstanceOf(PreflightFailure.class);
    }
    @Test void sourceAndRootScopeAreBoundAndReturnedPlansAreImmutable() {
        for(String source:new String[]{"", "x".repeat(129),"../other","UPPER"})assertThatThrownBy(()->ContentOperationPlan.original(root,run,attempt,source)).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->ContentOperationPlan.original(root,run,UUID.randomUUID(),"source")).hasMessage("INPUT_INVALID");
        var plan=original();assertThatThrownBy(()->plan.reservations().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(()->plan.reservations().values().iterator().next().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
}
