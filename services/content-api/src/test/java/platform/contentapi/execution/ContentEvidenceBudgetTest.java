package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class ContentEvidenceBudgetTest {
    @TempDir Path temp;InstallationHostGuard guard;UUID run=UUID.randomUUID(),attempt=UUID.randomUUID();Path root;
    @BeforeEach void setup()throws Exception{guard=InstallationHostGuard.acquire(OwnershipFixtures.host(temp.toRealPath()));ContentEvidenceBudget.initialize(guard,run,attempt);root=guard.binding().root().resolve("runs/"+run+"/attempts/"+attempt);}
    @AfterEach void close(){guard.close();}
    Path directory(String relative)throws Exception{Path p=root;for(String s:relative.split("/")){p=p.resolve(s);try{Files.createDirectory(p,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));}catch(FileAlreadyExistsException ignored){}}return p;}
    @Test void chargesBothNamesAndAllDirectoriesBeforeCreatingThem()throws Exception{
        Path p=root.resolve("verify-result/"+UUID.randomUUID());var before=ContentEvidenceBudget.totals(root);
        ContentEvidenceBudget.reserve(p,Map.of("evidence.json",123));var after=ContentEvidenceBudget.totals(root);
        assertThat(Files.exists(p)).isFalse();assertThat(after.reservedBytes()-before.reservedBytes()).isEqualTo(246);
        assertThat(after.reservedEntries()-before.reservedEntries()).isEqualTo(4);
        assertThat(after.inventoryMaximum()).isGreaterThan(before.inventoryMaximum());
    }
    @Test void exactly64MiBOfReservationFitsAndTheFirstExtraByteIsRejected(){
        int m=1024*1024;
        ContentEvidenceBudget.reserve(root,Map.of("first.bin",8*m,"second.bin",8*m,"third.bin",8*m,"fourth.bin",8*m-ContentEvidenceBudget.LEDGER));
        assertThat(ContentEvidenceBudget.totals(root).reservedBytes()).isEqualTo(ContentEvidenceBudget.BYTES);
        assertThatThrownBy(()->ContentEvidenceBudget.reserve(root,Map.of("extra.bin",1))).hasMessage("RESOURCE_LIMIT");
        assertThat(ContentEvidenceBudget.totals(root).reservedBytes()).isEqualTo(ContentEvidenceBudget.BYTES);
    }
    @Test void writingAnUnreservedFileOrIncreasingItsAllowanceIsRejected()throws Exception{
        ContentEvidenceBudget.reserve(root,Map.of("evidence.json",2));
        assertThatThrownBy(()->PreparedCommand.writeOnce(root,"evidence.json",new byte[3],3)).hasMessage("RESOURCE_LIMIT");
        assertThatThrownBy(()->PreparedCommand.writeOnce(root,"result.json",new byte[1],1)).hasMessage("RESOURCE_LIMIT");
        assertThatThrownBy(()->ContentEvidenceBudget.reserve(root,Map.of("evidence.json",3))).hasMessage("INPUT_CHANGED");
        assertThat(Files.exists(root.resolve("result.json.preparing"))).isFalse();
    }
    @Test void completeAndInterruptedWritesRemainChargedWithoutReclamation()throws Exception{
        ContentEvidenceBudget.reserve(root,Map.of("evidence.json",10,"result.json",10));
        PreparedCommand.writeOnce(root,"evidence.json",new byte[8],10);
        Files.write(root.resolve("result.json.preparing"),new byte[4],StandardOpenOption.CREATE_NEW);Files.setPosixFilePermissions(root.resolve("result.json.preparing"),PosixFilePermissions.fromString("rw-------"));
        var before=ContentEvidenceBudget.totals(root);assertThat(before.actualBytes()).isLessThanOrEqualTo(before.reservedBytes());
        assertThatThrownBy(()->PreparedCommand.writeOnce(root,"result.json",new byte[10],10)).hasMessage("INPUT_CHANGED");
        assertThat(Files.readAllBytes(root.resolve("result.json.preparing"))).hasSize(4);assertThat(ContentEvidenceBudget.totals(root).reservedBytes()).isEqualTo(before.reservedBytes());
    }
    @Test void corruptMissingOrInterruptedLedgerStopsFurtherWrites()throws Exception{
        ContentEvidenceBudget.reserve(root,Map.of("evidence.json",10));
        byte[] ledger=Files.readAllBytes(root.resolve(ContentEvidenceBudget.MANIFEST));
        Files.write(root.resolve(ContentEvidenceBudget.MANIFEST),new byte[]{'{'});
        assertThatThrownBy(()->PreparedCommand.writeOnce(root,"evidence.json",new byte[1],10)).isInstanceOf(PreflightFailure.class);
        Files.write(root.resolve(ContentEvidenceBudget.MANIFEST),ledger);Files.delete(root.resolve(ContentEvidenceBudget.MANIFEST));
        assertThatThrownBy(()->PreparedCommand.writeOnce(root,"evidence.json",new byte[1],10)).hasMessage("INPUT_UNAVAILABLE");
        Files.write(root.resolve(ContentEvidenceBudget.MANIFEST),ledger);Files.write(root.resolve(ContentEvidenceBudget.MANIFEST+".preparing"),new byte[0]);
        assertThatThrownBy(()->ContentEvidenceBudget.reserve(root,Map.of("result.json",1))).hasMessage("INPUT_CHANGED");
        assertThat(Files.exists(root.resolve("evidence.json"))).isFalse();
    }
    @Test void unsupportedDepthAndUnreservedSubtreesAreNotAdopted()throws Exception{
        assertThatThrownBy(()->ContentEvidenceBudget.reserve(root.resolve("a/b/c/d"),Map.of("result.json",1))).isInstanceOf(PreflightFailure.class);
        Path alien=directory("unknown");assertThatThrownBy(()->ContentEvidenceBudget.reserve(root,Map.of("result.json",1))).hasMessage("INPUT_CHANGED");assertThat(Files.exists(alien)).isTrue();
    }
    @Test void concurrentRepeatedQualificationsCannotOverbookOrSilentlyDropReservations()throws Exception{
        try(var pool=Executors.newFixedThreadPool(8)){
            var calls=new ArrayList<Callable<Boolean>>();
            for(int i=0;i<40;i++)calls.add(()->{try{ContentEvidenceBudget.reserve(root.resolve("verify-result/"+UUID.randomUUID()),Map.of("evidence.json",1024*1024));return true;}catch(PreflightFailure ex){assertThat(ex.code()).isEqualTo(PreflightFailure.Code.RESOURCE_LIMIT);return false;}});
            int accepted=0;for(var result:pool.invokeAll(calls))if(result.get())accepted++;
            assertThat(accepted).isEqualTo(31);var totals=ContentEvidenceBudget.totals(root);assertThat(totals.reservedBytes()).isEqualTo(62L*1024*1024+2L*ContentEvidenceBudget.LEDGER);assertThat(totals.reservedEntries()).isLessThanOrEqualTo(4096);
        }
    }
    @Test void longNamesChargeCanonicalInventoryBeforeByteCapacity(){
        var slots=new LinkedHashMap<String,Integer>();for(int i=0;i<1000;i++)slots.put("x".repeat(57)+String.format(java.util.Locale.ROOT,"%04d",i),1);
        assertThatThrownBy(()->ContentEvidenceBudget.reserve(root.resolve("a".repeat(128)+"/"+"b".repeat(128)+"/"+"c".repeat(128)),slots)).hasMessage("RESOURCE_LIMIT");
        assertThat(ContentEvidenceBudget.totals(root).reservedEntries()).isEqualTo(4);
    }
    @Test void losingBothReservationFilesCannotDowngradeAnInitializedProducer()throws Exception{
        ContentEvidenceBudget.reserve(root,Map.of("evidence.json",10));Files.delete(root.resolve(ContentEvidenceBudget.MANIFEST));Files.delete(root.resolve(ContentEvidenceBudget.LOCK));
        assertThatThrownBy(()->PreparedCommand.writeOnce(root,"evidence.json",new byte[1],10)).hasMessage("INPUT_UNAVAILABLE");assertThat(Files.exists(root.resolve("evidence.json"))).isFalse();
    }
    @Test void interruptedLedgerIsCapturableButNeverAuthorizesAnotherWrite()throws Exception{
        ContentEvidenceBudget.reserve(root,Map.of("evidence.json",10));Files.writeString(root.resolve(ContentEvidenceBudget.MANIFEST+".preparing"),"{");Files.setPosixFilePermissions(root.resolve(ContentEvidenceBudget.MANIFEST+".preparing"),PosixFilePermissions.fromString("rw-------"));
        var capture=ContentRecoveryFiles.capture(guard,run,attempt,List.of(),true);
        assertThat(capture.inventory().get("files")).anySatisfy(f->assertThat(f.get("address").stringValue()).endsWith(".content-evidence.json.preparing"));
        assertThatThrownBy(()->ContentEvidenceBudget.reserve(root,Map.of("result.json",1))).hasMessage("INPUT_CHANGED");
    }
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(strings={"child.json","result.json"})
    void boundedProcessPrefixIsRetainedAndMarkedUnresolved(String name)throws Exception{
        ContentEvidenceBudget.reserve(root,Map.of(name,65536));Files.writeString(root.resolve(name+".preparing"),"{\"pid\":\"123");Files.setPosixFilePermissions(root.resolve(name+".preparing"),PosixFilePermissions.fromString("rw-------"));
        var capture=ContentRecoveryFiles.capture(guard,run,attempt,List.of(),true);assertThat(capture.inventory().get("processEvidenceIncomplete").booleanValue()).isTrue();assertThat(capture.descendantPids()).isEmpty();
    }
    @Test void boundedCaptureCannotIgnoreDeletedLedgerOrAdoptUnreservedEvidence()throws Exception{
        ContentEvidenceBudget.reserve(root,Map.of("evidence.json",10));PreparedCommand.writeOnce(root,"evidence.json",new byte[1],10);
        Files.delete(root.resolve(ContentEvidenceBudget.MANIFEST));Files.delete(root.resolve(ContentEvidenceBudget.LOCK));
        assertThatThrownBy(()->ContentRecoveryFiles.capture(guard,run,attempt,List.of(),true)).hasMessage("EVIDENCE_UNAVAILABLE");
        assertThatThrownBy(()->ContentEvidenceBudget.initialize(guard,run,attempt)).hasMessage("INPUT_CHANGED");assertThat(Files.size(root.resolve("evidence.json"))).isOne();
    }
    @Test void bothRecoveryObservationsMustBeReservedBeforeTheFirstSeal()throws Exception{
        var first=new PreflightFiles(guard,run,attempt,UUID.randomUUID(),"VERIFY_RESULT");first.write("command.json",new byte[]{'{','}'},65536);
        var before=ContentRecoveryFiles.capture(guard,run,attempt,List.of(),true).inventory();
        UUID next=UUID.randomUUID();Path future=root.resolve("verify-result/"+next);ContentEvidenceBudget.reserve(future,ContentEvidenceBudget.slots("VERIFY_RESULT"));
        var reserved=ContentRecoveryFiles.capture(guard,run,attempt,List.of(),true).inventory();
        var oldLedger=java.util.stream.StreamSupport.stream(before.get("files").spliterator(),false).filter(f->f.get("address").stringValue().endsWith(ContentEvidenceBudget.MANIFEST)).findFirst().orElseThrow();
        var ledger=java.util.stream.StreamSupport.stream(reserved.get("files").spliterator(),false).filter(f->f.get("address").stringValue().endsWith(ContentEvidenceBudget.MANIFEST)).findFirst().orElseThrow();
        assertThat(ledger.get("sha256")).isNotEqualTo(oldLedger.get("sha256"));
        var second=new PreflightFiles(guard,run,attempt,next,"VERIFY_RESULT");second.write("command.json",new byte[]{'{','}'},65536);
        var after=ContentRecoveryFiles.capture(guard,run,attempt,List.of(),true).inventory();
        assertThat(after.get("files")).anySatisfy(f->assertThat(f).isEqualTo(ledger));
    }
    @Test void packetTreeReservesExactly4096EntriesAndRejectsTheNextDirectory()throws Exception{
        UUID control=UUID.randomUUID();ContentEvidenceBudget.initializePackets(guard,run,control);Path packet=guard.binding().root().resolve("recovery/"+control);
        var ledger=(tools.jackson.databind.node.ObjectNode)platform.contentapi.management.SourceJson.parse(Files.readAllBytes(packet.resolve(ContentEvidenceBudget.MANIFEST)));var dirs=ledger.putArray("directories").add("");
        for(int i=0;i<4092;i++)dirs.add("d"+Integer.toString(i,36));Files.write(packet.resolve(ContentEvidenceBudget.MANIFEST),platform.contentapi.management.SourceJson.canonical(ledger));
        assertThat(ContentEvidenceBudget.totals(packet).reservedEntries()).isEqualTo(4096);
        assertThatThrownBy(()->ContentEvidenceBudget.reserve(packet.resolve("extra"),Map.of())).hasMessage("RESOURCE_LIMIT");assertThat(Files.exists(packet.resolve("extra"))).isFalse();
    }
    @Test void completeFixedPlanIncludesLongestSourceNamesAndOneExtraInspection()throws Exception{
        String source="x".repeat(128);UUID checkpoint=UUID.randomUUID();new RssJournalFiles(guard,run,attempt,checkpoint,source,"runs/"+run+"/attempts/"+attempt+"/fetch-source/"+source+"/"+checkpoint,true);
        var counts=Map.ofEntries(Map.entry("STORE_BRONZE",1),Map.entry("INSPECT_BRONZE",4),Map.entry("SILVER",1),Map.entry("VERIFY_SILVER",1),Map.entry("RECHECK_SILVER",1),Map.entry("PREPARE_GOLD",1),Map.entry("PUBLISH_GOLD",1),Map.entry("VERIFY_RESULT",2),Map.entry("CHECK_INPUTS",3),Map.entry("PUBLISH_CONTENT",1));
        for(var entry:counts.entrySet())for(int i=0;i<entry.getValue();i++)new PreflightFiles(guard,run,attempt,UUID.randomUUID(),entry.getKey());
        var clean=ContentEvidenceBudget.totals(root);assertThat(clean.reservedBytes()).isEqualTo(61_997_056);assertThat(clean.reservedEntries()).isEqualTo(201);assertThat(clean.inventoryMaximum()).isLessThan(100*1024);
        new PreflightFiles(guard,run,attempt,UUID.randomUUID(),"INSPECT_BRONZE");new PreflightFiles(guard,run,attempt,UUID.randomUUID(),"CHECK_INPUTS");
        var full=ContentEvidenceBudget.totals(root);assertThat(full.reservedBytes()).isEqualTo(ContentEvidenceBudget.BYTES);assertThat(full.reservedEntries()).isEqualTo(221);assertThat(full.inventoryMaximum()).isLessThan(100*1024);
        Path next=root.resolve("inspect-bronze/"+UUID.randomUUID());assertThatThrownBy(()->ContentEvidenceBudget.reserve(next,ContentEvidenceBudget.slots("INSPECT_BRONZE"))).hasMessage("RESOURCE_LIMIT");assertThat(Files.exists(next)).isFalse();
    }
    @Test void boundedPacketsCheckTheirOwnSizeAndExhaustReservationsWithoutTruncation()throws Exception{
        UUID control=UUID.randomUUID();var n=platform.contentapi.management.SourceJson.object().put("pad","");int overhead=platform.contentapi.management.SourceJson.canonical(n).length;n.put("pad","x".repeat(RecoveryFiles.MAXIMUM-overhead));byte[] maximum=platform.contentapi.management.SourceJson.canonical(n);
        String address=RecoveryFiles.writeBounded(guard,run,control,UUID.randomUUID(),maximum);assertThat(RecoveryFiles.read(guard,address)).isEqualTo(maximum);
        UUID oversized=UUID.randomUUID();assertThatThrownBy(()->RecoveryFiles.writeBounded(guard,run,control,oversized,Arrays.copyOf(maximum,maximum.length+1))).hasMessage("EVIDENCE_UNAVAILABLE");Path packet=guard.binding().root().resolve("recovery/"+control);assertThat(Files.exists(packet.resolve(oversized.toString()))).isFalse();
        for(int i=0;i<30;i++)RecoveryFiles.writeBounded(guard,run,control,UUID.randomUUID(),new byte[]{'{','}'});
        var totals=ContentEvidenceBudget.totals(packet);assertThat(totals.reservedBytes()).isEqualTo(62L*1024*1024+2L*ContentEvidenceBudget.LEDGER);
        UUID exhausted=UUID.randomUUID();assertThatThrownBy(()->RecoveryFiles.writeBounded(guard,run,control,exhausted,new byte[]{'{','}'})).hasMessage("EVIDENCE_UNAVAILABLE");assertThat(Files.exists(packet.resolve(exhausted.toString()))).isFalse();assertThat(RecoveryFiles.read(guard,address)).isEqualTo(maximum);assertThat(ContentEvidenceBudget.totals(packet)).isEqualTo(totals);
    }
    @Test void existingTreesCannotBeRetrofittedByIgnoringTheirEvidence()throws Exception{
        UUID other=UUID.randomUUID();Path p=guard.binding().root().resolve("runs/"+run+"/attempts/"+other);Files.createDirectory(p,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));Files.writeString(p.resolve("evidence.json"),"retained");
        assertThatThrownBy(()->ContentEvidenceBudget.initialize(guard,run,other)).hasMessage("INPUT_CHANGED");assertThat(Files.readString(p.resolve("evidence.json"))).isEqualTo("retained");
    }
}
