package platform.contentapi.importing;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;
class ImportBoundaryTest {
    byte[] catalog(String address,String kind){return ("{\"catalogVersion\":1,\"operationId\":\"10000000-0000-4000-8000-000000000001\",\"executionProfileId\":\"import-v1\",\"controlBindingId\":\"control\",\"artifacts\":[{\"kind\":\""+kind+"\",\"productId\":\"content-intelligence\",\"storageBindingId\":\"refs\",\"relativeAddress\":\""+address+"\",\"contractVersion\":1,\"processorRevision\":1,\"identity\":{\"referenceId\":\"test\",\"revision\":\"r1\"},\"fingerprint\":\""+"a".repeat(64)+"\"}]}").getBytes();}
    @Test void validCatalogIsNormalizedAndDefensive(){var c=ImportCatalog.parse(catalog("reference_id=test/revision=r1","PUBLISHER_REFERENCE"));assertThat(c.entries()).hasSize(1);byte[] before=c.requestBytes();c.requestBytes()[0]=0;assertThat(c.requestBytes()).isEqualTo(before);assertThat(c.toString()).doesNotContain("reference_id");}
    @ParameterizedTest @ValueSource(strings={"/private/path","../test","reference_id=test/revision=r1/..","reference_id=test/revision=*","reference_id=test/revision=r2"})
    void unsafeOrInexactAddress(String address){assertThatThrownBy(()->ImportCatalog.parse(catalog(address,"PUBLISHER_REFERENCE"))).hasMessage("INVALID_CATALOG").hasNoCause();}
    @ParameterizedTest @ValueSource(strings={"GOLD","CONTENT","RELIABILITY_V2","unknown"})
    void excludedKinds(String kind){assertThatThrownBy(()->ImportCatalog.parse(catalog("reference_id=test/revision=r1",kind))).hasMessage("INVALID_CATALOG");}
    @Test void strictUnknownDuplicateAndSize(){for(byte[] raw:List.of("{\"a\":1,\"a\":2}".getBytes(),new byte[65537],"{} {}".getBytes(),"{\"x\":1.2}".getBytes()))assertThatThrownBy(()->ImportCatalog.parse(raw)).hasMessage("INVALID_CATALOG");}
    @Test void duplicateSelectionsRejected(){var n=SourceJson.parse(catalog("reference_id=test/revision=r1","PUBLISHER_REFERENCE"));var array=(tools.jackson.databind.node.ArrayNode)n.get("artifacts");array.add(array.get(0).deepCopy());assertThatThrownBy(()->ImportCatalog.parse(SourceJson.canonical(n))).hasMessage("INVALID_CATALOG");}
    List<String> command(String mode){return List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),ImportProcessChild.class.getName(),mode);}
    @Test void processCompletesWithBoundedOutput(){var r=new NativeImportProcess().run(command("good"),Path.of("."),3,"{}".getBytes());assertThat(r.exitCode()).isZero();assertThat(r.stdout()).isEqualTo("{}".getBytes());}
    @ParameterizedTest @ValueSource(strings={"large","error"}) void streamLimits(String mode){assertThatThrownBy(()->new NativeImportProcess().run(command(mode),Path.of("."),3,new byte[0])).hasMessage("RESULT_INVALID").hasNoCause();}
    @Test void timeoutIsCleaned(){assertThatThrownBy(()->new NativeImportProcess().run(command("wait"),Path.of("."),1,new byte[0])).hasMessage("TIMEOUT").satisfies(e->assertThat(((ImportFailure)e).quiescent()).isTrue());}
    @Test void descendantsRequireExplicitReconciliation(){assertThatThrownBy(()->new NativeImportProcess().run(command("fork"),Path.of("."),1,new byte[0])).hasMessage("CLEANUP_UNCERTAIN").satisfies(e->assertThat(((ImportFailure)e).quiescent()).isFalse());}
    @Test void startupFailure(){assertThatThrownBy(()->new NativeImportProcess().run(List.of("/missing-private-executable"),Path.of("."),1,new byte[0])).hasMessage("START_FAILED").hasNoCause();}
    @Test void interruptionIsPreserved(){Thread.currentThread().interrupt();try{assertThatThrownBy(()->new NativeImportProcess().run(command("wait"),Path.of("."),1,new byte[0])).hasMessage("INTERRUPTED");assertThat(Thread.currentThread().isInterrupted()).isTrue();}finally{Thread.interrupted();}}
    @Test void interruptionDuringChildWaitCleansAndPreservesFlag() throws Exception {
        var observed=new java.util.concurrent.atomic.AtomicReference<ImportFailure>();var interrupted=new java.util.concurrent.atomic.AtomicBoolean();
        Thread owner=Thread.ofPlatform().start(()->{try{new NativeImportProcess().run(command("wait"),Path.of("."),10,new byte[0]);}catch(ImportFailure f){observed.set(f);interrupted.set(Thread.currentThread().isInterrupted());}});
        Thread.sleep(250);owner.interrupt();owner.join(java.time.Duration.ofSeconds(5));assertThat(owner.isAlive()).isFalse();
        assertThat(observed.get().code()).isEqualTo(ImportFailure.Code.INTERRUPTED);assertThat(observed.get().quiescent()).isTrue();assertThat(interrupted).isTrue();
    }

    @Test void sparkPathAcceptsReapedChildOnly() {
        var r=new NativeImportProcess().runSpark(command("joined"),Path.of("."),3,new byte[0],Path.of(System.getProperty("java.home")));assertThat(r.exitCode()).isZero();
    }
    @ParameterizedTest @ValueSource(strings={"wait","large","fork"})
    void sparkInterruptedOrOverflowedProcessRemainsUncertain(String mode) {
        assertThatThrownBy(()->new NativeImportProcess().runSpark(command(mode),Path.of("."),1,new byte[0],Path.of(System.getProperty("java.home"))))
            .isInstanceOf(ImportFailure.class).satisfies(e->assertThat(((ImportFailure)e).quiescent()).isFalse());
    }

}
