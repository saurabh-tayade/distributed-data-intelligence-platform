package platform.ingestion.managed;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StrictBronzeEvidenceTest {
    private static final String INSTALL="11111111-1111-4111-8111-111111111111";
    private static final String WORKER="22222222-2222-4222-8222-222222222222";
    private static final String RUN="33333333-3333-4333-8333-333333333333";
    private static final String ATTEMPT="44444444-4444-4444-8444-444444444444";
    private static final String HASH="a".repeat(64);
    private static final byte[] JSON={'{','}'};
    @TempDir Path temp;
    private record Fixture(StrictBronzeEvidence.Expected expected,StrictBronzeEvidence.Current current,Path root,Path operation) {
        StrictBronzeEvidence sink()throws IOException{return new StrictBronzeEvidence(expected,current);}
    }
    private Fixture fixture()throws IOException{
        Path control=directory(temp.resolve(UUID.randomUUID().toString())),path=control;
        for(String segment:List.of("runs",RUN,"attempts",ATTEMPT))path=directory(path.resolve(segment));
        Path root=path;file(root.resolve(StrictBronzeEvidence.LOCK),new byte[0]);
        var frozen=new StrictBronzeEvidence.Frozen(HASH,HASH,HASH);
        var writer=new StrictBronzeEvidence.Writer(INSTALL,WORKER,3,RUN,ATTEMPT,"STORE_BRONZE",0,
                StrictBronzeEvidence.checkpoint(RUN,ATTEMPT,"STORE_BRONZE",0),StrictFetchJson.sha(JSON));
        var initial=new StrictBronzeEvidence.Expected(INSTALL,StrictBronzeEvidence.POLICY,"ORIGINAL",RUN,ATTEMPT,"source",control,
                identity(control),identity(root),identity(root.resolve(StrictBronzeEvidence.LOCK)),HASH,frozen,writer);
        byte[] ledger=StrictBronzeEvidence.ledger(initial);
        var expected=new StrictBronzeEvidence.Expected(INSTALL,StrictBronzeEvidence.POLICY,"ORIGINAL",RUN,ATTEMPT,"source",control,
                initial.controlIdentity(),initial.rootIdentity(),initial.lockIdentity(),StrictFetchJson.sha(ledger),frozen,writer);
        file(root.resolve(StrictBronzeEvidence.MANIFEST),ledger);
        Path operation=directory(directory(root.resolve("store-bronze")).resolve(writer.checkpointId()));file(operation.resolve("command.json"),JSON);
        return new Fixture(expected,new StrictBronzeEvidence.Current(StrictBronzeEvidence.POLICY,frozen,writer),root,operation);
    }
    private static Path directory(Path path)throws IOException{return Files.createDirectory(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));}
    private static void file(Path path,byte[] bytes)throws IOException{Files.createFile(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));Files.write(path,bytes);}
    private static StrictBronzeEvidence.Identity identity(Path path)throws IOException{return new StrictBronzeEvidence.Identity(((Number)Files.getAttribute(path,"unix:dev")).longValue(),((Number)Files.getAttribute(path,"unix:ino")).longValue());}
    private static Map<String,String> snapshot(Path root)throws IOException{
        var result=new TreeMap<String,String>();try(var paths=Files.walk(root)){for(Path p:paths.toList())result.put(root.relativize(p).toString(),Files.getLastModifiedTime(p).toString()+":"+(Files.isRegularFile(p)?StrictFetchJson.sha(Files.readAllBytes(p)):"directory"));}return result;
    }
    @Test void exactPlanMatchesFetchGrammarAndHasUnchangedBounds()throws Exception{
        var f=fixture();var e=f.expected();var w=e.writer();
        var fetchWriter=new StrictFetchEvidence.Writer(INSTALL,WORKER,3,RUN,ATTEMPT,"FETCH_SOURCE",0,StrictFetchEvidence.checkpoint(RUN,ATTEMPT,"FETCH_SOURCE",0),w.commandSha256());
        var fetch=new StrictFetchEvidence.Expected(INSTALL,e.policy(),"ORIGINAL",RUN,ATTEMPT,"source",e.controlRoot(),
                new StrictFetchEvidence.Identity(e.controlIdentity().device(),e.controlIdentity().inode()),
                new StrictFetchEvidence.Identity(e.rootIdentity().device(),e.rootIdentity().inode()),
                new StrictFetchEvidence.Identity(e.lockIdentity().device(),e.lockIdentity().inode()),e.ledgerSha256(),new StrictFetchEvidence.Frozen(HASH,HASH,HASH),fetchWriter);
        byte[] ledger=StrictBronzeEvidence.ledger(e);assertArrayEquals(StrictFetchEvidence.ledger(fetch),ledger);
        var plan=StrictFetchJson.parse(ledger,256*1024);assertEquals(221,plan.get("directories").size()+plan.get("files").size()+3);
        long bytes=512*1024;for(var cap:plan.get("files"))bytes+=cap.longValue();assertEquals(64L*1024*1024,bytes);
        assertNotEquals(fetchWriter.checkpointId(),w.checkpointId());assertArrayEquals(JSON,f.sink().readCommand());
    }
    @Test void onlyLiveInvocationCanWriteOnlyEvidenceAndResult()throws Exception{
        var f=fixture();var sink=f.sink();var before=snapshot(f.root());
        assertThrows(IOException.class,()->sink.write("evidence.json",JSON));assertThrows(IOException.class,()->sink.write("result.json",JSON));assertEquals(before,snapshot(f.root()));
        sink.runFresh(()->{
            for(String name:List.of("command.json","verified.json","complete.json","payload.xml","metadata.json","descriptor.json","../result.json"))assertThrows(IOException.class,()->sink.write(name,JSON));
            sink.write("evidence.json",JSON);assertArrayEquals(JSON,sink.readCandidate("evidence.json"));
            assertThrows(IOException.class,()->sink.write("evidence.json",JSON));sink.write("result.json",JSON);return null;
        });sink.inspect();assertThrows(IOException.class,()->sink.write("evidence.json",JSON));
        assertFalse(Files.exists(f.operation().resolve("verified.json")));assertFalse(Files.exists(f.operation().resolve("result.json.preparing")));
    }
    @Test void eachWritableSlotPreservesExactLimitAndRejectsOversizeBeforeEffects()throws Exception{
        for(String name:List.of("evidence.json","result.json")){
            var f=fixture();var sink=f.sink();sink.runFresh(()->{
                var before=snapshot(f.root());assertThrows(IOException.class,()->sink.write(name,new byte[65537]));assertEquals(before,snapshot(f.root()));
                byte[] exact=new byte[65536];Arrays.fill(exact,(byte)17);sink.write(name,exact);assertArrayEquals(exact,Files.readAllBytes(f.operation().resolve(name)));
                if(!name.equals("result.json"))sink.write("result.json",JSON);return null;
            });sink.inspect();
        }
    }
    @Test void prefixIsDurableBeforeCallbackAndFreshProcessRefuses()throws Exception{
        var f=fixture();var sink=f.sink();var called=new AtomicInteger();
        sink.runFresh(()->{called.incrementAndGet();assertEquals(0,Files.size(f.operation().resolve("result.json.preparing")));assertThrows(IOException.class,f::sink);sink.write("result.json",JSON);return null;});
        assertEquals(1,called.get());assertThrows(IOException.class,()->f.sink().runFresh(()->{called.incrementAndGet();return null;}));assertEquals(1,called.get());
    }
    @Test void callbackFailureRetainsConsumedPrefix()throws Exception{
        var f=fixture();var sink=f.sink();assertThrows(IOException.class,()->sink.runFresh(()->{throw new IOException("injected");}));
        assertEquals(0,Files.size(f.operation().resolve("result.json.preparing")));assertThrows(IOException.class,f::sink);assertThrows(IOException.class,sink::inspect);
    }
    @Test void allPreNetworkForceBoundariesFailWithoutCallback()throws Exception{
        for(var boundary:List.of(StrictBronzeEvidence.Boundary.MARKER_CREATED,StrictBronzeEvidence.Boundary.MARKER_FORCED,StrictBronzeEvidence.Boundary.MARKER_DIRECTORY_FORCED)){
            var f=fixture();var calls=new AtomicInteger();var sink=new StrictBronzeEvidence(f.expected(),f.current(),at->{if(at==boundary)throw new IOException("injected");});
            assertThrows(IOException.class,()->sink.runFresh(()->{calls.incrementAndGet();return null;}));assertEquals(0,calls.get());assertThrows(IOException.class,f::sink);
        }
    }
    @Test void resultLinkInterruptionRetainsBothNamesAndCannotRestart()throws Exception{
        var f=fixture();var sink=new StrictBronzeEvidence(f.expected(),f.current(),at->{if(at==StrictBronzeEvidence.Boundary.RESULT_LINKED)throw new IOException("injected");});
        assertThrows(IOException.class,()->sink.runFresh(()->{sink.write("result.json",JSON);return null;}));
        assertTrue(Files.exists(f.operation().resolve("result.json.preparing")));assertEquals(2,((Number)Files.getAttribute(f.operation().resolve("result.json"),"unix:nlink")).intValue());assertThrows(IOException.class,f::sink);
    }
    @Test void evidenceLinkInterruptionRetainsPrefixAndCannotPublishResult()throws Exception{
        var f=fixture();var sink=new StrictBronzeEvidence(f.expected(),f.current(),at->{if(at==StrictBronzeEvidence.Boundary.EVIDENCE_LINKED)throw new IOException("injected");});
        assertThrows(IOException.class,()->sink.runFresh(()->{sink.write("evidence.json",JSON);sink.write("result.json",JSON);return null;}));
        assertTrue(Files.exists(f.operation().resolve("evidence.json.preparing")));assertTrue(Files.exists(f.operation().resolve("result.json.preparing")));
        assertFalse(Files.exists(f.operation().resolve("result.json")));assertEquals(2,((Number)Files.getAttribute(f.operation().resolve("evidence.json"),"unix:nlink")).intValue());assertThrows(IOException.class,f::sink);
    }
    @Test void replacementOfOwnedPrefixCannotBeAdopted()throws Exception{
        var f=fixture();var sink=f.sink();assertThrows(IOException.class,()->sink.runFresh(()->{
            Path prefix=f.operation().resolve("result.json.preparing");Files.move(prefix,temp.resolve("removed"));file(prefix,new byte[0]);sink.write("result.json",JSON);return null;
        }));assertFalse(Files.exists(f.operation().resolve("result.json")));
    }
    @Test void commandAndOperationDirectorySameBytesReplacementFailClosed()throws Exception{
        for(String kind:List.of("command","directory","ledger")){
            var f=fixture();var sink=f.sink();
            Path p=kind.equals("command")?f.operation().resolve("command.json"):kind.equals("ledger")?f.root().resolve(StrictBronzeEvidence.MANIFEST):f.operation();
            Path removed=temp.resolve(UUID.randomUUID().toString());var time=Files.getLastModifiedTime(p);byte[] bytes=kind.equals("directory")?null:Files.readAllBytes(p);Files.move(p,removed);
            if(bytes==null){directory(p);file(p.resolve("command.json"),JSON);}else file(p,bytes);Files.setLastModifiedTime(p,time);
            assertThrows(IOException.class,sink::inspect);assertThrows(IOException.class,()->sink.runFresh(()->fail("unexpected effect")));
        }
    }
    @Test void unknownEntriesPrefixesSymlinksAndHardlinksAreRejectedWithoutMutation()throws Exception{
        var f=fixture();var sink=f.sink();
        for(String name:List.of("evidence.json.preparing","verified.json.preparing","unplanned.json")){
            Path p=f.operation().resolve(name);file(p,new byte[0]);var before=snapshot(f.root());assertThrows(IOException.class,sink::inspect);assertEquals(before,snapshot(f.root()));Files.delete(p);
        }
        Path p=f.root().resolve("unknown");Files.createSymbolicLink(p,Path.of(StrictBronzeEvidence.MANIFEST));assertThrows(IOException.class,sink::inspect);Files.delete(p);
        Path link=temp.resolve("hardlink");Files.createLink(link,f.operation().resolve("command.json"));assertThrows(IOException.class,sink::inspect);Files.delete(link);
        // Changing the command's link metadata is also retained in its pinned stamp.
        assertThrows(IOException.class,sink::inspect);
    }
    @Test void completedFetchFilesAllowedButDependencyPrefixesRefused()throws Exception{
        var f=fixture();Path p=directory(directory(directory(f.root().resolve("fetch-source")).resolve("source")).resolve(StrictBronzeEvidence.checkpoint(RUN,ATTEMPT,"FETCH_SOURCE",0)));
        for(String name:List.of("command.json","payload.xml","metadata.json","descriptor.json","result.json","complete.json"))file(p.resolve(name),JSON);
        f.sink().inspect();file(p.resolve("result.json.preparing"),new byte[0]);assertThrows(IOException.class,f::sink);
    }
    @Test void concurrentCallCannotClearOwnerAndInspectionDoesNotWaitForNetwork()throws Exception{
        var f=fixture();var sink=f.sink();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var pool=Executors.newFixedThreadPool(2)){
            var first=pool.submit(()->sink.runFresh(()->{entered.countDown();assertTrue(release.await(10,TimeUnit.SECONDS));sink.write("result.json",JSON);return "done";}));
            assertTrue(entered.await(10,TimeUnit.SECONDS));
            try{
                var inspect=pool.submit(()->assertThrows(IOException.class,f::sink));assertNotNull(inspect.get(2,TimeUnit.SECONDS));
                assertThrows(IOException.class,()->sink.runFresh(()->fail("second effect")));
                assertThrows(IOException.class,()->sink.write("result.json",JSON));
            }finally{release.countDown();}
            assertEquals("done",first.get(10,TimeUnit.SECONDS));
        }sink.inspect();
    }
    @Test void wrongStageEpochFrozenIdentityAndLegacyLedgerReject()throws Exception{
        var f=fixture();var e=f.expected();var w=e.writer();
        for(String stage:List.of("FETCH_SOURCE","INSPECT_BRONZE")){
            var wrong=new StrictBronzeEvidence.Writer(INSTALL,WORKER,3,RUN,ATTEMPT,stage,0,StrictBronzeEvidence.checkpoint(RUN,ATTEMPT,stage,0),w.commandSha256());
            assertThrows(IOException.class,()->StrictBronzeEvidence.validateExpectations(e,new StrictBronzeEvidence.Current(e.policy(),e.frozen(),wrong)));
        }
        var zero=new StrictBronzeEvidence.Writer(INSTALL,WORKER,0,RUN,ATTEMPT,"STORE_BRONZE",0,w.checkpointId(),w.commandSha256());
        assertThrows(IOException.class,()->StrictBronzeEvidence.validateExpectations(e,new StrictBronzeEvidence.Current(e.policy(),e.frozen(),zero)));
        assertThrows(IOException.class,()->StrictBronzeEvidence.validateExpectations(e,new StrictBronzeEvidence.Current(e.policy(),new StrictBronzeEvidence.Frozen("b".repeat(64),HASH,HASH),w)));
        var ledger=StrictFetchJson.parse(Files.readAllBytes(f.root().resolve(StrictBronzeEvidence.MANIFEST)),256*1024);((com.fasterxml.jackson.databind.node.ObjectNode)ledger).put("version",1);
        Files.write(f.root().resolve(StrictBronzeEvidence.MANIFEST),StrictFetchJson.canonical(ledger));assertThrows(IOException.class,f::sink);
        assertFalse(e.toString().contains(RUN));assertFalse(w.toString().contains(WORKER));
    }
}
