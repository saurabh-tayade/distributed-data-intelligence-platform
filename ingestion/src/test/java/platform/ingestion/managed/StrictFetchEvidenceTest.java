package platform.ingestion.managed;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StrictFetchEvidenceTest {
    private static final String INSTALL="11111111-1111-4111-8111-111111111111";
    private static final String WORKER="22222222-2222-4222-8222-222222222222";
    private static final String RUN="33333333-3333-4333-8333-333333333333";
    private static final String ATTEMPT="44444444-4444-4444-8444-444444444444";
    private static final String HASH="a".repeat(64);
    @TempDir Path temp;
    private record Fixture(StrictFetchEvidence.Expected expected, StrictFetchEvidence.Current current, Path root, Path operation) {
        StrictFetchEvidence sink() throws IOException { return new StrictFetchEvidence(expected,current); }
    }
    private Fixture fixture() throws IOException {
        Path control=directory(temp.resolve("control")); Path path=control;
        for(String segment:List.of("runs",RUN,"attempts",ATTEMPT))path=directory(path.resolve(segment));
        Path root=path;
        file(root.resolve(StrictFetchEvidence.LOCK),new byte[0]);
        var frozen=new StrictFetchEvidence.Frozen(HASH,HASH,HASH);
        var writer=new StrictFetchEvidence.Writer(INSTALL,WORKER,3,RUN,ATTEMPT,"FETCH_SOURCE",0,
                StrictFetchEvidence.checkpoint(RUN,ATTEMPT,"FETCH_SOURCE",0),StrictFetchJson.sha(new byte[]{'{','}'}));
        var initial=new StrictFetchEvidence.Expected(INSTALL,StrictFetchEvidence.POLICY,"ORIGINAL",RUN,ATTEMPT,"source",control,
                identity(control),identity(root),identity(root.resolve(StrictFetchEvidence.LOCK)),HASH,frozen,writer);
        byte[] ledger=StrictFetchEvidence.ledger(initial);
        var expected=new StrictFetchEvidence.Expected(INSTALL,StrictFetchEvidence.POLICY,"ORIGINAL",RUN,ATTEMPT,"source",control,
                initial.controlIdentity(),initial.rootIdentity(),initial.lockIdentity(),StrictFetchJson.sha(ledger),frozen,writer);
        file(root.resolve(StrictFetchEvidence.MANIFEST),ledger);
        Path operation=root;
        for(String segment:List.of("fetch-source","source",writer.checkpointId()))operation=directory(operation.resolve(segment));
        file(operation.resolve("command.json"),new byte[]{'{','}'});
        return new Fixture(expected,new StrictFetchEvidence.Current(StrictFetchEvidence.POLICY,frozen,writer),root,operation);
    }
    private static Path directory(Path path) throws IOException {
        return Files.createDirectory(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    }
    private static void file(Path path, byte[] bytes) throws IOException {
        Files.createFile(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        Files.write(path,bytes);
    }
    private static StrictFetchEvidence.Identity identity(Path path) throws IOException {
        return new StrictFetchEvidence.Identity(((Number)Files.getAttribute(path,"unix:dev")).longValue(),
                ((Number)Files.getAttribute(path,"unix:ino")).longValue());
    }
    @Test void reconstructsCompleteFixedOriginalPlanAndLimits() throws Exception {
        Fixture f=fixture(); var plan=StrictFetchJson.parse(StrictFetchEvidence.ledger(f.expected()),256*1024);
        assertEquals(2,plan.path("version").intValue());
        assertEquals(221,plan.path("directories").size()+plan.path("files").size()+3);
        long bytes=512*1024; for(var cap:plan.path("files"))bytes+=cap.longValue();
        assertEquals(64L*1024*1024,bytes);
        var checkpoints=new HashSet<String>();
        plan.path("files").properties().forEach(entry->{ String name=entry.getKey(); checkpoints.add(name.substring(0,name.lastIndexOf('/'))); });
        assertEquals(19,checkpoints.size());
        assertEquals(StrictFetchEvidence.checkpoint(RUN,ATTEMPT,"SILVER",0),StrictFetchEvidence.checkpoint(RUN,ATTEMPT,"VERIFY_SILVER",0));
        assertArrayEquals(new byte[]{'{','}'},f.sink().readCommand());
    }
    @Test void writesOnlyChildSlotsAndNeverOverwrites() throws Exception {
        Fixture f=fixture(); var sink=f.sink();
        sink.write("payload.xml",new byte[]{1,2}); sink.write("metadata.json",new byte[]{'{','}'});
        assertThrows(IOException.class,()->sink.write("payload.xml",new byte[]{3}));
        for(String name:List.of("command.json","complete.json","child.json","../result.json"))
            assertThrows(IOException.class,()->sink.write(name,new byte[0]));
        assertThrows(IOException.class,()->sink.write("result.json",new byte[65537]));
        assertThrows(IOException.class,sink::requireFresh);
    }
    @Test void refusesRetainedPrefixUnknownEntrySymlinkAndHardlink() throws Exception {
        Fixture f=fixture(); var sink=f.sink(); Path prefix=f.operation().resolve("payload.xml.preparing");
        file(prefix,new byte[0]); assertThrows(IOException.class,sink::inspect); Files.delete(prefix);
        Path unknown=f.root().resolve("unknown"); file(unknown,new byte[0]); assertThrows(IOException.class,sink::inspect); Files.delete(unknown);
        Files.createSymbolicLink(unknown,Path.of(".content-evidence.json")); assertThrows(IOException.class,sink::inspect); Files.delete(unknown);
        Path link=temp.resolve("hardlink"); Files.createLink(link,f.operation().resolve("command.json"));
        assertThrows(IOException.class,sink::inspect); Files.delete(link); sink.inspect();
    }
    @Test void refusesChangedLedgerAndChangedLockIdentity() throws Exception {
        Fixture f=fixture(); var sink=f.sink(); byte[] ledger=Files.readAllBytes(f.root().resolve(StrictFetchEvidence.MANIFEST));
        Files.write(f.root().resolve(StrictFetchEvidence.MANIFEST),new byte[]{'{','}'}); assertThrows(IOException.class,sink::inspect);
        Files.write(f.root().resolve(StrictFetchEvidence.MANIFEST),ledger); sink.inspect();
        Path lock=f.root().resolve(StrictFetchEvidence.LOCK); Files.move(lock,f.root().resolve("old-lock")); file(lock,new byte[0]);
        assertThrows(IOException.class,sink::inspect);
    }
    @Test void durableMarkerPrecedesCallbackAndOnlyOwnerCanFinish() throws Exception {
        Fixture f=fixture(); var sink=f.sink(); AtomicInteger calls=new AtomicInteger();
        assertEquals("done",sink.runFresh(()-> {
            calls.incrementAndGet(); Path prefix=f.operation().resolve("result.json.preparing");
            assertEquals(0,Files.size(prefix));
            assertThrows(IOException.class,f::sink);
            sink.write("payload.xml",new byte[]{1}); sink.write("metadata.json",new byte[]{'{','}'});
            sink.write("descriptor.json",new byte[]{'{','}'}); sink.write("result.json",new byte[]{'{','}'});
            return "done";
        }));
        assertEquals(1,calls.get()); sink.inspect();
        assertFalse(Files.exists(f.operation().resolve("result.json.preparing")));
        assertThrows(IOException.class,()->f.sink().runFresh(()-> { calls.incrementAndGet(); return null; }));
        assertEquals(1,calls.get());
    }
    @Test void failureRetainsMarkerAndPreventsSecondFetch() throws Exception {
        Fixture f=fixture(); var sink=f.sink(); AtomicInteger calls=new AtomicInteger();
        assertThrows(IOException.class,()->sink.runFresh(()-> { calls.incrementAndGet(); throw new IOException("injected"); }));
        assertEquals(0,Files.size(f.operation().resolve("result.json.preparing")));
        assertThrows(IOException.class,f::sink); assertThrows(IOException.class,sink::inspect);
        assertThrows(IOException.class,()->sink.runFresh(()-> { calls.incrementAndGet(); return null; }));
        assertEquals(1,calls.get());
    }
    @Test void replacementOfOwnedPrefixCannotBeAdopted() throws Exception {
        Fixture f=fixture(); var sink=f.sink();
        assertThrows(IOException.class,()->sink.runFresh(()-> {
            Path prefix=f.operation().resolve("result.json.preparing"); Files.move(prefix,temp.resolve("removed-prefix"));
            file(prefix,new byte[0]); sink.write("result.json",new byte[]{'{','}'}); return null;
        }));
        assertFalse(Files.exists(f.operation().resolve("result.json")));
    }
    @Test void rejectsOtherStageAndRedactsExpectations() throws Exception {
        Fixture f=fixture(); var original=f.expected(); var w=original.writer();
        var writer=new StrictFetchEvidence.Writer(w.installationId(),w.workerInstanceId(),w.epoch(),RUN,ATTEMPT,
                "STORE_BRONZE",0,StrictFetchEvidence.checkpoint(RUN,ATTEMPT,"STORE_BRONZE",0),w.commandSha256());
        assertThrows(IOException.class,()->StrictFetchEvidence.validateExpectations(original,
                new StrictFetchEvidence.Current(StrictFetchEvidence.POLICY,original.frozen(),writer)));
        assertFalse(original.toString().contains(RUN)); assertFalse(w.toString().contains(WORKER));
    }
    @Test void prefixFaultsBeforeNetworkRetainEvidenceWithoutCallback() throws Exception {
        for(var boundary:List.of(StrictFetchEvidence.Boundary.MARKER_CREATED,
                StrictFetchEvidence.Boundary.MARKER_FORCED,StrictFetchEvidence.Boundary.MARKER_DIRECTORY_FORCED)) {
            Path original=temp; temp=directory(original.resolve(boundary.name()));
            Fixture f=fixture(); temp=original;
            var sink=new StrictFetchEvidence(f.expected(),f.current(),point->{ if(point==boundary)throw new IOException("injected"); });
            AtomicInteger calls=new AtomicInteger();
            assertThrows(IOException.class,()->sink.runFresh(()-> { calls.incrementAndGet(); return null; }));
            assertEquals(0,calls.get()); assertTrue(Files.exists(f.operation().resolve("result.json.preparing")));
            assertThrows(IOException.class,f::sink);
        }
    }
    @Test void finalLinkCrashRetainsBothNamesAndIsRefused() throws Exception {
        Fixture f=fixture();
        var sink=new StrictFetchEvidence(f.expected(),f.current(),point->{
            if(point==StrictFetchEvidence.Boundary.RESULT_LINKED)throw new IOException("injected");
        });
        assertThrows(IOException.class,()->sink.runFresh(()-> { sink.write("result.json",new byte[]{'{','}'}); return null; }));
        assertTrue(Files.exists(f.operation().resolve("result.json.preparing")));
        assertTrue(Files.exists(f.operation().resolve("result.json")));
        assertEquals(2,((Number)Files.getAttribute(f.operation().resolve("result.json"),"unix:nlink")).intValue());
        assertThrows(IOException.class,f::sink);
    }
    @Test void competingSameInstanceCallCannotClearOriginalOwnership() throws Exception {
        Fixture f=fixture(); var sink=f.sink();
        CountDownLatch entered=new CountDownLatch(1), release=new CountDownLatch(1);
        try(var executor=Executors.newSingleThreadExecutor()) {
            var first=executor.submit(()->sink.runFresh(()-> {
                entered.countDown(); assertTrue(release.await(10,TimeUnit.SECONDS));
                sink.write("result.json",new byte[]{'{','}'}); return "done";
            }));
            assertTrue(entered.await(10,TimeUnit.SECONDS));
            try { assertThrows(IOException.class,()->sink.runFresh(()->fail("second fetch"))); }
            finally { release.countDown(); }
            assertEquals("done",first.get(10,TimeUnit.SECONDS));
        }
        sink.inspect();
    }
    @Test void fullLedgerMatchesIndependentPythonAndBackendPlanGrammar() throws Exception {
        Path repository=Path.of("").toAbsolutePath();
        while(repository!=null && !Files.exists(repository.resolve("processing/narrow_evidence_sink.py")))repository=repository.getParent();
        assertNotNull(repository,"Repository root required for mandatory plan parity");
        var requests=new ArrayList<String>(); var ledgers=new ArrayList<byte[]>();
        for(String source:List.of("a","source-one","x".repeat(128))) {
            String run=UUID.randomUUID().toString(),attempt=UUID.randomUUID().toString();
            var request=StrictFetchJson.object().put("runId",run).put("attemptId",attempt).put("sourceId",source)
                    .put("rootDevice","123456789").put("rootInode","987654321");
            requests.add(new String(StrictFetchJson.canonical(request),StandardCharsets.UTF_8));
            var frozen=new StrictFetchEvidence.Frozen(HASH,HASH,HASH);
            var writer=new StrictFetchEvidence.Writer(INSTALL,WORKER,1,run,attempt,"FETCH_SOURCE",0,
                    StrictFetchEvidence.checkpoint(run,attempt,"FETCH_SOURCE",0),HASH);
            var expected=new StrictFetchEvidence.Expected(INSTALL,StrictFetchEvidence.POLICY,"ORIGINAL",run,attempt,source,
                    Path.of("/unused-test-control"),new StrictFetchEvidence.Identity(1,1),
                    new StrictFetchEvidence.Identity(123456789,987654321),new StrictFetchEvidence.Identity(1,2),HASH,frozen,writer);
            ledgers.add(StrictFetchEvidence.ledger(expected));
        }
        Path input=temp.resolve("plan-input"),output=temp.resolve("plan-output"),errors=temp.resolve("plan-errors");
        Files.write(input,requests,StandardCharsets.UTF_8);
        Process process=new ProcessBuilder(System.getProperty("transport.python","python3"),"-m","processing.tests.narrow_evidence_sink_parity")
                .directory(repository.toFile()).redirectInput(input.toFile()).redirectOutput(output.toFile()).redirectError(errors.toFile()).start();
        boolean exited=process.waitFor(60,TimeUnit.SECONDS); if(!exited)process.destroyForcibly();
        assertTrue(exited,"Independent plan probe timed out"); assertEquals(0,process.exitValue(),Files.readString(errors));
        List<String> actual=Files.readAllLines(output,StandardCharsets.UTF_8); assertEquals(ledgers.size(),actual.size());
        for(int i=0;i<ledgers.size();i++)assertArrayEquals(ledgers.get(i),Base64.getDecoder().decode(actual.get(i)));
    }

    @Test void eachChildSlotAcceptsExactBoundAndRejectsOneByteOverWithoutPrefix() throws Exception {
        for(String name:List.of("payload.xml","metadata.json","descriptor.json","result.json")) {
            Path original=temp; temp=directory(original.resolve(name.replace('.','-')));
            Fixture f=fixture(); temp=original; var sink=f.sink();
            int cap=name.equals("payload.xml")?5*1024*1024:65536;
            var before=snapshot(f.root());
            assertThrows(IOException.class,()->sink.write(name,new byte[cap+1]));
            assertEquals(before,snapshot(f.root()));
            assertFalse(Files.exists(f.operation().resolve(name+".preparing")));
            byte[] bytes=new byte[cap]; Arrays.fill(bytes,(byte)17); sink.write(name,bytes);
            assertArrayEquals(bytes,Files.readAllBytes(f.operation().resolve(name))); sink.inspect();
        }
    }
    @Test void refusingUnplannedEvidenceDoesNotMutateBytesOrModificationTimes() throws Exception {
        Fixture f=fixture(); var sink=f.sink(); file(f.operation().resolve("unplanned.json"),new byte[]{7,8,9});
        var before=snapshot(f.root());
        assertThrows(IOException.class,sink::inspect); assertThrows(IOException.class,sink::readCommand);
        assertThrows(IOException.class,sink::requireFresh); assertThrows(IOException.class,f::sink);
        assertThrows(IOException.class,()->sink.write("payload.xml",new byte[]{1}));
        assertEquals(before,snapshot(f.root()));
    }
    private static Map<String,String> snapshot(Path root) throws IOException {
        var result=new TreeMap<String,String>();
        try(var paths=Files.walk(root)) {
            for(Path path:paths.toList())result.put(root.relativize(path).toString(),
                    Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS).toString()+":"
                            +(Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)?StrictFetchJson.sha(Files.readAllBytes(path)):"directory"));
        }
        return result;
    }

}
