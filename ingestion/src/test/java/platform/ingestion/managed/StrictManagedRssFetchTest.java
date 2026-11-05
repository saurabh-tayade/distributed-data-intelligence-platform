package platform.ingestion.managed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.io.TempDir;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

/** These processes use only explicitly constructed test packets, never launcher-derived authority. */
class StrictManagedRssFetchTest {
    static final ObjectMapper JSON = new ObjectMapper();
    static final String CHILD = "platform.ingestion.managed.StrictManagedRssFetch";

    static Process launch(String... args) throws IOException {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin/java").toString(), "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), CHILD));
        command.addAll(List.of(args));
        return new ProcessBuilder(command).start();
    }

    static void finish(Process child, int expected) throws Exception {
        assertTrue(child.waitFor(12, TimeUnit.SECONDS), "fresh child did not terminate");
        assertEquals(expected, child.exitValue());
        assertEquals("", new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
        String stderr = new String(child.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        if (expected == 2) assertEquals("STRICT_FETCH_REJECTED\n", stderr);
        else assertEquals("", stderr);
    }

    static void kill(Process child) throws InterruptedException {
        child.destroyForcibly();
        child.waitFor(10, TimeUnit.SECONDS);
    }

    static void reject(byte[] bytes) throws Exception {
        Process child = launch();
        try {
            child.getOutputStream().write(bytes);
            child.getOutputStream().close();
            finish(child, 2);
        } finally { kill(child); }
    }

    @Test void argumentsRejectWithoutWaitingForStdin() throws Exception {
        Process child = launch("command.json");
        try { finish(child, 2); } finally { kill(child); }
    }

    @ParameterizedTest
    @ValueSource(strings={"", "{", "{}", "null", "[]", "{}{}", "{\"bootstrapVersion\":1,\"bootstrapVersion\":1}", "\ufeff{}"})
    void malformedTruncatedDuplicateAndTrailingPacketsFailClosed(String packet) throws Exception {
        reject(packet.getBytes(StandardCharsets.UTF_8));
    }

    @Test void exactlyCapBytesRejectWithoutWaitingForEof() throws Exception {
        Process child = launch();
        try {
            // A pipe may close while this write completes: that is the intended cap rejection.
            try { child.getOutputStream().write(new byte[65536]); child.getOutputStream().flush(); }
            catch (IOException ignored) { }
            assertTrue(child.waitFor(4, TimeUnit.SECONDS), "cap must reject immediately, without EOF");
            finish(child, 2);
        } finally { kill(child); }
    }

    @Test void noEofRejectsWithinTheTotalReceiveDeadline() throws Exception {
        Process child = launch();
        long start = System.nanoTime();
        try {
            child.getOutputStream().write('{');
            child.getOutputStream().flush();
            assertFalse(child.waitFor(250, TimeUnit.MILLISECONDS));
            finish(child, 2);
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-start);
            assertTrue(millis >= 4000 && millis < 9000, "total receive deadline: " + millis);
        } finally { kill(child); }
    }

    @Test void trickledBytesDoNotResetTotalReceiveDeadline() throws Exception {
        Process child = launch();
        long start = System.nanoTime();
        try {
            for (int i=0; i<8 && child.isAlive(); i++) {
                try { child.getOutputStream().write(' '); child.getOutputStream().flush(); }
                catch (IOException ignored) { break; }
                child.waitFor(800, TimeUnit.MILLISECONDS);
            }
            finish(child, 2);
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-start) < 9000,
                    "input activity must not extend total deadline");
        } finally { kill(child); }
    }
    @TempDir Path temporary;

    static void send(Process child, StrictFetchFixture fixture) throws Exception {
        child.getOutputStream().write(fixture.packet(child));child.getOutputStream().close();
    }
    static Map<String,String> snapshot(Path root) throws Exception {
        var result=new TreeMap<String,String>();
        try(var entries=Files.walk(root)) {
            for(Path path:entries.filter(Files::isRegularFile).toList())
                result.put(root.relativize(path).toString(),StrictFetchFixture.sha(Files.readAllBytes(path)));
        }
        return result;
    }

    @Test void validPacketCannotTouchEvidenceOrHttpUntilEofThenReopensByteIdentically() throws Exception {
        try(var f=new StrictFetchFixture(temporary)) {
            Process child=launch();
            try {
                assertNotEquals(f.packet.get("expected").get("frozen").get("runtimeSha256"),f.packet.get("fetch").get("runtimeSha256"));
                var before=snapshot(f.root);
                child.getOutputStream().write(f.packet(child));child.getOutputStream().flush();
                assertFalse(child.waitFor(600,TimeUnit.MILLISECONDS));
                assertEquals(0,f.requests.get());assertEquals(before,snapshot(f.root));
                child.getOutputStream().close();finish(child,0);
                assertEquals(1,f.requests.get());
                var completed=snapshot(f.root);
                var feed=ManagedRssFetch.reopen(f.directory,f.command);
                assertArrayEquals(f.body,feed.body());assertEquals(200,feed.httpStatus());
                assertEquals("application/atom+xml; charset=UTF-8",feed.contentType().orElseThrow());
                assertEquals(f.body.length,feed.contentLength().orElseThrow());
                assertEquals(completed,snapshot(f.root),"reopening must be read-only");
                assertFalse(Files.exists(f.directory.resolve("complete.json")));
                assertEquals("OK",JSON.readTree(Files.readAllBytes(f.directory.resolve("result.json"))).get("status").textValue());
            } finally { kill(child); }
        }
    }

    @ParameterizedTest @ValueSource(strings={"pid","controlIdentity","rootIdentity","lockIdentity","command","runtime","current","ledger","unknown","stringNumber","upperHash","uuid","duplicate","whitespace","trailing","overflow","boolean","recovery","stage"})
    void wrongBindingsAndNoncanonicalPacketsRejectBeforeHttp(String fault) throws Exception {
        try(var f=new StrictFetchFixture(temporary)) {
            Process child=launch();
            try {
                f.packet(child);
                ObjectNode e=(ObjectNode)f.packet.get("expected"), fetch=(ObjectNode)f.packet.get("fetch");
                switch(fault) {
                    case "pid" -> f.packet.put("childPid","1");
                    case "controlIdentity","rootIdentity","lockIdentity" -> ((ObjectNode)e.get(fault)).put("inode","1");
                    case "command" -> StrictFetchFixture.privateWrite(f.directory.resolve("command.json"),"{}".getBytes(StandardCharsets.UTF_8));
                    case "runtime" -> fetch.put("javaRuntime","21.0.0-invalid");
                    case "current" -> ((ObjectNode)f.packet.get("current").get("writer")).put("epoch","4");
                    case "ledger" -> e.put("ledgerSha256","f".repeat(64));
                    case "unknown" -> fetch.put("unexpected",true);
                    case "stringNumber" -> fetch.put("revision","7");
                    case "upperHash" -> fetch.put("endpointSha256","B".repeat(64));
                    case "uuid" -> e.put("runId","1-1-1-1-1");
                    case "overflow" -> fetch.put("revision",new java.math.BigInteger("18446744073709551623"));
                    case "boolean" -> fetch.put("testEndpoint",1);
                    case "recovery" -> e.put("planKind","RECOVERY");
                    case "stage" -> {for(String section:List.of("expected","current"))((ObjectNode)f.packet.get(section).get("writer")).put("stage","STORE_BRONZE");}
                    default -> { }
                }
                byte[] packet=StrictFetchFixture.canonical(f.packet);
                String text=new String(packet,StandardCharsets.UTF_8);
                if(fault.equals("duplicate"))packet=text.replace("\"bootstrapVersion\":1", "\"bootstrapVersion\":1,\"bootstrapVersion\":1").getBytes(StandardCharsets.UTF_8);
                if(fault.equals("whitespace"))packet=(" "+text).getBytes(StandardCharsets.UTF_8);
                if(fault.equals("trailing"))packet=(text+"{}").getBytes(StandardCharsets.UTF_8);
                var before=snapshot(f.root);
                child.getOutputStream().write(packet);child.getOutputStream().close();finish(child,2);
                assertEquals(0,f.requests.get());assertEquals(before,snapshot(f.root));
            } finally { kill(child); }
        }
    }

    @Test void completedCandidateCannotFetchAgain() throws Exception {
        try(var f=new StrictFetchFixture(temporary)) {
            Process first=launch();try {send(first,f);finish(first,0);}finally{kill(first);}
            var before=snapshot(f.root);
            Process second=launch();try{send(second,f);finish(second,2);}finally{kill(second);}
            assertEquals(1,f.requests.get());assertEquals(before,snapshot(f.root));
        }
    }

    @ParameterizedTest @ValueSource(strings={"payload.xml.preparing","metadata.json.preparing","descriptor.json.preparing","result.json.preparing","complete.json.preparing","payload.xml","result.json"})
    void retainedPrefixesAndPriorEvidenceBlockFetchWithoutCleanup(String name) throws Exception {
        try(var f=new StrictFetchFixture(temporary)) {
            StrictFetchFixture.privateWrite(f.directory.resolve(name),new byte[]{1,2,3});
            var before=snapshot(f.root);
            Process child=launch();try{send(child,f);finish(child,2);}finally{kill(child);}
            assertEquals(0,f.requests.get());assertEquals(before,snapshot(f.root));
        }
    }

    @ParameterizedTest @ValueSource(ints={500,302})
    void httpFailureProducesOnlyFailureResultWithoutRetry(int status) throws Exception {
        try(var f=new StrictFetchFixture(temporary)) {
            var redirected=new AtomicInteger();
            f.server.createContext("/redirect-target",exchange->{redirected.incrementAndGet();exchange.sendResponseHeaders(200,-1);exchange.close();});
            f.location="http://127.0.0.1:"+f.server.getAddress().getPort()+"/redirect-target";
            f.status=status;Process child=launch();try{send(child,f);finish(child,1);}finally{kill(child);}
            assertEquals(1,f.requests.get());assertEquals(0,redirected.get(),"redirect target must never be requested");
            assertEquals("FAILED",JSON.readTree(Files.readAllBytes(f.directory.resolve("result.json"))).get("status").textValue());
            assertFalse(Files.exists(f.directory.resolve("payload.xml")));assertFalse(Files.exists(f.directory.resolve("complete.json")));
            Process again=launch();try{send(again,f);finish(again,2);}finally{kill(again);}
            assertEquals(1,f.requests.get());
        }
    }

    @ParameterizedTest @ValueSource(ints={5242880,5242881})
    void exactPayloadLimitAndFirstRejectedByte(int length) throws Exception {
        try(var f=new StrictFetchFixture(temporary)) {
            f.body=new byte[length];Process child=launch();
            try{send(child,f);finish(child,length==5242880?0:1);}finally{kill(child);}
            assertEquals(1,f.requests.get());
            if(length==5242880)assertArrayEquals(f.body,ManagedRssFetch.reopen(f.directory,f.command).body());
            else assertFalse(Files.exists(f.directory.resolve("payload.xml")));
            assertFalse(Files.exists(f.directory.resolve("complete.json")));
        }
    }

    @Test void concurrentChildrenRefuseClaimedFetchWhileFirstHttpIsPending() throws Exception {
        try(var f=new StrictFetchFixture(temporary)) {
            f.entered=new CountDownLatch(1);f.release=new CountDownLatch(1);
            Process first=launch(), second=launch();
            try {
                send(first,f);assertTrue(f.entered.await(8,TimeUnit.SECONDS));send(second,f);
                finish(second,2);assertEquals(1,f.requests.get());
                f.release.countDown();finish(first,0);assertEquals(1,f.requests.get());
            } finally {f.release.countDown();kill(first);kill(second);}
        }
    }

    @Test void killedAfterHttpStartedRetainsOneShotMarkerAndRestartCannotFetch() throws Exception {
        try(var f=new StrictFetchFixture(temporary)) {
            f.entered=new CountDownLatch(1);f.release=new CountDownLatch(1);
            Process first=launch();
            try {
                send(first,f);assertTrue(f.entered.await(8,TimeUnit.SECONDS));
                assertTrue(Files.isRegularFile(f.directory.resolve("result.json.preparing")));
            } finally {kill(first);f.release.countDown();}
            var before=snapshot(f.root);
            Process again=launch();try{send(again,f);finish(again,2);}finally{kill(again);}
            assertEquals(1,f.requests.get());assertEquals(before,snapshot(f.root));
            assertFalse(Files.exists(f.directory.resolve("complete.json")));
        }
    }

    @ParameterizedTest @ValueSource(strings={"revision","sourceId","feedUri","javaRuntime","runtimeSha256","ownerEpoch","itemOrdinal","testEndpoint","httpTimeoutMillis","protocolVersion","unknown","duplicate","noncanonical"})
    void commandMustMatchIndependentExpectedValuesEvenWhenItsDigestMatches(String field) throws Exception {
        try(var f=new StrictFetchFixture(temporary)) {
            ObjectNode command=f.commandNode.deepCopy();
            switch(field) {
                case "revision" -> command.put(field,8);
                case "sourceId" -> command.put(field,"other-feed");
                case "feedUri" -> command.put(field,command.get(field).textValue()+"/other");
                case "javaRuntime" -> command.put(field,"not-this-runtime");
                case "runtimeSha256" -> command.put(field,"f".repeat(64));
                case "ownerEpoch" -> command.put(field,new java.math.BigInteger("18446744073709551619"));
                case "itemOrdinal" -> command.put(field,1);
                case "testEndpoint" -> command.put(field,"true");
                case "httpTimeoutMillis" -> command.put(field,30001);
                case "protocolVersion" -> command.put(field,"1");
                case "unknown" -> command.put(field,true);
                default -> { }
            }
            byte[] changed=StrictFetchFixture.canonical(command);
            if(field.equals("duplicate"))changed=new String(changed,StandardCharsets.UTF_8).replace("\"revision\":7","\"revision\":7,\"revision\":7").getBytes(StandardCharsets.UTF_8);
            if(field.equals("noncanonical"))changed=(" "+new String(changed,StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
            StrictFetchFixture.privateWrite(f.directory.resolve("command.json"),changed);
            // Hash agreement alone is not semantic authority: fetch expectations remain unchanged.
            for(String section:List.of("expected","current"))
                ((ObjectNode)f.packet.get(section).get("writer")).put("commandSha256",StrictFetchFixture.sha(changed));
            var before=snapshot(f.root);
            Process child=launch();try{send(child,f);finish(child,2);}finally{kill(child);}
            assertEquals(0,f.requests.get());assertEquals(before,snapshot(f.root));
        }
    }

    @Test void completeJsonPrefixDoesNotInspectAnAbsentCommandBeforeEof() throws Exception {
        try(var f=new StrictFetchFixture(temporary)) {
            Files.delete(f.directory.resolve("command.json"));
            Process child=launch();
            try {
                child.getOutputStream().write(f.packet(child));child.getOutputStream().flush();
                assertFalse(child.waitFor(600,TimeUnit.MILLISECONDS),"command absence must not be inspected before EOF");
                assertEquals(0,f.requests.get());
                StrictFetchFixture.privateWrite(f.directory.resolve("command.json"),f.command);
                child.getOutputStream().close();finish(child,0);assertEquals(1,f.requests.get());
            } finally {kill(child);}
        }
    }

    @Test void regularFileStdinIsNotAnAnonymousBootstrapPipe() throws Exception {
        Path packet=temporary.resolve("packet.json");Files.writeString(packet,"{}");
        List<String> args=List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",
                System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),CHILD);
        Process child=new ProcessBuilder(args).redirectInput(packet.toFile()).start();
        try {finish(child,2);}finally{kill(child);}
    }

    @Test void receiveReturnsAll65535BytesOnlyAtEofAndRejectsThe65536th() throws Exception {
        byte[] maximum=new byte[65535];java.util.Arrays.fill(maximum,(byte)'x');
        assertArrayEquals(maximum,StrictManagedRssFetch.receive(new ByteArrayInputStream(maximum)));
        assertThrows(IOException.class,()->StrictManagedRssFetch.receive(new ByteArrayInputStream(new byte[65536])));
    }

    @Test void malformedUtf8AndLoneSurrogatesReject() throws Exception {
        for(byte[] packet:List.of(
                new byte[]{'{','"','x','"',':','"',(byte)0xc3,(byte)0x28,'"','}'},
                "{\"x\":\"\\ud800\"}".getBytes(StandardCharsets.UTF_8),
                "{\"x\":\"\\udc00\"}".getBytes(StandardCharsets.UTF_8))) {
            assertThrows(IOException.class,()->StrictFetchJson.parse(packet,65535));reject(packet);
        }
    }

    @Test void nestingBeyondSixteenIsRejectedAtTheJsonBoundary() throws Exception {
        byte[] packet=("[".repeat(17)+"0"+"]".repeat(17)).getBytes(StandardCharsets.UTF_8);
        assertThrows(IOException.class,()->StrictFetchJson.parse(packet,65535));
        reject(packet);
    }

    @Test void interruptedHttpInFreshJvmPersistsFailureRestoresFlagAndCannotRefetch() throws Exception {
        List<String> args=List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",
                System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                InterruptedFetchDriver.class.getName(),temporary.toString());
        Process child=new ProcessBuilder(args).start();
        try {finish(child,0);}finally{kill(child);}
    }

    /** Isolates the deliberate interrupt from both the test runner and production main. */
    public static final class InterruptedFetchDriver {
        private InterruptedFetchDriver() { }
        public static void main(String[] args) throws Exception {
            try(var f=new StrictFetchFixture(Path.of(args[0]))) {
                f.entered=new CountDownLatch(1);f.release=new CountDownLatch(1);
                f.packet.put("childPid",Long.toString(ProcessHandle.current().pid()));
                byte[] packet=StrictFetchFixture.canonical(f.packet);
                var status=new AtomicInteger(-1);var restored=new AtomicBoolean();var failure=new AtomicReference<Throwable>();
                Thread worker=Thread.ofPlatform().start(()-> {
                    try {status.set(StrictManagedRssFetch.execute(new ByteArrayInputStream(packet)));}
                    catch(Throwable error) {failure.set(error);}
                    finally {restored.set(Thread.currentThread().isInterrupted());}
                });
                try {
                    assertTrue(f.entered.await(6,TimeUnit.SECONDS),"wait until network, not a file lock, before interrupt");
                    worker.interrupt();worker.join(6000);
                    assertFalse(worker.isAlive(),"interrupted fetch must terminate");
                    assertNull(failure.get(),()->"unexpected interrupted execute failure: "+failure.get());
                    assertEquals(1,status.get());assertTrue(restored.get(),"caller interrupt flag must be restored");
                    var result=JSON.readTree(Files.readAllBytes(f.directory.resolve("result.json")));
                    assertEquals("FAILED",result.get("status").textValue());
                    assertEquals("FETCH_INTERRUPTED",result.get("code").textValue());
                    assertFalse(Files.exists(f.directory.resolve("complete.json")));
                    var before=snapshot(f.root);
                    assertThrows(Exception.class,()->StrictManagedRssFetch.execute(new ByteArrayInputStream(packet)));
                    assertEquals(before,snapshot(f.root));assertEquals(1,f.requests.get());
                } finally {f.release.countDown();worker.interrupt();worker.join(2000);}
            }
        }
    }

    @ParameterizedTest @ValueSource(strings={"http://user:pass@127.0.0.1/feed","http://127.0.0.1/feed?query=1","http://127.0.0.1/feed#fragment","http://feeds.example.org/feed"})
    void syntheticEndpointRestrictionsRejectBeforeEvidenceOrHttp(String endpoint) throws Exception {
        try(var f=new StrictFetchFixture(temporary)) {
            ((ObjectNode)f.packet.get("fetch")).put("feedUri",endpoint);
            var before=snapshot(f.root);
            Process child=launch();try{send(child,f);finish(child,2);}finally{kill(child);}
            assertEquals(0,f.requests.get());assertEquals(before,snapshot(f.root));
        }
    }

}
