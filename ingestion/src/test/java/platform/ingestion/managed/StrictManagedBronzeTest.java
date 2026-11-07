package platform.ingestion.managed;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

/** Real fresh JVMs and loopback SDK requests, never a production launcher or activation. */
class StrictManagedBronzeTest {
    @TempDir Path temporary;
    static Process launch(String... args)throws IOException {
        var command=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",
                System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),"platform.ingestion.managed.StrictManagedBronze"));
        command.addAll(List.of(args));return new ProcessBuilder(command).start();
    }
    static void finish(Process child,int expected)throws Exception {
        assertTrue(child.waitFor(20,TimeUnit.SECONDS),"child timed out");
        String stderr=new String(child.getErrorStream().readAllBytes(),StandardCharsets.UTF_8);
        assertEquals(expected,child.exitValue(),stderr);assertEquals("",new String(child.getInputStream().readAllBytes(),StandardCharsets.UTF_8));
        String nativeLoggingWarning="SLF4J: Failed to load class \"org.slf4j.impl.StaticLoggerBinder\".\n"
                +"SLF4J: Defaulting to no-operation (NOP) logger implementation\n"
                +"SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.\n";
        if(stderr.startsWith(nativeLoggingWarning))stderr=stderr.substring(nativeLoggingWarning.length());
        assertEquals(expected==2?"STRICT_BRONZE_REJECTED\n":"",stderr);
    }
    static void kill(Process child)throws Exception{child.destroyForcibly();assertTrue(child.waitFor(10,TimeUnit.SECONDS));}
    static void send(Process child,byte[] bytes)throws Exception{child.getOutputStream().write(bytes);child.getOutputStream().close();}
    static void run(StrictBronzeFixture f,int code)throws Exception{Process child=launch();try{send(child,f.packet(child));finish(child,code);}finally{kill(child);}}
    static Map<String,String> snapshot(Path root)throws Exception {
        var out=new TreeMap<String,String>();try(var paths=Files.walk(root)){for(Path p:paths.filter(Files::isRegularFile).toList())out.put(root.relativize(p).toString(),StrictFetchJson.sha(Files.readAllBytes(p)));}return out;
    }
    static void unchangedParents(StrictBronzeFixture f,Map<String,String> before)throws Exception {
        var after=snapshot(f.root);for(var entry:before.entrySet())assertEquals(entry.getValue(),after.get(entry.getKey()),entry.getKey());
        String prefix=f.root.relativize(f.directory)+"/";
        for(String key:after.keySet())if(!before.containsKey(key))assertTrue(Set.of(prefix+"evidence.json",prefix+"result.json",prefix+"result.json.preparing").contains(key),key);
        assertArrayEquals(f.ledger,Files.readAllBytes(f.root.resolve(".content-evidence.json")));
    }
    @Test void exactPairProducesReservedCandidateOnlyAndCannotReplay()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){f.pair();var before=snapshot(f.root);run(f,0);
            assertEquals("EXACT",f.evidence().get("state").textValue());assertEquals("OK",f.result().get("status").textValue());assertEquals(0,f.puts.get());
            unchangedParents(f,before);var completed=snapshot(f.root);int gets=f.gets.get();run(f,2);assertEquals(gets,f.gets.get());assertEquals(completed,snapshot(f.root));}
    }
    @Test void absentPairWritesExactBytesThenIndependentlyReverifies()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){var before=snapshot(f.root);run(f,0);assertEquals(2,f.puts.get());assertTrue(f.gets.get()>=8);
            assertArrayEquals(f.payload,f.objects.get(f.payloadPath));assertArrayEquals(f.metadata,f.objects.get(f.metadataPath));unchangedParents(f,before);}
    }
    @ParameterizedTest @ValueSource(strings={"PAYLOAD_ONLY","METADATA_ONLY","CONFLICT","UNAVAILABLE"})
    void initialNonAbsentNonExactStateNeverInvokesWriter(String state)throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){
            if(state.equals("PAYLOAD_ONLY"))f.objects.put(f.payloadPath,f.payload);
            if(state.equals("METADATA_ONLY"))f.objects.put(f.metadataPath,f.metadata);
            if(state.equals("CONFLICT")){f.pair();f.objects.put(f.payloadPath,new byte[]{1});}
            if(state.equals("UNAVAILABLE"))f.error="AccessDenied";
            run(f,1);assertEquals(state,f.evidence().get("state").textValue());assertEquals("BRONZE_"+state,f.result().get("code").textValue());assertTrue(f.evidence().get("receipt").isNull());assertEquals(0,f.puts.get());}
    }
    @Test void ambiguousCommittedPutIsIndependentlyClassifiedWithoutBlindReplay()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){f.acknowledgeLost=true;run(f,1);
            assertEquals("PAYLOAD_ONLY",f.evidence().get("state").textValue());assertEquals(1,f.puts.get());
            assertArrayEquals(f.payload,f.objects.get(f.payloadPath));assertFalse(f.objects.containsKey(f.metadataPath));int gets=f.gets.get();run(f,2);assertEquals(gets,f.gets.get());assertEquals(1,f.puts.get());}
    }
    @Test void boundNoncanonicalCredentialJsonKeepsExistingStorageSemantics()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){
            Files.writeString(f.credentials,"{ \"secretKey\": \"fixture-secret\", \"accessKey\": \"fixture-access\" }\n");
            var storage=(ObjectNode)f.packet.path("bronze").path("storage");storage.put("credentialsSha256",StrictFetchJson.sha(Files.readAllBytes(f.credentials)));storage.set("credentialsStamp",StrictBronzeFixture.stamp(f.credentials));
            f.command.set("storage",storage.deepCopy());byte[] command=StrictFetchJson.canonical(f.command);Files.write(f.directory.resolve("command.json"),command);
            for(String kind:List.of("expected","current"))((ObjectNode)f.packet.path(kind).path("writer")).put("commandSha256",StrictFetchJson.sha(command));
            f.pair();run(f,0);assertEquals(0,f.puts.get());}
    }
    @ParameterizedTest @ValueSource(strings={"writer","fresh-verifier"})
    void networkInterruptionPreservesFlagAndAttemptsBoundedEvidence(String phase)throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){
            var thread=new java.util.concurrent.atomic.AtomicReference<Thread>();var interrupted=new java.util.concurrent.atomic.AtomicBoolean();
            var failure=new java.util.concurrent.atomic.AtomicReference<Throwable>();var status=new java.util.concurrent.atomic.AtomicInteger(-1);
            if(phase.equals("writer"))f.afterPut=n->{if(n==1)thread.get().interrupt();};
            else{f.pair();f.beforeGet=n->{if(n==3)thread.get().interrupt();};}
            f.packet.put("childPid",Long.toString(ProcessHandle.current().pid()));byte[] packet=StrictFetchJson.canonical(f.packet);
            Thread execution=new Thread(()->{try{status.set(StrictManagedBronze.execute(new java.io.ByteArrayInputStream(packet)));}catch(Throwable error){failure.set(error);}finally{interrupted.set(Thread.currentThread().isInterrupted());Thread.interrupted();}},"strict-bronze-interruption");
            thread.set(execution);execution.start();execution.join(35000);assertFalse(execution.isAlive(),"interrupted SDK call must remain bounded");
            assertNull(failure.get(),()->String.valueOf(failure.get()));assertTrue(interrupted.get(),"interrupt flag must be restored to caller");
            assertTrue(Files.exists(f.directory.resolve("result.json")));assertTrue(Files.exists(f.directory.resolve("evidence.json")));
            assertEquals(status.get()==0?"OK":"FAILED",f.result().path("status").textValue());assertTrue(status.get()==0 || status.get()==1);
        }
    }
    @Test void initiallyExactPairChangedBeforeFreshReadbackCannotEmitReceipt()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){f.pair();f.beforeGet=n->{if(n==3)f.objects.put(f.payloadPath,new byte[]{1});};
            run(f,1);assertEquals("CONFLICT",f.evidence().path("state").textValue());assertTrue(f.evidence().path("receipt").isNull());
            assertEquals("FAILED",f.result().path("status").textValue());assertEquals("BRONZE_CONFLICT",f.result().path("code").textValue());assertEquals(0,f.puts.get());}
    }
    @Test void newlyWrittenPairChangedBeforeReadbackCannotEmitReceipt()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){f.beforeGet=n->{if(n==5)f.objects.put(f.payloadPath,new byte[]{1});};
            run(f,1);assertEquals("CONFLICT",f.evidence().path("state").textValue());assertTrue(f.evidence().path("receipt").isNull());
            assertEquals("FAILED",f.result().path("status").textValue());assertEquals("BRONZE_CONFLICT",f.result().path("code").textValue());assertEquals(2,f.puts.get());}
    }
    @Test void nativeWriterRetainsPayloadOnlySecondReadRaceSemantics()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){f.beforeGet=n->{if(n==3)f.objects.put(f.payloadPath,f.payload);};run(f,0);
            assertEquals(1,f.puts.get(),"native writer completes metadata after racing exact payload appears");assertEquals("EXACT",f.evidence().get("state").textValue());}
    }
    @ParameterizedTest @ValueSource(strings={"childPid","strictRuntimeSha256","mode","stage","epoch","sourceId","checkpoint","ledger"})
    void substitutedAuthorityRejectsWithoutStorageOrFiles(String key)throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){Process child=launch();try{
            f.packet(child);var writer=(ObjectNode)f.packet.path("current").path("writer");
            switch(key){case "childPid"->f.packet.put(key,"1");case "strictRuntimeSha256"->f.packet.put(key,"a".repeat(64));case "mode"->f.packet.put(key,"PUBLISH");case "stage"->writer.put("stage","FETCH_SOURCE");case "epoch"->writer.put("epoch","4");case "sourceId"->((ObjectNode)f.packet.get("expected")).put("sourceId","other");case "checkpoint"->writer.put("checkpointId",f.fetched.commandNode.get("checkpointId").textValue());case "ledger"->{var p=f.root.resolve(".content-evidence.json");var n=(ObjectNode)StrictFetchJson.parse(Files.readAllBytes(p),65536);n.put("version",1);Files.write(p,StrictFetchJson.canonical(n));}}
            var before=snapshot(f.root);send(child,StrictFetchJson.canonical(f.packet));finish(child,2);assertEquals(0,f.gets.get());assertEquals(0,f.puts.get());assertEquals(before,snapshot(f.root));
        }finally{kill(child);}}
    }
    @ParameterizedTest @ValueSource(strings={"credentials","payload","completion","command","credential-inode","credential-symlink"})
    void changedDependenciesRejectBeforeNetwork(String target)throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){
            Path p=switch(target){case "payload"->f.fetched.directory.resolve("payload.xml");case "completion"->f.fetched.directory.resolve("complete.json");case "command"->f.directory.resolve("command.json");default->f.credentials;};
            if(target.equals("credential-inode")){byte[] bytes=Files.readAllBytes(p);Path replacement=p.resolveSibling("replacement");StrictFetchFixture.privateWrite(replacement,bytes);Files.move(replacement,p,StandardCopyOption.REPLACE_EXISTING);}
            else if(target.equals("credential-symlink")){Path replacement=p.resolveSibling("replacement");Files.move(p,replacement);Files.createSymbolicLink(p,replacement);}
            else Files.write(p,new byte[]{0});
            var before=snapshot(f.root);run(f,2);assertEquals(0,f.gets.get());assertEquals(0,f.puts.get());assertEquals(before,snapshot(f.root));}
    }
    @Test void commandCannotRedirectTheOriginalFetchJournal()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){
            ((ObjectNode)f.command.get("journal")).put("directory",temporary.toString());byte[] command=StrictFetchJson.canonical(f.command);Files.write(f.directory.resolve("command.json"),command);
            for(String kind:List.of("expected","current"))((ObjectNode)f.packet.path(kind).path("writer")).put("commandSha256",StrictFetchJson.sha(command));
            var before=snapshot(f.root);run(f,2);assertEquals(0,f.gets.get());assertEquals(before,snapshot(f.root));}
    }
    @Test void sameByteCompletionReplacementCannotReuseTheOriginalReference()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){
            Path complete=f.fetched.directory.resolve("complete.json"),replacement=temporary.resolve("completion-replacement");StrictFetchFixture.privateWrite(replacement,Files.readAllBytes(complete));Files.move(replacement,complete,StandardCopyOption.REPLACE_EXISTING);
            run(f,2);assertEquals(0,f.gets.get());assertFalse(Files.exists(f.directory.resolve("result.json.preparing")));}
    }
    @Test void fetchPayloadMutationDuringReadCannotProduceEvidence()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){f.pair();f.beforeGet=n->{if(n==1)try{Files.write(f.fetched.directory.resolve("payload.xml"),new byte[]{1});}catch(IOException e){throw new RuntimeException(e);}};
            run(f,2);assertTrue(Files.exists(f.directory.resolve("result.json.preparing")));assertFalse(Files.exists(f.directory.resolve("evidence.json")));assertFalse(Files.exists(f.directory.resolve("result.json")));assertEquals(0,f.puts.get());}
    }
    @Test void credentialMutationDuringReadLeavesConsumedPrefixWithoutEvidence()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){f.pair();f.beforeGet=n->{if(n==1)try{Files.writeString(f.credentials,"changed");}catch(IOException e){throw new RuntimeException(e);}};
            run(f,2);assertTrue(Files.exists(f.directory.resolve("result.json.preparing")));assertFalse(Files.exists(f.directory.resolve("result.json")));assertEquals(0,f.puts.get());int gets=f.gets.get();run(f,2);assertEquals(gets,f.gets.get());}
    }
    @Test void forcedPrefixPrecedesFirstGetAndConcurrentOrKilledChildCannotBeAdopted()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){f.entered=new CountDownLatch(1);f.release=new CountDownLatch(1);Process child=launch();try{
            send(child,f.packet(child));assertTrue(f.entered.await(15,TimeUnit.SECONDS));assertTrue(Files.exists(f.directory.resolve("result.json.preparing")));assertEquals(0,Files.size(f.directory.resolve("result.json.preparing")));
            int gets=f.gets.get();run(f,2);assertEquals(gets,f.gets.get());kill(child);f.release.countDown();run(f,2);assertEquals(gets,f.gets.get());assertEquals(0,f.puts.get());assertFalse(Files.exists(f.directory.resolve("result.json")));
        }finally{f.release.countDown();kill(child);}}
    }
    @Test void killedAfterCommittedPayloadDoesNotRetryOrCompletePartialObject()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){var entered=new CountDownLatch(1);f.release=new CountDownLatch(1);f.afterPut=n->{if(n==1){entered.countDown();try{f.release.await();}catch(InterruptedException e){Thread.currentThread().interrupt();}}};
            Process child=launch();try{send(child,f.packet(child));assertTrue(entered.await(15,TimeUnit.SECONDS));assertArrayEquals(f.payload,f.objects.get(f.payloadPath));kill(child);f.release.countDown();int gets=f.gets.get();run(f,2);assertEquals(gets,f.gets.get());assertEquals(1,f.puts.get());assertFalse(f.objects.containsKey(f.metadataPath));assertFalse(Files.exists(f.directory.resolve("result.json")));}finally{f.release.countDown();kill(child);}}
    }
    @ParameterizedTest @ValueSource(strings={"","{","{}","null","[]","{}{}","{\"bootstrapVersion\":1,\"bootstrapVersion\":1}","\ufeff{}"})
    void malformedFramingRejects(String packet)throws Exception{Process child=launch();try{send(child,packet.getBytes(StandardCharsets.UTF_8));finish(child,2);}finally{kill(child);}}
    @Test void argumentsRejectBeforeReadingStdin()throws Exception{Process child=launch("command.json");try{finish(child,2);}finally{kill(child);}}
    @Test void regularFileStdinIsNotBootstrapAuthority()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){Path input=temporary.resolve("packet.json");Files.write(input,StrictFetchJson.canonical(f.packet));var cmd=List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),"platform.ingestion.managed.StrictManagedBronze");Process child=new ProcessBuilder(cmd).redirectInput(input.toFile()).start();try{finish(child,2);assertEquals(0,f.gets.get());}finally{kill(child);}}
    }
    @Test void capRejectsWithoutEof()throws Exception{Process child=launch();try{try{child.getOutputStream().write(new byte[65536]);child.getOutputStream().flush();}catch(IOException ignored){}assertTrue(child.waitFor(4,TimeUnit.SECONDS));finish(child,2);}finally{kill(child);}}
    @Test void completePacketWithoutEofHasBoundedReceiveDeadlineAndNoEffects()throws Exception {
        try(var f=new StrictBronzeFixture(temporary)){Process child=launch();try{child.getOutputStream().write(f.packet(child));child.getOutputStream().flush();finish(child,2);assertEquals(0,f.gets.get());assertFalse(Files.exists(f.directory.resolve("result.json.preparing")));}finally{kill(child);}}
    }
}
