package platform.ingestion.managed;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

/** Real independent verifier JVMs against a loopback GET fixture. No publisher execution. */
class StrictBronzeVerifierTest {
    @TempDir Path temporary;
    static Process launch(String... args)throws IOException{
        var cmd=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",
                System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),StrictBronzeVerifier.class.getName()));
        cmd.addAll(List.of(args));return new ProcessBuilder(cmd).start();
    }
    static ObjectNode packet(StrictBronzeFixture f)throws Exception{
        var original=f.packet.deepCopy();original.put("childPid",Long.toString(ProcessHandle.current().pid()));
        var facts=StrictManagedBronze.parse(StrictFetchJson.canonical(original));var dependency=StrictManagedBronze.dependency(facts);
        var evidence=StrictFetchJson.object().put("bronzeEvidenceVersion",1).put("commandSha256",StrictFetchJson.sha(StrictFetchJson.canonical(f.command))).put("state","EXACT");
        evidence.set("receipt",StrictBronzeVerifier.receipt(facts,dependency,"application/octet-stream","application/json"));
        StrictFetchFixture.privateWrite(f.directory.resolve("evidence.json"),StrictFetchJson.canonical(evidence));
        var result=StrictFetchJson.object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))result.set(key,f.command.get(key));
        result.put("status","OK").putNull("code");result.set("evidenceId",f.command.get("checkpointId"));result.putObject("counts");
        StrictFetchFixture.privateWrite(f.directory.resolve("result.json"),StrictFetchJson.canonical(result));
        var packet=original.deepCopy().put("mode","STORE_BRONZE_VERIFY").put("strictRuntimeSha256",StrictFetchJson.sha(StrictFetchJson.canonical(StrictBronzeVerifier.runtimeIdentity())));
        packet.put("candidateSha256",StrictBronzeVerifier.candidate(f.directory,StrictFetchJson.canonical(f.command),facts,dependency));return packet;
    }
    static byte[] finish(Process child,int exit)throws Exception{
        assertTrue(child.waitFor(20,TimeUnit.SECONDS));byte[] output=child.getInputStream().readAllBytes();
        String error=new String(child.getErrorStream().readAllBytes(),StandardCharsets.UTF_8);
        assertEquals(exit,child.exitValue(),error);assertTrue(output.length<=65536);
        String warning="SLF4J: Failed to load class \"org.slf4j.impl.StaticLoggerBinder\".\n"
                +"SLF4J: Defaulting to no-operation (NOP) logger implementation\n"
                +"SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.\n";
        if(error.startsWith(warning))error=error.substring(warning.length());
        assertEquals(exit==2?StrictBronzeVerifier.ERROR+"\n":"",error);
        if(exit!=0)assertEquals(0,output.length);return output;
    }
    static byte[] run(ObjectNode packet,int exit)throws Exception{
        Process child=launch();try{packet.put("childPid",Long.toString(child.pid()));StrictManagedBronzeTest.send(child,StrictFetchJson.canonical(packet));return finish(child,exit);}
        finally{StrictManagedBronzeTest.kill(child);}
    }
    @ParameterizedTest @ValueSource(strings={"EXACT","ABSENT","PAYLOAD_ONLY","METADATA_ONLY","CONFLICT","UNAVAILABLE"})
    void independentlyClassifiesAllStatesWithoutWrites(String state)throws Exception{
        try(var f=new StrictBronzeFixture(temporary)){
            var p=packet(f);switch(state){case "EXACT"->f.pair();case "PAYLOAD_ONLY"->f.objects.put(f.payloadPath,f.payload);case "METADATA_ONLY"->f.objects.put(f.metadataPath,f.metadata);
                case "CONFLICT"->{f.pair();f.objects.put(f.payloadPath,new byte[]{1});}case "UNAVAILABLE"->f.error="AccessDenied";default->{}}
            var before=StrictManagedBronzeTest.snapshot(temporary);byte[] output=run(p,state.equals("EXACT")?0:1);
            if(state.equals("EXACT"))assertArrayEquals(StrictFetchJson.canonical(f.evidence().get("receipt")),output);
            assertEquals(2,f.gets.get());assertEquals(0,f.puts.get());assertEquals(0,f.unexpected.get());assertEquals(before,StrictManagedBronzeTest.snapshot(temporary));
        }
    }
    @ParameterizedTest @ValueSource(strings={"application/octet-stream","application/xml","text/xml","application/xml; charset=utf-8"})
    void receiptUsesIndependentlyObservedExactMediaType(String type)throws Exception{
        try(var f=new StrictBronzeFixture(temporary)){var p=packet(f);f.pair();f.types.put(f.payloadPath,type);
            boolean exact=Set.of("application/octet-stream","application/xml").contains(type);byte[] output=run(p,exact?0:1);
            if(exact)assertEquals(type,StrictFetchJson.parse(output,65536).get("payloadContentType").textValue());assertEquals(0,f.puts.get());}
    }
    @Test void metadataMediaTypeMustBeExact()throws Exception{
        try(var f=new StrictBronzeFixture(temporary)){var p=packet(f);f.pair();f.types.put(f.metadataPath,"application/json; charset=utf-8");run(p,1);assertEquals(0,f.puts.get());}
    }
    @ParameterizedTest @ValueSource(strings={"credentials","payload","completion","evidence","result","candidate-inode"})
    void mutationAfterFirstGetStopsBeforeSecondGet(String target)throws Exception{
        try(var f=new StrictBronzeFixture(temporary)){var p=packet(f);f.pair();f.beforeGet=n->{if(n==1)try{
            Path path=switch(target){case "credentials"->f.credentials;case "payload"->f.fetched.directory.resolve("payload.xml");case "completion"->f.fetched.directory.resolve("complete.json");case "result"->f.directory.resolve("result.json");default->f.directory.resolve("evidence.json");};
            if(target.equals("candidate-inode")){Path replacement=temporary.resolve("replacement");StrictFetchFixture.privateWrite(replacement,Files.readAllBytes(path));Files.move(replacement,path,StandardCopyOption.REPLACE_EXISTING);}else Files.write(path,new byte[]{1});
        }catch(Exception ex){throw new IllegalStateException(ex);}};run(p,2);assertEquals(1,f.gets.get());assertEquals(0,f.puts.get());}
    }
    @ParameterizedTest @ValueSource(strings={"credentials","payload","completion","candidate-inode"})
    void mutationBeforeNetworkRejectsWithoutEffects(String target)throws Exception{
        try(var f=new StrictBronzeFixture(temporary)){var p=packet(f);Path path=switch(target){case "credentials"->f.credentials;case "payload"->f.fetched.directory.resolve("payload.xml");case "completion"->f.fetched.directory.resolve("complete.json");default->f.directory.resolve("evidence.json");};
            if(target.equals("candidate-inode")){Path replacement=temporary.resolve("replacement");StrictFetchFixture.privateWrite(replacement,Files.readAllBytes(path));Files.move(replacement,path,StandardCopyOption.REPLACE_EXISTING);}else Files.write(path,new byte[]{1});
            var before=StrictManagedBronzeTest.snapshot(temporary);run(p,2);assertEquals(0,f.gets.get());assertEquals(0,f.puts.get());assertEquals(before,StrictManagedBronzeTest.snapshot(temporary));}
    }
    @ParameterizedTest @ValueSource(strings={"candidateSha256","strictRuntimeSha256","mode","unknown","childPid"})
    void substitutedBootstrapRejectedWithoutNetwork(String key)throws Exception{
        try(var f=new StrictBronzeFixture(temporary)){var p=packet(f);Process child=launch();try{p.put("childPid",Long.toString(child.pid()));p.put(key,key.equals("childPid")?"1":"0".repeat(64));
            StrictManagedBronzeTest.send(child,StrictFetchJson.canonical(p));finish(child,2);assertEquals(0,f.gets.get());assertEquals(0,f.puts.get());}finally{StrictManagedBronzeTest.kill(child);}}
    }
    @Test void completePrefixRequiresEofBeforeDeadline()throws Exception{
        try(var f=new StrictBronzeFixture(temporary)){var p=packet(f);Process child=launch();try{p.put("childPid",Long.toString(child.pid()));child.getOutputStream().write(StrictFetchJson.canonical(p));child.getOutputStream().flush();finish(child,2);assertEquals(0,f.gets.get());}finally{StrictManagedBronzeTest.kill(child);}}
    }
    @Test void argumentsRejected()throws Exception{Process child=launch("command.json");try{finish(child,2);}finally{StrictManagedBronzeTest.kill(child);}}
    @ParameterizedTest @ValueSource(strings={"","{}","{}{}","{\"mode\":1,\"mode\":2}","null","[]"})
    void malformedFrameRejected(String packet)throws Exception{Process child=launch();try{StrictManagedBronzeTest.send(child,packet.getBytes(StandardCharsets.UTF_8));finish(child,2);}finally{StrictManagedBronzeTest.kill(child);}}
    @Test void mutationDuringSecondGetSuppressesReceipt()throws Exception{
        try(var f=new StrictBronzeFixture(temporary)){var p=packet(f);f.pair();f.beforeGet=n->{if(n==2)try{Files.write(f.credentials,new byte[]{1});}catch(IOException ex){throw new IllegalStateException(ex);}};
            run(p,2);assertEquals(2,f.gets.get());assertEquals(0,f.puts.get());}
    }
    @ParameterizedTest @ValueSource(strings={"result.json.preparing","verified.json","other.json"})
    void extraCandidateEntriesAreNeverAdopted(String name)throws Exception{
        try(var f=new StrictBronzeFixture(temporary)){var p=packet(f);StrictFetchFixture.privateWrite(f.directory.resolve(name),new byte[]{1});
            var before=StrictManagedBronzeTest.snapshot(temporary);run(p,2);assertEquals(0,f.gets.get());assertEquals(before,StrictManagedBronzeTest.snapshot(temporary));}
    }
    @Test void boundNonExactCandidateClaimDoesNotReplaceIndependentObservation()throws Exception{
        try(var f=new StrictBronzeFixture(temporary)){var p=packet(f);f.pair();
            var evidence=(ObjectNode)f.evidence();evidence.put("state","ABSENT").putNull("receipt");Files.write(f.directory.resolve("evidence.json"),StrictFetchJson.canonical(evidence));
            var result=(ObjectNode)f.result();result.put("status","FAILED").put("code","BRONZE_ABSENT").putNull("evidenceId");Files.write(f.directory.resolve("result.json"),StrictFetchJson.canonical(result));
            var facts=StrictManagedBronze.parseFacts(StrictFetchJson.parse(StrictFetchJson.canonical(p),65535));p.put("candidateSha256",StrictBronzeVerifier.candidate(f.directory,StrictFetchJson.canonical(f.command),facts,StrictManagedBronze.dependency(facts)));
            byte[] observed=run(p,0);assertEquals("application/octet-stream",StrictFetchJson.parse(observed,65536).get("payloadContentType").textValue());assertEquals(0,f.puts.get());}
    }
    @Test void oversizedFrameRejectsWithoutEof()throws Exception{
        Process child=launch();try{try{child.getOutputStream().write(new byte[65536]);child.getOutputStream().flush();}catch(IOException ignored){}finish(child,2);}finally{StrictManagedBronzeTest.kill(child);}
    }
    @Test void regularFileStdinRejected()throws Exception{
        Path file=temporary.resolve("packet");Files.writeString(file,"{}");
        var cmd=List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),StrictBronzeVerifier.class.getName());
        Process child=new ProcessBuilder(cmd).redirectInput(file.toFile()).start();try{finish(child,2);}finally{StrictManagedBronzeTest.kill(child);}
    }

}
