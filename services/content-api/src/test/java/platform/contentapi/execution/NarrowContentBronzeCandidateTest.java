package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

class NarrowContentBronzeCandidateTest {
    @TempDir Path temp;
    Path directory;byte[] command,descriptor;ObjectNode evidence,result;
    @BeforeEach void setup()throws Exception{
        directory=Files.createDirectory(temp.resolve("bronze"),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        var c=SourceJson.object().put("protocolVersion",1).put("runId",UUID.randomUUID().toString()).put("attemptId",UUID.randomUUID().toString())
                .put("stage","STORE_BRONZE").put("itemOrdinal",0).put("checkpointId",UUID.randomUUID().toString());
        c.putObject("storage").put("bindingId","bronze").put("bindingSha256","a".repeat(64)).putObject("configuration").put("bucket","bronze");command=SourceJson.canonical(c);
        var d=SourceJson.object();for(String key:List.of("runId","attemptId"))d.set(key,c.get(key));d.put("sourceId","feed").put("revision",1).put("capturedConfigSha256","b".repeat(64)).put("ingestionId",UUID.randomUUID().toString())
                .put("ingestedAt","2026-11-07T00:00:00Z").put("payloadByteLength",4).put("payloadSha256","c".repeat(64));
        d.putObject("expected").put("payloadKey","payload").put("metadataKey","metadata").put("metadataByteLength",4).put("metadataSha256","d".repeat(64));descriptor=SourceJson.canonical(d);
        write("command.json",command);
    }
    void write(String name,byte[] bytes)throws Exception{Path p=directory.resolve(name);if(Files.notExists(p))Files.createFile(p,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));Files.write(p,bytes);}
    void candidate(String state)throws Exception{
        var c=SourceJson.parse(command);evidence=SourceJson.object().put("bronzeEvidenceVersion",1).put("commandSha256",SourceJson.sha(command)).put("state",state);
        result=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))result.set(key,c.get(key));
        if(state.equals("EXACT")){
            var receipt=evidence.putObject("receipt").put("receiptVersion",1);var d=SourceJson.parse(descriptor);
            for(String key:List.of("runId","attemptId","sourceId","revision","capturedConfigSha256","ingestionId","ingestedAt","payloadByteLength","payloadSha256"))receipt.set(key,d.get(key));
            receipt.put("journalSha256",SourceJson.sha(descriptor)).put("storageBindingId","bronze").put("storageBindingSha256","a".repeat(64)).put("bucket","bronze");receipt.set("expected",d.get("expected"));receipt.put("payloadContentType","application/xml").put("metadataContentType","application/json");
            result.put("status","OK").putNull("code");result.set("evidenceId",c.get("checkpointId"));
        }else{evidence.putNull("receipt");result.put("status","FAILED").put("code","BRONZE_"+state).putNull("evidenceId");}
        result.putObject("counts");flush();
    }
    void flush()throws Exception{write("evidence.json",SourceJson.canonical(evidence));write("result.json",SourceJson.canonical(result));}
    @ParameterizedTest @ValueSource(strings={"EXACT","ABSENT","PAYLOAD_ONLY","METADATA_ONLY","CONFLICT","UNAVAILABLE"})
    void allSixStatesRequireExactCorrespondingExitAndCandidateFiles(String state)throws Exception{
        candidate(state);int exit=state.equals("EXACT")?0:1;var found=NarrowContentBronzeLaunch.candidate(directory,command,descriptor,exit);
        assertThat(found.state()).isEqualTo(state);assertThat(found.exitCode()).isEqualTo(exit);assertThat(found.sha256()).matches("[a-f0-9]{64}");
        assertThat(NarrowContentBronzeLaunch.candidate(directory,command,descriptor,exit)).isEqualTo(found);
        assertThatThrownBy(()->NarrowContentBronzeLaunch.candidate(directory,command,descriptor,1-exit)).hasMessage("RUNTIME_MISMATCH");
    }
    @ParameterizedTest @ValueSource(strings={"unknown-state","version-string","version-float","foreign-command","unknown-field","bad-status","bad-code","bad-id","nonempty-counts","false-receipt"})
    void malformedFailureCannotBecomeCandidate(String change)throws Exception{
        candidate("UNAVAILABLE");switch(change){case "unknown-state"->evidence.put("state","SUCCESS");case "version-string"->evidence.put("bronzeEvidenceVersion","1");case "version-float"->{/* Written below as noncanonical raw bytes. */}
            case "foreign-command"->evidence.put("commandSha256","0".repeat(64));case "unknown-field"->evidence.put("unknown",true);case "bad-status"->result.put("status","OK");case "bad-code"->result.put("code","OTHER");
            case "bad-id"->result.put("evidenceId",UUID.randomUUID().toString());case "nonempty-counts"->((ObjectNode)result.get("counts")).put("rows",1);case "false-receipt"->evidence.putObject("receipt");}flush();
        if(change.equals("version-float"))write("evidence.json",new String(SourceJson.canonical(evidence),java.nio.charset.StandardCharsets.UTF_8).replace("\"bronzeEvidenceVersion\":1","\"bronzeEvidenceVersion\":1.0").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThatThrownBy(()->NarrowContentBronzeLaunch.candidate(directory,command,descriptor,1)).isInstanceOf(RuntimeException.class);
    }
    @ParameterizedTest @ValueSource(strings={"receipt","identity","type","extra","prefix","hardlink"})
    void claimedExactNeedsStructuralCorrespondenceAndCleanInventory(String change)throws Exception{
        candidate("EXACT");switch(change){case "receipt"->evidence.putNull("receipt");case "identity"->((ObjectNode)evidence.get("receipt")).put("ingestionId",UUID.randomUUID().toString());case "type"->((ObjectNode)evidence.get("receipt")).put("metadataContentType","text/plain");
            case "extra"->write("verified.json",new byte[0]);case "prefix"->write("result.json.preparing",new byte[0]);case "hardlink"->Files.createLink(temp.resolve("alias"),directory.resolve("evidence.json"));}if(!change.equals("hardlink"))flush();
        assertThatThrownBy(()->NarrowContentBronzeLaunch.candidate(directory,command,descriptor,0)).isInstanceOf(RuntimeException.class);
    }
    @Test void sameByteReplacementChangesCandidateInventory()throws Exception{
        candidate("UNAVAILABLE");var before=NarrowContentBronzeLaunch.candidate(directory,command,descriptor,1);Path p=directory.resolve("evidence.json");byte[] bytes=Files.readAllBytes(p);var modified=Files.getLastModifiedTime(p);
        Files.move(p,temp.resolve("old-evidence"));write("evidence.json",bytes);Files.setLastModifiedTime(p,modified);
        assertThat(NarrowContentBronzeLaunch.candidate(directory,command,descriptor,1).sha256()).isNotEqualTo(before.sha256());
    }
    @Test void onlyExactNativeLoggingWarningOrSilenceIsAccepted(){
        String warning="SLF4J: Failed to load class \"org.slf4j.impl.StaticLoggerBinder\".\n"
                +"SLF4J: Defaulting to no-operation (NOP) logger implementation\n"
                +"SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.\n";
        for(String allowed:List.of("",warning)){
            byte[] bytes=allowed.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            assertThat(NarrowContentBronzeLaunch.outputAccepted(new ReadOnlyVerifierProcess.Result(1,null,0,bytes.length,Set.of(),SourceJson.sha(bytes)))).isTrue();
        }
        for(String denied:List.of("x".repeat(warning.length()),warning+"STRICT_BRONZE_REJECTED\n",warning+warning,warning.substring(1),"prefix"+warning)){
            byte[] bytes=denied.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            assertThat(NarrowContentBronzeLaunch.outputAccepted(new ReadOnlyVerifierProcess.Result(1,null,0,bytes.length,Set.of(),SourceJson.sha(bytes)))).isFalse();
        }
        assertThat(NarrowContentBronzeLaunch.outputAccepted(new ReadOnlyVerifierProcess.Result(1,null,1,0,Set.of()))).isFalse();
        assertThat(NarrowContentBronzeLaunch.outputAccepted(new ReadOnlyVerifierProcess.Result(1,null,0,210,Set.of()))).isFalse();
    }
    @Test void noncanonicalAndDuplicateEvidenceRejected()throws Exception{
        candidate("UNAVAILABLE");write("evidence.json",(" "+new String(SourceJson.canonical(evidence),java.nio.charset.StandardCharsets.UTF_8)).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThatThrownBy(()->NarrowContentBronzeLaunch.candidate(directory,command,descriptor,1)).isInstanceOf(RuntimeException.class);
        String raw=new String(SourceJson.canonical(evidence),java.nio.charset.StandardCharsets.UTF_8).replace("\"bronzeEvidenceVersion\":1","\"bronzeEvidenceVersion\":1,\"bronzeEvidenceVersion\":1");write("evidence.json",raw.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThatThrownBy(()->NarrowContentBronzeLaunch.candidate(directory,command,descriptor,1)).isInstanceOf(RuntimeException.class);
    }
}
