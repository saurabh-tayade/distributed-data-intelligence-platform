package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

/** Independent read-only verification of genuine strict-child HTTP journals. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentFetchCandidateVerificationIT extends NarrowContentFetchLaunchFixtures {
    byte[] body;
    @Override byte[] responsePayload(){return body==null?super.responsePayload():body;}
    @BeforeEach void resetBody(){body=null;}
    NarrowContentFetchCandidateVerificationTestSupport.Observation verify(){return verify(b->{});}
    NarrowContentFetchCandidateVerificationTestSupport.Observation verify(Consumer<String> hook){return NarrowContentFetchCandidateVerificationTestSupport.inspect(jdbc,tx,guard,operation,hook);}
    void candidate(){prepared();launch(b->{});}
    Map<String,List<String>> allMetadata(){
        var result=new TreeMap<>(metadata());
        for(String table:List.of("content_native_operations","content_native_launch_phases","content_fetch_launch_profiles","execution_profiles","storage_bindings","controls"))
            result.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));
        return result;
    }
    void rejectedWithoutWrites()throws Exception{
        var before=allMetadata();var files=filesystem();
        assertThatThrownBy(this::verify).isInstanceOf(RuntimeException.class);
        assertThat(allMetadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);retained();
    }
    void write(String name,byte[] bytes)throws Exception{Files.write(fetchDirectory().resolve(name),bytes);}
    ObjectNode json(String name)throws Exception{return (ObjectNode)SourceJson.parse(Files.readAllBytes(fetchDirectory().resolve(name)));}
    void writeJson(String name,ObjectNode value)throws Exception{write(name,SourceJson.canonical(value));}
    static void privateFile(Path path)throws Exception{Files.createFile(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));}
    static void privateDirectory(Path path)throws Exception{Files.createDirectory(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));}

    @Test void genuineCandidateIsVerifiedRepeatedlyWithoutAnyMutationOrAuthority()throws Exception{
        candidate();var before=allMetadata();var files=filesystem();var boundaries=new ArrayList<String>();var result=verify(boundaries::add);
        assertThat(result.operation()).isEqualTo(operation);assertThat(result.run()).isEqualTo(owner.runId());assertThat(result.attempt()).isEqualTo(owner.attemptId());
        assertThat(result.checkpoint()).isEqualTo(jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.content_native_operations WHERE operation_id=?",UUID.class,operation));
        assertThat(result.candidateSha256()).isEqualTo(facts().candidateSha256());assertThat(result.payloadBytes()).isEqualTo(payload.length);
        assertThat(result.rawHttpJournalVerified()).isTrue();assertThat(result.completed()).isFalse();assertThat(result.nativeLaunchCompatible()).isFalse();
        assertThat(boundaries).containsExactly("FACTS_READ","INVENTORY_READ","JOURNAL_VALIDATED","RECHECKED");
        assertThat(verify()).isEqualTo(result);assertThat(allMetadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);retained();
    }
    @ParameterizedTest @ValueSource(strings={"","this is not XML","<feed><broken>","\u0000\ufffdnot an RSS document"})
    void rawHttpBytesDoNotRequireXmlValidation(String raw)throws Exception{
        body=raw.getBytes(java.nio.charset.StandardCharsets.UTF_8);candidate();var before=allMetadata();var files=filesystem();
        assertThat(verify().rawHttpJournalVerified()).isTrue();assertThat(verify().payloadBytes()).isEqualTo(body.length);
        assertThat(Files.readAllBytes(fetchDirectory().resolve("payload.xml"))).isEqualTo(body);
        assertThat(allMetadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);retained();
    }
    @ParameterizedTest @ValueSource(strings={"command.json","result.json","metadata.json","descriptor.json","payload.xml"})
    void changedCandidateBytesFailClosed(String name)throws Exception{candidate();Files.writeString(fetchDirectory().resolve(name),"changed");rejectedWithoutWrites();}

    @ParameterizedTest @ValueSource(strings={"descriptor-identity","descriptor-status","descriptor-length","descriptor-payload-hash","descriptor-key","descriptor-field","metadata-uri","metadata-status","metadata-field","metadata-schema-fraction","metadata-schema-string"})
    void matchingRecordedInventoryDoesNotSubstituteForSemanticVerification(String change)throws Exception{
        prepared();launch(boundary->{if(boundary.equals("BEFORE_CANDIDATE"))try{
            var descriptor=json("descriptor.json");var metadata=json("metadata.json");
            switch(change){
                case "descriptor-identity"->descriptor.put("checkpointId",UUID.randomUUID().toString());
                case "descriptor-status"->descriptor.put("httpStatus",500);
                case "descriptor-length"->descriptor.put("payloadByteLength",payload.length+1);
                case "descriptor-payload-hash"->descriptor.put("payloadSha256","0".repeat(64));
                case "descriptor-key"->((ObjectNode)descriptor.get("expected")).put("payloadKey","../payload.xml");
                case "descriptor-field"->descriptor.put("unexpected",true);
                case "metadata-uri"->metadata.put("sourceUri","http://127.0.0.1/foreign");
                case "metadata-status"->metadata.put("httpStatus",201);
                case "metadata-field"->metadata.put("unexpected",true);
                case "metadata-schema-fraction"->metadata.put("schemaVersion",1.5);
                case "metadata-schema-string"->metadata.put("schemaVersion","1");
            }
            byte[] raw=SourceJson.canonical(metadata);write("metadata.json",raw);
            ((ObjectNode)descriptor.get("expected")).put("metadataByteLength",raw.length).put("metadataSha256",SourceJson.sha(raw));
            writeJson("descriptor.json",descriptor);
        }catch(Exception ex){throw new IllegalStateException(ex);}});
        assertThat(facts().candidateForVerification()).isTrue();rejectedWithoutWrites();
    }
    @ParameterizedTest @ValueSource(strings={"payload-bound","metadata-bound","complete","prefix","stage","extra-file","extra-source","extra-checkpoint","file-permission","directory-permission","hardlink","symlink","changed-ledger","replaced-lock","missing-payload","missing-directory"})
    void unsafeOrUnexpectedEvidenceIsRejected(String change)throws Exception{
        candidate();Path directory=fetchDirectory(),payloadPath=directory.resolve("payload.xml");
        switch(change){
            case "payload-bound"->Files.write(payloadPath,new byte[5*1024*1024+1]);
            case "metadata-bound"->Files.write(directory.resolve("metadata.json"),new byte[65537]);
            case "complete"->privateFile(directory.resolve("complete.json"));
            case "prefix"->privateFile(directory.resolve("result.json.preparing"));
            case "stage"->privateDirectory(attemptRoot().resolve("check-inputs"));
            case "extra-file"->privateFile(directory.resolve("unexpected"));
            case "extra-source"->privateDirectory(directory.getParent().getParent().resolve("foreign-source"));
            case "extra-checkpoint"->privateDirectory(directory.resolveSibling(UUID.randomUUID().toString()));
            case "file-permission"->Files.setPosixFilePermissions(payloadPath,PosixFilePermissions.fromString("rw-r--r--"));
            case "directory-permission"->Files.setPosixFilePermissions(directory,PosixFilePermissions.fromString("rwxr-xr-x"));
            case "hardlink"->Files.createLink(temp.resolve("payload-link-"+UUID.randomUUID()),payloadPath);
            case "symlink"->{Path saved=temp.resolve("payload-original-"+UUID.randomUUID());Files.move(payloadPath,saved);Files.createSymbolicLink(payloadPath,saved);}
            case "changed-ledger"->Files.writeString(attemptRoot().resolve(".content-evidence.json"),"{}");
            case "replaced-lock"->{Path lock=attemptRoot().resolve(".content-evidence.lock");Files.move(lock,temp.resolve("old-lock-"+UUID.randomUUID()));privateFile(lock);}
            case "missing-payload"->Files.delete(payloadPath);
            case "missing-directory"->Files.move(directory,temp.resolve("candidate-"+UUID.randomUUID()));
        }
        rejectedWithoutWrites();
    }
    @ParameterizedTest @ValueSource(strings={"FACTS_READ","INVENTORY_READ","JOURNAL_VALIDATED","RECHECKED"})
    void mutationDuringInspectionIsDetectedWithoutRepair(String boundary)throws Exception{
        candidate();var before=allMetadata();
        assertThatThrownBy(()->verify(b->{if(b.equals(boundary))try{Files.writeString(fetchDirectory().resolve("payload.xml"),"changed during inspection");}catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);
        assertThat(allMetadata()).isEqualTo(before);assertThat(Files.readString(fetchDirectory().resolve("payload.xml"))).isEqualTo("changed during inspection");assertThat(requests).hasValue(1);retained();
    }
    @ParameterizedTest @ValueSource(strings={"FACTS_READ","INVENTORY_READ","JOURNAL_VALIDATED","RECHECKED"})
    void lostGuardDuringInspectionCannotReturnObservation(String boundary){
        candidate();var before=allMetadata();assertThatThrownBy(()->verify(b->{if(b.equals(boundary))guard.close();})).isInstanceOf(RuntimeException.class);
        assertThat(allMetadata()).isEqualTo(before);assertThat(requests).hasValue(1);retained();
    }
    @Test void unknownOperationAndAbsentLaunchCannotAuthorizeVerification()throws Exception{
        prepared();var before=allMetadata();var files=filesystem();assertThatThrownBy(this::verify).isInstanceOf(RuntimeException.class);
        assertThat(allMetadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(0);noExecution();
    }
    @Test void missingFinalLaunchPhaseCannotVerifyEvenIfCandidateFilesExist()throws Exception{
        prepared();assertThatThrownBy(()->launch(b->{if(b.equals("BEFORE_CANDIDATE_PHASE"))throw new IllegalStateException("TEST_STOP");})).isInstanceOf(RuntimeException.class);
        assertThat(facts().candidateForVerification()).isFalse();rejectedWithoutWrites();
    }
    @ParameterizedTest @ValueSource(strings={"INVENTORY_READ","JOURNAL_VALIDATED","RECHECKED"})
    void identicalBytesAtNewFileIdentityAreNotTheInspectedCandidate(String boundary)throws Exception{
        candidate();var before=allMetadata();
        assertThatThrownBy(()->verify(b->{if(b.equals(boundary))try{
            Path path=fetchDirectory().resolve("payload.xml");byte[] raw=Files.readAllBytes(path);var modified=Files.getLastModifiedTime(path);
            Files.move(path,temp.resolve("old-payload-"+UUID.randomUUID()));privateFile(path);Files.write(path,raw);Files.setLastModifiedTime(path,modified);
        }catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);
        assertThat(allMetadata()).isEqualTo(before);assertThat(requests).hasValue(1);retained();
    }
    @Test void historicalCandidateDoesNotNeedInstalledRuntimeOrAnActiveOwner()throws Exception{
        candidate();Files.delete(installedFetchJar);guard.close();guard=InstallationHostGuard.acquire(binding);
        var before=allMetadata();var files=filesystem();assertThat(verify().rawHttpJournalVerified()).isTrue();
        assertThat(allMetadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);retained();
    }
    @Test void unknownOperationDoesNotSelectAnotherCandidate()throws Exception{
        candidate();operation=UUID.randomUUID();rejectedWithoutWrites();
    }
    @Test void verifierRequiresLiveInstallationGuard()throws Exception{candidate();guard.close();rejectedWithoutWrites();}
}
