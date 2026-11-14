package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;

/** Offline evidence tests; these create no database, native child or public request. */
class PackagedPublicContentAcceptanceTest {
    @TempDir Path temp;
    @Test void evidenceCannotOverwriteAnEarlierObservation()throws Exception{
        Path path=temp.resolve("observation.json");PackagedPublicContentAcceptance.write(path,new byte[]{1,2});
        assertThatThrownBy(()->PackagedPublicContentAcceptance.write(path,new byte[]{3})).isInstanceOf(FileAlreadyExistsException.class);
        assertThat(Files.readAllBytes(path)).containsExactly(1,2);
        assertThat(Files.getPosixFilePermissions(path)).isEqualTo(java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
    }
    @Test void evidenceCannotFollowASymlink()throws Exception{
        Path target=Files.write(temp.resolve("retained"),new byte[]{1});Path link=Files.createSymbolicLink(temp.resolve("link"),target);
        assertThatThrownBy(()->PackagedPublicContentAcceptance.write(link,new byte[]{3})).isInstanceOf(FileAlreadyExistsException.class);
        assertThat(Files.readAllBytes(target)).containsExactly(1);
    }
    @Test void inventoryIsStableAndDetectsRewrites()throws Exception{
        Path root=temp.toRealPath();Path file=Files.write(root.resolve("complete.json"),new byte[]{1,2});
        var first=PackagedPublicContentAcceptance.inventory(root);
        assertThat(PackagedPublicContentAcceptance.inventory(root)).isEqualTo(first);
        Files.write(file,new byte[]{1,3});assertThat(PackagedPublicContentAcceptance.inventory(root)).isNotEqualTo(first);
    }
    @Test void inventoryCannotEscapeThroughASymlink()throws Exception{
        Path root=Files.createDirectory(temp.resolve("root")).toRealPath();Path outside=Files.write(temp.resolve("outside"),new byte[]{1});
        Files.createSymbolicLink(root.resolve("escape"),outside);
        assertThatThrownBy(()->PackagedPublicContentAcceptance.inventory(root)).hasMessage("FILE_INVENTORY_UNSAFE");
        assertThat(Files.readAllBytes(outside)).containsExactly(1);
    }
    @Test void inventoryRejectsHardLinkedEvidence()throws Exception{
        Path root=Files.createDirectory(temp.resolve("root")).toRealPath();Path file=Files.write(root.resolve("native.json"),new byte[]{1});
        Files.createLink(temp.resolve("alias"),file);
        assertThatThrownBy(()->PackagedPublicContentAcceptance.inventory(root)).hasMessage("INVENTORY_HARD_LINK");
    }
    @Test void depthLimitCannotSilentlyOmitNestedEvidence()throws Exception{
        Path root=temp.toRealPath(),directory=root;
        for(int i=0;i<32;i++)directory=Files.createDirectory(directory.resolve("level"));
        Files.write(directory.resolve("native"),new byte[]{1});
        assertThatThrownBy(()->PackagedPublicContentAcceptance.inventory(root)).hasMessage("FILE_INVENTORY_UNSAFE");
    }
    @Test void boundedReadsRejectOversizedOrLinkedFiles()throws Exception{
        Path file=Files.write(temp.resolve("raw"),new byte[]{1,2});
        assertThatThrownBy(()->PackagedPublicContentAcceptance.bounded(file,1)).hasMessage("INPUT_TOO_LARGE");
        assertThatThrownBy(()->PackagedPublicContentAcceptance.bounded(Files.createSymbolicLink(temp.resolve("link"),file),2)).hasMessage("INPUT_NOT_REGULAR");
    }
    @Test void sqlByteExportsAreExactReusableAndCannotBeRewritten()throws Exception{
        var plan=SourceJson.object().put("evidenceDirectory",temp.toRealPath().toString()).put("installationRoot",temp.toRealPath().resolve("unused").toString()).put("apiPort",18080);
        plan.putObject("profiles");var runner=new PackagedPublicContentAcceptance(plan);
        byte[] raw=new byte[]{1,2,3};var first=runner.values(raw);assertThat(runner.values(raw)).isEqualTo(first);
        Path file=temp.resolve(first.get("file").stringValue());assertThat(Files.readAllBytes(file)).isEqualTo(raw);
        Files.write(file,new byte[]{4,5,6});assertThatThrownBy(()->runner.values(raw)).hasMessage("SQL_BYTE_EVIDENCE_CHANGED");
    }
    @Test void sqlProjectionKeepsNullIntegerAndUuidTypesWithoutSecrets()throws Exception{
        var plan=SourceJson.object().put("evidenceDirectory",temp.toRealPath().toString()).put("installationRoot",temp.toRealPath().resolve("unused").toString()).put("apiPort",18080);
        plan.putObject("profiles");var runner=new PackagedPublicContentAcceptance(plan);var map=new HashMap<String,Object>();
        map.put("empty",null);map.put("number",42L);map.put("id",UUID.fromString("10000000-0000-4000-8000-000000000001"));
        var value=runner.values(map);assertThat(value.get("empty").isNull()).isTrue();assertThat(value.get("number").longValue()).isEqualTo(42);
        assertThat(value.get("id").stringValue()).isEqualTo(map.get("id").toString());
    }
    @Test void productionAuthorityTypesAreAbsentFromTheStandaloneHelper()throws Exception{
        String source=Files.readString(Path.of("src/test/java/platform/contentapi/execution/PackagedPublicContentAcceptance.java"));
        for(String forbidden:List.of("WorkerRepository.Owner","WorkerTestSupport","executeOriginal(","originalPipeline(","destroyForcibly(","extends Ordinary"))
            assertThat(source).doesNotContain(forbidden);
    }
    PackagedPublicContentAcceptance frozenReader(tools.jackson.databind.JsonNode captured){
        var plan=SourceJson.object().put("evidenceDirectory",temp.toString()).put("installationRoot",temp.resolve("unused").toString()).put("apiPort",18080);
        plan.putObject("profiles").put("sourceId","qualification-feed").put("executionProfileId","approved-profile")
                .put("executionProfileSha256","a".repeat(64)).put("publisherReferenceArtifactId","approved-reference").put("publisherReferenceEvidenceSha256","b".repeat(64));
        var runner=new PackagedPublicContentAcceptance(plan);byte[] raw=SourceJson.canonical(captured);
        // Deterministic read-only SQL seam; no database, claim, Owner or native bootstrap exists.
        runner.jdbc=new JdbcTemplate(){@Override public <T> T queryForObject(String sql,Class<T> type,Object...args){
            assertThat(sql).startsWith("SELECT captured_selection ");return type.cast(raw);
        }};return runner;
    }
    tools.jackson.databind.node.ObjectNode selected(){
        var n=SourceJson.object();n.putArray("sources").addObject().put("sourceId","qualification-feed").put("revision",1)
                .putObject("endpoint").put("uri",PackagedPublicContentAcceptance.FIXTURE);
        n.putObject("executionProfile").put("profileId","approved-profile").put("configurationSha256","a".repeat(64));
        n.putArray("inputs").addObject().put("role","PUBLISHER_REFERENCE").put("ordinal",0)
                .put("artifactId","approved-reference").put("evidenceSha256","b".repeat(64));return n;
    }
    @Test void exactFrozenFixtureProfileAndReferenceAreReadOnlyObservations(){
        var n=selected();frozenReader(n).verifyFrozen(UUID.randomUUID(),SourceJson.canonical(n));
    }
    @ParameterizedTest @ValueSource(strings={"uri","source","revision","extra-source","profile","profile-sha","reference","reference-sha"})
    void changedFrozenSelectionIsRejectedBeforeAWorkerCanStart(String key){
        var n=selected();var source=(tools.jackson.databind.node.ObjectNode)n.get("sources").get(0);
        var profile=(tools.jackson.databind.node.ObjectNode)n.get("executionProfile");var reference=(tools.jackson.databind.node.ObjectNode)n.get("inputs").get(0);
        switch(key){
            case "uri" -> ((tools.jackson.databind.node.ObjectNode)source.get("endpoint")).put("uri","https://example.org/substitution");
            case "source" -> source.put("sourceId","other-feed");case "revision" -> source.put("revision",2);
            case "extra-source" -> n.withArray("sources").add(source.deepCopy());case "profile" -> profile.put("profileId","other-profile");
            case "profile-sha" -> profile.put("configurationSha256","c".repeat(64));case "reference" -> reference.put("artifactId","other-reference");
            case "reference-sha" -> reference.put("evidenceSha256","c".repeat(64));
        }
        var runner=frozenReader(n);assertThatThrownBy(()->runner.verifyFrozen(UUID.randomUUID(),SourceJson.canonical(n)))
                .isInstanceOf(PackagedPublicContentAcceptance.GateFailure.class);
        assertThat(runner.api).isNull();assertThat(runner.worker).isNull();
    }
}
