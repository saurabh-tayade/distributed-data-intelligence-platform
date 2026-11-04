package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.execution.NarrowContentPreparationFixtures.*;

/** Filesystem-only fresh JVM evidence. Not PostgreSQL restart, power loss, or READY authority. */
class NarrowContentPreparationRestartTest {
    @TempDir Path temp;
    private record Fixture(NarrowContentPreparation.Specification spec,Path host,Path input,Path expected){}
    private Fixture fixture()throws Exception{
        var host=OwnershipFixtures.host(temp.toRealPath());var spec=specification(host);
        return new Fixture(spec,Files.write(temp.resolve("test-host.json"),host.descriptor()),
                Files.write(temp.resolve("test-spec.json"),specificationBytes(spec)),temp.resolve("test-expectation.json"));
    }
    @Test void freshProcessPreparesAndAnotherOnlyInspectsWithoutReinitialization()throws Exception{
        var fixture=fixture();assertThat(child(fixture,"PREPARE",0)).isEqualTo("PREPARED");
        var before=snapshot(fixture.spec().host().root());
        assertThat(child(fixture,"INSPECT",0)).isEqualTo("INSPECTED 67108864 221");
        assertThat(child(fixture,"PREPARE",0)).isEqualTo("REJECTED INPUT_CHANGED");
        assertThat(snapshot(fixture.spec().host().root())).isEqualTo(before);
    }
    @ParameterizedTest @EnumSource(NarrowContentPreparation.Boundary.class)
    void hardExitLeavesPrefixesAndFreshProcessNeverAdoptsExistingRoot(NarrowContentPreparation.Boundary stop)throws Exception{
        var fixture=fixture();assertThat(child(fixture,stop.name(),43)).isEqualTo("HALTED "+stop.name());
        assertThat(Files.exists(fixture.expected())).isFalse();
        var before=snapshot(fixture.spec().host().root());
        if(Files.exists(root(fixture.spec()),LinkOption.NOFOLLOW_LINKS)){
            assertThat(child(fixture,"PREPARE",0)).isEqualTo("REJECTED INPUT_CHANGED");
            assertThat(snapshot(fixture.spec().host().root())).isEqualTo(before);
        }else{
            assertThat(stop).isIn(NarrowContentPreparation.Boundary.ANCESTOR_CREATED,NarrowContentPreparation.Boundary.ANCESTOR_PARENT_FORCED);
            assertThat(Files.isDirectory(fixture.spec().host().root().resolve("runs"))).isTrue();
            // An absent attempt root is not permission to retry durable PREPARING in a fresh owner.
        }
    }
    @ParameterizedTest @ValueSource(strings={"root","lock","manifest","downgrade","alternate-source"})
    void freshReaderRejectsMissingOrChangedPreparedEvidenceWithoutRecreatingIt(String mutation)throws Exception{
        var fixture=fixture();assertThat(child(fixture,"PREPARE",0)).isEqualTo("PREPARED");Path root=root(fixture.spec());
        if(mutation.equals("root")){
            try(var tree=Files.walk(root)){for(var path:tree.sorted(Comparator.reverseOrder()).toList())Files.delete(path);}
        }else if(mutation.equals("lock"))Files.delete(root.resolve(ContentEvidenceBudget.LOCK));
        else if(mutation.equals("manifest"))Files.delete(root.resolve(ContentEvidenceBudget.MANIFEST));
        else{
            var n=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(Files.readAllBytes(root.resolve(ContentEvidenceBudget.MANIFEST)),ContentEvidenceBudget.LEDGER);
            if(mutation.equals("downgrade")){
                n.put("version",1);n.remove("reservationPolicy");n.remove("planKind");n.remove("sourceId");
            }else ContentOperationPlan.original(root,fixture.spec().run(),fixture.spec().attempt(),"other-source").apply(n);
            Files.write(root.resolve(ContentEvidenceBudget.MANIFEST),SourceJson.canonical(n));
        }
        var before=snapshot(fixture.spec().host().root());
        assertThat(child(fixture,"INSPECT",0)).matches("REJECTED (INPUT_CHANGED|INPUT_UNAVAILABLE)");
        assertThat(snapshot(fixture.spec().host().root())).isEqualTo(before);
    }
    private String child(Fixture fixture,String mode,int exit)throws Exception{
        String classpath=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));
        Path output=temp.resolve("child-"+UUID.randomUUID()+".txt");
        var process=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",classpath,
                NarrowContentPreparationTestChild.class.getName(),fixture.host().toString(),fixture.input().toString(),mode,fixture.expected().toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try{
            assertThat(process.pid()).isNotEqualTo(ProcessHandle.current().pid());
            assertThat(process.waitFor(20,TimeUnit.SECONDS)).as("bounded preparation child must exit").isTrue();
            String text=Files.readString(output);assertThat(process.exitValue()).withFailMessage("Unexpected child output: %s",text).isEqualTo(exit);
            var lines=text.lines().toList();assertThat(lines).withFailMessage("Unexpected child output: %s",text).hasSize(2);
            assertThat(lines.getFirst()).isEqualTo("PID "+process.pid());return lines.get(1);
        }finally{
            if(process.isAlive()){
                process.descendants().forEach(ProcessHandle::destroyForcibly);process.destroyForcibly();
                assertThat(process.waitFor(5,TimeUnit.SECONDS)).as("timed-out test child cleanup").isTrue();
            }
        }
    }
}
