package platform.contentapi.execution;

import java.util.*;
import org.junit.jupiter.api.Test;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;

class NarrowContentFetchReservationTest {
    final NarrowContentRuntimeQualificationTest.Fixture f=new NarrowContentRuntimeQualificationTest.Fixture();
    final UUID operation=UUID.randomUUID();
    final NarrowContentFetchReservation.Identity owner=new NarrowContentFetchReservation.Identity(f.host.installationId(),UUID.randomUUID(),7,f.run,f.attempt);
    NarrowContentFetchReservation.Payload build(){return NarrowContentFetchReservation.build(f.ready(),f.inspect(),owner,owner,operation);}

    @Test void canonicalHistoricalCommandAndIndependentBootstrapHaveExactFieldsAndDifferentRuntimeRoles(){
        var p=build();var c=SourceJson.parse(p.commandBytes());var b=SourceJson.parse(p.bootstrapBytes());
        var command=SourceJson.object().put("protocolVersion",1).put("runId",f.run.toString()).put("attemptId",f.attempt.toString())
                .put("ownerEpoch",7).put("stage","FETCH_SOURCE").put("itemOrdinal",0).put("checkpointId",p.checkpoint().toString())
                .put("sourceId","source-one").put("revision",1).put("capturedConfigSha256",f.frozen.get("sources").get(0).get("configurationSha256").stringValue())
                .put("endpointSha256",f.frozen.get("sources").get(0).get("endpointSha256").stringValue()).put("feedUri","http://127.0.0.1:1234/feed")
                .put("runtimeSha256",NarrowContentRuntimeQualificationTest.sha(f.configs.get("fetch"))).put("javaRuntime","21").put("testEndpoint",true).put("httpTimeoutMillis",1000);
        assertThat(p.commandBytes()).containsExactly(SourceJson.canonical(command));assertThat(c).isEqualTo(command);
        SourceJson.fields(b,"reservationVersion","mode","operationId","expected","current","fetch");
        assertThat(b.get("mode").stringValue()).isEqualTo("FETCH_SOURCE_RESERVATION");assertThat(b.has("childPid")).isFalse();
        assertThat(b.get("expected").get("writer")).isEqualTo(b.get("current").get("writer"));
        assertThat(b.get("expected").get("frozen")).isEqualTo(b.get("current").get("frozen"));
        assertThat(b.get("fetch").get("runtimeSha256").stringValue()).isNotEqualTo(b.get("expected").get("frozen").get("runtimeSha256").stringValue());
        assertThat(b.get("expected").get("writer").get("commandSha256").stringValue()).isEqualTo(SourceJson.sha(p.commandBytes()));
        assertThat(p.bootstrapBytes()).containsExactly(SourceJson.canonical(b));
        assertThat(p.commandBytes().length).isLessThanOrEqualTo(65536);assertThat(p.bootstrapBytes().length).isLessThan(65536);
    }
    @Test void fixedCheckpointDoesNotDependOnOperationIdAndEveryReservationBindsItsOperation(){
        var a=build();var b=NarrowContentFetchReservation.build(f.ready(),f.inspect(),owner,owner,UUID.randomUUID());
        assertThat(a.checkpoint()).isEqualTo(b.checkpoint());assertThat(a.commandBytes()).containsExactly(b.commandBytes());
        assertThat(a.bootstrapBytes()).isNotEqualTo(b.bootstrapBytes());
        var root=f.host.root().resolve("runs/"+f.run+"/attempts/"+f.attempt);
        assertThat(a.checkpoint().toString()).isEqualTo(ContentOperationPlan.original(root,f.run,f.attempt,"source-one").directory("FETCH_SOURCE",0).getFileName().toString());
    }
    @Test void bytesAndSnapshotsAreDefensiveCopies(){
        var p=build();byte[] c=p.commandBytes(),b=p.bootstrapBytes();c[0]=0;b[0]=0;
        assertThat(p.commandBytes()[0]).isEqualTo((byte)'{');assertThat(p.bootstrapBytes()[0]).isEqualTo((byte)'{');
        assertThat(p.toString()).doesNotContain(f.host.root().toString(),f.run.toString(),"1234");
    }
    @Test void independentCurrentOwnerCannotBeSubstituted(){
        for(var changed:List.of(new NarrowContentFetchReservation.Identity(UUID.randomUUID(),owner.instance(),7,f.run,f.attempt),
                new NarrowContentFetchReservation.Identity(owner.installation(),UUID.randomUUID(),7,f.run,f.attempt),
                new NarrowContentFetchReservation.Identity(owner.installation(),owner.instance(),8,f.run,f.attempt),
                new NarrowContentFetchReservation.Identity(owner.installation(),owner.instance(),7,UUID.randomUUID(),f.attempt),
                new NarrowContentFetchReservation.Identity(owner.installation(),owner.instance(),7,f.run,UUID.randomUUID())))
            assertThatThrownBy(()->NarrowContentFetchReservation.build(f.ready(),f.inspect(),owner,changed,operation)).hasMessage("RUNTIME_MISMATCH");
    }
    @Test void selfConsistentWrongOwnerCannotOverrideIndependentReady(){
        var different=new NarrowContentFetchReservation.Identity(owner.installation(),owner.instance(),7,UUID.randomUUID(),f.attempt);
        assertThatThrownBy(()->NarrowContentFetchReservation.build(f.ready(),f.inspect(),different,different,operation)).hasMessage("RUNTIME_MISMATCH");
    }
}
