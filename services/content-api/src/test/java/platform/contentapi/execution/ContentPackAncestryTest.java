package platform.contentapi.execution;

import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;

class ContentPackAncestryTest {
    private static final Instant START=Instant.parse("2026-01-01T00:00:00Z");
    private static JsonNode ancestry(String first,String second,boolean reverse){
        var n=SourceJson.object();n.putObject("proof").put("sha256","retained-proof");
        var ps=n.putArray("recordedProcesses");
        for(long pid:reverse?List.of(20L,10L):List.of(10L,20L))ps.addObject().put("pid",Long.toString(pid)).put("startedAt",START.toString()).put("state",pid==10?first:second);
        return n;
    }
    @Test void orderAndSafeProcessStateChangesDoNotChangeSealedIdentities(){
        var first=ancestry("ABSENT","REUSED",false);var second=ancestry("REUSED","ABSENT",true);
        ((tools.jackson.databind.node.ArrayNode)second.get("recordedProcesses")).add(second.get("recordedProcesses").get(0).deepCopy());
        var firstSaved=first.deepCopy();var secondSaved=second.deepCopy();
        var stable=RecoveryHandoff.contentPackAncestry(first);
        assertThat(stable).isEqualTo(RecoveryHandoff.contentPackAncestry(second));
        assertThat(stable.get("recordedProcesses").get(0).get("pid").stringValue()).isEqualTo("10");
        assertThat(stable.get("recordedProcesses").get(0).has("state")).isFalse();
        assertThat(first).isEqualTo(firstSaved);assertThat(second).isEqualTo(secondSaved);
        assertThat(first.get("recordedProcesses").get(0).get("state").stringValue()).isEqualTo("ABSENT");
        assertThat(second.get("recordedProcesses").get(0).get("state").stringValue()).isEqualTo("ABSENT");
    }
    @Test void identityAndProofChangesRemainVisible(){
        var first=ancestry("ABSENT","ABSENT",false);var changed=ancestry("ABSENT","ABSENT",false);
        ((tools.jackson.databind.node.ObjectNode)changed.get("recordedProcesses").get(0)).put("startedAt",START.plusSeconds(1).toString());
        assertThat(RecoveryHandoff.contentPackAncestry(first)).isNotEqualTo(RecoveryHandoff.contentPackAncestry(changed));
        changed=ancestry("ABSENT","ABSENT",false);((tools.jackson.databind.node.ObjectNode)changed.get("proof")).put("sha256","changed-proof");
        assertThat(RecoveryHandoff.contentPackAncestry(first)).isNotEqualTo(RecoveryHandoff.contentPackAncestry(changed));
    }
    @ParameterizedTest @EnumSource(RecoveryQuiescence.State.class)
    void stableProjectionDoesNotReplaceFreshProcessVeto(RecoveryQuiescence.State state){
        var stable=RecoveryHandoff.contentPackAncestry(ancestry("ABSENT","ABSENT",false));
        var identities=new ArrayList<RecoveryQuiescence.Identity>();
        for(var p:stable.get("recordedProcesses"))identities.add(new RecoveryQuiescence.Identity(Long.parseLong(p.get("pid").stringValue()),Instant.parse(p.get("startedAt").stringValue())));
        String digest="a".repeat(64);var review=new RecoveryQuiescence.Review(digest,START,true,true,true,Set.of(),List.of());
        if(state==RecoveryQuiescence.State.ABSENT || state==RecoveryQuiescence.State.REUSED)
            assertThatCode(()->RecoveryQuiescence.check(review,digest,Set.of(),identities,id->state,START)).doesNotThrowAnyException();
        else assertThatThrownBy(()->RecoveryQuiescence.check(review,digest,Set.of(),identities,id->state,START)).hasMessage("PROCESS_UNRESOLVED");
    }
    @Test void diagnosticReportsOnlyChangedFieldPaths(){
        var expected=SourceJson.object();expected.set("ancestry",ancestry("ABSENT","ABSENT",false));
        var changed=expected.deepCopy();((tools.jackson.databind.node.ObjectNode)changed.get("ancestry").get("proof")).put("sha256","private-value-not-for-logs");
        assertThat(ContentInheritedRecoveryIT.changedPackFields(expected,changed)).containsExactly("$.ancestry.proof");
    }
}
