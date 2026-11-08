package platform.contentapi.execution;

import org.junit.jupiter.api.Test;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

class NarrowContentFetchHistoricalSchemaTest {
    private ObjectNode historical(){
        var n=SourceJson.object();n.putObject("attempt").put("attempt_id","original").put("state","INTERRUPTED");
        n.putArray("history").add(n.get("attempt").deepCopy());n.put("unchanged","retained");return n;
    }
    private ObjectNode upgraded(ObjectNode old){
        var n=old.deepCopy();((ObjectNode)n.get("attempt")).putNull("content_terminal_policy");
        ((ObjectNode)n.get("history").get(0)).putNull("content_terminal_policy");return n;
    }
    @Test void absentHistoricalMarkerMatchesOnlyAdditiveNullWithoutChangingRecordedSnapshot(){
        var before=historical();var actual=upgraded(before);byte[] raw=SourceJson.canonical(before),current=SourceJson.canonical(actual);
        assertThat(NarrowContentFetchRecoveryHandoff.sameHistoricalAttemptSchema(before,actual)).isTrue();
        assertThat(SourceJson.canonical(before)).isEqualTo(raw);assertThat(SourceJson.canonical(actual)).isEqualTo(current);
        assertThat(NarrowContentFetchRecoveryHandoff.sameHistoricalAttemptSchema(before,before)).isTrue();
        assertThat(NarrowContentFetchRecoveryHandoff.sameHistoricalAttemptSchema(actual,actual)).isTrue();
        assertThat(NarrowContentFetchRecoveryHandoff.sameHistoricalAttemptSchema(actual,before)).isFalse();
    }
    @Test void nonNullTerminalAuthorityAndEveryUnrelatedDifferenceRemainVisible(){
        var before=historical();var actual=upgraded(before);
        ((ObjectNode)actual.get("attempt")).put("content_terminal_policy","ddip-content-original-safe-end-v1");
        assertThat(NarrowContentFetchRecoveryHandoff.sameHistoricalAttemptSchema(before,actual)).isFalse();
        actual=upgraded(before);((ObjectNode)actual.get("history").get(0)).put("content_terminal_policy","ddip-content-original-safe-end-v1");
        assertThat(NarrowContentFetchRecoveryHandoff.sameHistoricalAttemptSchema(before,actual)).isFalse();
        assertThat(NarrowContentFetchRecoveryHandoff.sameHistoricalAttemptSchema(actual,actual)).isFalse();
        actual=upgraded(before);actual.put("unchanged","altered");assertThat(NarrowContentFetchRecoveryHandoff.sameHistoricalAttemptSchema(before,actual)).isFalse();
        actual=upgraded(before);((ObjectNode)actual.get("history").get(0)).put("unknown_nullable_field",(String)null);
        assertThat(NarrowContentFetchRecoveryHandoff.sameHistoricalAttemptSchema(before,actual)).isFalse();
    }
}
