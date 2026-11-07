package platform.contentapi.management;

import java.time.*;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import static org.assertj.core.api.Assertions.*;

public class RunInspectionTest {
    final String key=Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
    @Test void expiresAtFixedHorizonWithoutRenewalAndBindsScope() {
        var signer=new SourceCursor(key);var clock=Clock.fixed(Instant.parse("2026-10-21T00:00:00Z"),ZoneOffset.UTC);var c=new RunPageCursor(signer,clock);var first=c.first(Long.MAX_VALUE,true);String token=c.next("runs","a".repeat(64),first,9007199254740993L);
        assertThat(token.length()).isLessThanOrEqualTo(512);assertThat(c.decode(token,"runs","a".repeat(64),true).after()).isEqualTo(9007199254740993L);
        assertThatThrownBy(()->c.decode(token,"runs","b".repeat(64),true)).hasMessage("INVALID_ARGUMENT");assertThatThrownBy(()->c.decode(token,"events","a".repeat(64),false)).hasMessage("INVALID_ARGUMENT");
        var expired=new RunPageCursor(signer,Clock.offset(clock,Duration.ofDays(1)));assertThatThrownBy(()->expired.decode(token,"runs","a".repeat(64),true)).hasMessage("INVALID_ARGUMENT");
        var position=c.decode(token,"runs","a".repeat(64),true);assertThat(signer.decode(c.next("runs","a".repeat(64),position,1)).get("expires")).isEqualTo(signer.decode(token).get("expires"));
    }
    @Test void malformedSignedStateAndUnknownVersionFail(){var signer=new SourceCursor(key);var c=new RunPageCursor(signer,Clock.systemUTC());var n=SourceJson.object().put("v",2).put("kind","runs").put("scope","a".repeat(64)).put("high","2").put("after","1").put("expires",Long.toString(Instant.now().getEpochSecond()+100));assertThatThrownBy(()->c.decode(signer.encode(n),"runs","a".repeat(64),true)).hasMessage("INVALID_ARGUMENT");n.put("v",1).put("after","3");assertThatThrownBy(()->c.decode(signer.encode(n),"runs","a".repeat(64),true)).hasMessage("INVALID_ARGUMENT");}
    @Test void safeCountsAreExactStringsAndUnknownFieldsFailClosed(){var n=SourceJson.object().put("rows","9007199254740993");assertThat(RunInspectionValues.counts(SourceJson.canonical(n))).isEqualTo(n);assertThat(RunInspectionValues.counts(null)).isNull();n.put("privateCredential","123");assertThatThrownBy(()->RunInspectionValues.counts(SourceJson.canonical(n))).hasMessage("INTERNAL_FAILURE");}
    @Test void unsafeCounterTypesClassificationsAndOverflowNeverLeak(){for(String s:new String[]{"{\"rows\":1}","{\"rows\":\"01\"}","{\"rows\":\"9223372036854775808\"}"})assertThatThrownBy(()->RunInspectionValues.counts(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))).hasMessage("INTERNAL_FAILURE");assertThatThrownBy(()->RunInspectionValues.code("PRIVATE_SECRET")).hasMessage("INTERNAL_FAILURE");assertThatThrownBy(()->RunInspectionValues.action("PRIVATE_ACTION")).hasMessage("INTERNAL_FAILURE");}
    @Test void nativeFetchTerminalClassificationsRemainExplicitAndFailClosed() {
        for(String code:new String[]{"NATIVE_FETCH_HANDOFF","NATIVE_FETCH_INTERRUPTED","RECONCILED_FAILED"})
            assertThat(RunInspectionValues.code(code)).isEqualTo(code);
        for(String action:new String[]{"NATIVE_FETCH_HANDOFF","NATIVE_FETCH_TERMINAL_ATTEMPT","NATIVE_FETCH_TERMINAL_RUN","NATIVE_FETCH_TERMINAL_CONTROL","NATIVE_FETCH_TERMINAL_WORKER","NATIVE_FETCH_TERMINAL_RELEASE"})
            assertThat(RunInspectionValues.action(action)).isEqualTo(action);
        assertThat(RunInspectionValues.state("STOPPED")).isEqualTo("STOPPED");
        for(String privateCode:new String[]{"NATIVE_FETCH_PRIVATE_REVIEW","NATIVE_FETCH_INTERRUPTED:PRIVATE_SECRET","NATIVE_FETCH_TERMINAL_UNKNOWN"}) {
            assertThatThrownBy(()->RunInspectionValues.code(privateCode)).hasMessage("INTERNAL_FAILURE");
            assertThatThrownBy(()->RunInspectionValues.action(privateCode)).hasMessage("INTERNAL_FAILURE");
        }
        assertThatThrownBy(()->RunInspectionValues.state("PRIVATE_PROCESS_STATE")).hasMessage("INTERNAL_FAILURE");
    }
    @Test void stageCursorBindsIdentityOrderingAndFixedExpiry() {
        var signer=new SourceCursor(key);var clock=Clock.fixed(Instant.parse("2026-11-02T00:00:00Z"),ZoneOffset.UTC);var c=new StagePageCursor(signer,clock);
        var high=new StagePageCursor.Key("VERIFY_RESULT",511);var after=new StagePageCursor.Key("FETCH_SOURCE",0);var first=c.first(high);
        String token=c.next("a".repeat(64),first,after);assertThat(token.length()).isLessThanOrEqualTo(512);
        var p=c.decode(token,"a".repeat(64));assertThat(p.high()).isEqualTo(high);assertThat(p.after()).isEqualTo(after);
        assertThat(c.decode(c.next("a".repeat(64),p,new StagePageCursor.Key("VERIFY_RESULT",510)),"a".repeat(64)).expires()).isEqualTo(first.expires());
        assertThatThrownBy(()->c.decode(token,"b".repeat(64))).hasMessage("INVALID_ARGUMENT");
        assertThatThrownBy(()->new StagePageCursor(signer,Clock.offset(clock,Duration.ofDays(1))).decode(token,"a".repeat(64))).hasMessage("INVALID_ARGUMENT");
        var n=signer.decode(token).deepCopy();
        for(String value:new String[]{"VERIFY_RESULT:511","VERIFY_RESULT:512","FETCH_SOURCE:01","PRIVATE:0","FETCH_SOURCE:-1","FETCH_SOURCE","FETCH_SOURCE:0:1"}) {
            var invalid=((tools.jackson.databind.node.ObjectNode)n).deepCopy().put("after",value);
            assertThatThrownBy(()->c.decode(signer.encode(invalid),"a".repeat(64))).hasMessage("INVALID_ARGUMENT");
        }
        assertThatThrownBy(()->c.decode(signer.encode(((tools.jackson.databind.node.ObjectNode)n).deepCopy().put("v",2)),"a".repeat(64))).hasMessage("INVALID_ARGUMENT");
        assertThatThrownBy(()->c.decode(signer.encode(((tools.jackson.databind.node.ObjectNode)n).deepCopy().put("kind","attempts")),"a".repeat(64))).hasMessage("INVALID_ARGUMENT");
        var payload=(tools.jackson.databind.node.ObjectNode)n;
        for(var invalid:java.util.List.of(payload.deepCopy().put("high","CHECK_INPUTS:0"),payload.deepCopy().put("high","PRIVATE:0"),payload.deepCopy().put("expires",1),payload.deepCopy().put("expires","01"),payload.deepCopy().put("expires",Long.toString(first.expires()+1)),payload.deepCopy().put("extra",true)))assertThatThrownBy(()->c.decode(signer.encode(invalid),"a".repeat(64))).hasMessage("INVALID_ARGUMENT");
    }
    @Test void stageCountsAllowOnlyReviewedKeysAndNormalizeNativeIntegersWithoutPrecisionLoss() {
        var n=SourceJson.object().put("accepted",9007199254740993L).put("rows","9223372036854775807").put("gold_articles",2).put("privateCredential","PRIVATE");
        assertThat(RunInspectionValues.stageCounts(SourceJson.canonical(n))).isEqualTo(SourceJson.object().put("accepted","9007199254740993").put("rows","9223372036854775807").put("gold_articles","2"));
        assertThat(RunInspectionValues.stageCounts(null)).isNull();
        assertThat(RunInspectionValues.stageCounts(SourceJson.canonical(SourceJson.object().put("private","PRIVATE"))).isEmpty()).isTrue();
        for(String value:new String[]{"-1","1.5","true","null","\"01\"","9223372036854775808"})assertThatThrownBy(()->RunInspectionValues.stageCounts(("{\"rows\":"+value+"}").getBytes(java.nio.charset.StandardCharsets.UTF_8))).hasMessage("INTERNAL_FAILURE");
        assertThatThrownBy(()->RunInspectionValues.stage("PRIVATE")).hasMessage("INTERNAL_FAILURE");
        assertThatThrownBy(()->RunInspectionValues.stageState("SUCCEEDED")).hasMessage("INTERNAL_FAILURE");
    }
    public static void oversized(jakarta.servlet.http.HttpServletRequest request) {
        SourceResponses.response(SourceJson.object().put("large","x".repeat(1048576)),200,null,null,request);
    }
    @Test void responseLimitIsCheckedBeforeAnyResponseCanBeWritten(){var request=new MockHttpServletRequest();request.setAttribute(ManagementBoundary.CORRELATION,"opaque");var huge=SourceJson.object().put("large","a".repeat(1048576));assertThatThrownBy(()->SourceResponses.response(huge,200,null,null,request)).hasMessage("INTERNAL_FAILURE");}
}
