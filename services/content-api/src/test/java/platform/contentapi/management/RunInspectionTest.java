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
    public static void oversized(jakarta.servlet.http.HttpServletRequest request) {
        SourceResponses.response(SourceJson.object().put("large","x".repeat(1048576)),200,null,null,request);
    }
    @Test void responseLimitIsCheckedBeforeAnyResponseCanBeWritten(){var request=new MockHttpServletRequest();request.setAttribute(ManagementBoundary.CORRELATION,"opaque");var huge=SourceJson.object().put("large","a".repeat(1048576));assertThatThrownBy(()->SourceResponses.response(huge,200,null,null,request)).hasMessage("INTERNAL_FAILURE");}
}
