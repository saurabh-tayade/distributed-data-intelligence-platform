package platform.contentapi.management;

import java.time.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.KEY;

class DiscoveryCursorTest {
    private final Clock clock=Clock.fixed(Instant.parse("2026-11-01T00:00:00Z"),ZoneOffset.UTC);
    private final SourceCursor signer=new SourceCursor(KEY);
    private final DiscoveryCursor cursors=new DiscoveryCursor(signer,clock);
    private final String id="10000000-0000-4000-8000-000000000001",scope="a".repeat(64);
    @Test void signedEndpointFilterScopedTraversalKeepsOriginalExpiry() {
        var first=cursors.position(null,"artifacts",scope);String token=cursors.next("artifacts",scope,first,id);
        assertThat(token.length()).isLessThanOrEqualTo(512);var page=cursors.position(token,"artifacts",scope);
        assertThat(page.after()).isEqualTo(id);assertThat(page.expires()).isEqualTo(first.expires());
        assertThatThrownBy(()->cursors.position(token,"execution-profiles",scope)).hasMessage("INVALID_ARGUMENT");
        assertThatThrownBy(()->cursors.position(token,"artifacts","b".repeat(64))).hasMessage("INVALID_ARGUMENT");
        String changed=token.substring(0,token.length()-1)+(token.endsWith("A")?"B":"A");
        assertThatThrownBy(()->cursors.position(changed,"artifacts",scope)).hasMessage("INVALID_ARGUMENT");
        var later=new DiscoveryCursor(signer,Clock.offset(clock,Duration.ofHours(1)));
        assertThat(later.position(token,"artifacts",scope).expires()).isEqualTo(first.expires());
    }
    @Test void expiredAndExtendedLifetimeCursorsFailClosed() {
        var p=cursors.position(null,"execution-profiles",scope);String token=cursors.next("execution-profiles",scope,p,id);
        var expired=new DiscoveryCursor(signer,Clock.offset(clock,Duration.ofDays(1)));
        assertThatThrownBy(()->expired.position(token,"execution-profiles",scope)).hasMessage("INVALID_ARGUMENT");
        var oldClock=new DiscoveryCursor(signer,Clock.offset(clock,Duration.ofSeconds(-1)));
        assertThatThrownBy(()->oldClock.position(token,"execution-profiles",scope)).hasMessage("INVALID_ARGUMENT");
    }
    @ParameterizedTest @ValueSource(strings={"","not-signed","a.b","../../private","0","{\"v\":1}"})
    void invalidTokensHaveOnlyFixedDiagnostics(String token) {
        assertThatThrownBy(()->cursors.position(token,"artifacts",scope)).isInstanceOf(SourceFailure.class).hasMessage("INVALID_ARGUMENT").hasNoCause();
    }
    @Test void malformedAuthenticatedFieldsAndLegacyHistoryCursorsAreRejected() {
        var n=SourceJson.object().put("v",1).put("kind","artifacts").put("scope",scope).put("after",id).put("expires",Long.toString(clock.instant().getEpochSecond()+60));
        for(var invalid:java.util.List.of(n.deepCopy().put("extra",true),n.deepCopy().put("v",2),n.deepCopy().put("after","NOT-A-UUID"),n.deepCopy().put("expires",1),n.deepCopy().put("expires","01"))) {
            String token=signer.encode(invalid);assertThatThrownBy(()->cursors.position(token,"artifacts",scope)).hasMessage("INVALID_ARGUMENT");
        }
        var legacy=new RunPageCursor(signer,clock);var p=legacy.first(10,true);String token=legacy.next("runs",scope,p,8);
        assertThatThrownBy(()->cursors.position(token,"artifacts",scope)).hasMessage("INVALID_ARGUMENT");
    }
}
