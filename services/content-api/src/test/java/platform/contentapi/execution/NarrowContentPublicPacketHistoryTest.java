package platform.contentapi.execution;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;

/** Pure version dispatch only; synthetic packet fields are not registered launch authority. */
class NarrowContentPublicPacketHistoryTest {
    @Test void historicalV1PacketBytesStayExactAndDoNotGainPublicPolicy(){
        var reservation=SourceJson.object().put("reservationVersion",1).put("mode","FETCH_SOURCE_RESERVATION").put("operationId","historical");
        var expected=SourceJson.object().put("bootstrapVersion",1).put("mode","FETCH_SOURCE_CANDIDATE").put("childPid","41");
        for(String field:List.of("expected","current","fetch")){var n=SourceJson.object().put("fixture",field);reservation.set(field,n);expected.set(field,n);}
        assertThat(NarrowContentFetchLaunch.packet(SourceJson.canonical(reservation),41)).isEqualTo(SourceJson.canonical(expected));
        assertThat(SourceJson.parse(NarrowContentFetchLaunch.packet(SourceJson.canonical(reservation),41)).has("publicFetch")).isFalse();
    }
    @ParameterizedTest @ValueSource(strings={"unknown-version","float-version","missing-public","v1-public","wrong-mode","extra"})
    void packetShapeCannotSilentlyCrossVersionBoundary(String change){
        var reservation=SourceJson.object().put("reservationVersion",2).put("mode","PUBLIC_FETCH_SOURCE_RESERVATION").put("operationId","synthetic");
        for(String field:List.of("expected","current","fetch","publicFetch"))reservation.putObject(field);
        switch(change){
            case "unknown-version"->reservation.put("reservationVersion",3);
            case "float-version"->reservation.put("reservationVersion",2.0);
            case "missing-public"->reservation.remove("publicFetch");
            case "v1-public"->reservation.put("reservationVersion",1).put("mode","FETCH_SOURCE_RESERVATION");
            case "wrong-mode"->reservation.put("mode","FETCH_SOURCE_RESERVATION");
            case "extra"->reservation.put("resume",true);
        }
        assertThatThrownBy(()->NarrowContentFetchLaunch.packet(SourceJson.canonical(reservation),41)).isInstanceOf(RuntimeException.class);
    }
}
