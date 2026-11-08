package platform.contentapi.execution;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
class NarrowContentTerminalTest {
    @Test void capabilitiesHaveOnlyPrivateConstructorsAndNoExecutionSeams(){
        for(var type:List.of(NarrowContentTerminal.Target.class,NarrowContentTerminal.Qualified.class)){
            assertThat(Modifier.isPublic(type.getModifiers())).isFalse();
            assertThat(Arrays.stream(type.getDeclaredConstructors())).allMatch(c->Modifier.isPrivate(c.getModifiers()));
        }
        assertThat(Arrays.stream(NarrowContentTerminal.class.getDeclaredMethods()).map(java.lang.reflect.Method::getName)).doesNotContain("resume","authorize","bootstrap","dispatch","transferOwner");
    }
    @Test void reviewRequiresExplicitNamespaceAndRemoteWriterAttestations(){
        var q=new RecoveryQuiescence.Review("a".repeat(64),Instant.EPOCH,true,true,true,Set.of(),List.of());
        var review=new NarrowContentTerminal.Review(q,UUID.randomUUID(),true,true,true,true,true);
        assertThat(review.json().get("sameHostAndProcessNamespace").booleanValue()).isTrue();assertThat(review.json().get("remoteWritersStopped").booleanValue()).isTrue();
        assertThat(review.toString()).doesNotContain("a".repeat(64));
        assertThatThrownBy(()->new NarrowContentTerminal.Review(q,null,true,true,true,true,true)).hasMessage("INVALID_REQUEST");
    }
}
