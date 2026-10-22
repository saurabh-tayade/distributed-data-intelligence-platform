package platform.contentapi.execution;

import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

class RecoveryQuiescenceTest {
    static final String HASH="a".repeat(64);static final Instant NOW=Instant.parse("2026-10-21T12:00:00Z");
    RecoveryQuiescence.Review review(boolean handlers,boolean unmanaged,boolean orphans,Set<UUID> stages){return new RecoveryQuiescence.Review(HASH,NOW,handlers,unmanaged,orphans,stages,List.of());}
    @Test void exactReviewIsRequiredEvenWhenEveryPidIsAbsent(){for(boolean absent:List.of(true,false)){var identity=new RecoveryQuiescence.Identity(100,NOW);assertThatThrownBy(()->RecoveryQuiescence.check(review(false,true,true,Set.of()),HASH,Set.of(),List.of(identity),p->absent?RecoveryQuiescence.State.ABSENT:RecoveryQuiescence.State.REUSED,NOW)).hasMessage("QUIESCENCE_REQUIRED");}}
    @ParameterizedTest @ValueSource(strings={"handlers","unmanaged","orphans"}) void unresolvedOperatorReviewVetoes(String missing){assertThatThrownBy(()->RecoveryQuiescence.check(review(!missing.equals("handlers"),!missing.equals("unmanaged"),!missing.equals("orphans"),Set.of()),HASH,Set.of(),List.of(),RecoveryQuiescence.LOCAL,NOW)).hasMessage("QUIESCENCE_REQUIRED");}
    @Test void targetAndEveryUnresolvedCheckpointMustMatch(){UUID checkpoint=UUID.randomUUID();assertThatThrownBy(()->RecoveryQuiescence.check(review(true,true,true,Set.of()),HASH,Set.of(checkpoint),List.of(),RecoveryQuiescence.LOCAL,NOW)).hasMessage("QUIESCENCE_REQUIRED");assertThatThrownBy(()->RecoveryQuiescence.check(review(true,true,true,Set.of(checkpoint)),HASH,Set.of(),List.of(),RecoveryQuiescence.LOCAL,NOW)).hasMessage("QUIESCENCE_REQUIRED");assertThatThrownBy(()->RecoveryQuiescence.check(review(true,true,true,Set.of()),"b".repeat(64),Set.of(),List.of(),RecoveryQuiescence.LOCAL,NOW)).hasMessage("QUIESCENCE_REQUIRED");}
    @ParameterizedTest @ValueSource(ints={-1,61}) void expiredOrFutureInspectionCannotAuthorize(int age){assertThatThrownBy(()->RecoveryQuiescence.fresh(NOW.minusSeconds(age),NOW)).hasMessage("FRESH_QUALIFICATION_REQUIRED");}
    @Test void freshnessIsNotALeaseOrAProcessDeathClaim(){RecoveryQuiescence.fresh(NOW.minusSeconds(60),NOW);assertThat(RecoveryQuiescence.FRESHNESS).isEqualTo(Duration.ofSeconds(60));}
    @ParameterizedTest @ValueSource(strings={"LIVE","UNAVAILABLE","exception"}) void processUncertaintyFailsClosed(String observation){var id=new RecoveryQuiescence.Identity(100,NOW);assertThatThrownBy(()->RecoveryQuiescence.check(review(true,true,true,Set.of()),HASH,Set.of(),List.of(id),p->{if(observation.equals("exception"))throw new UnsupportedOperationException("PRIVATE");return RecoveryQuiescence.State.valueOf(observation);},NOW)).hasMessage("PROCESS_UNRESOLVED").hasNoCause();}
    @Test void actualJvmIdentityAndPidReuseAreDifferentObservations(){var current=ProcessHandle.current();var exact=new RecoveryQuiescence.Identity(current.pid(),current.info().startInstant().orElseThrow());assertThat(RecoveryQuiescence.LOCAL.inspect(exact)).isEqualTo(RecoveryQuiescence.State.LIVE);assertThat(RecoveryQuiescence.LOCAL.inspect(new RecoveryQuiescence.Identity(current.pid(),Instant.EPOCH))).isEqualTo(RecoveryQuiescence.State.REUSED);}
    @Test void actualChildExitIsObservedButDoesNotProveOrphanQuiescence()throws Exception {
        String cp=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));var p=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",cp,"platform.contentapi.management.ControlTestProcess").start();
        try{assertThat(p.inputReader().readLine()).isEqualTo("READY");var identity=new RecoveryQuiescence.Identity(p.pid(),p.info().startInstant().orElseThrow());assertThat(RecoveryQuiescence.LOCAL.inspect(identity)).isEqualTo(RecoveryQuiescence.State.LIVE);p.getOutputStream().close();assertThat(p.waitFor(10,TimeUnit.SECONDS)).isTrue();assertThat(RecoveryQuiescence.LOCAL.inspect(identity)).isEqualTo(RecoveryQuiescence.State.ABSENT);assertThatThrownBy(()->RecoveryQuiescence.check(review(true,true,false,Set.of()),HASH,Set.of(),List.of(identity),RecoveryQuiescence.LOCAL,NOW)).hasMessage("QUIESCENCE_REQUIRED");}finally{p.destroyForcibly();}
    }
    @Test void boundedAndRedactedPrivateReview(){assertThatThrownBy(()->new RecoveryQuiescence.Identity(0,NOW)).hasMessage("INVALID_REQUEST");var identities=new ArrayList<RecoveryQuiescence.Identity>();for(int i=1;i<=33;i++)identities.add(new RecoveryQuiescence.Identity(i,NOW));assertThatThrownBy(()->new RecoveryQuiescence.Review(HASH,NOW,true,true,true,Set.of(),identities)).hasMessage("INVALID_REQUEST");assertThat(review(true,true,true,Set.of()).toString()).isEqualTo("RecoveryReview[redacted]");}
    @Test void internalSeamHasNoProductionActivationSurface(){assertThat(java.lang.reflect.Modifier.isPublic(RecoveryHandoff.class.getModifiers())).isFalse();assertThat(RecoveryHandoff.class.getAnnotations()).isEmpty();assertThat(Arrays.stream(RecoveryHandoff.class.getDeclaredMethods()).map(java.lang.reflect.Method::getName)).doesNotContain("main","release","execute","publish");}
}
