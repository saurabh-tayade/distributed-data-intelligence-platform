package platform.contentapi.management;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import static platform.contentapi.management.SourceFailure.Code.STATE_CONFLICT;

/** Negative veto only: absence/reused PID does not establish old-writer or descendant quiescence. */
final class ControlProcesses {
    record Identity(long pid,Instant start,UUID instance) { }
    static void check(List<Identity> recorded) {
        try {
            for(var identity:recorded) {
                var found=ProcessHandle.of(identity.pid());
                if(found.isPresent() && found.get().isAlive()) {
                    var start=found.get().info().startInstant();
                    if(start.isEmpty() || start.get().truncatedTo(ChronoUnit.MICROS).equals(identity.start()))throw new SourceFailure(STATE_CONFLICT);
                }
            }
        }catch(SecurityException | UnsupportedOperationException ex){throw new SourceFailure(STATE_CONFLICT);}
    }
}
