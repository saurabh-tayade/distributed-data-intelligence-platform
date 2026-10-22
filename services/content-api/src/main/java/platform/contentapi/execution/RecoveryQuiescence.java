package platform.contentapi.execution;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** Negative process veto plus fresh, exactly bound operator review. Never a process killer or proof of all orphans. */
final class RecoveryQuiescence {
    static final Duration FRESHNESS=Duration.ofSeconds(60);
    record Identity(long pid,Instant startedAt) {
        Identity {need(pid>0 && startedAt!=null,INVALID_REQUEST);startedAt=startedAt.truncatedTo(ChronoUnit.MICROS);}
    }
    enum State { ABSENT, REUSED, LIVE, UNAVAILABLE }
    interface Processes { State inspect(Identity identity); }
    static final Processes LOCAL=identity->{
        try {
            var found=ProcessHandle.of(identity.pid());
            if(found.isEmpty() || !found.get().isAlive())return State.ABSENT;
            var started=found.get().info().startInstant();
            if(started.isEmpty())return State.UNAVAILABLE;
            return started.get().truncatedTo(ChronoUnit.MICROS).equals(identity.startedAt())?State.LIVE:State.REUSED;
        }catch(SecurityException | UnsupportedOperationException ex){return State.UNAVAILABLE;}
    };
    /** Private trusted-operator input; no HTTP/configuration binding. All unresolved checkpoints require explicit review. */
    record Review(String targetSha256,Instant inspectedAt,boolean handlersStopped,boolean unmanagedWritersStopped,
                  boolean orphanReviewComplete,Set<UUID> reviewedCheckpoints,List<Identity> descendants) {
        Review {need(targetSha256!=null && targetSha256.matches("[0-9a-f]{64}") && inspectedAt!=null && reviewedCheckpoints!=null && descendants!=null && reviewedCheckpoints.size()<=512 && descendants.size()<=32,INVALID_REQUEST);reviewedCheckpoints=Set.copyOf(reviewedCheckpoints);descendants=List.copyOf(descendants);need(new HashSet<>(descendants).size()==descendants.size(),INVALID_REQUEST);}
        @Override public String toString(){return "RecoveryReview[redacted]";}
        JsonNode json(){var n=SourceJson.object().put("targetSha256",targetSha256).put("inspectedAt",inspectedAt.toString()).put("handlersStopped",handlersStopped).put("unmanagedWritersStopped",unmanagedWritersStopped).put("orphanReviewComplete",orphanReviewComplete);var stages=n.putArray("reviewedCheckpoints");reviewedCheckpoints.stream().map(UUID::toString).sorted().forEach(stages::add);var ps=n.putArray("descendants");descendants.stream().sorted(Comparator.comparingLong(Identity::pid).thenComparing(Identity::startedAt)).forEach(p->ps.addObject().put("pid",Long.toString(p.pid())).put("startedAt",p.startedAt().toString()));return n;}
    }
    static void fresh(Instant inspectedAt,Instant now){need(!inspectedAt.isAfter(now) && Duration.between(inspectedAt,now).compareTo(FRESHNESS)<=0,FRESH_QUALIFICATION_REQUIRED);}
    static void check(Review review,String target,Set<UUID> unresolved,List<Identity> recorded,Processes processes,Instant now) {
        need(review!=null && review.targetSha256().equals(target),QUIESCENCE_REQUIRED);fresh(review.inspectedAt(),now);
        need(review.handlersStopped() && review.unmanagedWritersStopped() && review.orphanReviewComplete() && review.reviewedCheckpoints().equals(unresolved),QUIESCENCE_REQUIRED);
        var all=new ArrayList<>(recorded);all.addAll(review.descendants());
        for(var identity:all){State observed;try{observed=processes.inspect(identity);}catch(RuntimeException ex){throw new RecoveryFailure(PROCESS_UNRESOLVED);}need(observed==State.ABSENT || observed==State.REUSED,PROCESS_UNRESOLVED);}
        // ABSENT/REUSED only veto contradictions. Neither replaces the explicit orphan/unmanaged-writer review.
    }
    private RecoveryQuiescence(){ }
}
