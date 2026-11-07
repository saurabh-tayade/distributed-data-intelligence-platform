package platform.contentapi.execution;

import java.time.*;
import java.util.*;
import java.util.function.*;
import org.springframework.jdbc.core.JdbcTemplate;
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** Test-only composition, never packaged as an operator command or runtime entry point. */
public final class NarrowContentFetchRecoveryComposition {
    public record Target(String digest,UUID checkpoint) { }
    /** Every attestation must be supplied explicitly by the test's trusted-local reviewer. */
    public record Review(String digest,Instant at,boolean handlers,boolean unmanaged,boolean orphans,
                         Set<UUID> checkpoints,List<ProcessIdentity> descendants,boolean descendantsReviewed,boolean gaps) {
        public Review {checkpoints=Set.copyOf(checkpoints);descendants=List.copyOf(descendants);}
        NarrowContentFetchRecoveryHandoff.Review value(){
            return new NarrowContentFetchRecoveryHandoff.Review(new RecoveryQuiescence.Review(digest,at,handlers,unmanaged,orphans,
                    checkpoints,descendants.stream().map(p->new RecoveryQuiescence.Identity(p.pid(),p.startedAt())).toList()),descendantsReviewed,gaps);
        }
    }
    public record ProcessIdentity(long pid,Instant startedAt) { }
    private final JdbcTemplate jdbc;
    private final ManagementTransactions tx;
    private final RecoveryQuiescence.Processes processes;
    private final NarrowContentFetchRecoveryHandoff handoffs;
    private final NarrowContentFetchRecoveryFinalization terminals;
    private final Consumer<String> boundary;
    private NarrowContentFetchRecoveryHandoff.Owner owner;
    private NarrowContentFetchRecoveryHandoff.Qualified handoffQualification;
    private NarrowContentFetchRecoveryFinalization.Qualified terminalQualification;
    private boolean started;
    private UUID finalization;

    public NarrowContentFetchRecoveryComposition(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Clock clock,
                                                  Supplier<String> observations,Consumer<String> boundary){
        this.jdbc=jdbc;this.tx=tx;this.boundary=boundary;
        processes=p->RecoveryQuiescence.State.valueOf(observations.get());
        handoffs=new NarrowContentFetchRecoveryHandoff(jdbc,tx,installation,clock,processes,b->boundary.accept("HANDOFF_"+b));
        terminals=new NarrowContentFetchRecoveryFinalization(jdbc,tx,installation,clock,processes,b->boundary.accept("TERMINAL_"+b));
    }
    /** Single first handoff. A failed/uncertain return never reconstructs an owner from a receipt. */
    public synchronized void start(InstallationHostGuard guard,UUID run,UUID attempt,WorkerRepository.Session recovery,
                                   UUID control,UUID successor,UUID handoff,UUID finalization,Function<Target,Review> reviewer){
        need(!started,INVALID_REQUEST);started=true;this.finalization=finalization;
        var assessment=NarrowContentFetchRecoveryAssessment.inspect(jdbc,tx,guard,run,attempt,processes,b->boundary.accept("ASSESSMENT_"+b));
        need(assessment.durable().prefix()==NarrowContentFetchRecoveryAssessment.Prefix.CHECKPOINT
                && assessment.integrity()==NarrowContentFetchRecoveryAssessment.Integrity.VERIFIED_AT_INSPECTION,EVIDENCE_UNAVAILABLE);
        boundary.accept("ASSESSED");
        var target=handoffs.inspect(guard,recovery,control,successor,handoff);
        need(target.checkpoint().equals(assessment.durable().checkpoint()),EVIDENCE_CHANGED);
        var review=reviewer.apply(new Target(target.digest(),target.checkpoint()));need(review!=null,INVALID_REQUEST);
        handoffQualification=handoffs.qualify(guard,target,review.value());
        var result=handoffs.transfer(guard,handoffQualification);
        need(result.receipt().status()==NarrowContentFetchRecoveryHandoff.Status.COMMITTED && result.owner()!=null,OWNERSHIP_LOST);
        owner=result.owner();boundary.accept("OWNER_RETAINED");
    }
    /** A separate review is mandatory. Only this still-live instance can hold the returned owner. */
    public synchronized String finish(InstallationHostGuard guard,Function<Target,Review> reviewer){
        need(owner!=null,OWNERSHIP_LOST);
        if(terminalQualification!=null){
            var receipt=terminals.resolve(terminalQualification);
            if(receipt.status()==NarrowContentFetchRecoveryFinalization.Status.CONFIRMED_TERMINAL)return receipt.status().name();
            need(receipt.status()==NarrowContentFetchRecoveryFinalization.Status.CONFIRMED_ROLLBACK,HANDOFF_UNRESOLVED);
        }
        var target=terminals.inspect(guard,owner,finalization);
        var review=reviewer.apply(new Target(target.digest(),target.checkpoint()));need(review!=null,INVALID_REQUEST);
        terminalQualification=terminals.qualify(guard,target,review.value());
        return terminals.finish(guard,terminalQualification).status().name();
    }
    public synchronized String resolveHandoff(){need(handoffQualification!=null,INVALID_REQUEST);return handoffs.resolve(handoffQualification).status().name();}
    public synchronized String resolveTerminal(){need(terminalQualification!=null,INVALID_REQUEST);return terminals.resolve(terminalQualification).status().name();}
    public String lookupHandoff(UUID id){return handoffs.lookup(id).status().name();}
    public String lookupTerminal(UUID id){return terminals.lookup(id).status().name();}
    @Override public String toString(){return "TestOnlyNativeFetchRecovery[redacted]";}
}
