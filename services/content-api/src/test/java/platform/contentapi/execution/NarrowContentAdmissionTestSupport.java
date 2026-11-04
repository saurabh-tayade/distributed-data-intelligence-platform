package platform.contentapi.execution;

import java.nio.file.Path;
import java.util.UUID;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test-classpath-only bridge. No production visibility or execution capability is added. */
public final class NarrowContentAdmissionTestSupport {
    public static final String POLICY="ddip-content-narrow-v1";
    public static final String MANIFEST=ContentEvidenceBudget.MANIFEST;
    public static final String LOCK=ContentEvidenceBudget.LOCK;
    /** Facts only: deliberately no Owner, Session, dispatcher or continuation method. */
    public record Facts(String policy,UUID run,UUID attempt,String plan,String source,
                        String selectionSha256,String profileSha256,String runtimeSha256,String ledgerSha256,
                        InstallationHostGuard.Identity rootIdentity,InstallationHostGuard.Identity lockIdentity) { }

    public static WorkerRepository.Claim claim(WorkerRepository workers,InstallationHostGuard guard,
                                               WorkerRepository.Session session,UUID attempt,UUID run) {
        return workers.claimNarrow(guard,session,attempt,run);
    }
    public static Facts prepare(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,
                                InstallationHostGuard guard,WorkerRepository.Owner owner) {
        return prepare(jdbc,tx,workers,guard,owner,ignored->{});
    }
    public static Facts prepare(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,
                                InstallationHostGuard guard,WorkerRepository.Owner owner,Consumer<String> boundary) {
        return facts(new NarrowContentAdmission(jdbc,tx,workers).prepare(guard,owner,b->boundary.accept(b.name())));
    }
    public static Facts inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt) {
        return facts(NarrowContentAdmission.inspectReady(jdbc,tx,guard,run,attempt));
    }
    public static void inspectLegacyRecovery(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,
                                             WorkerRepository.Session recovery,UUID control) {
        new RecoveryHandoff(jdbc,tx,guard.binding().installationId()).inspect(guard,recovery,control);
    }
    public static void alternateOriginal(Path root,UUID run,UUID attempt,String source) throws Exception {
        var ledger=(tools.jackson.databind.node.ObjectNode)platform.contentapi.management.SourceJson.parse(
                java.nio.file.Files.readAllBytes(root.resolve(MANIFEST)),ContentEvidenceBudget.LEDGER);
        ContentOperationPlan.original(root,run,attempt,source).apply(ledger);
        java.nio.file.Files.write(root.resolve(MANIFEST),platform.contentapi.management.SourceJson.canonical(ledger));
        // The adversarial ledger is internally coherent; only the independent READY binding rejects it.
        ContentEvidenceBudget.inspect(root);
    }
    private static Facts facts(NarrowContentEvidenceContext.Expected e) {
        return new Facts(e.policy(),e.run(),e.attempt(),e.kind().name(),e.source(),e.frozen().selectionSha256(),
                e.frozen().profileSha256(),e.frozen().runtimeSha256(),e.ledgerSha256(),e.rootIdentity(),e.lockIdentity());
    }
    private NarrowContentAdmissionTestSupport() { }
}
