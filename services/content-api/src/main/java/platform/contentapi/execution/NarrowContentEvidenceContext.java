package platform.contentapi.execution;

import java.nio.file.Path;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/**
 * Inactive, read-only expectation contract. Callers must supply expectations independently
 * of the inspected ledger. This type does not establish provenance, persistence or authority.
 */
final class NarrowContentEvidenceContext {
    enum PlanKind { ORIGINAL, RECOVERY }

    /** Opaque identities only; no production frozen-selection or receipt encoding is selected. */
    record FrozenIdentities(String selectionSha256,String profileSha256,String runtimeSha256) {
        FrozenIdentities { hash(selectionSha256);hash(profileSha256);hash(runtimeSha256); }
        @Override public String toString(){return "FrozenIdentities[redacted]";}
    }
    record Expected(String policy,UUID run,UUID attempt,PlanKind kind,String source,
                    InstallationHostGuard.Binding host,InstallationHostGuard.Identity rootIdentity,
                    InstallationHostGuard.Identity lockIdentity,FrozenIdentities frozen,String ledgerSha256) {
        Expected {
            need(NarrowContentProfile.POLICY.equals(policy) && run!=null && attempt!=null && kind!=null
                    && host!=null && rootIdentity!=null && lockIdentity!=null && frozen!=null,INPUT_INVALID);
            need(kind==PlanKind.ORIGINAL?source!=null && source.matches("[a-z0-9][a-z0-9_-]{0,127}"):source==null,INPUT_INVALID);
            hash(ledgerSha256);
        }
        @Override public String toString(){return "NarrowContentExpectation[redacted]";}
    }
    private final Expected expected;
    private final Path root;
    private final ContentOperationPlan plan;

    NarrowContentEvidenceContext(Expected expected) {
        need(expected!=null,INPUT_INVALID);this.expected=expected;
        root=expected.host().root().resolve("runs/"+expected.run()+"/attempts/"+expected.attempt());
        plan=expected.kind()==PlanKind.ORIGINAL?ContentOperationPlan.original(root,expected.run(),expected.attempt(),expected.source())
                :ContentOperationPlan.recovery(root,expected.run(),expected.attempt());
    }
    ContentEvidenceBudget.Totals inspect(InstallationHostGuard guard,FrozenIdentities independentlySupplied) {
        return ContentEvidenceBudget.inspect(this,guard,independentlySupplied);
    }
    Path root(){return root;}
    Path originalDirectory(String stage,int ordinal){need(expected.kind()==PlanKind.ORIGINAL,INPUT_INVALID);return plan.directory(stage,ordinal);}
    FrozenIdentities frozen(){return expected.frozen();}
    void check(InstallationHostGuard guard,FrozenIdentities independentlySupplied) {
        need(guard!=null && expected.frozen().equals(independentlySupplied),RUNTIME_MISMATCH);
        need(expected.host().equals(guard.binding()),RUNTIME_MISMATCH);
        guard.checkHeld(expected.host().installationId());
        Path ancestor=expected.host().root();
        for(Path segment:ancestor.relativize(root)){ancestor=ancestor.resolve(segment);PreflightFiles.privateEntry(ancestor,true);}
        need(expected.rootIdentity().equals(InstallationHostGuard.identity(root)),INPUT_CHANGED);
        Path lock=root.resolve(ContentEvidenceBudget.LOCK);PreflightFiles.privateEntry(lock,false);
        need(expected.lockIdentity().equals(InstallationHostGuard.identity(lock)),INPUT_CHANGED);
    }
    void ledger(byte[] bytes,JsonNode value) {
        need(SourceJson.sha(bytes).equals(expected.ledgerSha256()),INPUT_CHANGED);
        need(value.get("version").isIntegralNumber() && value.get("version").intValue()==2
                && expected.policy().equals(value.get("reservationPolicy").stringValue())
                && expected.run().toString().equals(value.get("runId").stringValue())
                && expected.attempt().toString().equals(value.get("attemptId").stringValue())
                && expected.kind().name().equals(value.get("planKind").stringValue())
                && (expected.source()==null?value.get("sourceId").isNull():expected.source().equals(value.get("sourceId").stringValue())),INPUT_CHANGED);
        var planned=(ObjectNode)value.deepCopy();plan.apply(planned);
        need(Arrays.equals(bytes,SourceJson.canonical(planned)),INPUT_CHANGED);
    }
    private static void hash(String value){need(value!=null && value.matches("[0-9a-f]{64}"),INPUT_INVALID);}
    @Override public String toString(){return "NarrowContentEvidenceContext[redacted]";}
}
