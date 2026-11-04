package platform.contentapi.execution;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;

/** Test-only process boundary. Fixture transport is not a production receipt format. */
public final class NarrowContentEvidenceTestChild {
    private NarrowContentEvidenceTestChild() { }

    public static void main(String[] args) throws Exception {
        var actualHost=OwnershipFixtures.readBinding(Path.of(args[0]));
        var expectedHost=OwnershipFixtures.readBinding(Path.of(args[1]));
        var fixture=SourceJson.parse(Files.readAllBytes(Path.of(args[2])),65536);
        var expected=new NarrowContentEvidenceContext.Expected(
                fixture.get("policy").stringValue(),UUID.fromString(fixture.get("run").stringValue()),
                UUID.fromString(fixture.get("attempt").stringValue()),
                NarrowContentEvidenceContext.PlanKind.valueOf(fixture.get("kind").stringValue()),
                fixture.get("source").isNull()?null:fixture.get("source").stringValue(),expectedHost,
                identity(fixture,"root"),identity(fixture,"lock"),frozen(fixture.get("expectedFrozen")),
                fixture.get("ledgerSha256").stringValue());
        var current=frozen(fixture.get("currentFrozen"));
        System.out.println("PID "+ProcessHandle.current().pid());
        // This JVM never initializes, reserves, discovers markers or enrolls a REQUIRED root.
        // Acquiring the independently supplied installation guard is the only setup action.
        try(var guard=InstallationHostGuard.acquire(actualHost)) {
            try {
                var totals=new NarrowContentEvidenceContext(expected).inspect(guard,current);
                System.out.println("OK "+totals.actualBytes()+" "+totals.actualEntries()+" "
                        +totals.reservedBytes()+" "+totals.reservedEntries()+" "+totals.inventoryMaximum());
            } catch(PreflightFailure failure) {
                System.out.println("REJECTED "+failure.code());
            }
        }
    }

    private static InstallationHostGuard.Identity identity(JsonNode fixture,String prefix) {
        return new InstallationHostGuard.Identity(Long.parseLong(fixture.get(prefix+"Device").stringValue()),
                Long.parseLong(fixture.get(prefix+"Inode").stringValue()));
    }

    private static NarrowContentEvidenceContext.FrozenIdentities frozen(JsonNode fixture) {
        return new NarrowContentEvidenceContext.FrozenIdentities(fixture.get("selection").stringValue(),
                fixture.get("profile").stringValue(),fixture.get("runtime").stringValue());
    }
}
