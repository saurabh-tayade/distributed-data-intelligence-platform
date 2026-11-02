package platform.contentapi.importing;

import java.util.UUID;
import platform.contentapi.management.SourceJson;

/** Test-only bridge for existing synthetic registry qualifications, never a native verification claim. */
public final class RegisteredImportFixture {
    private RegisteredImportFixture() { }
    public static QualifiedArtifact qualified(UUID operation,byte[] evidence) {
        var validated=RegisteredImportEvidence.validate(evidence,SourceJson.sha(evidence));
        return new QualifiedArtifact(operation,validated.selection(),evidence);
    }
}
