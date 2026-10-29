package platform.contentapi.management;

import org.junit.jupiter.api.Test;
import platform.contentapi.importing.RegisteredImportEvidence;
import static org.assertj.core.api.Assertions.*;

class RegisteredSilverIdentityTest {
    @Test void importedAndManagedSilverIdentitiesRequireTheSameQualifiedEvidence(){
        var evidence=RunFixtures.silver("silver","bronze");byte[] raw=SourceJson.canonical(evidence);var q=RegisteredImportEvidence.validate(raw,SourceJson.sha(raw));
        assertThat(RegisteredImportEvidence.matchesIdentity(q,q.selection().nativeIdentity())).isTrue();
        assertThat(RegisteredImportEvidence.matchesIdentity(q,evidence.get("verification").get("inventoryIdentity").get("sha256").stringValue())).isTrue();
        assertThat(RegisteredImportEvidence.matchesIdentity(q,"0".repeat(64))).isFalse();
    }
}
