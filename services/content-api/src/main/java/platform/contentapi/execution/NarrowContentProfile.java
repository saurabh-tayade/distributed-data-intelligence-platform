package platform.contentapi.execution;

import java.util.Arrays;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/** Versioned admission building blocks, not a runtime capability or a publication verifier. */
final class NarrowContentProfile {
    static final String POLICY = "ddip-content-narrow-v1";
    static final int SILVER_CAPTURE = 32 * 1024;
    static final int SILVER_IDENTITY = 16 * 1024;
    static final int SILVER_VERIFICATION = 52 * 1024;
    static final int SILVER_QUALIFICATION = 60 * 1024;
    static final int SILVER_SELECTION = 36 * 1024;
    static final int REPORT_ROW = 60 * 1024;

    /** Measure actual canonical bytes, then exercise the unchanged downstream reader. */
    static byte[] envelope(JsonNode value, int maximum, int readerMaximum) {
        need(maximum > 0 && maximum <= readerMaximum && readerMaximum <= PreflightFiles.LARGE, INPUT_INVALID);
        try {
            byte[] bytes = SourceJson.canonical(value);
            need(bytes.length <= maximum, RESOURCE_LIMIT);
            integerTokens(value);
            var parsed = SourceJson.parse(bytes, readerMaximum);
            need(Arrays.equals(bytes, SourceJson.canonical(parsed)), INPUT_INVALID);
            return bytes;
        } catch (PreflightFailure error) {
            throw error;
        } catch (RuntimeException error) {
            throw new PreflightFailure(INPUT_INVALID);
        }
    }

    private static void integerTokens(JsonNode value) {
        // Jackson counts digits, not the sign. The profile conservatively counts
        // the whole canonical token, matching the versioned Python guard.
        if (value.isIntegralNumber()) need(value.bigIntegerValue().toString().length() <= 20, INPUT_INVALID);
        else if (value.isContainer()) for (var child : value) integerTokens(child);
    }

    /** Budget rejection only: full native/RegisteredImportEvidence validation is still mandatory. */
    static byte[] silverQualification(JsonNode qualification) {
        try {
            SourceJson.fields(qualification, "qualificationVersion", "selection", "verification");
            var verification = qualification.get("verification");
            SourceJson.fields(verification, "evidenceVersion", "native", "capture", "inventoryIdentity");
            SourceJson.fields(verification.get("capture"), "local", "bronze");
            SourceJson.fields(verification.get("inventoryIdentity"), "kind", "sha256", "value");
            envelope(verification.get("capture").get("local"), SILVER_CAPTURE, 65536);
            envelope(verification.get("inventoryIdentity").get("value"), SILVER_IDENTITY, 65536);
            envelope(verification, SILVER_VERIFICATION, 65536);
            return envelope(qualification, SILVER_QUALIFICATION, 65536);
        } catch (PreflightFailure error) {
            throw error;
        } catch (RuntimeException error) {
            throw new PreflightFailure(INPUT_INVALID);
        }
    }

    static byte[] silverSelection(JsonNode selection) {
        try {
            SourceJson.fields(selection, "manifest", "inputSetFingerprint");
            return envelope(selection, SILVER_SELECTION, 65536);
        } catch (PreflightFailure error) {
            throw error;
        } catch (RuntimeException error) {
            throw new PreflightFailure(INPUT_INVALID);
        }
    }

    /** Leave a reconciliation attempt after the next effectful continuation is admitted. */
    static void continuationCapacity(long existingAttempts) {
        need(existingAttempts >= 0 && existingAttempts < 19, RESOURCE_LIMIT);
    }

    private NarrowContentProfile() { }
}
