package platform.contentapi.execution;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static platform.contentapi.execution.NarrowPublicationEnvelopes.*;

/** Serializer/reader tests only. Synthetic commands must never be executed or registered. */
class NarrowPublicationEnvelopesTest {
    private static final String SHA = "a".repeat(64);
    private static final UUID ID = UUID.fromString("00000000-0000-4000-8000-000000000001");
    private static final int SLOT = 1024 * 1024;
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Test void everyDestinationUsesItsCurrentWholeSlotAndReader() {
        for (Destination destination : Destination.values()) {
            int maximum = destination.maximum();
            assertThat(maximum).isEqualTo(destination == Destination.RESULT ? 65536 : SLOT);
            assertThat(destination.readerMaximum()).isEqualTo(destination == Destination.RESULT ? 65536 : destination == Destination.RECOVERY_OBSERVATION ? RecoveryFiles.MAXIMUM : PreflightFiles.LARGE);
            assertThat(qualifyV1(document(maximum), destination)).hasSize(maximum);
            rejected(() -> qualifyV1(document(maximum + 1), destination), PreflightFailure.Code.RESOURCE_LIMIT);
        }
        // A valid legacy read is not sufficient for the bounded destination.
        byte[] over = SourceJson.canonical(document(SLOT + 1));
        assertThatCode(() -> SourceJson.parse(over, PreflightFiles.LARGE)).doesNotThrowAnyException();
        rejected(() -> qualifyV1(JSON.readTree(over), Destination.CONTENT_COMMAND), PreflightFailure.Code.RESOURCE_LIMIT);
    }

    @Test void grammarIsTheActualReaderNotAdditionalComponentCaps() {
        qualifyV1(SourceJson.object().put("s", "x".repeat(131072)), Destination.CONTENT_EVIDENCE);
        rejected(() -> qualifyV1(SourceJson.object().put("s", "x".repeat(131073)), Destination.CONTENT_EVIDENCE), PreflightFailure.Code.INPUT_INVALID);
        qualifyV1(SourceJson.object().put("s", "🙂".repeat(65536)), Destination.CONTENT_EVIDENCE);
        rejected(() -> qualifyV1(SourceJson.object().put("s", "🙂".repeat(65536) + "x"), Destination.CONTENT_EVIDENCE), PreflightFailure.Code.INPUT_INVALID);
        qualifyV1(SourceJson.object().put("k".repeat(50000), 0), Destination.CONTENT_EVIDENCE);
        rejected(() -> qualifyV1(SourceJson.object().put("k".repeat(50001), 0), Destination.CONTENT_EVIDENCE), PreflightFailure.Code.INPUT_INVALID);
        qualifyV1(nested(16, false), Destination.CONTENT_EVIDENCE);
        rejected(() -> qualifyV1(nested(17, false), Destination.CONTENT_EVIDENCE), PreflightFailure.Code.INPUT_INVALID);
        qualifyV1(nested(16, true), Destination.CONTENT_EVIDENCE);
        rejected(() -> qualifyV1(nested(17, true), Destination.CONTENT_EVIDENCE), PreflightFailure.Code.INPUT_INVALID);
        for (String token : List.of("99999999999999999999", "-99999999999999999999")) {
            // Jackson's numeric limit counts digits, not the minus sign. No new token cap.
            qualifyV1(SourceJson.object().put("n", new BigInteger(token)), Destination.CONTENT_EVIDENCE);
        }
        rejected(() -> qualifyV1(SourceJson.object().put("n", new BigInteger("100000000000000000000")), Destination.CONTENT_EVIDENCE), PreflightFailure.Code.INPUT_INVALID);
        rejected(() -> qualifyV1(SourceJson.object().put("n", 1.5), Destination.CONTENT_EVIDENCE), PreflightFailure.Code.INPUT_INVALID);
        rejected(() -> qualifyV1(SourceJson.object().put("s", "\ud800"), Destination.CONTENT_EVIDENCE), PreflightFailure.Code.INPUT_INVALID);
    }

    @Test void goldProofRetainsCompleteOriginalCommandAndHashesCompleteArtifact() {
        ObjectNode command = goldCommand(), verification = goldVerification();
        JsonNode original = command.deepCopy(), before = verification.deepCopy();
        ObjectNode proof = goldProofV1(command, verification, "gold", ID);
        assertThat(proof.get("command")).isEqualTo(command);
        assertThat(proof.get("command").get("mode").stringValue()).isEqualTo("PUBLISH");
        assertThat(proof.get("artifact").get("verification")).isEqualTo(verification);
        assertThat(proof.get("evidenceSha256").stringValue()).isEqualTo(SourceJson.sha(SourceJson.canonical(proof.get("artifact"))));
        assertThat(proof.get("artifact").get("inputs").get(0).propertyNames()).containsExactlyInAnyOrder("artifactId", "evidenceSha256");
        ((ObjectNode) proof.get("command")).put("mode", "CHANGED");
        ((ObjectNode) proof.get("artifact").get("verification")).put("status", "CHANGED");
        assertThat(command).isEqualTo(original); assertThat(verification).isEqualTo(before);
        qualifyGoldV1(command, verification, "gold", receipts());
    }

    @Test void contentRecheckChargesRepeatedProofPreparationAndVerificationTogether() {
        ObjectNode publish = contentCommand(), verification = contentVerification();
        ObjectNode recheck = commandV1("CONTENT", publish, "RECHECK", publish.get("prepared"), verification);
        assertThat(recheck.get("goldProof")).isEqualTo(publish.get("goldProof"));
        assertThat(recheck.get("verified")).isEqualTo(verification);
        assertThat(recheck.get("prepared")).isEqualTo(verification.get("prepared"));
        qualifyContentV1(publish, verification, "content", receipts());
        // Every individual native body and artifact fits; the complete RECHECK does not.
        int overhead = SourceJson.canonical(contentArtifactV1(publish, verification, "content")).length - SourceJson.canonical(verification).length;
        resizeVerification(verification, SLOT - overhead);
        qualifyV1(verification, Destination.CONTENT_EVIDENCE);
        assertThat(qualifyV1(contentArtifactV1(publish, verification, "content"), Destination.CONTENT_ARTIFACT)).hasSize(SLOT);
        rejected(() -> qualifyContentV1(publish, verification, "content", receipts()), PreflightFailure.Code.RESOURCE_LIMIT);
    }

    @Test void goldRecheckRejectsCombinedSizeWithoutInventingFileCounts() {
        ObjectNode publish = goldCommand(), verification = goldVerification();
        int overhead = SourceJson.canonical(goldArtifactV1(publish, verification, "gold")).length - SourceJson.canonical(verification).length;
        resizeVerification(verification, SLOT - overhead);
        qualifyV1(verification, Destination.GOLD_EVIDENCE);
        assertThat(qualifyV1(goldArtifactV1(publish, verification, "gold"), Destination.GOLD_ARTIFACT)).hasSize(SLOT);
        rejected(() -> qualifyGoldV1(publish, verification, "gold", receipts()), PreflightFailure.Code.RESOURCE_LIMIT);
    }

    @Test void actualRecoveryPathClearsVerificationAndIncludesFullObservation() {
        ObjectNode publish = contentCommand(), verification = contentVerification();
        JsonNode recovery = recoveryCommandV1("CONTENT", publish);
        assertThat(recovery.get("original").get("mode").stringValue()).isEqualTo("VERIFY");
        assertThat(recovery.get("original").get("verified").isNull()).isTrue();
        assertThat(publish.get("mode").stringValue()).isEqualTo("PUBLISH");
        ObjectNode artifact = contentArtifactV1(publish, verification, "content");
        JsonNode observed = contentObservationV1(artifact, receipts());
        assertThat(observed.propertyNames()).containsExactlyInAnyOrder("state", "code", "receipts", "artifact");
        assertThat(observed.get("receipts")).isEqualTo(receipts());
        assertThat(observed.get("artifact")).isEqualTo(artifact);
        qualifyV1(observed, Destination.RECOVERY_OBSERVATION);
        JsonNode largeReceipts = document(SLOT - 100);
        rejected(() -> qualifyContentV1(publish, verification, "content", largeReceipts), PreflightFailure.Code.RESOURCE_LIMIT);
        rejected(() -> qualifyGoldV1(goldCommand(), goldVerification(), "gold", largeReceipts), PreflightFailure.Code.RESOURCE_LIMIT);
        ObjectNode unprepared = commandV1("GOLD", goldCommand(), "PREPARE", null, null);
        assertThat(recoveryCommandV1("GOLD", unprepared).get("original").get("mode").stringValue()).isEqualTo("PREPARE");
    }

    @Test void preparedCheckpointObservationsRetainFullPreparedResults() {
        for (String kind : List.of("GOLD", "CONTENT")) {
            ObjectNode publish = kind.equals("GOLD") ? goldCommand() : contentCommand();
            ObjectNode original = commandV1(kind, publish, "PREPARE", null, null);
            ObjectNode prepared = preparedV1(publish.get("prepared"));
            ObjectNode observed = checkpointObservationV1(prepared, receipts());
            assertThat(observed.get("checkpoint").get("status").stringValue()).isEqualTo("PREPARED");
            assertThat(observed.get("checkpoint").get("prepared")).isEqualTo(publish.get("prepared"));
            assertThat(recoveryCommandV1(kind, original).get("original").get("prepared").isNull()).isTrue();
            qualifyV1(observed, Destination.RECOVERY_OBSERVATION);
            rejected(() -> qualifyV1(checkpointObservationV1(prepared, document(SLOT - 50)), Destination.RECOVERY_OBSERVATION), PreflightFailure.Code.RESOURCE_LIMIT);
        }
    }

    @Test void nestingMustFitAtOuterBoundaryNotOnlyInsideVerification() {
        ObjectNode verification = goldVerification();
        verification.set("capture", nested(12, false));
        qualifyV1(verification, Destination.GOLD_EVIDENCE);
        ObjectNode gold = goldCommand();
        ObjectNode proof = goldProofV1(gold, verification, "gold", ID);
        ObjectNode content = contentCommand(); content.set("goldProof", proof);
        qualifyV1(content, Destination.CONTENT_COMMAND);
        // Complete command is at reader depth 16; its actual recovery wrapper is not.
        rejected(() -> qualifyV1(recoveryCommandV1("CONTENT", content), Destination.RECOVERY_COMMAND), PreflightFailure.Code.INPUT_INVALID);
    }

    @Test void resumeRequestAndResultPreserveAllRepeatedEvidenceAtBothBoundaries() {
        ObjectNode publish = contentCommand();
        for (String boundary : List.of("GOLD", "CONTENT_INPUTS")) {
            JsonNode prepared = boundary.equals("GOLD") ? null : publish.get("prepared");
            ObjectNode command = commandV1("CONTENT", publish, "PREPARE", prepared, null);
            ObjectNode request = resumeCommandV1(boundary, publish.get("reference"), publish.get("referenceBinding"), publish.get("content"), command);
            ObjectNode result = resumeEvidenceV1(request, goldVerification(), publish.get("prepared"));
            assertThat(result.propertyNames()).containsExactlyInAnyOrder("boundary", "gold", "prepared", "reference");
            assertThat(result.get("reference")).isEqualTo(publish.get("reference").get("qualification").get("verification"));
            assertThat(result.get("gold")).isEqualTo(goldVerification());
            qualifyResumeV1(request, goldVerification(), publish.get("prepared"));
        }
        ObjectNode before = resumeCommandV1("BEFORE_FETCH", publish.get("reference"), publish.get("referenceBinding"), publish.get("content"), null);
        assertThat(resumeEvidenceV1(before, null, null).propertyNames()).containsExactlyInAnyOrder("boundary", "reference");
        qualifyResumeV1(before, null, null);
        rejected(() -> resumeCommandV1("GOLD", publish.get("reference"), publish.get("referenceBinding"), publish.get("content"), publish), PreflightFailure.Code.INPUT_INVALID);
        rejected(() -> resumeEvidenceV1(before, goldVerification(), null), PreflightFailure.Code.INPUT_INVALID);
    }

    @Test void resumeResultMustFitEvenWhenGoldBoundaryRequestFits() {
        ObjectNode publish = contentCommand();
        ObjectNode prepare = commandV1("CONTENT", publish, "PREPARE", null, null);
        ObjectNode request = resumeCommandV1("GOLD", publish.get("reference"), publish.get("referenceBinding"), publish.get("content"), prepare);
        qualifyV1(request, Destination.RESUME_COMMAND);
        ObjectNode prepared = contentPrepared(); prepared.set("manifest", document(SLOT - 100));
        rejected(() -> qualifyResumeV1(request, goldVerification(), prepared), PreflightFailure.Code.RESOURCE_LIMIT);
    }

    @Test void committedProofHasNoInventedIndependentSlot() {
        ObjectNode publish = goldCommand(), verification = goldVerification();
        publish.set("runtime", document(600000)); resizeVerification(verification, 600000);
        qualifyV1(publish, Destination.GOLD_COMMAND); qualifyV1(verification, Destination.GOLD_EVIDENCE);
        JsonNode proof = goldProofV1(publish, verification, "gold", ID);
        assertThat(SourceJson.canonical(proof).length).isGreaterThan(SLOT);
        ObjectNode content = contentCommand(); content.set("goldProof", proof);
        rejected(() -> qualifyV1(content, Destination.CONTENT_COMMAND), PreflightFailure.Code.RESOURCE_LIMIT);
    }

    @Test void malformedProjectionInputsFailClosedWithFixedDiagnostics() {
        ObjectNode command = goldCommand(); command.put("unexpected", true);
        rejected(() -> goldProofV1(command, goldVerification(), "gold", ID), PreflightFailure.Code.INPUT_INVALID);
        ObjectNode different = goldVerification(); ((ObjectNode) different.get("prepared")).put("inputSetFingerprint", "b".repeat(64));
        rejected(() -> goldArtifactV1(goldCommand(), different, "gold"), PreflightFailure.Code.INPUT_INVALID);
        rejected(() -> qualifyV1(null, Destination.CONTENT_EVIDENCE), PreflightFailure.Code.INPUT_INVALID);
        rejected(() -> commandV1("ARBITRARY", goldCommand(), "PUBLISH", null, null), PreflightFailure.Code.INPUT_INVALID);
        rejected(() -> contentObservationV1(goldArtifactV1(goldCommand(), goldVerification(), "gold"), receipts()), PreflightFailure.Code.INPUT_INVALID);
    }

    @Test void compareCompleteNativeMeasurementCorpusWithoutExecutingCommands() throws Exception {
        String path = System.getProperty("envelope.qualification.directory");
        assumeTrue(path != null && !path.isBlank(), "Optional generated native measurement corpus not supplied");
        Path root = Path.of(path);
        for (String name : List.of("small_nonempty", "gold_902_empty_escaped", "content_902_empty_escaped", "both_902_empty_escaped")) {
            Path directory = root.resolve(name);
            JsonNode publishGold = read(directory, "java-envelopes/gold-publish.json");
            JsonNode verifiedGold = read(directory, "envelopes/gold-verification.json");
            JsonNode publishContent = read(directory, "java-envelopes/content-publish.json");
            JsonNode verifiedContent = read(directory, "envelopes/content-verification.json");
            JsonNode originalProof = publishContent.get("goldProof");
            JsonNode proof = goldProofV1(publishGold, verifiedGold, "gold-measurement", UUID.fromString(originalProof.get("artifactId").stringValue()));
            assertThat(SourceJson.canonical(proof)).isEqualTo(SourceJson.canonical(originalProof));
            assertThat(commandV1("CONTENT", publishContent, "RECHECK", publishContent.get("prepared"), verifiedContent)).isEqualTo(read(directory, "java-envelopes/content-recheck.json"));
            assertThat(recoveryCommandV1("CONTENT", publishContent)).isEqualTo(read(directory, "java-envelopes/content-recovery.json"));
            assertThat(contentArtifactV1(publishContent, verifiedContent, "content-measurement")).isEqualTo(read(directory, "java-envelopes/content-artifact.json"));
            JsonNode resume = read(directory, "java-envelopes/content-resume.json");
            assertThat(resumeCommandV1("CONTENT_INPUTS", resume.get("reference"), resume.get("referenceBinding"), resume.get("content"), resume.get("contentCommand"))).isEqualTo(resume);
            if (name.equals("small_nonempty")) {
                qualifyGoldV1(publishGold, verifiedGold, "gold-measurement", receipts());
                qualifyContentV1(publishContent, verifiedContent, "content-measurement", receipts());
                qualifyResumeV1(resume, verifiedGold, publishContent.get("prepared"));
            } else {
                if (name.startsWith("gold") || name.startsWith("both")) rejected(() -> qualifyGoldV1(publishGold, verifiedGold, "gold-measurement", receipts()), PreflightFailure.Code.RESOURCE_LIMIT);
                rejected(() -> qualifyContentV1(publishContent, verifiedContent, "content-measurement", receipts()), PreflightFailure.Code.RESOURCE_LIMIT);
            }
        }
    }

    private static JsonNode read(Path directory, String name) throws Exception { return JSON.readTree(Files.readAllBytes(directory.resolve(name))); }
    private static ObjectNode common() {
        return SourceJson.object().put("protocolVersion", 1).put("mode", "PUBLISH").put("runId", ID.toString())
                .put("attemptId", ID.toString()).put("ownerEpoch", 1L).put("checkpointId", ID.toString())
                .put("runtimeSha256", SHA).put("inputSha256", SHA).putNull("verified");
    }
    private static ObjectNode goldCommand() {
        ObjectNode c = common().put("silverBindingId", "silver");
        c.putArray("inputs").addObject().put("artifactId", ID.toString()).put("evidenceSha256", SHA);
        c.set("silver", binding("silver")); c.set("gold", binding("gold")); c.set("runtime", binding("scratch"));
        c.set("prepared", goldPrepared()); return c;
    }
    private static ObjectNode goldPrepared() {
        ObjectNode n = SourceJson.object().put("inputSetFingerprint", SHA); n.putObject("manifest");
        n.putObject("counts").put("accepted", 1).put("rejected", 0); return n;
    }
    private static ObjectNode goldVerification() {
        ObjectNode n = SourceJson.object().put("status", "VERIFIED").put("completionSha256", SHA);
        n.set("prepared", goldPrepared()); n.putObject("completion"); n.putObject("capture");
        n.putArray("verifiers").add("gold_publication.verify_publication").add("gold_analytics.open_publication"); return n;
    }
    private static ObjectNode contentCommand() {
        ObjectNode c = common().put("referenceBindingId", "reference");
        c.set("goldProof", goldProofV1(goldCommand(), goldVerification(), "gold", ID));
        ObjectNode reference = SourceJson.object().put("artifactId", ID.toString()).put("evidenceSha256", SHA);
        reference.putObject("qualification").putObject("verification").put("verifier", "fixture-only"); c.set("reference", reference);
        c.set("referenceBinding", binding("reference")); c.set("content", binding("content")); c.set("runtime", binding("scratch"));
        c.set("prepared", contentPrepared()); return c;
    }
    private static ObjectNode contentPrepared() { ObjectNode n = SourceJson.object().put("fingerprint", SHA); n.putObject("manifest"); return n; }
    private static ObjectNode contentVerification() {
        ObjectNode n = SourceJson.object().put("status", "VERIFIED").put("completionSha256", SHA).put("verifier", "content_analytics.open_publication");
        n.set("prepared", contentPrepared()); n.putObject("completion"); n.putObject("capture"); n.putObject("coverage"); n.putArray("smoke"); return n;
    }
    private static ObjectNode binding(String name) { return SourceJson.object().put("root", "/fixture/" + name).put("rootDevice", "1").put("rootInode", "2"); }
    private static JsonNode receipts() { var array = SourceJson.object().putArray("receipts"); array.addObject().put("sourceId", "fixture").put("payloadSha256", SHA); return array; }
    private static JsonNode nested(int depth, boolean empty) {
        JsonNode value = empty ? SourceJson.object().putArray("n") : JSON.getNodeFactory().numberNode(0);
        for (int i = empty ? 1 : 0; i < depth; i++) { var next = SourceJson.object().putArray("n"); next.add(value); value = next; }
        return value;
    }
    private static JsonNode document(int size) {
        StringBuilder s = new StringBuilder("["); int remaining = size - 2;
        while (remaining > 4098) { s.append('"').append("x".repeat(4095)).append("\","); remaining -= 4098; }
        s.append('"').append("x".repeat(remaining - 2)).append("\"]");
        return JSON.readTree(s.toString().getBytes(StandardCharsets.UTF_8));
    }
    private static void resizeVerification(ObjectNode verification, int length) {
        verification.set("capture", document(2 + 2));
        int remaining = length - SourceJson.canonical(verification).length + 4;
        verification.set("capture", document(remaining));
        assertThat(SourceJson.canonical(verification)).hasSize(length);
    }
    private static void rejected(org.assertj.core.api.ThrowableAssert.ThrowingCallable call, PreflightFailure.Code code) {
        assertThatThrownBy(call).isInstanceOfSatisfying(PreflightFailure.class, error -> {
            assertThat(error.code()).isEqualTo(code); assertThat(error.getMessage()).isEqualTo(code.name());
        });
    }
}
