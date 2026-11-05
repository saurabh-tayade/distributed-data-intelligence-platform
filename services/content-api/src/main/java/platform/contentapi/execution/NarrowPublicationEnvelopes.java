package platform.contentapi.execution;

import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/**
 * Inactive v1 whole-envelope construction and byte/reader qualification only.
 * Inputs are complete projected values, not estimates or component allowances.
 * No native validity, filesystem projection, reservation or dispatch authority is granted.
 * No caller outside tests uses this class; historical constructors/readers stay unchanged.
 */
final class NarrowPublicationEnvelopes {
    enum Destination {
        GOLD_COMMAND("PUBLISH_GOLD", "command.json"),
        GOLD_EVIDENCE("VERIFY_RESULT", "evidence.json"),
        GOLD_ARTIFACT("PUBLISH_GOLD", "selection.json"),
        CONTENT_COMMAND("PUBLISH_CONTENT", "command.json"),
        CONTENT_EVIDENCE("VERIFY_RESULT", "evidence.json"),
        CONTENT_ARTIFACT("VERIFY_RESULT", "selection.json"),
        RECOVERY_COMMAND("VERIFY_RESULT", "command.json"),
        RECOVERY_EVIDENCE("VERIFY_RESULT", "evidence.json"),
        RESUME_COMMAND("VERIFY_RESULT", "command.json"),
        RESUME_EVIDENCE("VERIFY_RESULT", "evidence.json"),
        RESULT("VERIFY_RESULT", "result.json"),
        RECOVERY_OBSERVATION(null, null);

        final String stage, file;
        Destination(String stage, String file) { this.stage = stage; this.file = file; }
        int maximum() { return stage == null ? RecoveryFiles.MAXIMUM : ContentEvidenceBudget.slots(stage).get(file); }
        int readerMaximum() { return this == RESULT ? 65536 : this == RECOVERY_OBSERVATION ? RecoveryFiles.MAXIMUM : PreflightFiles.LARGE; }
    }

    /** Exact existing slot and actual downstream parser, including all nested overhead. */
    static byte[] qualifyV1(JsonNode complete, Destination destination) {
        try {
            need(destination != null, INPUT_INVALID);
            byte[] bytes = SourceJson.canonical(complete);
            need(bytes.length <= destination.maximum(), RESOURCE_LIMIT);
            JsonNode read = SourceJson.parse(bytes, destination.readerMaximum());
            need(Arrays.equals(bytes, SourceJson.canonical(read)), INPUT_INVALID);
            return bytes;
        } catch (PreflightFailure error) { throw error; }
        catch (RuntimeException error) { throw new PreflightFailure(INPUT_INVALID); }
    }

    /** No independent 1 MiB cap on a committed proof: only its actual enclosing slots apply. */
    static ObjectNode goldProofV1(JsonNode publish, JsonNode verification, String bindingId, UUID artifactId) {
        return checked(() -> {
            ObjectNode artifact = goldArtifactV1(publish, verification, bindingId);
            need(artifactId != null, INPUT_INVALID);
            ObjectNode proof = SourceJson.object().put("artifactId", artifactId.toString())
                    .put("evidenceSha256", SourceJson.sha(SourceJson.canonical(artifact)));
            proof.set("artifact", artifact);
            proof.set("command", publish.deepCopy());
            return proof;
        });
    }

    static ObjectNode goldArtifactV1(JsonNode publish, JsonNode verification, String bindingId) {
        return checked(() -> {
            goldCommand(publish); publishing(publish); goldVerification(verification);
            need(publish.get("prepared").equals(verification.get("prepared")), INPUT_INVALID);
            ObjectNode artifact = artifact("Gold", "GOLD", text(verification.get("prepared"), "inputSetFingerprint"), bindingId, "input_set", verification);
            var inputs = artifact.putArray("inputs");
            need(publish.get("inputs").isArray(), INPUT_INVALID);
            for (JsonNode input : publish.get("inputs")) inputs.addObject()
                    .put("artifactId", text(input, "artifactId")).put("evidenceSha256", text(input, "evidenceSha256"));
            return artifact;
        });
    }

    static ObjectNode contentArtifactV1(JsonNode publish, JsonNode verification, String bindingId) {
        return checked(() -> {
            contentCommand(publish); publishing(publish); contentVerification(verification);
            need(publish.get("prepared").equals(verification.get("prepared")), INPUT_INVALID);
            ObjectNode artifact = artifact("Content", "CONTENT", text(verification.get("prepared"), "fingerprint"), bindingId, "publication", verification);
            artifact.set("inputs", RssContentInputs.dependencies(publish.get("goldProof"), publish.get("reference")));
            return artifact;
        });
    }

    /** Preserves the full command, including duplicated preparation and original Gold proof. */
    static ObjectNode commandV1(String kind, JsonNode original, String mode, JsonNode prepared, JsonNode verified) {
        return checked(() -> {
            command(kind, original);
            need(Set.of("PREPARE", "PUBLISH", "VERIFY", "RECHECK").contains(mode), INPUT_INVALID);
            ObjectNode result = (ObjectNode) original.deepCopy();
            result.put("mode", mode); result.set("prepared", copy(prepared)); result.set("verified", copy(verified));
            return result;
        });
    }

    /** Actual recovery path uses VERIFY/PREPARE and clears verified, never wraps RECHECK. */
    static ObjectNode recoveryCommandV1(String kind, JsonNode original) {
        return checked(() -> {
            command(kind, original);
            ObjectNode result = SourceJson.object().put("kind", kind);
            result.set("original", commandV1(kind, original, original.get("prepared").isNull() ? "PREPARE" : "VERIFY", original.get("prepared"), null));
            return result;
        });
    }

    static ObjectNode checkpointObservationV1(JsonNode checkpoint, JsonNode receipts) {
        return checked(() -> {
            ObjectNode result = observation(receipts);
            result.set("checkpoint", checkpoint.deepCopy()); return result;
        });
    }

    static ObjectNode contentObservationV1(JsonNode artifact, JsonNode receipts) {
        return checked(() -> {
            need("CONTENT".equals(text(artifact, "kind")), INPUT_INVALID);
            ObjectNode result = observation(receipts).put("state", "SUCCEEDED").put("code", "RECOVERED_PUBLICATION");
            result.set("artifact", artifact.deepCopy()); return result;
        });
    }

    static ObjectNode resumeCommandV1(String boundary, JsonNode reference, JsonNode referenceBinding, JsonNode contentBinding, JsonNode contentCommand) {
        return checked(() -> {
            need(Set.of("BEFORE_FETCH", "GOLD", "CONTENT_INPUTS").contains(boundary), INPUT_INVALID);
            if (boundary.equals("BEFORE_FETCH")) need(contentCommand == null || contentCommand.isNull(), INPUT_INVALID);
            else {
                contentCommand(contentCommand);
                need("PREPARE".equals(text(contentCommand, "mode")) && contentCommand.get("verified").isNull()
                        && contentCommand.get("reference").equals(reference)
                        && contentCommand.get("referenceBinding").equals(referenceBinding)
                        && contentCommand.get("content").equals(contentBinding), INPUT_INVALID);
                need(boundary.equals("GOLD") ? contentCommand.get("prepared").isNull() : !contentCommand.get("prepared").isNull(), INPUT_INVALID);
            }
            ObjectNode result = SourceJson.object().put("boundary", boundary);
            result.set("reference", reference.deepCopy()); result.set("referenceBinding", referenceBinding.deepCopy());
            result.set("content", contentBinding.deepCopy()); result.set("contentCommand", copy(contentCommand)); return result;
        });
    }

    /** Complete content_resume_verifier.execute result, including repeated Gold and reference. */
    static ObjectNode resumeEvidenceV1(JsonNode request, JsonNode goldVerification, JsonNode prepared) {
        return checked(() -> {
            SourceJson.fields(request, "boundary", "reference", "referenceBinding", "content", "contentCommand");
            String boundary = text(request, "boundary");
            resumeCommandV1(boundary, request.get("reference"), request.get("referenceBinding"), request.get("content"), request.get("contentCommand"));
            ObjectNode result = SourceJson.object().put("boundary", boundary);
            result.set("reference", request.get("reference").get("qualification").get("verification").deepCopy());
            if (!boundary.equals("BEFORE_FETCH")) {
                goldVerification(goldVerification);
                need(goldVerification.equals(request.get("contentCommand").get("goldProof").get("artifact").get("verification")), INPUT_INVALID);
                SourceJson.fields(prepared, "manifest", "fingerprint");
                JsonNode old = request.get("contentCommand").get("prepared");
                need(old.isNull() || old.equals(prepared), INPUT_INVALID);
                result.set("gold", goldVerification.deepCopy()); result.set("prepared", prepared.deepCopy());
            } else need(goldVerification == null && prepared == null, INPUT_INVALID);
            return result;
        });
    }

    /** Gold-local records only. Acceptance does not promise a future Content stage can fit. */
    static void qualifyGoldV1(JsonNode publish, JsonNode verification, String bindingId, JsonNode receipts) {
        ObjectNode artifact = goldArtifactV1(publish, verification, bindingId);
        qualifyV1(publish, Destination.GOLD_COMMAND);
        qualifyV1(commandV1("GOLD", publish, "VERIFY", publish.get("prepared"), null), Destination.GOLD_COMMAND);
        ObjectNode preparing = commandV1("GOLD", publish, "PREPARE", null, null);
        ObjectNode prepared = preparedV1(publish.get("prepared"));
        qualifyV1(preparing, Destination.GOLD_COMMAND);
        qualifyV1(prepared, Destination.GOLD_EVIDENCE);
        qualifyV1(recoveryCommandV1("GOLD", preparing), Destination.RECOVERY_COMMAND);
        qualifyV1(checkpointObservationV1(prepared, receipts), Destination.RECOVERY_OBSERVATION);
        qualifyV1(verification, Destination.GOLD_EVIDENCE);
        qualifyV1(artifact, Destination.GOLD_ARTIFACT);
        qualifyV1(commandV1("GOLD", publish, "RECHECK", publish.get("prepared"), verification), Destination.GOLD_COMMAND);
        qualifyV1(recoveryCommandV1("GOLD", publish), Destination.RECOVERY_COMMAND);
        qualifyV1(verification, Destination.RECOVERY_EVIDENCE);
        qualifyV1(checkpointObservationV1(verification, receipts), Destination.RECOVERY_OBSERVATION);
    }

    /** All complete Content publication/recheck and successful recovery envelopes, not subcaps. */
    static void qualifyContentV1(JsonNode publish, JsonNode verification, String bindingId, JsonNode receipts) {
        ObjectNode artifact = contentArtifactV1(publish, verification, bindingId);
        qualifyV1(publish, Destination.CONTENT_COMMAND);
        qualifyV1(commandV1("CONTENT", publish, "VERIFY", publish.get("prepared"), null), Destination.CONTENT_COMMAND);
        ObjectNode preparing = commandV1("CONTENT", publish, "PREPARE", null, null);
        ObjectNode prepared = preparedV1(publish.get("prepared"));
        qualifyV1(preparing, Destination.CONTENT_COMMAND);
        qualifyV1(prepared, Destination.CONTENT_EVIDENCE);
        qualifyV1(recoveryCommandV1("CONTENT", preparing), Destination.RECOVERY_COMMAND);
        qualifyV1(checkpointObservationV1(prepared, receipts), Destination.RECOVERY_OBSERVATION);
        qualifyV1(verification, Destination.CONTENT_EVIDENCE);
        qualifyV1(artifact, Destination.CONTENT_ARTIFACT);
        qualifyV1(commandV1("CONTENT", publish, "RECHECK", publish.get("prepared"), verification), Destination.CONTENT_COMMAND);
        qualifyV1(recoveryCommandV1("CONTENT", publish), Destination.RECOVERY_COMMAND);
        qualifyV1(verification, Destination.RECOVERY_EVIDENCE);
        qualifyV1(contentObservationV1(artifact, receipts), Destination.RECOVERY_OBSERVATION);
    }

    static void qualifyResumeV1(JsonNode request, JsonNode goldVerification, JsonNode prepared) {
        qualifyV1(request, Destination.RESUME_COMMAND);
        qualifyV1(resumeEvidenceV1(request, goldVerification, prepared), Destination.RESUME_EVIDENCE);
    }

    static ObjectNode preparedV1(JsonNode prepared) {
        return checked(() -> {
            need(prepared != null && prepared.isObject(), INPUT_INVALID);
            ObjectNode result = SourceJson.object().put("status", "PREPARED");
            result.set("prepared", prepared.deepCopy()); return result;
        });
    }

    private static ObjectNode artifact(String managed, String kind, String fingerprint, String bindingId, String namespace, JsonNode verification) {
        need(bindingId != null && !bindingId.isEmpty() && fingerprint.matches("[0-9a-f]{64}"), INPUT_INVALID);
        ObjectNode result = SourceJson.object().put("managed" + managed + "Version", 1).put("kind", kind)
                .put("productId", "content-intelligence").put("contractVersion", 1).put("processorRevision", 1)
                .put("nativeFingerprint", fingerprint).put("storageBindingId", bindingId)
                .put("relativeAddress", "v1/processor_revision=1/" + namespace + "=" + fingerprint);
        result.set("verification", verification.deepCopy()); return result;
    }
    private static ObjectNode observation(JsonNode receipts) {
        need(receipts != null && receipts.isArray(), INPUT_INVALID);
        ObjectNode result = SourceJson.object().put("state", "PAUSED").put("code", "RECOVERED_CHECKPOINT");
        result.set("receipts", receipts.deepCopy()); return result;
    }
    private static JsonNode copy(JsonNode value) { return value == null ? SourceJson.object().putNull("value").get("value") : value.deepCopy(); }
    private static String text(JsonNode value, String key) { return SourceJson.text(value.get(key)); }
    private static void publishing(JsonNode command) { need("PUBLISH".equals(text(command, "mode")) && command.get("verified").isNull() && !command.get("prepared").isNull(), INPUT_INVALID); }
    private static void command(String kind, JsonNode value) {
        need(Set.of("GOLD", "CONTENT").contains(kind), INPUT_INVALID);
        if (kind.equals("GOLD")) goldCommand(value); else contentCommand(value);
    }
    private static void goldCommand(JsonNode value) {
        SourceJson.fields(value, "protocolVersion", "mode", "runId", "attemptId", "ownerEpoch", "checkpointId", "runtimeSha256", "inputSha256", "silverBindingId", "inputs", "silver", "gold", "runtime", "prepared", "verified");
        version(value);
    }
    private static void contentCommand(JsonNode value) {
        SourceJson.fields(value, "protocolVersion", "mode", "runId", "attemptId", "ownerEpoch", "checkpointId", "runtimeSha256", "inputSha256", "goldProof", "reference", "referenceBindingId", "referenceBinding", "content", "runtime", "prepared", "verified");
        version(value);
    }
    private static void version(JsonNode value) { need(value.get("protocolVersion").isIntegralNumber() && value.get("protocolVersion").canConvertToInt() && value.get("protocolVersion").intValue() == 1, INPUT_INVALID); }
    private static void goldVerification(JsonNode value) {
        SourceJson.fields(value, "status", "prepared", "completion", "capture", "completionSha256", "verifiers");
        SourceJson.fields(value.get("prepared"), "manifest", "inputSetFingerprint", "counts");
        need("VERIFIED".equals(text(value, "status")), INPUT_INVALID);
    }
    private static void contentVerification(JsonNode value) {
        SourceJson.fields(value, "status", "prepared", "completion", "completionSha256", "capture", "coverage", "smoke", "verifier");
        SourceJson.fields(value.get("prepared"), "manifest", "fingerprint");
        need("VERIFIED".equals(text(value, "status")), INPUT_INVALID);
    }
    private interface Builder { ObjectNode build(); }
    private static ObjectNode checked(Builder builder) {
        try { return builder.build(); }
        catch (PreflightFailure error) { throw error; }
        catch (RuntimeException error) { throw new PreflightFailure(INPUT_INVALID); }
    }
    private NarrowPublicationEnvelopes() { }
}
