package platform.contentapi.execution;

import org.junit.jupiter.api.Test;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static platform.contentapi.execution.NarrowPublicationEnvelopes.*;

/** Mandatory full-envelope parity. Conversion below exists only in this test. */
class NarrowProjectionTransportTest {
    @Test
    void completeV2EnvelopesMatchUnchangedReadersAndHistoricalConstructorShapes() throws Exception {
        Map<String, JsonNode> original = new LinkedHashMap<>();
        Set<Destination> covered = EnumSet.noneOf(Destination.class);
        for (String line : export()) {
            String[] columns = line.split("\t", -1);
            assertEquals(3, columns.length);
            byte[] raw = Base64.getDecoder().decode(columns[2]);
            int readerMaximum;
            if (columns[1].equals("NO_INDEPENDENT_SLOT")) {
                readerMaximum = PreflightFiles.LARGE; // Proof has no independent stage slot.
            } else {
                Destination destination = Destination.valueOf(columns[1]);
                covered.add(destination);
                assertEquals(destination == Destination.RESULT ? 65536 : 1024 * 1024, destination.maximum());
                assertTrue(raw.length <= destination.maximum(), columns[0]);
                readerMaximum = destination.readerMaximum();
            }
            JsonNode value = SourceJson.parse(raw, readerMaximum);
            assertArrayEquals(raw, SourceJson.canonical(value), columns[0]);
            assertNull(original.put(columns[0], value), "Duplicate exported vector");
        }
        assertEquals(33, original.size());
        assertEquals(EnumSet.allOf(Destination.class), covered);
        Map<String, JsonNode> legacyShapes = new LinkedHashMap<>();
        original.forEach((name, value) -> legacyShapes.put(name, testOnlyV1Shape(value)));
        for (String kind : List.of("GOLD", "CONTENT")) {
            String prefix = kind.toLowerCase(Locale.ROOT) + "_";
            JsonNode publish = legacyShapes.get(prefix + "publish");
            JsonNode verification = legacyShapes.get(prefix + "verification");
            JsonNode prepared = preparedV1(publish.get("prepared"));
            for (String mode : List.of("PREPARE", "VERIFY", "RECHECK")) {
                JsonNode expected = commandV1(kind, publish, mode,
                        mode.equals("PREPARE") ? null : publish.get("prepared"), mode.equals("RECHECK") ? verification : null);
                assertEquals(expected, legacyShapes.get(prefix + mode.toLowerCase(Locale.ROOT)));
            }
            assertEquals(prepared, legacyShapes.get(prefix + "prepared"));
            String binding = original.get(prefix + "publish").get("projectionContext").get("outputBindingId").stringValue();
            JsonNode artifact = kind.equals("GOLD") ? goldArtifactV1(publish, verification, binding)
                    : contentArtifactV1(publish, verification, binding);
            assertEquals(artifact, legacyShapes.get(prefix + "artifact"));
            // Artifacts have no embedded commands, so their exact hashes do not change.
            assertEquals(original.get(prefix + "artifact"), artifact);
            assertEquals(recoveryCommandV1(kind, legacyShapes.get(prefix + "prepare")), legacyShapes.get(prefix + "recovery_prepare"));
            assertEquals(recoveryCommandV1(kind, publish), legacyShapes.get(prefix + "recovery_verify"));
            assertEquals(verification, legacyShapes.get(prefix + "recovery_evidence"));
            JsonNode receipts = original.get(prefix + "observation_prepared").get("receipts");
            assertEquals(original.get("gold_publish").get("inputs").get(0).get("receipt"), receipts.get(0));
            assertEquals(checkpointObservationV1(prepared, receipts), legacyShapes.get(prefix + "observation_prepared"));
            assertEquals(kind.equals("GOLD") ? checkpointObservationV1(verification, receipts)
                    : contentObservationV1(artifact, receipts), legacyShapes.get(prefix + "observation_verified"));
            assertEquals(kind.equals("GOLD") ? 1 : 2, countV2Contexts(original.get(prefix + "publish")));
            assertEquals(kind.equals("GOLD") ? 1 : 2, countV2Contexts(original.get(prefix + "recheck")));
            assertEquals(kind.equals("GOLD") ? 1 : 2, countV2Contexts(original.get(prefix + "recovery_verify")));
        }
        JsonNode content = legacyShapes.get("content_publish");
        assertEquals(commandV1("CONTENT", content, "VERIFY", content.get("prepared"),
                legacyShapes.get("content_verification")), legacyShapes.get("content_inspect"));
        assertEquals(2, countV2Contexts(original.get("content_inspect")));
        JsonNode proof = original.get("gold_proof");
        assertEquals(goldProofV1(legacyShapes.get("gold_publish"), legacyShapes.get("gold_verification"),
                "gold", UUID.fromString(proof.get("artifactId").stringValue())), legacyShapes.get("gold_proof"));
        assertEquals(SourceJson.sha(SourceJson.canonical(original.get("gold_artifact"))), proof.get("evidenceSha256").stringValue());
        assertEquals(proof, original.get("content_publish").get("goldProof"));
        assertEquals(1, countV2Contexts(proof));
        for (String boundary : List.of("BEFORE_FETCH", "GOLD", "CONTENT_INPUTS")) {
            String prefix = "resume_" + boundary;
            JsonNode request = legacyShapes.get(prefix + "_command");
            assertEquals(resumeCommandV1(boundary, request.get("reference"), request.get("referenceBinding"),
                    request.get("content"), request.get("contentCommand")), request);
            boolean before = boundary.equals("BEFORE_FETCH");
            assertEquals(resumeEvidenceV1(request, before ? null : legacyShapes.get("gold_verification"),
                    before ? null : legacyShapes.get("content_publish").get("prepared")), legacyShapes.get(prefix + "_evidence"));
            assertEquals(before ? 0 : 2, countV2Contexts(original.get(prefix + "_command")));
        }
    }

    /** Explicit recursive shape comparison only: never a production protocol downgrade. */
    private static JsonNode testOnlyV1Shape(JsonNode original) {
        JsonNode copy = original.deepCopy();
        convertTestCopy(copy);
        return copy;
    }

    private static void convertTestCopy(JsonNode node) {
        if (node.isObject() && node.has("projectionContext")) {
            assertEquals(2, node.get("protocolVersion").intValue());
            ((ObjectNode) node).remove("projectionContext");
            ((ObjectNode) node).put("protocolVersion", 1);
        }
        if (node.isContainer()) for (JsonNode child : node) convertTestCopy(child);
    }

    private static int countV2Contexts(JsonNode node) {
        int count = 0;
        if (node.isObject() && node.has("protocolVersion")) {
            assertEquals(2, node.get("protocolVersion").intValue());
            assertTrue(node.has("projectionContext"));
            assertEquals(1, node.get("projectionContext").get("projectionContextVersion").intValue());
            count++;
        }
        if (node.isContainer()) for (JsonNode child : node) count += countV2Contexts(child);
        return count;
    }

    private static List<String> export() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        while (root != null && !Files.exists(root.resolve("processing/tests/projection_transport_parity.py"))) root = root.getParent();
        assertNotNull(root, "Repository root required for mandatory complete-envelope parity");
        Path output = Files.createTempFile("projection-transport-out-", ".txt");
        Path errors = Files.createTempFile("projection-transport-errors-", ".txt");
        try {
            Process process = new ProcessBuilder(System.getProperty("transport.python", "python3"),
                    "-m", "processing.tests.projection_transport_parity").directory(root.toFile())
                    .redirectOutput(output.toFile()).redirectError(errors.toFile()).start();
            boolean exited = process.waitFor(60, TimeUnit.SECONDS);
            if (!exited) process.destroyForcibly();
            assertTrue(exited, "Python complete-envelope export timed out");
            assertEquals(0, process.exitValue(), Files.readString(errors));
            return Files.readAllLines(output, StandardCharsets.UTF_8);
        } finally {
            Files.deleteIfExists(output); Files.deleteIfExists(errors);
        }
    }
}
