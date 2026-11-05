package platform.contentapi.execution;

import org.junit.jupiter.api.Test;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static platform.contentapi.execution.NarrowPublicationEnvelopes.Destination;

/** Both actual publisher pre-rename projections and native post-rename verification.
 * No RESULT is emitted by these libraries; adapter-result qualification is pending. */
class NarrowNativePublisherTransportTest {
    @Test
    void actualNativeBodiesFitUnchangedSourceJsonAndEveryCompleteEnvelope() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        while (root != null && !Files.exists(root.resolve("processing/tests/v2_publisher_native_parity.py"))) {
            root = root.getParent();
        }
        assertNotNull(root, "Repository required for mandatory native publisher parity");
        Path output = Files.createTempFile("native-v2-parity-", ".txt");
        Path errors = Files.createTempFile("native-v2-parity-", ".err");
        try {
            Process process = new ProcessBuilder(System.getProperty("transport.python", "python3"),
                    "-m", "processing.tests.v2_publisher_native_parity").directory(root.toFile())
                    .redirectOutput(output.toFile()).redirectError(errors.toFile()).start();
            boolean exited = process.waitFor(10, TimeUnit.MINUTES);
            if (!exited) process.destroyForcibly();
            assertTrue(exited, "Native v2 parity timed out: " + Files.readString(errors));
            assertEquals(0, process.exitValue(), Files.readString(errors));
            Map<String, JsonNode> values = new HashMap<>();
            EnumSet<Destination> covered = EnumSet.noneOf(Destination.class);
            for (String line : Files.readAllLines(output, StandardCharsets.UTF_8)) {
                String[] columns = line.split("\\t", -1);
                assertEquals(3, columns.length);
                Destination destination = Destination.valueOf(columns[1]);
                covered.add(destination);
                byte[] raw = Base64.getDecoder().decode(columns[2]);
                assertTrue(raw.length <= destination.maximum(), columns[0]);
                JsonNode parsed = SourceJson.parse(raw, destination.readerMaximum());
                assertArrayEquals(raw, SourceJson.canonical(parsed), columns[0]);
                assertNull(values.put(columns[0], parsed));
            }
            assertEquals(124, values.size());
            EnumSet<Destination> expected = EnumSet.allOf(Destination.class);
            expected.remove(Destination.RESULT);
            assertEquals(expected, covered);
            for (String fixture : new String[]{"nonempty_projected_", "nonempty_actual_", "reference_only_projected_", "reference_only_actual_"}) {
                JsonNode gold = values.get(fixture + "gold_verification");
                JsonNode content = values.get(fixture + "content_verification");
                assertEquals("VERIFIED", gold.get("status").stringValue());
                assertEquals("VERIFIED", content.get("status").stringValue());
                assertTrue(gold.get("capture").get("files").size() > 0);
                assertTrue(content.get("capture").get("files").size() > 0);
                assertEquals(gold, values.get(fixture + "content_publish").get("goldProof")
                        .get("artifact").get("verification"));
                assertEquals(gold, values.get(fixture + "gold_recovery_evidence"));
                assertEquals(content, values.get(fixture + "content_recovery_evidence"));
                for (String boundary : new String[]{"GOLD", "CONTENT_INPUTS"}) {
                    assertEquals(gold, values.get(fixture + "resume_" + boundary + "_evidence").get("gold"));
                    assertEquals(content.get("prepared"), values.get(fixture + "resume_" + boundary + "_evidence").get("prepared"));
                }
                JsonNode receipt = values.get(fixture + "gold_publish").get("inputs").get(0).get("receipt");
                assertEquals(17, receipt.size());
                assertEquals(receipt, values.get(fixture + "content_observation_verified").get("receipts").get(0));
            }
        } finally {
            Files.deleteIfExists(output);
            Files.deleteIfExists(errors);
        }
    }
}
