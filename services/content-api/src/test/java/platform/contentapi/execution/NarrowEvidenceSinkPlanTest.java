package platform.contentapi.execution;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import platform.contentapi.management.SourceJson;
import static org.junit.jupiter.api.Assertions.*;

/** Mandatory comparison with the unchanged production Java plan, not hand-authored UUID vectors. */
class NarrowEvidenceSinkPlanTest {
    @Test void pythonReconstructsEveryOriginalAddressAndSlotByteForByte() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        while (root != null && !Files.exists(root.resolve("processing/narrow_evidence_sink.py"))) root = root.getParent();
        assertNotNull(root, "Repository root required for mandatory cross-language parity");
        Path input = Files.createTempFile("narrow-sink-in-", ".txt");
        Path output = Files.createTempFile("narrow-sink-out-", ".txt");
        Path errors = Files.createTempFile("narrow-sink-errors-", ".txt");
        try {
            var requests = new ArrayList<String>();
            var expected = new ArrayList<byte[]>();
            for (String source : List.of("a", "source-one", "x".repeat(128))) {
                UUID run = UUID.randomUUID(), attempt = UUID.randomUUID();
                var request = SourceJson.object().put("runId", run.toString()).put("attemptId", attempt.toString())
                        .put("sourceId", source).put("rootDevice", "123456789").put("rootInode", "987654321");
                requests.add(new String(SourceJson.canonical(request), StandardCharsets.UTF_8));
                var ledger = SourceJson.object().put("version", 1).put("treeKind", "ATTEMPT")
                        .put("runId", run.toString()).put("attemptId", attempt.toString())
                        .put("rootDevice", "123456789").put("rootInode", "987654321");
                var plan = ContentOperationPlan.original(Path.of("/unused-test-control/runs/" + run + "/attempts/" + attempt),
                        run, attempt, source);
                assertEquals(19, plan.reservations().size());
                assertEquals(plan.directory("SILVER", 0).getFileName(), plan.directory("VERIFY_SILVER", 0).getFileName());
                plan.apply(ledger);
                expected.add(SourceJson.canonical(ledger));
            }
            Files.write(input, requests, StandardCharsets.UTF_8);
            Process process = new ProcessBuilder(System.getProperty("transport.python", "python3"),
                    "-m", "processing.tests.narrow_evidence_sink_parity").directory(root.toFile())
                    .redirectInput(input.toFile()).redirectOutput(output.toFile()).redirectError(errors.toFile()).start();
            boolean exited = process.waitFor(60, TimeUnit.SECONDS);
            if (!exited) process.destroyForcibly();
            assertTrue(exited, "Python plan probe timed out");
            assertEquals(0, process.exitValue(), Files.readString(errors));
            List<String> actual = Files.readAllLines(output, StandardCharsets.UTF_8);
            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < expected.size(); i++) {
                byte[] bytes = Base64.getDecoder().decode(actual.get(i));
                assertArrayEquals(expected.get(i), bytes, "Full production operation grammar must match");
                var ledger = SourceJson.parse(bytes, ContentEvidenceBudget.LEDGER);
                long reservation = 2L * ContentEvidenceBudget.LEDGER;
                for (String path : ledger.get("files").propertyNames()) reservation += ledger.get("files").get(path).longValue();
                assertEquals(ContentEvidenceBudget.BYTES, reservation);
                assertEquals(221, ledger.get("directories").size() + ledger.get("files").size() + 3);
            }
        } finally {
            Files.deleteIfExists(input); Files.deleteIfExists(output); Files.deleteIfExists(errors);
        }
    }
}
