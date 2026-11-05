package platform.contentapi.execution;

import org.junit.jupiter.api.Test;
import platform.contentapi.management.SourceJson;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Mandatory Python/unchanged production Java reader comparison, not an optional profile. */
class NarrowTransportJsonTest {
    record Vector(String name, byte[] raw, int maximum, boolean accepted) { }

    @Test
    void canonicalBoundaryVectorsMatchUnchangedSourceJson() throws Exception {
        var vectors = new ArrayList<Vector>();
        for (String raw : List.of("{}", "[]", "null", "true", "false", "0", "-1", "\"\"",
                "99999999999999999999", "-99999999999999999999",
                "{\"a\":\"\\u0000\\b\\f\\n\\r\\t\\\"\\\\\",\"\":0,\"😀\":1}")) {
            add(vectors, "scalar/ordering/escaping " + raw, raw, 1048576, true);
        }
        add(vectors, "small byte limit exact", "\"" + "x".repeat(65534) + "\"", 65536, true);
        add(vectors, "small byte limit over", "\"" + "x".repeat(65535) + "\"", 65536, false);
        for (String character : List.of("x", "é", "😀")) {
            int count = 131072 / character.length();
            add(vectors, "large UTF16 boundary " + character, "\"" + character.repeat(count) + "\"", 1048576, true);
            add(vectors, "large UTF16 over " + character, "\"" + character.repeat(count) + "x\"", 1048576, false);
        }
        for (String character : List.of("x", "😀")) {
            int count = 50000 / character.length();
            add(vectors, "key boundary " + character, "{\"" + character.repeat(count) + "\":0}", 1048576, true);
            add(vectors, "key over " + character, "{\"" + character.repeat(count) + "x\":0}", 1048576, false);
        }
        for (String leaf : List.of("0", "[]", "{}")) {
            int wrappers = leaf.equals("0") ? 16 : 15;
            String raw = "[".repeat(wrappers) + leaf + "]".repeat(wrappers);
            add(vectors, "depth boundary " + leaf, raw, 1048576, true);
            add(vectors, "depth over " + leaf, "[" + raw + "]", 1048576, false);
        }
        for (String raw : List.of("100000000000000000000", "-100000000000000000000",
                "1.0", "1e0", "{\"a\":0,\"a\":1}", "{}{}", "\ufeff{}", "\"\\ud800\"", "\"\\udc00\"", "")) {
            add(vectors, "invalid " + raw, raw, 1048576, false);
        }
        vectors.add(new Vector("invalid UTF8", new byte[]{'"', (byte) 0xff, '"'}, 1048576, false));
        List<String> python = python(vectors);
        for (int i = 0; i < vectors.size(); i++) {
            Vector vector = vectors.get(i);
            String expected = vector.accepted() ? Base64.getEncoder().encodeToString(vector.raw()) : "INVALID";
            assertEquals(expected, python.get(i), "Python: " + vector.name());
            String java;
            try {
                java = Base64.getEncoder().encodeToString(SourceJson.canonical(SourceJson.parse(vector.raw(), vector.maximum())));
            } catch (RuntimeException invalid) {
                java = "INVALID";
            }
            assertEquals(expected, java, "SourceJson: " + vector.name());
        }
    }

    private static void add(List<Vector> vectors, String name, String text, int maximum, boolean accepted) {
        vectors.add(new Vector(name, text.getBytes(StandardCharsets.UTF_8), maximum, accepted));
    }

    private static List<String> python(List<Vector> vectors) throws Exception {
        Path root = Path.of("").toAbsolutePath();
        while (root != null && !Files.exists(root.resolve("processing/narrow_transport_json.py"))) root = root.getParent();
        assertNotNull(root, "Repository root required for mandatory cross-language parity");
        Path input = Files.createTempFile("narrow-json-in-", ".txt");
        Path output = Files.createTempFile("narrow-json-out-", ".txt");
        Path errors = Files.createTempFile("narrow-json-errors-", ".txt");
        try {
            var lines = new ArrayList<String>();
            for (Vector vector : vectors) lines.add(vector.maximum() + "\t" + Base64.getEncoder().encodeToString(vector.raw()));
            Files.write(input, lines, StandardCharsets.UTF_8);
            Process process = new ProcessBuilder(System.getProperty("transport.python", "python3"),
                    "-m", "processing.tests.narrow_transport_json_parity").directory(root.toFile())
                    .redirectInput(input.toFile()).redirectOutput(output.toFile()).redirectError(errors.toFile()).start();
            boolean exited = process.waitFor(60, TimeUnit.SECONDS);
            if (!exited) process.destroyForcibly();
            assertTrue(exited, "Python parity process timed out");
            assertEquals(0, process.exitValue(), Files.readString(errors));
            List<String> result = Files.readAllLines(output, StandardCharsets.UTF_8);
            assertEquals(vectors.size(), result.size(), "Python parity response count");
            return result;
        } finally {
            Files.deleteIfExists(input); Files.deleteIfExists(output); Files.deleteIfExists(errors);
        }
    }
}
