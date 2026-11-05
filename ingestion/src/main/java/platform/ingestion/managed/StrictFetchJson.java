package platform.ingestion.managed;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Locale;

/** Inactive child JSON boundary. Canonical encoding matches the independent Content plan. */
final class StrictFetchJson {
    private static final ObjectMapper JSON = new ObjectMapper(JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(16)
                    .maxStringLength(65536).maxNumberLength(20).build())
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    static JsonNode parse(byte[] raw, int maximum) throws IOException {
        need(raw != null && raw.length > 0 && raw.length <= maximum);
        try {
            String value = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(raw)).toString();
            need(value.charAt(0) != '\ufeff');
            JsonNode node = JSON.readTree(value);
            need(Arrays.equals(raw, canonical(node)));
            return node;
        } catch (RuntimeException error) { throw invalid(); }
    }

    static ObjectNode object() { return JSON.createObjectNode(); }
    static byte[] canonical(JsonNode node) throws IOException {
        StringBuilder output = new StringBuilder();
        append(output, node, 0);
        return output.toString().getBytes(StandardCharsets.UTF_8);
    }
    private static void append(StringBuilder out, JsonNode node, int depth) throws IOException {
        need(node != null && depth <= 16);
        if (node.isObject()) {
            out.append('{');
            var keys = new ArrayList<String>(); node.properties().forEach(entry -> keys.add(entry.getKey()));
            keys.sort((a, b) -> Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8)));
            boolean first = true;
            for (String key : keys) {
                if (!first) out.append(','); first = false;
                quote(out, key); out.append(':'); append(out, node.get(key), depth + 1);
            }
            out.append('}');
        } else if (node.isArray()) {
            out.append('['); boolean first = true;
            for (JsonNode value : node) {
                if (!first) out.append(','); first = false; append(out, value, depth + 1);
            }
            out.append(']');
        } else if (node.isTextual()) quote(out, node.textValue());
        else if (node.isIntegralNumber()) out.append(node.bigIntegerValue());
        else if (node.isBoolean()) out.append(node.booleanValue());
        else if (node.isNull()) out.append("null");
        else throw invalid();
    }
    private static void quote(StringBuilder out, String value) throws IOException {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                need(i + 1 < value.length() && Character.isLowSurrogate(value.charAt(i + 1)));
                out.append(c).append(value.charAt(++i)); continue;
            }
            need(!Character.isLowSurrogate(c));
            switch (c) {
                case '"' -> out.append("\\\""); case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b"); case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n"); case '\r' -> out.append("\\r"); case '\t' -> out.append("\\t");
                default -> { if (c < 32) out.append(String.format(Locale.ROOT, "\\u%04x", (int)c)); else out.append(c); }
            }
        }
        out.append('"');
    }
    static String text(JsonNode node, String key) throws IOException {
        need(node != null && node.has(key) && node.get(key).isTextual()); return node.get(key).textValue();
    }
    static void fields(JsonNode node, String... keys) throws IOException {
        need(node != null && node.isObject() && node.size() == keys.length);
        for (String key : keys) need(node.has(key));
    }
    static long decimal(JsonNode node, String key, boolean positive) throws IOException {
        String value = text(node, key);
        need(value.matches("0|[1-9][0-9]{0,18}"));
        try { long result = Long.parseLong(value); need(!positive || result > 0); return result; }
        catch (NumberFormatException error) { throw invalid(); }
    }
    static String sha(byte[] raw) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw)); }
        catch (NoSuchAlgorithmException error) { throw new IllegalStateException("SHA256_UNAVAILABLE"); }
    }
    static void need(boolean value) throws IOException { if (!value) throw invalid(); }
    static IOException invalid() { return new IOException("FETCH_EVIDENCE_INVALID"); }
    private StrictFetchJson() { }
}
