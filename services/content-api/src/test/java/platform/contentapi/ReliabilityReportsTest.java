package platform.contentapi;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import static org.junit.jupiter.api.Assertions.*;
import static platform.contentapi.TestSupport.*;

class ReliabilityReportsTest {
    static final String START = "2026-10-01T00:00:00.000000Z";
    static final String END = "2026-10-01T00:10:00.000000Z";
    static ReliabilityRequestQuery query() { return new ReliabilityRequestQuery(START, END, null, null, 100, 0); }
    static ReliabilityAnalyticsProperties settings() {
        return new ReliabilityAnalyticsProperties(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                Path.of("../../processing"), Path.of("target/reliability's output"), Path.of("target/batches"),
                Path.of("target/reference"), PUBLICATION);
    }
    static String envelope() {
        return """
                {"status":"OK","product_id":"application-reliability","product_fingerprint":"%s",
                 "origin":"synthetic","label":"Synthetic request events — not production reliability.",
                 "product_contract_version":2,"processor_revision":2,"input_format":"JSONL",
                 "reference":{"reference_id":"services","revision":"r1","snapshot_fingerprint":"%s"},
                 "report":"requests_5m","filters":{"service_id":null,"environment":null,
                 "start":"2026-10-01T00:00:00.000000Z","end":"2026-10-01T00:10:00.000000Z"},
                 "limit":"100","offset":"0","has_more":false,"rows":[
                 {"window_start":"2026-10-01T00:00:00.000000Z","service_id":"svc00","environment":"test",
                  "request_count":"2","server_error_count":"1","server_error_rate":0.5,"duration_sum_ms":"30",
                  "mean_duration_ms":15.0,"p95_duration_ms_approx":"20","service_name":"<script>untrusted</script>",
                  "owner_team":null,"service_tier":null,"reference_status":"MATCHED"}]}
                """.formatted(PUBLICATION, "b".repeat(64));
    }
    static final JsonMapper JSON = JsonMapper.builder().build();
    static ObjectNode body() { return (ObjectNode) JSON.readTree(envelope()); }
    static ObjectNode row(ObjectNode body) { return (ObjectNode) body.get("rows").get(0); }
    static BoundedProcessRunner.Result result(JsonNode body) { return output(0, JSON.writeValueAsString(body)); }
    private final ReliabilityAnalyticsAdapter adapter = new ReliabilityAnalyticsAdapter(settings(), new BoundedProcessRunner(properties()));

    @Test void exactMappingAndFixedCommand() {
        var report = adapter.decode(output(0, envelope()), query());
        assertEquals(PUBLICATION, report.publication()); assertEquals(2, report.productContractVersion());
        assertEquals("synthetic", report.origin()); assertEquals(ReliabilityAnalyticsAdapter.LABEL, report.label());
        assertEquals("30", report.data().getFirst().durationSumMs()); assertEquals(START, report.data().getFirst().windowStart());
        assertEquals("20", report.data().getFirst().p95DurationMsApprox()); assertNull(report.data().getFirst().ownerTeam());
        assertEquals(0.5, report.data().getFirst().serverErrorRate()); assertEquals(15.0, report.data().getFirst().meanDurationMs());
        var command = adapter.command(new ReliabilityRequestQuery(START, END, "svc00", "production", 1, 1_000_000));
        assertEquals("requests_5m", command.get(command.indexOf("--report") + 1));
        assertEquals(PUBLICATION, command.get(command.indexOf("--publication") + 1));
        assertEquals("svc00", command.get(command.indexOf("--service-id") + 1));
        assertEquals("production", command.get(command.indexOf("--environment") + 1));
        assertTrue(command.get(6).endsWith("reliability_format_analytics.py"));
        assertFalse(command.contains("--source-id")); assertFalse(command.contains("--sql"));
    }

    @Test void nativeMaximumExactIntegersAndEmptyPages() {
        var body = body(); var row = row(body);
        row.put("request_count", "2000000"); row.put("server_error_count", "1000000");
        row.put("duration_sum_ms", "7200000000000"); row.put("mean_duration_ms", 3600000.0);
        row.put("p95_duration_ms_approx", "3600000");
        assertEquals("7200000000000", adapter.decode(result(body), query()).data().getFirst().durationSumMs());
        body.putArray("rows"); body.put("offset", "1000000");
        var result = adapter.decode(result(body), new ReliabilityRequestQuery(START, END, null, null, 100, 1_000_000));
        assertTrue(result.data().isEmpty()); assertFalse(result.meta().hasMore());
    }

    @Test void preservesBothNativeOwnerTeamsAndNullableTier() {
        for (String owner : List.of("example-platform", "example-service")) {
            var body = body(); row(body).put("owner_team", owner);
            assertEquals(owner, adapter.decode(result(body), query()).data().getFirst().ownerTeam());
        }
    }

    @Test void rejectsWrongEnvelopeIdentityPrimitiveTypesAndFilters() {
        for (String field : List.of("status", "product_id", "product_fingerprint", "origin", "label", "report", "input_format")) {
            var body = body(); body.put(field, "wrong"); invalid(body);
        }
        for (String field : List.of("limit", "offset", "product_contract_version", "processor_revision", "has_more")) {
            var body = body(); body.put(field, 1.5); invalid(body);
        }
        var body = body(); body.put("limit", 100); invalid(body);
        body = body(); body.put("offset", "00"); invalid(body);
        body = body(); body.put("has_more", true); invalid(body);
        body = body(); ((ObjectNode) body.get("filters")).put("service_id", "svc00"); invalid(body);
        body = body(); ((ObjectNode) body.get("filters")).put("start", END); invalid(body);
        body = body(); ((ObjectNode) body.get("reference")).put("snapshot_fingerprint", "latest"); invalid(body);
        body = body(); ((ObjectNode) body.get("reference")).put("revision", "r".repeat(33)); invalid(body);
        body = body(); body.put("extra", "private/path"); invalid(body);
        body = body(); body.remove("reference"); invalid(body);
    }

    @Test void rejectsMalformedMetricsTimestampsOrderAndReferenceStates() {
        for (String field : List.of("request_count", "server_error_count", "duration_sum_ms", "p95_duration_ms_approx")) {
            for (String value : List.of("-1", "01", "1e2", "1.0", "999999999999999999999999999999999999999")) {
                var body = body(); row(body).put(field, value); invalid(body);
            }
            var body = body(); row(body).put(field, 1); invalid(body);
        }
        for (String timestamp : List.of("2026-02-30T00:00:00.000000Z", "2026-10-01T00:01:00.000000Z",
                END, "2026-09-30T23:55:00.000000Z", "2026-10-01T00:00:00.000001Z", "2026-10-01T00:00:00Z")) {
            var body = body(); row(body).put("window_start", timestamp); invalid(body);
        }
        var body = body(); row(body).put("request_count", "0"); invalid(body);
        body = body(); row(body).put("server_error_count", "3"); invalid(body);
        body = body(); row(body).put("server_error_rate", 0.9); invalid(body);
        body = body(); row(body).put("mean_duration_ms", 1); invalid(body);
        body = body(); row(body).put("reference_status", "MISSING"); invalid(body);
        body = body(); row(body).putNull("service_name"); invalid(body);
        body = body(); row(body).put("service_id", "wrong/unsafe"); invalid(body);
        body = body(); ((tools.jackson.databind.node.ArrayNode) body.get("rows")).add(row(body).deepCopy()); invalid(body);
        body = body(); row(body).put("environment", "prod"); invalid(body);
        body = body(); row(body).put("owner_team", "not-in-vocabulary"); invalid(body);
        body = body(); row(body).put("service_tier", "not-in-vocabulary"); invalid(body);
        var filtered = new ReliabilityRequestQuery(START, END, "svc01", null, 100, 0);
        body = body(); ((ObjectNode) body.get("filters")).put("service_id", "svc01");
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () ->
                adapter.decode(result(bodyWithServiceFilter()), filtered)).code());
    }
    private ObjectNode bodyWithServiceFilter() {
        var body = body(); ((ObjectNode) body.get("filters")).put("service_id", "svc01"); return body;
    }

    @Test void rejectsAmbiguousJsonAndNeverPublishesFailureDiagnostics() {
        for (String text : List.of(envelope() + "{}", envelope().replace("\"status\":\"OK\"", "\"status\":\"OK\",\"status\":\"OK\""), "private/path")) {
            assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> adapter.decode(output(0, text), query())).code());
        }
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () ->
                adapter.decode(new BoundedProcessRunner.Result(0, new byte[]{(byte)0xff}), query())).code());
        assertEquals(ApiFailure.Code.ANALYTICS_EXECUTION_FAILED, assertThrows(ApiFailure.class, () ->
                adapter.decode(output(1, "private/path and diagnostics"), query())).code());
    }
    private void invalid(ObjectNode body) {
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> adapter.decode(result(body), query())).code());
    }
}
