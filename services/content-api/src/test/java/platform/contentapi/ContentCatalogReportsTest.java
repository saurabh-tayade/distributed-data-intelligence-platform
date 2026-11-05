package platform.contentapi;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import static org.junit.jupiter.api.Assertions.*;
import static platform.contentapi.TestSupport.*;

class ContentCatalogReportsTest {
    static final String GOLD = "b".repeat(64), REFERENCE = "c".repeat(64), ARTICLE = "d".repeat(64);
    static ContentCatalogQuery query() { return new ContentCatalogQuery(null, null, null, null, null, null, null, 100, 0); }
    static ContentAnalyticsProperties settings() {
        return new ContentAnalyticsProperties(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                Path.of("../../processing"), Path.of("target/content's output"), Path.of("target/gold"), Path.of("target/reference"), PUBLICATION);
    }
    static String envelope() {
        return """
                {"status":"OK","product_id":"content-intelligence","product_fingerprint":"%s","gold_fingerprint":"%s",
                 "reference":{"reference_id":"publishers","revision":"r1","snapshot_fingerprint":"%s"},
                 "report":"catalog","filters":{"source":null,"start_date":null,"end_date":null,"reference_status":null,
                 "publisher_category":null,"declared_language":null,"country_code":null},"limit":"100","offset":"0","has_more":false,"rows":[
                  {"article_id":"%s","source_id":"alpha","identifier_kind":"rss_guid","source_article_id":"native-id","feed_format":"rss",
                   "title":"<script>untrusted</script>","url":"javascript:alert(1)","summary":null,"published_at":null,"updated_at":null,
                   "quality_warnings":["INVALID_PUBLISHED_AT","INVALID_UPDATED_AT"],
                   "first_observed_at":"2026-10-12T23:59:59.999999Z","last_observed_at":"2026-10-13T00:00:00.000001Z",
                   "observation_count":"9007199254740993","distinct_ingestion_count":"2",
                   "discovered_at":"2026-10-12T23:59:59.999999Z","discovery_date":"2026-10-12",
                   "publisher_name":"Alpha Press","publisher_category":"science","declared_language":"en","country_code":"CA",
                   "reference_status":"MATCHED","reference_id":"publishers","reference_revision":"r1","reference_fingerprint":"%s"}]}
                """.formatted(PUBLICATION, GOLD, REFERENCE, ARTICLE, REFERENCE);
    }
    static final JsonMapper JSON = JsonMapper.builder().build();
    static ObjectNode body() { return (ObjectNode) JSON.readTree(envelope()); }
    static ObjectNode row(ObjectNode body) { return (ObjectNode) body.get("rows").get(0); }
    static BoundedProcessRunner.Result result(JsonNode body) { return output(0, JSON.writeValueAsString(body)); }
    private final ContentAnalyticsAdapter adapter = new ContentAnalyticsAdapter(settings(), new BoundedProcessRunner(properties()));

    @Test void exactProjectionAndFixedCommandPreserveNativeValues() {
        var report = adapter.decode(output(0, envelope()), query());
        assertEquals(PUBLICATION, report.publication()); assertEquals(GOLD, report.goldPublication());
        assertEquals(REFERENCE, report.reference().snapshotFingerprint());
        var row = report.data().getFirst();
        assertEquals("9007199254740993", row.observationCount()); assertEquals("2", row.distinctIngestionCount());
        assertEquals("2026-10-12T23:59:59.999999Z", row.discoveredAt()); assertNull(row.summary());
        assertEquals("javascript:alert(1)", row.url()); assertEquals("<script>untrusted</script>", row.title());
        assertEquals(List.of("INVALID_PUBLISHED_AT", "INVALID_UPDATED_AT"), row.qualityWarnings());
        var command = adapter.command(new ContentCatalogQuery("alpha", "0001-01-01", "9999-12-31", "MATCHED", "science", "en", "CA", 1, 1_000_000));
        assertEquals("catalog", command.get(command.indexOf("--report") + 1));
        assertEquals(PUBLICATION, command.get(command.indexOf("--publication") + 1));
        assertTrue(command.get(6).endsWith("content_analytics.py"));
        for (String filter : List.of("source", "start-date", "end-date", "reference-status", "publisher-category", "declared-language", "country-code")) assertTrue(command.contains("--" + filter));
        assertFalse(command.contains("--sql")); assertEquals("1000000", command.get(command.indexOf("--offset") + 1));
    }

    @Test void signedLongMaximumMatchedNullsAndMissingReferenceArePreserved() {
        var body = body(); var row = row(body);
        row.put("observation_count", "9223372036854775807"); row.put("distinct_ingestion_count", "9223372036854775807");
        for (String name : List.of("publisher_category", "declared_language", "country_code")) row.putNull(name);
        assertEquals("9223372036854775807", adapter.decode(result(body), query()).data().getFirst().observationCount());
        row.put("reference_status", "MISSING"); row.putNull("publisher_name");
        var missing = adapter.decode(result(body), query()).data().getFirst();
        assertNull(missing.publisherName()); assertEquals("publishers", missing.referenceId());
        body.putArray("rows"); body.put("offset", "1000000");
        assertTrue(adapter.decode(result(body), new ContentCatalogQuery(null,null,null,null,null,null,null,100,1_000_000)).data().isEmpty());
    }

    @Test void rejectsWrongEnvelopeIdentityFieldsPrimitivesAndFilters() {
        for (String field : List.of("status", "product_id", "product_fingerprint", "gold_fingerprint", "report")) {
            var body = body(); body.put(field, "wrong"); invalid(body);
        }
        for (String field : List.of("limit", "offset", "has_more")) { var body = body(); body.put(field, 1.5); invalid(body); }
        var body = body(); body.put("limit", 100); invalid(body);
        body = body(); body.put("offset", "00"); invalid(body);
        body = body(); body.put("has_more", true); invalid(body);
        for (String field : List.of("source", "start_date", "end_date", "reference_status", "publisher_category", "declared_language", "country_code")) {
            body = body(); ((ObjectNode)body.get("filters")).put(field, "wrong"); invalid(body);
        }
        for (String field : List.of("reference_id", "revision", "snapshot_fingerprint")) {
            body = body(); ((ObjectNode)body.get("reference")).put(field, "../private"); invalid(body);
        }
        body = body(); body.put("extra", "/private"); invalid(body);
        body = body(); body.remove("reference"); invalid(body);
        body = body(); row(body).put("source_uri", "/private"); invalid(body);
        body = body(); row(body).remove("summary"); invalid(body);
    }

    @Test void rejectsMalformedCountersTimestampsOrderIdentityAndReference() {
        for (String field : List.of("observation_count", "distinct_ingestion_count")) {
            for (String value : List.of("0", "-1", "01", "1e2", "1.0", "9223372036854775808")) {
                var body = body(); row(body).put(field, value); invalid(body);
            }
            var body = body(); row(body).put(field, 1); invalid(body);
        }
        for (String field : List.of("first_observed_at", "last_observed_at", "discovered_at", "published_at", "updated_at")) {
            for (String value : List.of("2026-02-30T00:00:00.000000Z", "0000-01-01T00:00:00.000000Z", "2026-10-12T00:00:00Z", "2026-10-12T00:00:00.000000+00:00")) {
                var body = body(); row(body).put(field, value); invalid(body);
            }
        }
        for (String field : List.of("article_id", "source_id", "identifier_kind", "feed_format", "reference_status", "reference_id", "reference_revision", "reference_fingerprint", "publisher_category", "declared_language", "country_code", "discovery_date")) {
            var body = body(); row(body).put(field, "wrong/unsafe"); invalid(body);
        }
        var body = body(); row(body).put("distinct_ingestion_count", "9007199254740994"); invalid(body);
        body = body(); row(body).put("last_observed_at", "2026-10-12T00:00:00.000000Z"); invalid(body);
        body = body(); row(body).put("discovered_at", "2026-10-12T00:00:00.000000Z"); invalid(body);
        body = body(); row(body).put("discovery_date", "2026-10-13"); invalid(body);
        body = body(); row(body).put("reference_status", "MISSING"); invalid(body);
        body = body(); row(body).putNull("publisher_name"); invalid(body);
        body = body(); row(body).put("title", ""); invalid(body);
        body = body(); row(body).put("source_article_id", ""); invalid(body);
        body = body(); row(body).put("feed_format", "atom"); invalid(body);
        body = body(); row(body).putArray("quality_warnings").add("INVALID_UPDATED_AT").add("INVALID_PUBLISHED_AT"); invalid(body);
        body = body(); row(body).putArray("quality_warnings").add("INVALID_PUBLISHED_AT").add("INVALID_PUBLISHED_AT"); invalid(body);
        body = body(); var rows = (tools.jackson.databind.node.ArrayNode)body.get("rows"); rows.add(row(body).deepCopy()); invalid(body);
        body = body(); var later = row(body).deepCopy(); later.put("article_id", "e".repeat(64)); later.put("last_observed_at", "2026-10-14T00:00:00.000000Z");
        ((tools.jackson.databind.node.ArrayNode)body.get("rows")).add(later); invalid(body);
    }

    @Test void pageBoundsRejectExtraRowsAndUnreturnedLookahead() {
        var body = body(); body.put("limit", "1");
        var next = row(body).deepCopy(); next.put("article_id", "e".repeat(64));
        ((tools.jackson.databind.node.ArrayNode) body.get("rows")).add(next);
        var query = new ContentCatalogQuery(null,null,null,null,null,null,null,1,0);
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> adapter.decode(result(body),query)).code());
        body.putArray("rows"); body.put("has_more", true);
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> adapter.decode(result(body),query)).code());
    }

    @Test void publisherNameUsesNativeScalarTextPolicy() {
        for (String name : List.of(" Alpha", "Alpha\u00a0", "Alpha\u0000Press", "Alpha\u2028Press", "Alpha\ufeffPress", "x".repeat(201))) {
            var body = body(); row(body).put("publisher_name", name); invalid(body);
        }
        var body = body(); row(body).put("publisher_name", "😀".repeat(200));
        assertEquals("😀".repeat(200), adapter.decode(result(body), query()).data().getFirst().publisherName());
    }

    @Test void matchingFilterEnvelopeCannotHideOutOfScopeRows() {
        String[][] filters = {{"source","beta"},{"start_date","2026-10-13"},{"end_date","2026-10-11"},{"reference_status","MISSING"},
                {"publisher_category","business"},{"declared_language","fr"},{"country_code","US"}};
        for (int i = 0; i < filters.length; i++) {
            var body = body(); ((ObjectNode)body.get("filters")).put(filters[i][0], filters[i][1]);
            String[] values = new String[7]; values[i] = filters[i][1];
            var query = new ContentCatalogQuery(values[0],values[1],values[2],values[3],values[4],values[5],values[6],100,0);
            assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> adapter.decode(result(body), query)).code());
        }
    }

    @Test void rejectsAmbiguousJsonUtf8AndOnlyReturnsSafeFailures() {
        for (String text : List.of(envelope()+"{}", envelope().replace("\"status\":\"OK\"", "\"status\":\"OK\",\"status\":\"OK\""),
                envelope().replace("native-id", "\\ud800"), "private/path")) {
            assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> adapter.decode(output(0,text),query())).code());
        }
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> adapter.decode(new BoundedProcessRunner.Result(0,new byte[]{(byte)0xff}),query())).code());
        assertEquals(ApiFailure.Code.ANALYTICS_EXECUTION_FAILED, assertThrows(ApiFailure.class, () -> adapter.decode(output(1,"private diagnostics"),query())).code());
    }
    private void invalid(ObjectNode body) {
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> adapter.decode(result(body),query())).code());
    }
}
