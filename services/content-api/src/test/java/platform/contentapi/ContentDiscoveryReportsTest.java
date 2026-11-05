package platform.contentapi;

import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.node.ArrayNode;
import static org.junit.jupiter.api.Assertions.*;
import static platform.contentapi.ContentCatalogReportsTest.*;
import static platform.contentapi.TestSupport.*;
import static platform.contentapi.ContentReportQuery.Report.*;

class ContentDiscoveryReportsTest {
    static ContentReportQuery daily() { return new ContentReportQuery(DISCOVERY, null, null, null, 100, 0); }
    static ContentReportQuery coverage() { return new ContentReportQuery(COVERAGE, null, null, null, 100, 0); }
    static ObjectNode dailyBody() {
        var value = body(); value.put("report", "source_discovery_daily");
        value.putObject("filters").putNull("source").putNull("start_date").putNull("end_date");
        value.putArray("rows").addObject().put("source_id", "alpha").put("discovery_date", "2026-10-12")
                .put("discovered_article_count", "9007199254740993").put("articles_missing_reference_count", "0");
        return value;
    }
    static ObjectNode coverageBody() {
        var value = body(); value.put("report", "reference_coverage"); value.putObject("filters").putNull("source");
        var rows = value.putArray("rows");
        rows.add(coverageRow("alpha", "USED", "9007199254740993"));
        rows.add(coverageRow("beta", "UNUSED", "0"));
        rows.add(coverageRow("gamma", "ABSENT", "1"));
        value.putObject("totals").put("article_count", "9007199254740994").put("matched_article_count", "9007199254740993")
                .put("missing_article_count", "1").put("catalog_source_count", "2").put("matched_source_count", "1")
                .put("missing_source_count", "1").put("matched_sources_missing_optional_attributes_count", "1")
                .put("reference_source_count", "2").put("unused_reference_source_count", "1");
        return value;
    }
    private static ObjectNode coverageRow(String source, String usage, String count) {
        var row = JSON.createObjectNode().put("source_id", source).put("reference_status", usage.equals("ABSENT") ? "MISSING" : "MATCHED")
                .put("reference_usage", usage).put("article_count", count).put("articles_missing_reference_count", usage.equals("ABSENT") ? count : "0")
                .putNull("publisher_category").putNull("declared_language").putNull("country_code");
        if (usage.equals("ABSENT")) row.putNull("publisher_name"); else row.put("publisher_name", "<script>untrusted</script>");
        var missing = row.putArray("missing_optional_attributes");
        if (!usage.equals("ABSENT")) for (String field : List.of("publisher_category", "declared_language", "country_code")) missing.add(field);
        return row;
    }
    private final ContentAnalyticsAdapter adapter = new ContentAnalyticsAdapter(settings(), new BoundedProcessRunner(properties()));

    @Test void projectsExactCountsDatesWholeScopeTotalsAndFixedCommands() {
        var daily = adapter.decodeDiscovery(result(dailyBody()), daily());
        assertEquals("9007199254740993", daily.data().getFirst().discoveredArticleCount());
        assertEquals("2026-10-12", daily.data().getFirst().discoveryDate());
        var coverage = adapter.decodeCoverage(result(coverageBody()), coverage());
        assertEquals("9007199254740994", coverage.totals().articleCount());
        assertEquals(List.of("USED", "UNUSED", "ABSENT"), coverage.data().stream().map(ContentCoverageResponse.Row::referenceUsage).toList());
        assertEquals("<script>untrusted</script>", coverage.data().getFirst().publisherName());
        var page = coverageBody(); var all = page.get("rows").deepCopy(); page.putArray("rows").add(all.get(1));
        page.put("offset", "1").put("limit", "1").put("has_more", true);
        var middle = adapter.decodeCoverage(result(page), new ContentReportQuery(COVERAGE, null, null, null, 1, 1));
        assertEquals(coverage.totals(), middle.totals()); assertEquals("UNUSED", middle.data().getFirst().referenceUsage());
        page.putArray("rows"); page.put("offset", "1000000").put("has_more", false);
        assertEquals(coverage.totals(), adapter.decodeCoverage(result(page), new ContentReportQuery(COVERAGE, null, null, null, 1, 1_000_000)).totals());
        for (var report : List.of(DISCOVERY, COVERAGE)) {
            var command = adapter.command(new ContentReportQuery(report, "alpha", null, null, 1000, 1_000_000));
            assertEquals(report.nativeName(), command.get(command.indexOf("--report") + 1));
            assertEquals(PUBLICATION, command.get(command.indexOf("--publication") + 1));
            assertFalse(command.contains("--sql")); assertFalse(command.contains("--reference-status"));
        }
    }

    @Test void signedLongMaximumSurvivesWhileNumericOrOverflowTotalsFailClosed() {
        var dailyBody = dailyBody(); row(dailyBody).put("discovered_article_count", "9223372036854775807");
        assertEquals("9223372036854775807", adapter.decodeDiscovery(result(dailyBody), daily()).data().getFirst().discoveredArticleCount());
        var body = coverageBody(); row(body).put("article_count", "9223372036854775806");
        var totals = (ObjectNode)body.get("totals");
        totals.put("article_count", "9223372036854775807").put("matched_article_count", "9223372036854775806");
        assertEquals("9223372036854775807", adapter.decodeCoverage(result(body), coverage()).totals().articleCount());
        totals.put("matched_article_count", "9223372036854775807"); invalid(body, coverage());
        for (String key : List.of("article_count", "matched_article_count", "missing_article_count", "catalog_source_count",
                "matched_source_count", "missing_source_count", "matched_sources_missing_optional_attributes_count",
                "reference_source_count", "unused_reference_source_count")) {
            var numeric = coverageBody(); ((ObjectNode)numeric.get("totals")).put(key, 1); invalid(numeric, coverage());
        }
    }

    @Test void dailyRejectsWrongFieldsCountsSortDatesAndScope() {
        for (Consumer<ObjectNode> change : List.<Consumer<ObjectNode>>of(
                b -> b.put("totals", "unexpected"), b -> b.put("report", "reference_coverage"),
                b -> row(b).put("extra", "private"), b -> row(b).remove("discovery_date"),
                b -> row(b).put("discovery_date", "0000-01-01"), b -> row(b).put("discovery_date", "2026-02-30"),
                b -> row(b).put("source_id", "UPPER"), b -> row(b).put("discovered_article_count", "0"),
                b -> row(b).put("articles_missing_reference_count", "9007199254740994"),
                b -> row(b).put("articles_missing_reference_count", "1"),
                b -> { var next = row(b).deepCopy().put("discovery_date", "2026-10-13").put("articles_missing_reference_count", "9007199254740993"); ((ArrayNode)b.get("rows")).add(next); },
                b -> ((ArrayNode)b.get("rows")).add(row(b).deepCopy()),
                b -> { var next = row(b).deepCopy().put("discovery_date", "2026-10-11"); ((ArrayNode)b.get("rows")).add(next); },
                b -> { var next = row(b).deepCopy().put("source_id", "a"); ((ArrayNode)b.get("rows")).add(next); })) {
            var body = dailyBody(); change.accept(body); invalid(body, daily());
        }
        for (String[] filter : List.of(new String[]{"source","beta"}, new String[]{"start_date","2026-10-13"}, new String[]{"end_date","2026-10-11"})) {
            var body = dailyBody(); ((ObjectNode)body.get("filters")).put(filter[0], filter[1]);
            var query = new ContentReportQuery(DISCOVERY, filter[0].equals("source") ? filter[1] : null,
                    filter[0].equals("start_date") ? filter[1] : null, filter[0].equals("end_date") ? filter[1] : null, 100, 0);
            invalid(body, query);
        }
        for (String date : List.of("0001-01-01", "1969-12-31", "9999-12-31")) {
            var body = dailyBody(); row(body).put("discovery_date", date);
            assertEquals(date, adapter.decodeDiscovery(result(body), daily()).data().getFirst().discoveryDate());
        }
    }

    @Test void coverageRejectsUsageNullsOptionalAttributeOrderAndWholeScopeArithmetic() {
        for (Consumer<ObjectNode> change : List.<Consumer<ObjectNode>>of(
                b -> row(b).put("reference_usage", "UNUSED"), b -> row(b).put("reference_status", "MISSING"),
                b -> row(b).putNull("publisher_name"), b -> row(b).put("publisher_name", " whitespace "),
                b -> row(b).put("publisher_category", "unknown"), b -> row(b).put("article_count", "0"),
                b -> row(b).put("articles_missing_reference_count", "1"), b -> row(b).putArray("missing_optional_attributes"),
                b -> row(b).putArray("missing_optional_attributes").add("country_code").add("declared_language").add("publisher_category"),
                b -> ((ObjectNode)b.get("rows").get(2)).put("publisher_name", "unexpected"),
                b -> ((ObjectNode)b.get("rows").get(2)).putArray("missing_optional_attributes").add("country_code"),
                b -> ((ObjectNode)b.get("rows").get(1)).put("source_id", "alpha"),
                b -> ((ObjectNode)b.get("rows").get(1)).put("source_id", "z"),
                b -> b.put("has_more", true), b -> ((ObjectNode)b.get("totals")).put("article_count", "9007199254740993"),
                b -> ((ObjectNode)b.get("totals")).put("catalog_source_count", "1"),
                b -> ((ObjectNode)b.get("totals")).put("reference_source_count", "1"),
                b -> ((ObjectNode)b.get("totals")).put("matched_sources_missing_optional_attributes_count", "2"),
                b -> ((ObjectNode)b.get("totals")).put("matched_sources_missing_optional_attributes_count", "0"),
                b -> ((ObjectNode)b.get("totals")).put("extra", "0"), b -> b.remove("totals"))) {
            var body = coverageBody(); change.accept(body); invalid(body, coverage());
        }
        var body = coverageBody(); ((ObjectNode)body.get("filters")).put("source", "alpha");
        invalid(body, new ContentReportQuery(COVERAGE, "alpha", null, null, 100, 0));
    }

    @Test void eachNewReportRejectsAmbiguousIdentityFiltersNumbersAndPageEnvelopes() {
        for (var query : List.of(daily(), coverage())) {
            for (String field : List.of("status", "product_id", "product_fingerprint", "gold_fingerprint", "report")) {
                var body = query.report() == DISCOVERY ? dailyBody() : coverageBody(); body.put(field, "wrong"); invalid(body, query);
            }
            for (String value : List.of("-1", "01", "1e2", "1.0", "9223372036854775808")) {
                var body = query.report() == DISCOVERY ? dailyBody() : coverageBody();
                row(body).put(query.report() == DISCOVERY ? "discovered_article_count" : "article_count", value); invalid(body, query);
            }
            for (Consumer<ObjectNode> change : List.<Consumer<ObjectNode>>of(b -> b.put("limit", 100), b -> b.put("offset", "00"),
                    b -> b.put("limit", "1"), b -> b.put("has_more", "false"), b -> b.putNull("rows"),
                    b -> ((ObjectNode)b.get("reference")).put("revision", "../private"),
                    b -> ((ObjectNode)b.get("filters")).put("source", "wrong"), b -> b.put("private", "/private"),
                    b -> { b.putArray("rows"); b.put("has_more", true); })) {
                var body = query.report() == DISCOVERY ? dailyBody() : coverageBody(); change.accept(body); invalid(body, query);
            }
        }
    }

    @Test void invalidNativeOutputAndFailuresNeverBecomePartialData() {
        for (var query : List.of(daily(), coverage())) {
            String valid = JSON.writeValueAsString(query.report() == DISCOVERY ? dailyBody() : coverageBody());
            for (String text : List.of(valid + "{}", valid.replace("\"status\":\"OK\"", "\"status\":\"OK\",\"status\":\"OK\""),
                    valid.replace("alpha", "\\ud800"), "private/path")) {
                assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> decode(output(0, text), query)).code());
            }
            assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class,
                    () -> decode(new BoundedProcessRunner.Result(0, new byte[]{(byte)0xff}), query)).code());
            assertEquals(ApiFailure.Code.ANALYTICS_EXECUTION_FAILED, assertThrows(ApiFailure.class,
                    () -> decode(output(1, "private/path"), query)).code());
        }
    }
    private Object decode(BoundedProcessRunner.Result result, ContentReportQuery query) {
        return query.report() == DISCOVERY ? adapter.decodeDiscovery(result, query) : adapter.decodeCoverage(result, query);
    }
    private void invalid(ObjectNode body, ContentReportQuery query) {
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> decode(result(body), query)).code());
    }
}
