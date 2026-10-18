package platform.contentapi;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;
import static platform.contentapi.TestSupport.*;
import static platform.contentapi.ApiFailure.Code.*;

class AnalyticsReportsTest {
    private final GoldAnalyticsAdapter adapter = adapter();
    static String article() {
        return """
            {"contract_version":1,"article_id":"%s","source_id":"alpha","identifier_kind":"id",
            "source_article_id":"publisher-id","source_uri":"https://example.test/feed","feed_format":"atom",
            "title":"<script>untrusted</script>","url":"javascript:untrusted()","summary":null,
            "published_at":null,"updated_at":"%s","quality_warnings":["INVALID_PUBLISHED_AT","INVALID_UPDATED_AT"],
            "first_observed_at":"%s","last_observed_at":"%s","observation_count":9223372036854775807,
            "distinct_ingestion_count":9007199254740993,"selected_silver_contract_version":1,
            "selected_ingestion_id":"uuid","selected_ingested_at":"%s","selected_entry_index":0,
            "selected_input_uri":"/private/input","selected_payload_sha256":"digest","selected_payload_byte_length":1,
            "selected_bronze_bucket":"private-bucket","selected_bronze_payload_key":"private-key",
            "selected_bronze_metadata_key":"private-meta"}
            """.formatted("1".repeat(64), TIMESTAMP, TIMESTAMP, TIMESTAMP, TIMESTAMP);
    }
    static String daily() {
        return """
            {"source_id":"alpha","ingestion_date":"2026-10-12","observation_count":9223372036854775807,
             "distinct_article_count":9007199254740993,"distinct_ingestion_count":1,
             "quality_warning_count":0,"latest_ingested_at":"%s"}
            """.formatted(TIMESTAMP);
    }
    static String quality() {
        return "{\"source_id\":\"alpha\",\"observation_count\":2,\"quality_warning_count\":4,\"warning_codes_per_observation\":2.0}";
    }
    static String envelope(String report, String rows) {
        return "{\"status\":\"OK\",\"publication\":\"" + PUBLICATION + "\",\"report\":\"" + report
                + "\",\"limit\":100,\"rows\":" + rows + (report.equals("articles") ? ",\"offset\":0,\"has_more\":false" : "") + "}";
    }
    static DateQuery dates() { return new DateQuery(null, 100, null, null); }
    private void decode(String report, String value) {
        switch (report) {
            case "articles" -> adapter.decodeArticles(output(0, value), new ArticleQuery(null, 100, 0));
            case "daily" -> adapter.decodeDaily(output(0, value), dates());
            case "quality" -> adapter.decodeQuality(output(0, value), dates());
            default -> fail();
        }
    }

    @Test
    void typedRowsPreserveNullsMicrosecondsCountsWarningsAndRates() {
        var article = adapter.decodeArticles(output(0, envelope("articles", "[" + article() + "]")), new ArticleQuery(null, 100, 0)).rows().getFirst();
        assertNull(article.summary());
        assertNull(article.publishedAt());
        assertEquals(TIMESTAMP, article.updatedAt());
        assertEquals(Long.MAX_VALUE, article.observationCount());
        assertEquals(9007199254740993L, article.distinctIngestionCount());
        assertEquals(List.of("INVALID_PUBLISHED_AT", "INVALID_UPDATED_AT"), article.qualityWarnings());
        assertEquals("javascript:untrusted()", article.url());
        var day = adapter.decodeDaily(output(0, envelope("daily", "[" + daily() + "]")), dates()).rows().getFirst();
        assertEquals(Long.MAX_VALUE, day.observationCount());
        assertEquals(9007199254740993L, day.distinctArticleCount());
        assertEquals(TIMESTAMP, day.latestIngestedAt());
        var quality = adapter.decodeQuality(output(0, envelope("quality", "[" + quality() + "]")), dates()).rows().getFirst();
        assertEquals(2.0, quality.warningCodesPerObservation());
        for (String report : List.of("articles", "daily", "quality")) decode(report, envelope(report, "[]"));
    }

    @Test
    void preservesAllNullableArticleFieldsAndEmptyWarningList() {
        String value = article().replace("\"url\":\"javascript:untrusted()\"", "\"url\":null")
                .replace("\"updated_at\":\"" + TIMESTAMP + "\"", "\"updated_at\":null")
                .replace("[\"INVALID_PUBLISHED_AT\",\"INVALID_UPDATED_AT\"]", "[]");
        var row = adapter.decodeArticles(output(0, envelope("articles", "[" + value + "]")), new ArticleQuery(null, 100, 0)).rows().getFirst();
        assertNull(row.url());
        assertNull(row.summary());
        assertNull(row.publishedAt());
        assertNull(row.updatedAt());
        assertTrue(row.qualityWarnings().isEmpty());
    }

    @Test
    void validatesDailyAndQualityOrderAndObservationBounds() {
        String laterDay = daily().replace("2026-10-12", "2026-10-13");
        decode("daily", envelope("daily", "[" + daily() + "," + laterDay + "]"));
        assertThrows(ApiFailure.class, () -> decode("daily", envelope("daily", "[" + laterDay + "," + daily() + "]")));
        for (var pair : List.of(List.of("daily", daily()), List.of("quality", quality()))) {
            String first = pair.get(1);
            String last = first.replace("alpha", "zeta");
            decode(pair.get(0), envelope(pair.get(0), "[" + first + "," + last + "]"));
            assertThrows(ApiFailure.class, () -> decode(pair.get(0), envelope(pair.get(0), "[" + last + "," + first + "]")));
        }
        String backwards = article().replace("\"first_observed_at\":\"2026-10-12", "\"first_observed_at\":\"2026-10-13");
        assertThrows(ApiFailure.class, () -> decode("articles", envelope("articles", "[" + backwards + "]")));
        String excessiveDistinct = article().replace("\"observation_count\":9223372036854775807", "\"observation_count\":1");
        assertThrows(ApiFailure.class, () -> decode("articles", envelope("articles", "[" + excessiveDistinct + "]")));
    }

    static Stream<String> invalidArticleRows() {
        String value = article();
        return Stream.of(value.replace("1".repeat(64), "not-an-id"), value.replace("<script>untrusted</script>", " "),
                value.replace("9223372036854775807", "0"), value.replace("9007199254740993", "9223372036854775808"),
                value.replace("9007199254740993", "1.0"), value.replace("9007199254740993", "\"1\""),
                value.replace("\"summary\":null", "\"summary\":false"), value.replace("\"url\":\"javascript:untrusted()\"", "\"url\":4"),
                value.replace("\"first_observed_at\":\"" + TIMESTAMP + "\"", "\"first_observed_at\":null"),
                value.replace("\"last_observed_at\":\"" + TIMESTAMP + "\"", "\"last_observed_at\":null"),
                value.replace(TIMESTAMP, "2026-02-30T10:20:30.123456Z"), value.replace(".123456Z", ".123Z"),
                value.replace("INVALID_PUBLISHED_AT", "UNKNOWN"), value.replace("INVALID_PUBLISHED_AT", "INVALID_UPDATED_AT"),
                value.replace("[\"INVALID_PUBLISHED_AT\",\"INVALID_UPDATED_AT\"]", "null"),
                value.replace("[\"INVALID_PUBLISHED_AT\",\"INVALID_UPDATED_AT\"]", "[null]"),
                value.replace("\"title\":", "\"unexpected\":1,\"title\":"));
    }
    @ParameterizedTest @MethodSource("invalidArticleRows")
    void rejectsInvalidArticleFields(String row) {
        assertEquals(ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class,
                () -> decode("articles", envelope("articles", "[" + row + "]"))).code());
    }

    @Test
    void rejectsBadEnvelopesForEveryNewReport() {
        for (String report : List.of("articles", "daily", "quality")) {
            String valid = envelope(report, "[]");
            for (String bad : List.of("{", "null", valid + " {}", valid.replace(PUBLICATION, "b".repeat(64)),
                    valid.replace(report, "publishers"), valid.replace("\"limit\":100", "\"limit\":99"),
                    valid.replace("\"OK\"", "\"FAILED\""), valid.replace("[]", "null"),
                    valid.replace("\"rows\":", "\"extra\":0,\"rows\":"))) {
                assertEquals(ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> decode(report, bad)).code());
            }
        }
        String page = envelope("articles", "[]");
        for (String bad : List.of(page.replace("false", "true"), page.replace("false", "0"),
                page.replace("\"offset\":0", "\"offset\":1"), page.replace("\"offset\":0", "\"offset\":0.0"),
                page.replace(",\"offset\":0,\"has_more\":false", ""))) {
            assertThrows(ApiFailure.class, () -> decode("articles", bad));
        }
    }

    @Test
    void rejectsBadDateRateOrderingAndFilters() {
        for (String row : List.of(daily().replace("2026-10-12", "2026-02-30"), daily().replace("\"quality_warning_count\":0", "\"quality_warning_count\":-1"),
                daily().replace("\"latest_ingested_at\":\"" + TIMESTAMP + "\"", "\"latest_ingested_at\":null"),
                daily().replace("\"distinct_ingestion_count\":1", "\"distinct_ingestion_count\":0"))) {
            assertThrows(ApiFailure.class, () -> decode("daily", envelope("daily", "[" + row + "]")));
        }
        for (String rate : List.of("-0.1", "1e999", "null", "\"2.0\"", "true")) {
            assertThrows(ApiFailure.class, () -> decode("quality", envelope("quality", "[" + quality().replace("2.0", rate) + "]")));
        }
        for (var pair : List.of(List.of("articles", article()), List.of("daily", daily()), List.of("quality", quality()))) {
            assertThrows(ApiFailure.class, () -> decode(pair.get(0), envelope(pair.get(0), "[" + pair.get(1) + "," + pair.get(1) + "]")));
        }
        var outside = new DateQuery("alpha", 100, LocalDate.of(2026, 10, 13), null);
        assertThrows(ApiFailure.class, () -> adapter.decodeDaily(output(0, envelope("daily", "[" + daily() + "]")), outside));
        assertThrows(ApiFailure.class, () -> adapter.decodeArticles(output(0, envelope("articles", "[" + article() + "]")), new ArticleQuery("beta", 100, 0)));
        assertThrows(ApiFailure.class, () -> adapter.decodeQuality(output(0, envelope("quality", "[" + quality() + "]")), new DateQuery("beta", 100, null, null)));
        String newer = article().replace("1".repeat(64), "2".repeat(64)).replace("2026-10-12", "2026-10-13");
        assertThrows(ApiFailure.class, () -> decode("articles", envelope("articles", "[" + article() + "," + newer + "]")));
        decode("articles", envelope("articles", "[" + newer + "," + article() + "]"));
        String tie = article().replace("1".repeat(64), "2".repeat(64));
        decode("articles", envelope("articles", "[" + article() + "," + tie + "]"));
        assertThrows(ApiFailure.class, () -> decode("articles", envelope("articles", "[" + tie + "," + article() + "]")));
    }

    @Test
    void constructsFixedArgumentsAndDateBounds() {
        var args = adapter.articleCommand(new ArticleQuery("alpha", 1000, ArticleQuery.MAX_OFFSET));
        assertEquals(List.of("--report", "articles", "--limit", "1000", "--source", "alpha", "--offset", "1000000"), args.subList(args.indexOf("--report"), args.size()));
        var dates = new DateQuery("alpha", 20, LocalDate.of(2026, 10, 12), LocalDate.of(2026, 10, 13));
        for (var report : List.of(GoldAnalyticsAdapter.Report.DAILY, GoldAnalyticsAdapter.Report.QUALITY)) {
            args = adapter.dateCommand(report, dates);
            assertEquals(List.of("--report", report.cliName(), "--limit", "20", "--source", "alpha", "--start-date", "2026-10-12", "--end-date", "2026-10-13"), args.subList(args.indexOf("--report"), args.size()));
            assertFalse(adapter.dateCommand(report, dates()).contains("--start-date"));
        }
    }
}
