package platform.contentapi;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static platform.contentapi.TestSupport.*;

class AnalyticsWebTest {
    private List<String> seen;
    private ApiFailure.Code failure;
    private boolean empty;
    private boolean largeQuality;
    private final GoldAnalyticsAdapter adapter = new GoldAnalyticsAdapter(properties(), new BoundedProcessRunner(properties()) {
        @Override public Result run(List<String> command, Path directory) {
            seen = command;
            assertEquals(properties().script().toAbsolutePath().getParent(), directory);
            if (failure != null) throw new ApiFailure(failure);
            String report = command.get(command.indexOf("--report") + 1);
            String row = switch (report) {
                case "articles" -> AnalyticsReportsTest.article();
                case "daily" -> AnalyticsReportsTest.daily();
                case "quality" -> AnalyticsReportsTest.quality();
                default -> throw new AssertionError("Unexpected report");
            };
            if (largeQuality && report.equals("quality")) row = row.replace("\"observation_count\":2", "\"observation_count\":9223372036854775807")
                    .replace("\"quality_warning_count\":4", "\"quality_warning_count\":9007199254740993")
                    .replace("2.0", "0.0009765625");
            String body = AnalyticsReportsTest.envelope(report, empty ? "[]" : "[" + row + "]")
                    .replace("\"limit\":100", "\"limit\":" + command.get(command.indexOf("--limit") + 1));
            if (report.equals("articles")) body = body.replace("\"offset\":0", "\"offset\":" + command.get(command.indexOf("--offset") + 1));
            return output(0, body);
        }
    });
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new AnalyticsController(new AnalyticsService(adapter)))
            .setControllerAdvice(new ApiExceptionHandler()).build();

    @Test
    void articleMappingDefaultPaginationAndNoInternalProvenance() throws Exception {
        mvc.perform(get("/api/v1/articles")).andExpect(status().isOk())
                .andExpect(jsonPath("$.publication").value(PUBLICATION))
                .andExpect(jsonPath("$.meta.returned").value(1)).andExpect(jsonPath("$.meta.limit").value(100))
                .andExpect(jsonPath("$.meta.offset").value(0)).andExpect(jsonPath("$.meta.hasMore").value(false))
                .andExpect(jsonPath("$.data[0].articleId").value("1".repeat(64)))
                .andExpect(jsonPath("$.data[0].title").value("<script>untrusted</script>"))
                .andExpect(jsonPath("$.data[0].url").value("javascript:untrusted()"))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("\"summary\":null")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("\"publishedAt\":null")))
                .andExpect(jsonPath("$.data[0].updatedAt").value(TIMESTAMP))
                .andExpect(jsonPath("$.data[0].firstObservedAt").value(TIMESTAMP))
                .andExpect(jsonPath("$.data[0].lastObservedAt").value(TIMESTAMP))
                .andExpect(jsonPath("$.data[0].observationCount").value("9223372036854775807"))
                .andExpect(jsonPath("$.data[0].distinctIngestionCount").value("9007199254740993"))
                .andExpect(jsonPath("$.data[0].qualityWarnings[1]").value("INVALID_UPDATED_AT"))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("private"))))
                .andExpect(jsonPath("$.data[0].selectedInputUri").doesNotExist());
        assertEquals(PUBLICATION, seen.get(seen.indexOf("--publication") + 1));
    }

    @Test
    void dailyAndQualityExactMappingAndDates() throws Exception {
        mvc.perform(get("/api/v1/daily").param("sourceId", "alpha").param("from", "2026-10-12").param("to", "2026-10-12"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.publication").value(PUBLICATION))
                .andExpect(jsonPath("$.data[0].ingestionDate").value("2026-10-12"))
                .andExpect(jsonPath("$.data[0].observationCount").value("9223372036854775807"))
                .andExpect(jsonPath("$.data[0].distinctArticleCount").value("9007199254740993"))
                .andExpect(jsonPath("$.data[0].distinctIngestionCount").value("1"))
                .andExpect(jsonPath("$.data[0].qualityWarningCount").value("0"))
                .andExpect(jsonPath("$.data[0].latestIngestedAt").value(TIMESTAMP));
        assertEquals("2026-10-12", seen.get(seen.indexOf("--start-date") + 1));
        assertEquals("2026-10-12", seen.get(seen.indexOf("--end-date") + 1));
        mvc.perform(get("/api/v1/quality").param("sourceId", "alpha").param("from", "2026-10-12"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.publication").value(PUBLICATION))
                .andExpect(jsonPath("$.data[0].sourceId").value("alpha"))
                .andExpect(jsonPath("$.data[0].observationCount").value("2"))
                .andExpect(jsonPath("$.data[0].qualityWarningCount").value("4"))
                .andExpect(jsonPath("$.data[0].warningCodesPerObservation").value(2.0));
        assertFalse(seen.contains("--end-date"));
        mvc.perform(get("/api/v1/quality").param("to", "2026-10-12")).andExpect(status().isOk());
        assertFalse(seen.contains("--start-date"));
    }

    @Test
    void qualityCountsNeverPassThroughFloatingPoint() throws Exception {
        largeQuality = true;
        mvc.perform(get("/api/v1/quality")).andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].observationCount").value("9223372036854775807"))
                .andExpect(jsonPath("$.data[0].qualityWarningCount").value("9007199254740993"));
    }

    @Test
    void explicitArticleOffsetAndEmptyPages() throws Exception {
        mvc.perform(get("/api/v1/articles").param("sourceId", "alpha").param("limit", "1").param("offset", "2"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.meta.offset").value(2))
                .andExpect(jsonPath("$.meta.limit").value(1));
        assertEquals("alpha", seen.get(seen.indexOf("--source") + 1));
        empty = true;
        for (String report : List.of("articles", "daily", "quality")) {
            mvc.perform(get("/api/v1/" + report)).andExpect(status().isOk())
                    .andExpect(jsonPath("$.data").isArray()).andExpect(jsonPath("$.data").isEmpty())
                    .andExpect(jsonPath("$.meta.returned").value(0));
        }
        mvc.perform(get("/api/v1/articles").param("offset", "1000000")).andExpect(status().isOk())
                .andExpect(jsonPath("$.meta.hasMore").value(false));
    }

    @Test
    void invalidParametersNeverStartProcess() throws Exception {
        for (String report : List.of("articles", "daily", "quality")) {
            for (String limit : List.of("0", "-1", "1001", "9999999999999", "1.0", "", " 1", "+1")) {
                invalid(report, "limit", limit);
            }
            for (String source : List.of("", " ", "UPPER", "--report", "a\nb", "a".repeat(129), "alpha' OR 1=1 --", "../gold", "$(cmd)")) {
                invalid(report, "sourceId", source);
            }
            for (String name : List.of("publication", "goldRoot", "sql", "report", "path")) invalid(report, name, "untrusted");
            for (String name : List.of("sourceId", "limit")) invalid(report, name, "1", "2");
        }
        for (String offset : List.of("-1", "1000001", "999999999999999999", "1.0", "", "bad", "+1", " 1", "1_0")) invalid("articles", "offset", offset);
        invalid("articles", "offset", "0", "1");
        invalid("articles", "from", "2026-10-12");
        for (String report : List.of("daily", "quality")) {
            invalid(report, "offset", "0");
            for (String name : List.of("from", "to")) {
                for (String date : List.of("", "2026-02-30", "2026-1-01", "20261012", "0000-01-01", "10000-01-01", "2026-10-12T00:00:00Z")) invalid(report, name, date);
                invalid(report, name, "2026-10-12", "2026-10-13");
            }
            mvc.perform(get("/api/v1/" + report).param("from", "2026-10-13").param("to", "2026-10-12"))
                    .andExpect(status().isBadRequest());
        }
        assertNull(seen);
    }
    private void invalid(String report, String key, String... values) throws Exception {
        mvc.perform(get("/api/v1/" + report).param(key, values)).andExpect(status().isBadRequest())
                .andExpect(content().json("{\"code\":\"INVALID_ARGUMENT\"}"));
        assertNull(seen);
    }

    @ParameterizedTest @EnumSource(ApiFailure.Code.class)
    void existingErrorMappingsApplyToEveryEndpoint(ApiFailure.Code code) throws Exception {
        failure = code;
        for (String report : List.of("articles", "daily", "quality")) {
            mvc.perform(get("/api/v1/" + report)).andExpect(status().is(code.status()))
                    .andExpect(content().json("{\"code\":\"" + code.name() + "\"}"));
        }
    }
}
