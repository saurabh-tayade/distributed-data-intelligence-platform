package platform.contentapi;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.node.ObjectNode;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static platform.contentapi.ContentCatalogReportsTest.*;
import static platform.contentapi.ContentDiscoveryReportsTest.*;
import static platform.contentapi.TestSupport.*;

class ContentDiscoveryWebTest {
    private List<String> seen;
    private ApiFailure.Code failure;
    private final ContentAnalyticsAdapter adapter = new ContentAnalyticsAdapter(settings(), new BoundedProcessRunner(properties()) {
        @Override public Result run(List<String> command, Path directory) {
            seen = command;
            if (failure != null) throw new ApiFailure(failure);
            boolean discovery = command.get(command.indexOf("--report") + 1).equals("source_discovery_daily");
            var body = discovery ? dailyBody() : coverageBody(); var filters = (ObjectNode)body.get("filters");
            for (String name : List.of("source", "start-date", "end-date")) {
                int index = command.indexOf("--" + name);
                if (index >= 0) filters.put(name.replace('-', '_'), command.get(index + 1));
            }
            for (String name : List.of("limit", "offset")) body.put(name, command.get(command.indexOf("--" + name) + 1));
            return result(body);
        }
    });
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new ContentCatalogController(adapter))
            .setControllerAdvice(new ApiExceptionHandler()).build();
    private static final List<String> ENDPOINTS = List.of("source-discovery-daily", "reference-coverage");

    @Test void fixedHttpProjectionsHaveIdentitiesExactCountsAndNoCache() throws Exception {
        for (String endpoint : ENDPOINTS) {
            mvc.perform(get("/api/v1/content/" + endpoint)).andExpect(status().isOk())
                    .andExpect(header().string("Cache-Control", "no-store"))
                    .andExpect(jsonPath("$.publication").value(PUBLICATION)).andExpect(jsonPath("$.goldPublication").value(GOLD))
                    .andExpect(jsonPath("$.reference.snapshotFingerprint").value(REFERENCE))
                    .andExpect(jsonPath("$.meta.limit").value(100)).andExpect(jsonPath("$.meta.offset").value(0))
                    .andExpect(jsonPath("$.root").doesNotExist()).andExpect(jsonPath("$.report").doesNotExist());
            assertEquals(endpoint.replace('-', '_'), seen.get(seen.indexOf("--report") + 1));
        }
        mvc.perform(get("/api/v1/content/source-discovery-daily")).andExpect(jsonPath("$.data[0].discoveredArticleCount").value("9007199254740993"))
                .andExpect(jsonPath("$.data[0].discoveryDate").value("2026-10-12")).andExpect(jsonPath("$.totals").doesNotExist());
        mvc.perform(get("/api/v1/content/reference-coverage")).andExpect(jsonPath("$.totals.articleCount").value("9007199254740994"))
                .andExpect(jsonPath("$.data[1].referenceUsage").value("UNUSED"))
                .andExpect(jsonPath("$.data[2].referenceStatus").value("MISSING"))
                .andExpect(jsonPath("$.data[0].missingOptionalAttributes[0]").value("publisher_category"))
                .andExpect(jsonPath("$.filters.from").doesNotExist());
    }

    @Test void supportedInclusiveDatesAndSourceArePassedExactly() throws Exception {
        mvc.perform(get("/api/v1/content/source-discovery-daily").param("sourceId", "alpha").param("from", "2026-10-12")
                .param("to", "2026-10-12").param("limit", "1")).andExpect(status().isOk())
                .andExpect(jsonPath("$.filters.sourceId").value("alpha")).andExpect(jsonPath("$.filters.from").value("2026-10-12"))
                .andExpect(jsonPath("$.filters.to").value("2026-10-12"));
        assertEquals("alpha", seen.get(seen.indexOf("--source") + 1));
        assertEquals("2026-10-12", seen.get(seen.indexOf("--start-date") + 1));
        assertEquals("2026-10-12", seen.get(seen.indexOf("--end-date") + 1));
    }

    @Test void allUnsupportedMalformedDuplicateBlankAndBoundaryInputsFailBeforeChild() throws Exception {
        for (String endpoint : ENDPOINTS) {
            for (String[] parameter : List.of(new String[]{"sourceId", "UPPER"}, new String[]{"sourceId", " alpha"},
                    new String[]{"sourceId", "a' OR 1=1"}, new String[]{"sourceId", "../private"},
                    new String[]{"limit", "0"}, new String[]{"limit", "1001"}, new String[]{"limit", "01"},
                    new String[]{"offset", "-1"}, new String[]{"offset", "1000001"}, new String[]{"offset", "00"},
                    new String[]{"publication", PUBLICATION}, new String[]{"root", "/private"}, new String[]{"report", "catalog"},
                    new String[]{"sql", "SELECT"}, new String[]{"sort", "source_id"}, new String[]{"referenceStatus", "MATCHED"},
                    new String[]{"publisherCategory", "science"}, new String[]{"declaredLanguage", "en"}, new String[]{"countryCode", "CA"})) {
                seen = null;
                mvc.perform(get("/api/v1/content/" + endpoint).param(parameter[0], parameter[1])).andExpect(status().isBadRequest())
                        .andExpect(content().json("{\"code\":\"INVALID_ARGUMENT\"}"));
                assertNull(seen);
            }
            for (String field : List.of("sourceId", "from", "to", "limit", "offset")) {
                seen = null;
                mvc.perform(get("/api/v1/content/" + endpoint).param(field, "")).andExpect(status().isBadRequest());
                mvc.perform(get("/api/v1/content/" + endpoint).param(field, "1", "2")).andExpect(status().isBadRequest());
                assertNull(seen);
            }
        }
        for (String field : List.of("from", "to")) {
            seen = null;
            mvc.perform(get("/api/v1/content/reference-coverage").param(field, "2026-10-12")).andExpect(status().isBadRequest());
            for (String date : List.of("0000-01-01", "2026-02-30", "2026-1-01", "10000-01-01", "2026-10-12T00:00:00Z"))
                mvc.perform(get("/api/v1/content/source-discovery-daily").param(field, date)).andExpect(status().isBadRequest());
            assertNull(seen);
        }
        seen = null;
        mvc.perform(get("/api/v1/content/source-discovery-daily").param("from", "2026-10-13").param("to", "2026-10-12"))
                .andExpect(status().isBadRequest());
        assertNull(seen);
    }

    @Test void bothReportFailuresReturnOnlySafeCodes() throws Exception {
        for (String endpoint : ENDPOINTS) for (ApiFailure.Code code : ApiFailure.Code.values()) {
            failure = code;
            mvc.perform(get("/api/v1/content/" + endpoint)).andExpect(status().is(code.status()))
                    .andExpect(content().string("{\"code\":\"" + code.name() + "\"}"));
        }
    }
}
