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
import static platform.contentapi.ReliabilityReportsTest.*;
import static platform.contentapi.TestSupport.*;

class ReliabilityWebTest {
    private List<String> seen;
    private ApiFailure.Code failure;
    private final ReliabilityAnalyticsAdapter adapter = new ReliabilityAnalyticsAdapter(settings(), new BoundedProcessRunner(properties()) {
        @Override public Result run(List<String> command, Path directory) {
            seen = command;
            if (failure != null) throw new ApiFailure(failure);
            var body = body();
            ObjectNode filters = (ObjectNode) body.get("filters");
            for (String name : List.of("start", "end", "service-id", "environment")) {
                int index = command.indexOf("--" + name);
                if (index >= 0) filters.put(name.replace('-', '_'), command.get(index + 1));
            }
            for (String name : List.of("limit", "offset")) body.put(name, command.get(command.indexOf("--" + name) + 1));
            if (filters.get("start").asString().equals("9999-12-31T23:55:00.000000Z")) body.putArray("rows");
            return result(body);
        }
    });
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new ReliabilityRequestController(adapter))
            .setControllerAdvice(new ApiExceptionHandler()).build();

    @Test void exactHttpProjectionAndNoPaths() throws Exception {
        mvc.perform(get("/api/v1/reliability/request-windows").param("start", START).param("end", END))
                .andExpect(status().isOk()).andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(jsonPath("$.publication").value(PUBLICATION))
                .andExpect(jsonPath("$.productId").value("application-reliability"))
                .andExpect(jsonPath("$.origin").value("synthetic"))
                .andExpect(jsonPath("$.label").value(ReliabilityAnalyticsAdapter.LABEL))
                .andExpect(jsonPath("$.meta.returned").value(1)).andExpect(jsonPath("$.meta.limit").value(100))
                .andExpect(jsonPath("$.meta.offset").value(0)).andExpect(jsonPath("$.meta.hasMore").value(false))
                .andExpect(jsonPath("$.data[0].requestCount").value("2"))
                .andExpect(jsonPath("$.data[0].durationSumMs").value("30"))
                .andExpect(jsonPath("$.data[0].p95DurationMsApprox").value("20"))
                .andExpect(jsonPath("$.data[0].windowStart").value(START))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("\"ownerTeam\":null")))
                .andExpect(jsonPath("$.root").doesNotExist());
        assertEquals("0", seen.get(seen.indexOf("--offset") + 1));
    }

    @Test void explicitFiltersAndNativeYear10000ExclusiveSentinel() throws Exception {
        mvc.perform(get("/api/v1/reliability/request-windows").param("start", START).param("end", END)
                .param("serviceId", "svc00").param("environment", "test").param("limit", "1").param("offset", "1000000"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.meta.offset").value(1000000));
        assertTrue(seen.contains("--service-id")); assertTrue(seen.contains("--environment"));
        mvc.perform(get("/api/v1/reliability/request-windows").param("start", "9999-12-31T23:55:00.000000Z")
                .param("end", "10000-01-01T00:00:00.000000Z"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.data").isEmpty());
    }

    @Test void invalidFiltersFailBeforeAnyChild() throws Exception {
        for (String[] bad : List.of(new String[]{"serviceId", ""}, new String[]{"serviceId", "a' OR 1=1"},
                new String[]{"serviceId", "../private"}, new String[]{"environment", "prod"}, new String[]{"environment", ""},
                new String[]{"limit", "0"}, new String[]{"limit", "1001"}, new String[]{"limit", "01"}, new String[]{"limit", "1e2"},
                new String[]{"offset", "-1"}, new String[]{"offset", "1000001"}, new String[]{"offset", "00"},
                new String[]{"publication", PUBLICATION}, new String[]{"sql", "select 1"}, new String[]{"root", "/private"},
                new String[]{"sourceId", "svc00"}, new String[]{"start", "2026-10-01"},
                new String[]{"start", "2026-02-30T00:00:00.000000Z"}, new String[]{"start", "0000-01-01T00:00:00.000000Z"},
                new String[]{"start", "2026-10-01T00:00:00.000001Z"}, new String[]{"start", "2026-10-01T00:01:00.000000Z"},
                new String[]{"start", END}, new String[]{"end", START}, new String[]{"end", "2026-11-01T00:05:00.000000Z"},
                new String[]{"end", "2026-10-01T00:10:00+00:00"})) {
            seen = null;
            var request = get("/api/v1/reliability/request-windows");
            if (!bad[0].equals("start")) request.param("start", START);
            if (!bad[0].equals("end")) request.param("end", END);
            mvc.perform(request.param(bad[0], bad[1])).andExpect(status().isBadRequest())
                    .andExpect(content().json("{\"code\":\"INVALID_ARGUMENT\"}"));
            assertNull(seen, java.util.Arrays.toString(bad));
        }
        seen = null;
        mvc.perform(get("/api/v1/reliability/request-windows").param("start", START).param("end", END).param("limit", "1", "2"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/reliability/request-windows")).andExpect(status().isBadRequest());
        assertNull(seen);
    }

    @Test void failureMappingsHaveOnlySafeCode() throws Exception {
        for (ApiFailure.Code code : ApiFailure.Code.values()) {
            failure = code;
            mvc.perform(get("/api/v1/reliability/request-windows").param("start", START).param("end", END))
                    .andExpect(status().is(code.status())).andExpect(content().string("{\"code\":\"" + code.name() + "\"}"));
        }
    }

    @Test void completeNativeTimeDomainAndThirtyOneDayLimit() {
        assertDoesNotThrow(() -> new ReliabilityRequestQuery("0001-01-01T00:00:00.000000Z", "0001-02-01T00:00:00.000000Z", null, null, 1000, 0));
        assertDoesNotThrow(() -> new ReliabilityRequestQuery("1969-12-31T23:55:00.000000Z", "1970-01-01T00:00:00.000000Z", null, null, 1, 0));
        assertThrows(ApiFailure.class, () -> new ReliabilityRequestQuery("10000-01-01T00:00:00.000000Z", ReliabilityRequestQuery.END_SENTINEL, null, null, 1, 0));
        assertThrows(ApiFailure.class, () -> new ReliabilityRequestQuery(START, "2026-11-01T00:00:00.000001Z", null, null, 1, 0));
    }
}
