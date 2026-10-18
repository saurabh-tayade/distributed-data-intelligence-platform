package platform.contentapi;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static platform.contentapi.TestSupport.*;

class PublisherWebTest {
    private final AtomicReference<PublisherQuery> seen = new AtomicReference<>();
    private final AtomicReference<ApiFailure.Code> failure = new AtomicReference<>();
    private boolean empty;
    private final GoldAnalyticsAdapter adapter = new GoldAnalyticsAdapter(properties(), new BoundedProcessRunner(properties())) {
        @Override public Result execute(Report report, PublisherQuery query) {
            assertEquals(Report.PUBLISHERS, report);
            seen.set(query);
            if (failure.get() != null) throw new ApiFailure(failure.get());
            return new Result(PUBLICATION, empty ? List.of() : List.of(new PublisherRow("alpha", 9007199254740993L,
                    Long.MAX_VALUE, TIMESTAMP, null, 0)));
        }
    };
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new PublisherController(new PublisherService(adapter)))
            .setControllerAdvice(new ApiExceptionHandler()).build();

    @Test
    void successDefaultLimitDtoMappingAndExactCounts() throws Exception {
        mvc.perform(get("/api/v1/publishers"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.publication").value(PUBLICATION))
                .andExpect(jsonPath("$.meta.returned").value(1)).andExpect(jsonPath("$.meta.limit").value(100))
                .andExpect(jsonPath("$.data[0].sourceId").value("alpha"))
                .andExpect(jsonPath("$.data[0].articleCount").value("9007199254740993"))
                .andExpect(jsonPath("$.data[0].observationCount").value("9223372036854775807"))
                .andExpect(jsonPath("$.data[0].articlesWithoutPublishedAt").value("0"))
                .andExpect(jsonPath("$.data[0].firstObservedAt").value(TIMESTAMP))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("\"lastObservedAt\":null")));
        assertEquals(new PublisherQuery(null, 100), seen.get());
    }

    @Test
    void filterAndExplicitLimit() throws Exception {
        mvc.perform(get("/api/v1/publishers").param("sourceId", "alpha").param("limit", "1"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.meta.limit").value(1));
        assertEquals(new PublisherQuery("alpha", 1), seen.get());
    }

    @Test
    void emptyResultsRemainArray() throws Exception {
        empty = true;
        mvc.perform(get("/api/v1/publishers")).andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray()).andExpect(jsonPath("$.data").isEmpty())
                .andExpect(jsonPath("$.meta.returned").value(0));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "1001", "999999999999999999", "1.0", "abc", "", " 1"})
    void invalidLimits(String limit) throws Exception {
        mvc.perform(get("/api/v1/publishers").param("limit", limit)).andExpect(status().isBadRequest())
                .andExpect(content().json("{\"code\":\"INVALID_ARGUMENT\"}"));
        assertNull(seen.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "a\nb", "--report", "../foo", "alpha' OR 1=1 --"})
    void invalidSources(String source) throws Exception {
        mvc.perform(get("/api/v1/publishers").param("sourceId", source)).andExpect(status().isBadRequest());
        assertNull(seen.get());
    }

    @Test
    void unknownDuplicateAndTooLongParameters() throws Exception {
        for (String name : List.of("goldRoot", "publication", "sql", "report")) {
            mvc.perform(get("/api/v1/publishers").param(name, "/secret")).andExpect(status().isBadRequest());
        }
        mvc.perform(get("/api/v1/publishers").param("limit", "1", "2")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/publishers").param("sourceId", "a".repeat(129))).andExpect(status().isBadRequest());
        assertNull(seen.get());
    }

    @ParameterizedTest
    @EnumSource(ApiFailure.Code.class)
    void stableErrorMapping(ApiFailure.Code code) throws Exception {
        failure.set(code);
        mvc.perform(get("/api/v1/publishers")).andExpect(status().is(code.status()))
                .andExpect(content().json("{\"code\":\"" + code.name() + "\"}"));
    }
}
