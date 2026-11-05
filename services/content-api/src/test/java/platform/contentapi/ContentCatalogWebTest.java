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
import static platform.contentapi.TestSupport.*;

class ContentCatalogWebTest {
    private List<String> seen;
    private ApiFailure.Code failure;
    private final ContentAnalyticsAdapter adapter = new ContentAnalyticsAdapter(settings(), new BoundedProcessRunner(properties()) {
        @Override public Result run(List<String> command, Path directory) {
            seen = command;
            if (failure != null) throw new ApiFailure(failure);
            var body = body(); var filters = (ObjectNode) body.get("filters");
            for (String name : List.of("source", "start-date", "end-date", "reference-status", "publisher-category", "declared-language", "country-code")) {
                int index = command.indexOf("--" + name);
                if (index >= 0) filters.put(name.replace('-', '_'), command.get(index + 1));
            }
            for (String name : List.of("limit", "offset")) body.put(name, command.get(command.indexOf("--" + name) + 1));
            return result(body);
        }
    });
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new ContentCatalogController(adapter))
            .setControllerAdvice(new ApiExceptionHandler()).build();

    @Test void exactHttpProjectionNoPathsAndIndependentSelection() throws Exception {
        mvc.perform(get("/api/v1/content/catalog"))
                .andExpect(status().isOk()).andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(jsonPath("$.publication").value(PUBLICATION)).andExpect(jsonPath("$.goldPublication").value(GOLD))
                .andExpect(jsonPath("$.productId").value("content-intelligence"))
                .andExpect(jsonPath("$.reference.snapshotFingerprint").value(REFERENCE))
                .andExpect(jsonPath("$.meta.returned").value(1)).andExpect(jsonPath("$.meta.limit").value(100))
                .andExpect(jsonPath("$.meta.offset").value(0)).andExpect(jsonPath("$.meta.hasMore").value(false))
                .andExpect(jsonPath("$.data[0].observationCount").value("9007199254740993"))
                .andExpect(jsonPath("$.data[0].discoveredAt").value("2026-10-12T23:59:59.999999Z"))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("\"summary\":null")))
                .andExpect(jsonPath("$.root").doesNotExist()).andExpect(jsonPath("$.data[0].selectedInputUri").doesNotExist());
        assertEquals("catalog", seen.get(seen.indexOf("--report") + 1));
    }

    @Test void allExactFiltersAreForwardedWithoutTrimmingOrSql() throws Exception {
        mvc.perform(get("/api/v1/content/catalog").param("sourceId","alpha").param("from","2026-10-12").param("to","2026-10-12")
                .param("referenceStatus","MATCHED").param("publisherCategory","science").param("declaredLanguage","en").param("countryCode","CA")
                .param("limit","1").param("offset","1000000"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.filters.sourceId").value("alpha"))
                .andExpect(jsonPath("$.filters.from").value("2026-10-12")).andExpect(jsonPath("$.meta.offset").value(1000000));
        assertTrue(seen.contains("--source")); assertTrue(seen.contains("--start-date")); assertTrue(seen.contains("--end-date"));
    }

    @Test void unknownBlankRepeatedMalformedAndOutOfRangeArgumentsFailBeforeAnyChild() throws Exception {
        for (String[] bad : List.of(new String[]{"sourceId",""},new String[]{"sourceId"," alpha"},new String[]{"sourceId","a' OR 1=1"},
                new String[]{"sourceId","../private"},new String[]{"sourceId","UPPER"},new String[]{"referenceStatus","matched"},
                new String[]{"publisherCategory","Science"},new String[]{"declaredLanguage","de"},new String[]{"countryCode","ca"},
                new String[]{"limit","0"},new String[]{"limit","1001"},new String[]{"limit","01"},new String[]{"limit","1e2"},
                new String[]{"offset","-1"},new String[]{"offset","1000001"},new String[]{"offset","00"},new String[]{"offset","2147483647"},
                new String[]{"publication",PUBLICATION},new String[]{"goldPublication",GOLD},new String[]{"sql","select 1"},new String[]{"root","/private"},
                new String[]{"report","catalog"},new String[]{"sort","title"},new String[]{"from","2026-02-30"},new String[]{"from","0000-01-01"},
                new String[]{"to","10000-01-01"},new String[]{"to","2026-1-01"},new String[]{"to","2026-10-12T00:00:00.000000Z"})) {
            seen = null;
            mvc.perform(get("/api/v1/content/catalog").param(bad[0],bad[1])).andExpect(status().isBadRequest())
                    .andExpect(content().json("{\"code\":\"INVALID_ARGUMENT\"}"));
            assertNull(seen,java.util.Arrays.toString(bad));
        }
        for (String name : List.of("sourceId","from","to","referenceStatus","publisherCategory","declaredLanguage","countryCode","limit","offset")) {
            seen = null;
            mvc.perform(get("/api/v1/content/catalog").param(name,"1","2")).andExpect(status().isBadRequest());
            mvc.perform(get("/api/v1/content/catalog").param(name,"")).andExpect(status().isBadRequest());
            assertNull(seen,name);
        }
        seen = null;
        mvc.perform(get("/api/v1/content/catalog").param("from","2026-10-13").param("to","2026-10-12")).andExpect(status().isBadRequest());
        assertNull(seen);
    }

    @Test void exactNativeDateDomainAndAllVocabularyChoicesAreAccepted() {
        assertDoesNotThrow(() -> new ContentCatalogQuery(null,"0001-01-01","9999-12-31",null,null,null,null,1000,1_000_000));
        assertDoesNotThrow(() -> new ContentCatalogQuery(null,"1969-12-31","1970-01-01",null,null,null,null,1,0));
        for (String status : ContentCatalogQuery.STATUSES) for (String category : ContentCatalogQuery.CATEGORIES)
            for (String language : ContentCatalogQuery.LANGUAGES) for (String country : ContentCatalogQuery.COUNTRIES)
                assertDoesNotThrow(() -> new ContentCatalogQuery("a".repeat(128),null,null,status,category,language,country,100,0));
    }

    @Test void everyFailureMappingContainsOnlySafeCode() throws Exception {
        for (ApiFailure.Code code : ApiFailure.Code.values()) {
            failure = code;
            mvc.perform(get("/api/v1/content/catalog")).andExpect(status().is(code.status()))
                    .andExpect(content().string("{\"code\":\""+code.name()+"\"}"));
        }
    }
}
