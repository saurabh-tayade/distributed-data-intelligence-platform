package platform.contentapi;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;
import static platform.contentapi.TestSupport.*;
import static platform.contentapi.GoldAnalyticsAdapter.Report.PUBLISHERS;
import static platform.contentapi.ApiFailure.Code.*;

class GoldAnalyticsAdapterTest {
    private final GoldAnalyticsAdapter adapter = adapter();
    private final PublisherQuery query = new PublisherQuery(null, 100);

    @Test
    void exactTypedValuesAndNulls() {
        var result = adapter.decode(output(0, envelope("[" + row() + "]")), PUBLISHERS, query);
        assertEquals(PUBLICATION, result.publication());
        var row = result.rows().getFirst();
        assertEquals(9007199254740993L, row.articleCount());
        assertEquals(Long.MAX_VALUE, row.observationCount());
        assertEquals(TIMESTAMP, row.firstObservedAt());
        assertNull(row.lastObservedAt());
        assertTrue(adapter.decode(output(0, envelope("[]")), PUBLISHERS, query).rows().isEmpty());
    }

    @Test
    void argumentListAndControlledWorkingDirectory() {
        var props = properties();
        var runner = new BoundedProcessRunner(props) {
            @Override public Result run(List<String> command, Path directory) {
                assertEquals(props.script().toAbsolutePath().getParent(), directory);
                assertEquals(List.of(props.pythonExecutable(), "-E", "-s", "-B", "-X", "utf8",
                        props.script().toAbsolutePath().toString(), "--gold-root", props.goldRoot().toAbsolutePath().toString(),
                        "--publication", PUBLICATION, "--report", "publishers", "--limit", "100", "--source", "alpha"), command);
                return output(0, envelope("[" + row() + "]"));
            }
        };
        assertEquals(1, new GoldAnalyticsAdapter(props, runner).execute(PUBLISHERS, new PublisherQuery("alpha", 100)).rows().size());
        assertFalse(adapter.command(PUBLISHERS, query).contains("--source"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"alpha' OR 1=1 --", "$(touch /tmp/unsafe)", "--report=articles", "../gold", "", " ", "a\nb", "UPPER"})
    void refusesArgumentInjectionAndInvalidSources(String source) {
        assertEquals(INVALID_ARGUMENT, assertThrows(ApiFailure.class, () -> new PublisherQuery(source, 100)).code());
    }

    static Stream<String> invalidEnvelopes() {
        String valid = envelope("[" + row() + "]");
        return Stream.of("{", "null", "[]", "{}", valid + " {}", valid.replace("\"OK\"", "\"FAILED\""),
                valid.replace(PUBLICATION, "b".repeat(64)), valid.replace("publishers", "articles"),
                valid.replace("\"limit\":100", "\"limit\":99"), valid.replace("\"limit\":100", "\"limit\":100.0"),
                valid.replace("\"status\":\"OK\"", "\"status\":\"OK\",\"status\":\"OK\""),
                envelope("null"), envelope("{}"), valid.replace("\"alpha\"", "\" \""),
                valid.replace("\"article_count\":9007199254740993", "\"article_count\":null"),
                valid.replace("9007199254740993", "1.0"), valid.replace("9007199254740993", "true"),
                valid.replace("9007199254740993", "-1"), valid.replace("9007199254740993", "9223372036854775808"),
                valid.replace("9007199254740993", "\"45\""), valid.replace("source_id", "wrong_field"),
                valid.replace(TIMESTAMP, "2026-10-12T10:20:30.123Z"),
                valid.replace(TIMESTAMP, "2026-02-30T10:20:30.123456Z"),
                valid.replace(TIMESTAMP, "2026-10-12T10:20:60.123456Z"),
                valid.replace(TIMESTAMP, "2026-10-12T10:20:30.123456+01:00"),
                envelope("[" + row() + "," + row() + "]"),
                envelope("[" + row().replace("alpha", "zeta") + "," + row() + "]"),
                valid.replace("\"status\":", "\"extra\":1,\"status\":"));
    }

    @ParameterizedTest
    @MethodSource("invalidEnvelopes")
    void rejectsMalformedOrMismatchedResponses(String json) {
        assertEquals(ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class,
                () -> adapter.decode(output(0, json), PUBLISHERS, query)).code());
    }

    @Test
    void rejectsInvalidEncodingExcessRowsAndWrongFilter() {
        assertEquals(ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> adapter.decode(
                new BoundedProcessRunner.Result(0, new byte[]{(byte) 0xff}), PUBLISHERS, query)).code());
        assertEquals(ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> adapter.decode(
                output(0, envelope("[" + row() + "]")), PUBLISHERS, new PublisherQuery("beta", 100))).code());
        assertEquals(ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class, () -> adapter.decode(
                output(0, envelope("[" + row() + "," + row().replace("alpha", "beta") + "]").replace("\"limit\":100", "\"limit\":1")),
                PUBLISHERS, new PublisherQuery(null, 1))).code());
    }

    @Test
    void exitCodeAndFailureEnvelopeMustAgree() {
        String failure = "{\"status\":\"FAILED\",\"code\":\"FILE_MISMATCH\",\"path\":\"/private/secret\"}";
        assertEquals(PUBLICATION_UNAVAILABLE, assertThrows(ApiFailure.class,
                () -> adapter.decode(output(1, failure), PUBLISHERS, query)).code());
        for (var output : List.of(output(0, failure), output(1, envelope("[]")), output(2, failure),
                output(1, "traceback /secret"), output(1, failure.replace("FILE_MISMATCH", "QUERY_FAILED")))) {
            var thrown = assertThrows(ApiFailure.class, () -> adapter.decode(output, PUBLISHERS, query));
            assertEquals(output.exitCode() == 0 ? ANALYTICS_RESPONSE_INVALID : ANALYTICS_EXECUTION_FAILED, thrown.code());
            assertFalse(thrown.getMessage().contains("secret"));
            assertNull(thrown.getCause());
        }
    }

    @Test
    void missingConfigurationOrRuntimeFailsWithoutChoosingPublication() {
        var base = properties();
        for (String publication : List.of("", PUBLICATION)) {
            var props = new AnalyticsProperties("/definitely/missing/python", Path.of("missing.py"), base.goldRoot(),
                    publication, base.timeout(), 1, 1024, 1024);
            var adapter = new GoldAnalyticsAdapter(props, new BoundedProcessRunner(props));
            assertEquals(publication.isEmpty() ? PUBLICATION_UNAVAILABLE : ANALYTICS_RUNTIME_UNAVAILABLE,
                    assertThrows(ApiFailure.class, () -> adapter.execute(PUBLISHERS, query)).code());
        }
    }
}
