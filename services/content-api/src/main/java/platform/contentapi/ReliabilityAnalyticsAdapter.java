package platform.contentapi;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import platform.contentapi.execution.AnalyticsRole;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import static platform.contentapi.ApiFailure.Code.*;
import static platform.contentapi.ReliabilityRequestsResponse.*;

/** A fixed projection of the verified v2 native report. No SQL or verification is reimplemented here. */
@AnalyticsRole
@Component
public class ReliabilityAnalyticsAdapter {
    static final String LABEL = "Synthetic request events — not production reliability.";
    private static final Logger LOG = LoggerFactory.getLogger(ReliabilityAnalyticsAdapter.class);
    private static final JsonMapper JSON = JsonMapper.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    private static final Set<String> ENVELOPE = Set.of("status", "product_id", "product_fingerprint", "origin", "label",
            "product_contract_version", "processor_revision", "input_format", "reference", "report", "filters", "limit", "offset", "has_more", "rows");
    private static final Set<String> ROW = Set.of("window_start", "service_id", "environment", "request_count", "server_error_count",
            "server_error_rate", "duration_sum_ms", "mean_duration_ms", "p95_duration_ms_approx", "service_name", "owner_team", "service_tier", "reference_status");
    private final ReliabilityAnalyticsProperties properties;
    private final BoundedProcessRunner runner;
    private final Path script;
    private final Path executable;

    public ReliabilityAnalyticsAdapter(ReliabilityAnalyticsProperties properties, BoundedProcessRunner runner) {
        this.properties = properties;
        this.runner = runner;
        script = properties.processingRoot().toAbsolutePath().normalize().resolve("reliability_format_analytics.py");
        executable = resolveExecutable(properties.pythonExecutable());
    }

    private static Path resolveExecutable(String configured) {
        Path path = Path.of(configured);
        if (path.isAbsolute() || path.getNameCount() > 1) return path.toAbsolutePath().normalize();
        for (String directory : System.getenv().getOrDefault("PATH", "").split(java.io.File.pathSeparator)) {
            if (directory.isBlank()) continue;
            Path candidate = Path.of(directory).resolve(path).toAbsolutePath().normalize();
            if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) return candidate;
        }
        return null;
    }

    public ReliabilityRequestsResponse requests(ReliabilityRequestQuery query) {
        long start = System.nanoTime();
        String outcome = "OK";
        String correlation = UUID.randomUUID().toString();
        try {
            if (properties.publication().isEmpty()) throw new ApiFailure(PUBLICATION_UNAVAILABLE);
            if (executable == null || !Files.isExecutable(executable) || !Files.isRegularFile(script)
                    || !Files.isReadable(script)) throw new ApiFailure(ANALYTICS_RUNTIME_UNAVAILABLE);
            return decode(runner.run(command(query), script.getParent()), query);
        } catch (ApiFailure failure) {
            outcome = failure.code().name();
            throw failure;
        } catch (RuntimeException ignored) {
            outcome = ANALYTICS_EXECUTION_FAILED.name();
            throw new ApiFailure(ANALYTICS_EXECUTION_FAILED);
        } finally {
            LOG.info("report=reliability_requests_5m executionId={} durationMs={} outcome={}", correlation,
                    (System.nanoTime() - start) / 1_000_000, outcome);
        }
    }

    List<String> command(ReliabilityRequestQuery query) {
        var args = new ArrayList<>(List.of(executable.toString(), "-E", "-s", "-B", "-X", "utf8", script.toString(),
                "--root", properties.root().toAbsolutePath().normalize().toString(), "--publication", properties.publication(),
                "--batch-root", properties.batchRoot().toAbsolutePath().normalize().toString(),
                "--reference-root", properties.referenceRoot().toAbsolutePath().normalize().toString(),
                "--report", "requests_5m", "--start", query.start(), "--end", query.end(),
                "--limit", Integer.toString(query.limit()), "--offset", Integer.toString(query.offset())));
        if (query.serviceId() != null) args.addAll(List.of("--service-id", query.serviceId()));
        if (query.environment() != null) args.addAll(List.of("--environment", query.environment()));
        return List.copyOf(args);
    }

    ReliabilityRequestsResponse decode(BoundedProcessRunner.Result result, ReliabilityRequestQuery query) {
        // The native CLI writes failure diagnostics only to stderr. The shared runner discards them;
        // never inspect or expose paths, raw content or exception text to classify a native failure.
        if (result.exitCode() != 0) throw new ApiFailure(ANALYTICS_EXECUTION_FAILED);
        try {
            String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(result.stdout())).toString();
            JsonNode body = JSON.readTree(text);
            fields(body, ENVELOPE);
            require("OK".equals(string(body.get("status"))) && "requests_5m".equals(string(body.get("report")))
                    && "application-reliability".equals(string(body.get("product_id")))
                    && properties.publication().equals(string(body.get("product_fingerprint")))
                    && "synthetic".equals(string(body.get("origin"))) && LABEL.equals(string(body.get("label"))));
            version(body.get("product_contract_version")); version(body.get("processor_revision"));
            String format = string(body.get("input_format"));
            require(Set.of("JSONL", "PARQUET").contains(format));
            require(Integer.toString(query.limit()).equals(decimal(body.get("limit"), 4))
                    && Integer.toString(query.offset()).equals(decimal(body.get("offset"), 7)));
            JsonNode filter = body.get("filters");
            fields(filter, Set.of("service_id", "environment", "start", "end"));
            require(Objects.equals(query.serviceId(), nullable(filter.get("service_id")))
                    && Objects.equals(query.environment(), nullable(filter.get("environment")))
                    && query.start().equals(string(filter.get("start"))) && query.end().equals(string(filter.get("end"))));
            JsonNode ref = body.get("reference");
            fields(ref, Set.of("reference_id", "revision", "snapshot_fingerprint"));
            var reference = new Reference(identifier(ref.get("reference_id"), 64), identifier(ref.get("revision"), 32),
                    string(ref.get("snapshot_fingerprint")));
            require(reference.snapshotFingerprint().matches("[0-9a-f]{64}"));
            JsonNode rows = body.get("rows"), more = body.get("has_more");
            require(rows.isArray() && rows.size() <= query.limit() && more.isBoolean()
                    && (!more.booleanValue() || rows.size() == query.limit()));
            var mapped = new ArrayList<Row>();
            Row previous = null;
            for (JsonNode row : rows) {
                fields(row, ROW);
                String window = string(row.get("window_start"));
                var time = ReliabilityRequestQuery.instant(window, false);
                require(time.getNano() == 0 && time.getEpochSecond() % 300 == 0
                        && !time.isBefore(ReliabilityRequestQuery.instant(query.start(), false))
                        && time.isBefore(ReliabilityRequestQuery.instant(query.end(), true)));
                String service = identifier(row.get("service_id"), 128), environment = string(row.get("environment"));
                require(Set.of("dev", "test", "production").contains(environment)
                        && (query.serviceId() == null || query.serviceId().equals(service))
                        && (query.environment() == null || query.environment().equals(environment)));
                require(previous == null || previous.windowStart().compareTo(window) < 0
                        || previous.windowStart().equals(window) && (previous.serviceId().compareTo(service) < 0
                        || previous.serviceId().equals(service) && previous.environment().compareTo(environment) < 0));
                String count = decimal(row.get("request_count"), 7), errors = decimal(row.get("server_error_count"), 7);
                var requests = new BigInteger(count); var serverErrors = new BigInteger(errors);
                require(requests.signum() > 0 && requests.compareTo(BigInteger.valueOf(2_000_000)) <= 0
                        && serverErrors.compareTo(requests) <= 0);
                String sum = decimal(row.get("duration_sum_ms"), 38), p95 = decimal(row.get("p95_duration_ms_approx"), 7);
                var durationSum = new BigInteger(sum);
                require(durationSum.compareTo(requests.multiply(BigInteger.valueOf(3_600_000))) <= 0
                        && new BigInteger(p95).compareTo(BigInteger.valueOf(3_600_000)) <= 0);
                double rate = number(row.get("server_error_rate"), 1), mean = number(row.get("mean_duration_ms"), 3_600_000);
                require(close(rate, serverErrors.doubleValue() / requests.doubleValue())
                        && close(mean, durationSum.doubleValue() / requests.doubleValue()));
                String name = nullable(row.get("service_name")), owner = nullable(row.get("owner_team")), tier = nullable(row.get("service_tier"));
                String status = string(row.get("reference_status"));
                require(status.equals("MISSING") && name == null && owner == null && tier == null
                        || status.equals("MATCHED") && name != null && !name.isBlank());
                require((owner == null || Set.of("example-platform", "example-service").contains(owner))
                        && (tier == null || Set.of("critical", "experimental", "standard").contains(tier)));
                var value = new Row(window, service, environment, count, errors, rate, sum, mean, p95, name, owner, tier, status);
                mapped.add(value); previous = value;
            }
            return new ReliabilityRequestsResponse(properties.publication(), "application-reliability", 2, 2, format,
                    "synthetic", LABEL, reference, new Filters(query.serviceId(), query.environment(), query.start(), query.end()),
                    List.copyOf(mapped), new Meta(mapped.size(), query.limit(), query.offset(), more.booleanValue()));
        } catch (Exception ignored) { throw new ApiFailure(ANALYTICS_RESPONSE_INVALID); }
    }

    private static boolean close(double actual, double expected) {
        return Math.abs(actual - expected) <= Math.max(1e-12, Math.abs(expected) * 1e-12);
    }
    private static double number(JsonNode value, double maximum) {
        require(value != null && value.isNumber());
        double result = value.doubleValue();
        require(Double.isFinite(result) && result >= 0 && result <= maximum);
        return result;
    }
    private static void version(JsonNode value) { require(value != null && value.isIntegralNumber() && value.canConvertToInt() && value.intValue() == 2); }
    private static String identifier(JsonNode value, int maximum) {
        String text = string(value);
        require(text.matches("[a-z0-9][a-z0-9_-]{0," + (maximum - 1) + "}"));
        return text;
    }
    private static String decimal(JsonNode value, int digits) {
        String text = string(value);
        require(text.matches("0|[1-9][0-9]{0," + (digits - 1) + "}"));
        return text;
    }
    private static String nullable(JsonNode value) { require(value != null); return value.isNull() ? null : string(value); }
    private static String string(JsonNode value) { require(value != null && value.isString()); return value.asString(); }
    private static void fields(JsonNode value, Set<String> expected) {
        require(value != null && value.isObject() && value.size() == expected.size() && expected.stream().allMatch(value::has));
    }
    private static void require(boolean valid) { if (!valid) throw new ApiFailure(ANALYTICS_RESPONSE_INVALID); }
}
