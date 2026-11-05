package platform.contentapi;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import static platform.contentapi.ApiFailure.Code.*;

/** Fail-closed admission of the fixed native coverage projections, never a second analytics engine. */
final class ReliabilityCoverageDecoder {
    private static final JsonMapper JSON = JsonMapper.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    private static final Set<String> ENVELOPE = keys("status product_id product_fingerprint origin label product_contract_version processor_revision input_format reference report filters limit offset has_more rows totals");
    private static final Set<String> INPUT = keys("source_id batch_id batch_fingerprint ingestion_id received_at input_file_count input_byte_length physical_occurrence_count valid_occurrence_count accepted_representative_count duplicate_occurrence_count rejected_occurrence_count first_event_time last_event_time future_occurrence_count min_receipt_delay_us max_receipt_delay_us rejected_reason_counts");
    private static final Set<String> REFERENCE = keys("service_id reference_status reference_usage service_name owner_team service_tier event_count missing_reference_event_count missing_optional_attributes");
    private static final Set<String> INPUT_TOTALS = keys("input_batch_count input_file_count input_byte_length physical_occurrence_count valid_occurrence_count accepted_representative_count duplicate_occurrence_count rejected_occurrence_count distinct_valid_identity_count duplicate_excess_in_scope");
    private static final Set<String> REF_TOTALS = keys("event_count matched_event_count missing_event_count event_service_count matched_service_count missing_service_count matched_services_missing_optional_attributes_count reference_service_count unused_reference_service_count");
    static final List<String> REASONS = List.of("BLANK_LINE", "JSON_SYNTAX", "JSON_NOT_OBJECT", "JSON_DUPLICATE_KEY", "JSON_NONFINITE", "FIELD_MISSING", "FIELD_NULL", "FIELD_TYPE", "FIELD_VALUE", "TIMESTAMP_INVALID");
    private final String publication;
    ReliabilityCoverageDecoder(String publication) { this.publication = publication; }

    ReliabilityCoverageResponse decode(BoundedProcessRunner.Result result, ReliabilityCoverageQuery query) {
        if (result.exitCode() != 0) throw new ApiFailure(ANALYTICS_EXECUTION_FAILED);
        try {
            require(result.stdout().length <= 1_048_576);
            String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(result.stdout())).toString();
            JsonNode body = JSON.readTree(text);
            fields(body, ENVELOPE);
            require("OK".equals(string(body.get("status"))) && query.report().equals(string(body.get("report")))
                    && "application-reliability".equals(string(body.get("product_id")))
                    && publication.equals(string(body.get("product_fingerprint")))
                    && "synthetic".equals(string(body.get("origin"))) && ReliabilityAnalyticsAdapter.LABEL.equals(string(body.get("label"))));
            for (String key : List.of("product_contract_version", "processor_revision")) {
                var value = body.get(key); require(value.isIntegralNumber() && value.canConvertToInt() && value.intValue() == 2);
            }
            String format = string(body.get("input_format")); require(Set.of("JSONL", "PARQUET").contains(format));
            require(decimal(body.get("limit")).equals(Integer.toString(query.limit()))
                    && decimal(body.get("offset")).equals(Integer.toString(query.offset())));
            boolean input = query.report().equals("input_coverage");
            var expectedFilters = new LinkedHashMap<String, String>();
            if (input) { expectedFilters.put("source_id", query.sourceId()); expectedFilters.put("batch_id", query.batchId()); }
            else expectedFilters.put("service_id", query.serviceId());
            fields(body.get("filters"), expectedFilters.keySet());
            for (var entry : expectedFilters.entrySet()) require(Objects.equals(entry.getValue(), nullable(body.get("filters").get(entry.getKey()))));
            var ref = body.get("reference"); fields(ref, keys("reference_id revision snapshot_fingerprint"));
            var reference = new ReliabilityRequestsResponse.Reference(identifier(ref.get("reference_id"), 64),
                    identifier(ref.get("revision"), 32), hash(ref.get("snapshot_fingerprint")));
            JsonNode rows = body.get("rows"), more = body.get("has_more"), totals = body.get("totals");
            require(rows.isArray() && rows.size() <= query.limit() && more.isBoolean()
                    && (!more.booleanValue() || rows.size() == query.limit()));
            fields(totals, input ? INPUT_TOTALS : REF_TOTALS);
            for (String key : input ? INPUT_TOTALS : REF_TOTALS) count(totals, key, maximum(key));
            if (input) inputTotals(totals, query); else referenceTotals(totals);
            long totalRows = input ? n(totals, "input_batch_count") : n(totals, "event_service_count") + n(totals, "unused_reference_service_count");
            require(query.serviceId() == null || totalRows <= 1);
            long remaining = Math.max(0, totalRows - query.offset());
            require(rows.size() == Math.min(query.limit(), remaining) && more.booleanValue() == (remaining > query.limit()));
            var mapped = new ArrayList<Map<String, Object>>();
            var pageTotals = new HashMap<String, Long>();
            String previous = null;
            long pageBytes = 0;
            for (JsonNode row : rows) {
                fields(row, input ? INPUT : REFERENCE);
                String order = input ? inputRow(row, query) : referenceRow(row, query);
                require(previous == null || previous.compareTo(order) < 0); previous = order;
                if (input) {
                    for (String key : INPUT_TOTALS) if (row.has(key)) pageTotals.merge(key, n(row, key), Long::sum);
                    pageTotals.merge("input_batch_count", 1L, Long::sum);
                } else {
                    long events = n(row, "event_count"); String usage = string(row.get("reference_usage"));
                    add(pageTotals, "event_count", events);
                    add(pageTotals, "matched_event_count", usage.equals("USED") ? events : 0);
                    add(pageTotals, "missing_event_count", usage.equals("ABSENT") ? events : 0);
                    add(pageTotals, "event_service_count", usage.equals("UNUSED") ? 0 : 1);
                    add(pageTotals, "matched_service_count", usage.equals("USED") ? 1 : 0);
                    add(pageTotals, "missing_service_count", usage.equals("ABSENT") ? 1 : 0);
                    add(pageTotals, "reference_service_count", usage.equals("ABSENT") ? 0 : 1);
                    add(pageTotals, "unused_reference_service_count", usage.equals("UNUSED") ? 1 : 0);
                    add(pageTotals, "matched_services_missing_optional_attributes_count", usage.equals("USED") && row.get("missing_optional_attributes").size() > 0 ? 1 : 0);
                }
                pageBytes += variableBytes(row, input);
                require(pageBytes <= 65_536);
                mapped.add(project(row));
            }
            for (var entry : pageTotals.entrySet()) {
                long total = n(totals, entry.getKey()); require(entry.getValue() <= total);
                if (query.offset() == 0 && !more.booleanValue()) require(entry.getValue() == total);
            }
            var filters = new LinkedHashMap<String, String>(); expectedFilters.forEach((k,v) -> filters.put(camel(k),v));
            var totalMap = new LinkedHashMap<String, String>();
            for (String key : input ? INPUT_TOTALS : REF_TOTALS) totalMap.put(camel(key), string(totals.get(key)));
            return new ReliabilityCoverageResponse(publication, "application-reliability", 2, 2, format, "synthetic",
                    ReliabilityAnalyticsAdapter.LABEL, reference, Collections.unmodifiableMap(filters), List.copyOf(mapped),
                    Collections.unmodifiableMap(totalMap), new ReliabilityRequestsResponse.Meta(mapped.size(), query.limit(), query.offset(), more.booleanValue()));
        } catch (Exception ignored) { throw new ApiFailure(ANALYTICS_RESPONSE_INVALID); }
    }
    private static void add(Map<String, Long> values, String key, long count) { values.merge(key, count, Long::sum); }
    private static long maximum(String key) {
        return switch (key) {
            case "input_batch_count" -> 32;
            case "input_file_count" -> 128;
            case "input_byte_length" -> 1_073_741_824;
            case "reference_service_count", "matched_service_count", "unused_reference_service_count", "matched_services_missing_optional_attributes_count" -> 10_000;
            default -> 2_000_000;
        };
    }
    private static void inputTotals(JsonNode t, ReliabilityCoverageQuery query) {
        reconcileInput(t);
        require(query.sourceId() == null || query.batchId() == null || n(t,"input_batch_count") <= 1);
        require(n(t,"valid_occurrence_count") == n(t,"distinct_valid_identity_count") + n(t,"duplicate_excess_in_scope")
                && n(t,"accepted_representative_count") <= n(t,"distinct_valid_identity_count")
                && (n(t,"valid_occurrence_count") == 0) == (n(t,"distinct_valid_identity_count") == 0)
                && n(t,"input_file_count") >= n(t,"input_batch_count")
                && n(t,"input_file_count") <= 16 * n(t,"input_batch_count"));
        if (query.sourceId() == null && query.batchId() == null) require(n(t,"accepted_representative_count") == n(t,"distinct_valid_identity_count"));
        if (n(t,"input_batch_count") == 0) for (String key : INPUT_TOTALS) require(n(t,key) == 0);
    }
    private static void reconcileInput(JsonNode t) {
        require(n(t,"physical_occurrence_count") == n(t,"valid_occurrence_count") + n(t,"rejected_occurrence_count")
                && n(t,"valid_occurrence_count") == n(t,"accepted_representative_count") + n(t,"duplicate_occurrence_count"));
    }
    private static void referenceTotals(JsonNode t) {
        require(n(t,"event_count") == n(t,"matched_event_count") + n(t,"missing_event_count")
                && n(t,"event_service_count") == n(t,"matched_service_count") + n(t,"missing_service_count")
                && n(t,"reference_service_count") == n(t,"matched_service_count") + n(t,"unused_reference_service_count")
                && n(t,"matched_services_missing_optional_attributes_count") <= n(t,"matched_service_count")
                && n(t,"matched_service_count") <= n(t,"matched_event_count")
                && n(t,"missing_service_count") <= n(t,"missing_event_count")
                && (n(t,"matched_service_count") == 0) == (n(t,"matched_event_count") == 0)
                && (n(t,"missing_service_count") == 0) == (n(t,"missing_event_count") == 0));
    }
    private static String inputRow(JsonNode row, ReliabilityCoverageQuery query) {
        String source = identifier(row.get("source_id"),128), batch = identifier(row.get("batch_id"),64), fingerprint = hash(row.get("batch_fingerprint"));
        require((query.sourceId() == null || query.sourceId().equals(source)) && (query.batchId() == null || query.batchId().equals(batch)));
        require(string(row.get("ingestion_id")).matches("[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}"));
        Instant receipt = time(row.get("received_at"));
        for (String key : keys("input_file_count input_byte_length physical_occurrence_count valid_occurrence_count accepted_representative_count duplicate_occurrence_count rejected_occurrence_count future_occurrence_count")) count(row,key,maximum(key));
        require(n(row,"input_file_count") >= 1 && n(row,"input_file_count") <= 16);
        reconcileInput(row);
        long valid = n(row,"valid_occurrence_count"), future = n(row,"future_occurrence_count"); require(future <= valid);
        if (valid == 0) {
            for (String key : keys("first_event_time last_event_time min_receipt_delay_us max_receipt_delay_us")) require(row.get(key).isNull());
        } else {
            Instant first = time(row.get("first_event_time")), last = time(row.get("last_event_time")); require(!first.isAfter(last));
            long min = signed(row.get("min_receipt_delay_us")), max = signed(row.get("max_receipt_delay_us"));
            require(min == micros(receipt) - micros(last) && max == micros(receipt) - micros(first)
                    && (future > 0) == (min < 0) && (future == valid) == (max < 0));
        }
        var reasons = row.get("rejected_reason_counts"); fields(reasons, Set.copyOf(REASONS)); long reasonSum = 0;
        for (String key : REASONS) reasonSum += count(reasons,key,n(row,"rejected_occurrence_count"));
        require(reasonSum >= n(row,"rejected_occurrence_count"));
        return source + "\u0000" + batch + "\u0000" + fingerprint;
    }
    private static String referenceRow(JsonNode row, ReliabilityCoverageQuery query) {
        String service = identifier(row.get("service_id"),128); require(query.serviceId() == null || query.serviceId().equals(service));
        String status = string(row.get("reference_status")), usage = string(row.get("reference_usage"));
        String name = nullable(row.get("service_name")), owner = nullable(row.get("owner_team")), tier = nullable(row.get("service_tier"));
        long events = count(row,"event_count",2_000_000), missing = count(row,"missing_reference_event_count",2_000_000);
        var optional = row.get("missing_optional_attributes"); require(optional.isArray()); var expected = new ArrayList<String>();
        if (usage.equals("ABSENT")) require(status.equals("MISSING") && name == null && owner == null && tier == null && events > 0 && missing == events);
        else {
            require(status.equals("MATCHED") && (usage.equals("USED") && events > 0 || usage.equals("UNUSED") && events == 0)
                    && missing == 0 && validName(name));
            require(owner == null || Set.of("example-platform","example-service").contains(owner));
            require(tier == null || Set.of("critical","experimental","standard").contains(tier));
            if (owner == null) expected.add("owner_team"); if (tier == null) expected.add("service_tier");
        }
        require(optional.size() == expected.size());
        for (int i=0;i<expected.size();i++) require(expected.get(i).equals(string(optional.get(i))));
        return service;
    }
    private static boolean validName(String name) {
        if (name == null || name.isEmpty() || name.codePointCount(0,name.length()) > 200) return false;
        if (boundaryWhitespace(name.codePointAt(0)) || boundaryWhitespace(name.codePointBefore(name.length()))) return false;
        return name.codePoints().noneMatch(c -> c < 32 || c >= 127 && c <= 159 || c == 8232 || c == 8233
                || c == 65279 || c >= 0xd800 && c <= 0xdfff);
    }
    private static boolean boundaryWhitespace(int c) {
        return c >= 9 && c <= 13 || c == 32 || c == 133 || c == 160 || c == 5760 || c >= 8192 && c <= 8202
                || c == 8232 || c == 8233 || c == 8239 || c == 8287 || c == 12288;
    }
    private static long variableBytes(JsonNode row, boolean input) {
        long bytes = 0;
        for (String key : input ? keys("source_id batch_id batch_fingerprint ingestion_id")
                : keys("service_id reference_status reference_usage service_name owner_team service_tier")) {
            String value = nullable(row.get(key));
            if (value != null) bytes += value.getBytes(StandardCharsets.UTF_8).length;
        }
        if (!input) for (JsonNode value : row.get("missing_optional_attributes")) bytes += string(value).getBytes(StandardCharsets.UTF_8).length;
        return bytes;
    }
    private static Map<String, Object> project(JsonNode row) {
        var result = new LinkedHashMap<String,Object>();
        for (var entry : row.properties()) {
            JsonNode value = entry.getValue(); Object mapped;
            if (value.isNull()) mapped = null;
            else if (value.isArray()) { var list = new ArrayList<String>(); value.forEach(v -> list.add(string(v))); mapped = List.copyOf(list); }
            else if (value.isObject()) { var map = new LinkedHashMap<String,String>(); for (String reason : REASONS) map.put(reason,string(value.get(reason))); mapped = Collections.unmodifiableMap(map); }
            else mapped = string(value);
            result.put(camel(entry.getKey()),mapped);
        }
        return Collections.unmodifiableMap(result);
    }
    private static String camel(String value) {
        String[] parts = value.split("_"); var result = new StringBuilder(parts[0]);
        for (int i=1;i<parts.length;i++) result.append(Character.toUpperCase(parts[i].charAt(0))).append(parts[i].substring(1));
        return result.toString();
    }
    private static long micros(Instant time) { return Math.addExact(Math.multiplyExact(time.getEpochSecond(),1_000_000),time.getNano()/1000); }
    private static Instant time(JsonNode value) { return ReliabilityRequestQuery.instant(string(value),false); }
    private static long signed(JsonNode value) { String text = string(value); require(text.matches("0|-?[1-9][0-9]{0,17}")); return Long.parseLong(text); }
    private static String hash(JsonNode value) { String text = string(value); require(text.matches("[0-9a-f]{64}")); return text; }
    private static String identifier(JsonNode value,int maximum) { String text=string(value); require(text.matches("[a-z0-9][a-z0-9_-]{0,"+(maximum-1)+"}")); return text; }
    private static long count(JsonNode value,String key,long maximum) { long result = n(value,key); require(result <= maximum); return result; }
    private static long n(JsonNode value,String key) { return new BigInteger(decimal(value.get(key))).longValueExact(); }
    private static String decimal(JsonNode value) { String text=string(value); require(text.matches("0|[1-9][0-9]{0,18}")); return text; }
    private static String nullable(JsonNode value) { require(value != null); return value.isNull() ? null : string(value); }
    private static String string(JsonNode value) { require(value != null && value.isString()); return value.asString(); }
    private static Set<String> keys(String value) { return Set.of(value.split(" ")); }
    private static void fields(JsonNode value,Set<String> keys) { require(value != null && value.isObject() && value.size()==keys.size() && keys.stream().allMatch(value::has)); }
    private static void require(boolean valid) { if (!valid) throw new ApiFailure(ANALYTICS_RESPONSE_INVALID); }
}
