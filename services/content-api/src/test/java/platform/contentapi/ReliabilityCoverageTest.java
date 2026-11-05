package platform.contentapi;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.node.ObjectNode;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static platform.contentapi.TestSupport.*;
import static platform.contentapi.ReliabilityReportsTest.*;

class ReliabilityCoverageTest {
    private final ReliabilityCoverageDecoder decoder = new ReliabilityCoverageDecoder(PUBLICATION);
    static ReliabilityCoverageQuery query(boolean input) {
        return new ReliabilityCoverageQuery(input ? "input_coverage" : "reference_coverage", null,null,null,100,0);
    }
    static ObjectNode coverage(boolean input) {
        var body = body(); body.put("report",input ? "input_coverage" : "reference_coverage");
        var filters = body.putObject("filters");
        if (input) { filters.putNull("source_id"); filters.putNull("batch_id"); } else filters.putNull("service_id");
        body.set("rows",JSON.readTree(input ? """
                [{"source_id":"source-a","batch_id":"b1","batch_fingerprint":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "ingestion_id":"00000000-0000-0000-0000-000000000001","received_at":"1970-01-01T00:00:00.000000Z",
                "input_file_count":"1","input_byte_length":"400","physical_occurrence_count":"4","valid_occurrence_count":"3",
                "accepted_representative_count":"2","duplicate_occurrence_count":"1","rejected_occurrence_count":"1",
                "first_event_time":"1969-12-31T23:59:59.999999Z","last_event_time":"1970-01-01T00:00:00.000001Z",
                "future_occurrence_count":"1","min_receipt_delay_us":"-1","max_receipt_delay_us":"1",
                "rejected_reason_counts":{"BLANK_LINE":"0","JSON_SYNTAX":"0","JSON_NOT_OBJECT":"0","JSON_DUPLICATE_KEY":"0","JSON_NONFINITE":"0","FIELD_MISSING":"1","FIELD_NULL":"0","FIELD_TYPE":"0","FIELD_VALUE":"1","TIMESTAMP_INVALID":"0"}}]
                """ : """
                [{"service_id":"service-a","reference_status":"MATCHED","reference_usage":"USED","service_name":"Service A",
                "owner_team":null,"service_tier":null,"event_count":"2","missing_reference_event_count":"0",
                "missing_optional_attributes":["owner_team","service_tier"]},
                {"service_id":"service-b","reference_status":"MISSING","reference_usage":"ABSENT","service_name":null,
                "owner_team":null,"service_tier":null,"event_count":"1","missing_reference_event_count":"1","missing_optional_attributes":[]},
                {"service_id":"service-c","reference_status":"MATCHED","reference_usage":"UNUSED","service_name":"Service C",
                "owner_team":"example-service","service_tier":"standard","event_count":"0","missing_reference_event_count":"0","missing_optional_attributes":[]}]
                """));
        body.set("totals", JSON.readTree(input ? """
                {"input_batch_count":"1","input_file_count":"1","input_byte_length":"400","physical_occurrence_count":"4",
                "valid_occurrence_count":"3","accepted_representative_count":"2","duplicate_occurrence_count":"1","rejected_occurrence_count":"1",
                "distinct_valid_identity_count":"2","duplicate_excess_in_scope":"1"}
                """ : """
                {"event_count":"3","matched_event_count":"2","missing_event_count":"1","event_service_count":"2",
                "matched_service_count":"1","missing_service_count":"1","matched_services_missing_optional_attributes_count":"1",
                "reference_service_count":"2","unused_reference_service_count":"1"}
                """));
        return body;
    }
    @Test void nativeProjectionsRetainExactSemantics() {
        var input = decoder.decode(result(coverage(true)),query(true));
        assertEquals("-1",input.data().getFirst().get("minReceiptDelayUs"));
        assertEquals("2", input.totals().get("distinctValidIdentityCount"));
        var reference = decoder.decode(result(coverage(false)),query(false));
        assertEquals(List.of("owner_team","service_tier"),reference.data().getFirst().get("missingOptionalAttributes"));
        assertEquals("ABSENT",reference.data().get(1).get("referenceUsage"));
        assertEquals("UNUSED",reference.data().get(2).get("referenceUsage"));
        assertEquals("3",reference.totals().get("eventCount"));
    }
    @Test void nativeTotalsRemainScopeWideAndStoredDuplicatesNeedNotEqualExcess() {
        var body = coverage(true); ((ObjectNode)body.get("filters")).put("source_id","source-a");
        var totals=(ObjectNode)body.get("totals"); totals.put("distinct_valid_identity_count","3"); totals.put("duplicate_excess_in_scope","0");
        var scoped = new ReliabilityCoverageQuery("input_coverage","source-a",null,null,100,0);
        assertEquals("0",decoder.decode(result(body),scoped).totals().get("duplicateExcessInScope"));
        body.putArray("rows"); body.put("offset","1000000");
        var empty = decoder.decode(result(body),new ReliabilityCoverageQuery("input_coverage","source-a",null,null,100,1_000_000));
        assertTrue(empty.data().isEmpty()); assertEquals("4",empty.totals().get("physicalOccurrenceCount"));
        body=coverage(false); body.put("limit","1"); body.put("has_more",true);
        body.putArray("rows").add(coverage(false).get("rows").get(0));
        assertEquals("3",decoder.decode(result(body),new ReliabilityCoverageQuery("reference_coverage",null,null,null,1,0)).totals().get("eventCount"));
    }
    @Test void filteredTotalsRemainBoundedEvenBeyondLastPage() {
        var input = coverage(true); input.putArray("rows"); input.put("offset", "1000000");
        var filters = (ObjectNode) input.get("filters"); filters.put("source_id", "source-a"); filters.put("batch_id", "b1");
        var totals = (ObjectNode) input.get("totals"); totals.put("input_batch_count", "2"); totals.put("input_file_count", "2");
        var exactBatch = new ReliabilityCoverageQuery("input_coverage", "source-a", "b1", null, 100, 1_000_000);
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID,
                assertThrows(ApiFailure.class, () -> decoder.decode(result(input), exactBatch)).code());
        // Batch ID alone can legitimately identify multiple source-scoped batches.
        filters.putNull("source_id");
        assertEquals("2", decoder.decode(result(input), new ReliabilityCoverageQuery("input_coverage", null, "b1", null, 100, 1_000_000))
                .totals().get("inputBatchCount"));
        var reference = coverage(false); reference.putArray("rows"); reference.put("offset", "1000000");
        ((ObjectNode) reference.get("filters")).put("service_id", "service-a");
        var exactService = new ReliabilityCoverageQuery("reference_coverage", null, null, "service-a", 100, 1_000_000);
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID,
                assertThrows(ApiFailure.class, () -> decoder.decode(result(reference), exactService)).code());
    }

    @Test void rejectedOnlyAndZeroByteBatchesHaveNullRanges() {
        var body=coverage(true); var row=row(body); var totals=(ObjectNode)body.get("totals");
        for(String key:List.of("valid_occurrence_count","accepted_representative_count","duplicate_occurrence_count")) {row.put(key,"0");totals.put(key,"0");}
        row.put("physical_occurrence_count","1"); totals.put("physical_occurrence_count","1"); row.put("future_occurrence_count","0");
        totals.put("distinct_valid_identity_count","0");totals.put("duplicate_excess_in_scope","0");
        for(String key:List.of("first_event_time","last_event_time","min_receipt_delay_us","max_receipt_delay_us")) row.putNull(key);
        assertNull(decoder.decode(result(body),query(true)).data().getFirst().get("firstEventTime"));
        for(String key:List.of("physical_occurrence_count","rejected_occurrence_count","input_byte_length")) {row.put(key,"0");totals.put(key,"0");}
        for(String key:ReliabilityCoverageDecoder.REASONS) ((ObjectNode)row.get("rejected_reason_counts")).put(key,"0");
        assertEquals("0",decoder.decode(result(body),query(true)).data().getFirst().get("inputByteLength"));
    }
    @Test void exactExtremeDelaysAndEmptyScopes() {
        var body = coverage(true); var row = row(body);
        row.put("received_at", "9999-12-31T23:59:59.999999Z");
        row.put("first_event_time", "0001-01-01T00:00:00.000000Z");
        row.put("last_event_time", "0001-01-01T00:00:00.000000Z");
        row.put("min_receipt_delay_us", "315537897599999999"); row.put("max_receipt_delay_us", "315537897599999999");
        row.put("future_occurrence_count", "0");
        assertEquals("315537897599999999", decoder.decode(result(body), query(true)).data().getFirst().get("maxReceiptDelayUs"));
        row.put("received_at", "0001-01-01T00:00:00.000000Z");
        row.put("first_event_time", "9999-12-31T23:59:59.999999Z"); row.put("last_event_time", "9999-12-31T23:59:59.999999Z");
        row.put("min_receipt_delay_us", "-315537897599999999"); row.put("max_receipt_delay_us", "-315537897599999999"); row.put("future_occurrence_count", "3");
        assertEquals("-315537897599999999", decoder.decode(result(body), query(true)).data().getFirst().get("minReceiptDelayUs"));
        for (boolean input : List.of(true,false)) {
            var empty = coverage(input); empty.putArray("rows");
            var totals = (ObjectNode) empty.get("totals");
            for (var entry : totals.properties()) totals.put(entry.getKey(), "0");
            assertTrue(decoder.decode(result(empty), query(input)).data().isEmpty());
        }
    }

    @Test void unconfiguredAndMissingRuntimeNeverLaunch() {
        var configured = settings();
        for (boolean missingPublication : List.of(true,false)) {
            seen = null;
            var properties = new ReliabilityAnalyticsProperties(missingPublication ? configured.pythonExecutable() : "/does-not-exist/python",
                    configured.processingRoot(), configured.root(), configured.batchRoot(), configured.referenceRoot(), missingPublication ? "" : PUBLICATION);
            var unavailable = new ReliabilityAnalyticsAdapter(properties, new BoundedProcessRunner(properties()) {
                @Override public Result run(List<String> command, Path directory) { seen = command; throw new AssertionError("Unexpected child"); }
            });
            assertEquals(missingPublication ? ApiFailure.Code.PUBLICATION_UNAVAILABLE : ApiFailure.Code.ANALYTICS_RUNTIME_UNAVAILABLE,
                    assertThrows(ApiFailure.class, () -> unavailable.coverage(query(true))).code());
            assertNull(seen);
        }
        var command = adapter.command(new ReliabilityCoverageQuery("input_coverage",null,"b1",null,1000,1000000));
        assertTrue(command.contains("--batch-id")); assertFalse(command.contains("--source-id")); assertFalse(command.contains("--service-id"));
        assertFalse(command.contains("--start")); assertFalse(command.contains("--environment"));
    }

    @Test void rejectsMalformedInputAndTotals() {
        for(String key:List.of("input_file_count","physical_occurrence_count","valid_occurrence_count","duplicate_occurrence_count")) {
            for(String value:List.of("-1","01","1e2","1.0","9223372036854775808")) bad(true,b -> row(b).put(key,value));
            bad(true,b -> row(b).put(key,1));
        }
        for(String value:List.of("-0","+1","01","-01","1e2","9999999999999999999")) bad(true,b -> row(b).put("min_receipt_delay_us",value));
        for(String value:List.of("0000-01-01T00:00:00.000000Z","2026-02-30T00:00:00.000000Z","10000-01-01T00:00:00.000000Z","1970-01-01T00:00:00Z")) bad(true,b -> row(b).put("received_at",value));
        bad(true,b -> row(b).put("max_receipt_delay_us","2"));
        bad(true,b -> row(b).put("future_occurrence_count","3"));
        bad(true,b -> row(b).put("ingestion_id","unsafe"));
        bad(true,b -> row(b).put("batch_fingerprint","latest"));
        bad(true,b -> row(b).put("batch_id","x".repeat(65)));
        bad(true,b -> ((ObjectNode)row(b).get("rejected_reason_counts")).put("UNKNOWN","1"));
        bad(true,b -> ((ObjectNode)row(b).get("rejected_reason_counts")).put("FIELD_VALUE","2"));
        bad(true,b -> ((ObjectNode)b.get("totals")).put("duplicate_excess_in_scope","0"));
        bad(true,b -> ((ObjectNode)b.get("totals")).put("input_byte_length","401"));
        bad(true,b -> b.put("has_more",true));
        bad(true,b -> b.putArray("rows"));
    }
    @Test void rejectsReferenceStatesOptionalOrderAndTotals() {
        bad(false,b -> row(b).put("reference_usage","UNUSED"));
        bad(false,b -> row(b).put("reference_status","MISSING"));
        bad(false,b -> row(b).put("missing_reference_event_count","2"));
        bad(false,b -> row(b).putArray("missing_optional_attributes").add("service_tier").add("owner_team"));
        for(String name:List.of("", " Service", "Service\u00a0", "a\nb", "a\u007fb", "x".repeat(201))) bad(false,b -> row(b).put("service_name",name));
        bad(false,b -> row(b).put("owner_team","private-team"));
        bad(false,b -> row(b).put("service_id","service-z"));
        bad(false,b -> ((ObjectNode)b.get("totals")).put("event_count","4"));
        bad(false,b -> ((ObjectNode)b.get("totals")).put("matched_services_missing_optional_attributes_count","0"));
        bad(false,b -> ((ObjectNode)b.get("totals")).put("raw_path","private"));
        bad(false,b -> ((ObjectNode)b.get("filters")).put("source_id", "x"));
    }
    @Test void nativeResponseBudgetsCannotBeExpandedByRunnerConfiguration() {
        String oversized = JSON.writeValueAsString(coverage(true)) + " ".repeat(1_048_576);
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID,
                assertThrows(ApiFailure.class, () -> decoder.decode(output(0, oversized), query(true))).code());
        var body = coverage(false); var template = row(body).deepCopy();
        template.put("service_name", "α".repeat(100));
        var rows = body.putArray("rows");
        for (int i = 0; i < 1000; i++) rows.add(template.deepCopy().put("service_id", "service-%04d".formatted(i)));
        body.put("limit", "1000");
        var totals = (ObjectNode) body.get("totals");
        for (String key : List.of("event_count", "matched_event_count")) totals.put(key, "2000");
        for (String key : List.of("event_service_count", "matched_service_count", "reference_service_count", "matched_services_missing_optional_attributes_count")) totals.put(key, "1000");
        for (String key : List.of("missing_event_count", "missing_service_count", "unused_reference_service_count")) totals.put(key, "0");
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID, assertThrows(ApiFailure.class,
                () -> decoder.decode(result(body), new ReliabilityCoverageQuery("reference_coverage", null, null, null, 1000, 0))).code());
    }

    @Test void rejectsMalformedEnvelopesAndFailedProcesses() {
        for(boolean input:List.of(true,false)) {
            bad(input,b -> b.put("product_contract_version",1)); bad(input,b -> b.put("processor_revision",2.0));
            bad(input,b -> b.put("input_format","CSV")); bad(input,b -> b.put("product_fingerprint","a"));
            bad(input,b -> b.put("raw_path","private")); bad(input,b -> b.put("limit",100));
            bad(input,b -> ((ObjectNode)b.get("reference")).put("snapshot_fingerprint","latest"));
            String text=JSON.writeValueAsString(coverage(input));
            for(String malformed:List.of(text+"{}",text.replace("\"status\":\"OK\"","\"status\":\"OK\",\"status\":\"OK\""),"private/path"))
                assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID,assertThrows(ApiFailure.class,() -> decoder.decode(output(0,malformed),query(input))).code());
            assertEquals(ApiFailure.Code.ANALYTICS_EXECUTION_FAILED,assertThrows(ApiFailure.class,() -> decoder.decode(output(1,"private/path"),query(input))).code());
            assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID,assertThrows(ApiFailure.class,() -> decoder.decode(new BoundedProcessRunner.Result(0,new byte[]{(byte)0xff}),query(input))).code());
        }
    }
    private void bad(boolean input,Consumer<ObjectNode> change) {
        var body=coverage(input);change.accept(body);
        assertEquals(ApiFailure.Code.ANALYTICS_RESPONSE_INVALID,assertThrows(ApiFailure.class,() -> decoder.decode(result(body),query(input))).code());
    }

    private List<String> seen;
    private ApiFailure.Code failure;
    private final ReliabilityAnalyticsAdapter adapter = new ReliabilityAnalyticsAdapter(settings(),new BoundedProcessRunner(properties()) {
        @Override public Result run(List<String> command,Path directory) {
            seen=command; if(failure != null) throw new ApiFailure(failure);
            var body=coverage(command.get(command.indexOf("--report")+1).equals("input_coverage"));
            for(String name:List.of("source-id","batch-id","service-id")) {
                int index=command.indexOf("--"+name); if(index>=0) ((ObjectNode)body.get("filters")).put(name.replace('-','_'),command.get(index+1));
            }
            for(String name:List.of("limit","offset")) body.put(name,command.get(command.indexOf("--"+name)+1));
            return result(body);
        }
    });
    private final MockMvc mvc=MockMvcBuilders.standaloneSetup(new ReliabilityRequestController(adapter)).setControllerAdvice(new ApiExceptionHandler()).build();
    @Test void httpProjectionAndFixedCommands() throws Exception {
        mvc.perform(get("/api/v1/reliability/input-coverage").param("sourceId","source-a").param("batchId","b1"))
                .andExpect(status().isOk()).andExpect(header().string("Cache-Control","no-store"))
                .andExpect(jsonPath("$.totals.duplicateExcessInScope").value("1"))
                .andExpect(jsonPath("$.data[0].minReceiptDelayUs").value("-1"))
                .andExpect(jsonPath("$.data[0].rejectedReasonCounts.FIELD_MISSING").value("1"));
        assertTrue(seen.contains("--source-id"));assertTrue(seen.contains("--batch-id"));assertFalse(seen.contains("--start"));
        mvc.perform(get("/api/v1/reliability/reference-coverage")).andExpect(status().isOk())
                .andExpect(jsonPath("$.data[2].referenceUsage").value("UNUSED"))
                .andExpect(jsonPath("$.totals.eventCount").value("3"));
    }
    @Test void invalidFiltersFailBeforeChildAndErrorsAreSafeAndUncached() throws Exception {
        for(String endpoint:List.of("input-coverage","reference-coverage")) {
            for(String[] bad:List.of(new String[]{"start",START},new String[]{"environment","test"},new String[]{"limit","01"},
                    new String[]{"limit","0"},new String[]{"limit","1001"},new String[]{"offset","1000001"},new String[]{"offset","-1"},
                    new String[]{"sql","select 1"},new String[]{"root","/private"},new String[]{"publication",PUBLICATION},
                    new String[]{endpoint.equals("input-coverage") ? "serviceId" : "sourceId","x"},
                    new String[]{endpoint.equals("input-coverage") ? "batchId" : "serviceId",""})) {
                seen=null;
                mvc.perform(get("/api/v1/reliability/"+endpoint).param(bad[0],bad[1])).andExpect(status().isBadRequest())
                        .andExpect(header().string("Cache-Control","no-store")).andExpect(content().json("{\"code\":\"INVALID_ARGUMENT\"}"));
                assertNull(seen);
            }
            seen=null; mvc.perform(get("/api/v1/reliability/"+endpoint).param("limit","1","2")).andExpect(status().isBadRequest());assertNull(seen);
            for(ApiFailure.Code code:ApiFailure.Code.values()) {
                failure=code;
                mvc.perform(get("/api/v1/reliability/"+endpoint)).andExpect(status().is(code.status()))
                        .andExpect(header().string("Cache-Control","no-store")).andExpect(content().string("{\"code\":\""+code.name()+"\"}"));
            }
            failure=null;
        }
    }
}
