package platform.contentapi.management;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;

/** Real source/reference pressure. No widened runtime, synthetic native files or dispatch authority. */
class NarrowContentPressureIT extends NarrowContentPipelineFixtures {
    byte[] pressurePayload;
    @Override List<String> referenceCommand()throws Exception{return command("reference",roots.get(REFS).toString(),REFS,"10000");}
    @Override byte[] responsePayload(){return pressurePayload==null?super.responsePayload():pressurePayload;}
    private List<String> command(String... args)throws Exception{
        var c=new ArrayList<>(List.of(System.getenv("DDIP_TEST_PYTHON"),"-I","-B",Path.of("../../processing/tests/content_pressure_fixture.py").toRealPath().toString()));
        c.addAll(List.of(args));return c;
    }
    private byte[] fixture(String... args)throws Exception{
        var p=new ProcessBuilder(command(args)).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        try(var readers=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()){
            var output=readers.submit(()->p.getInputStream().readNBytes(5*1024*1024+1));
            try{assertThat(p.waitFor(90,TimeUnit.SECONDS)).isTrue();assertThat(p.exitValue()).isZero();byte[] bytes=output.get(10,TimeUnit.SECONDS);assertThat(bytes.length).isLessThanOrEqualTo(5*1024*1024);return bytes;}
            finally{if(p.isAlive()){p.destroyForcibly();p.waitFor(10,TimeUnit.SECONDS);}output.cancel(true);}
        }
    }
    private Path contentCommand(UUID attempt){
        UUID slot=jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.content_pipeline_operations WHERE attempt_id=? AND step_index=14",UUID.class,attempt);
        UUID run=jdbc.queryForObject("SELECT run_id FROM ddip_management.attempts WHERE attempt_id=?",UUID.class,attempt);
        return binding.root().resolve("runs/"+run+"/attempts/"+attempt+"/publish-content/"+slot+"/command.json");
    }
    private JsonNode inspect(UUID attempt,boolean complete)throws Exception{
        return SourceJson.parse(fixture(complete?"inspection":"incomplete",contentCommand(attempt).toString()),1024*1024);
    }
    private void counts(JsonNode observed,int articles){
        assertThat(observed.get("catalogRows").intValue()).isEqualTo(articles);
        assertThat(observed.get("referenceCoverageRows").intValue()).isEqualTo(10000);
        assertThat(observed.get("coverage").get("reference_source_count").asText()).isEqualTo("10000");
        assertThat(observed.get("coverage").get("unused_reference_source_count").asText()).isEqualTo("9999");
    }
    @Test void exactRowsWithMaximumReferenceAndMixedSilverReachVerifiedSuccess()throws Exception{
        pressurePayload=fixture("feed","61440,61440");
        // Native rejections populate the second Silver dataset without fabricating its layout.
        pressurePayload=new String(pressurePayload,java.nio.charset.StandardCharsets.UTF_8).replace("</feed>","<entry><id>urn:pressure:rejected</id></entry></feed>").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        original();var completed=NarrowContentPipelineTestSupport.execute(jdbc,tx,workers,guard,owner,PIPELINE_PROFILE,e->{});
        var before=filesystem();int reads=objectRequests.get();var observed=inspect(owner.attemptId(),true);counts(observed,2);
        assertThat(observed.get("catalogMinBytes").intValue()).isEqualTo(61440);assertThat(observed.get("catalogMaxBytes").intValue()).isEqualTo(61440);
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        assertThat(count("content_pipeline_checkpoints")).isEqualTo(15);assertThat(completed.artifact()).isNotNull();
        var publish=SourceJson.parse(Files.readAllBytes(contentCommand(owner.attemptId())),1024*1024);
        var silver=publish.get("goldProof").get("command").get("inputs").get(0).get("qualification").get("verification");
        assertThat(silver.get("native").get("counts").get("accepted").intValue()).isEqualTo(2);
        assertThat(silver.get("native").get("counts").get("rejected").intValue()).isEqualTo(1);
        var report=NarrowContentEnvelopeTestSupport.measure(jdbc,attemptRoot(),owner.runId(),owner.attemptId(),"demo-feed");
        report.set("nativePressure",observed);System.out.println("CONTENT_PRESSURE_MEASUREMENT "+report);
        assertThat(filesystem()).isEqualTo(before);assertThat(objectRequests).hasValue(reads);assertThat(requests).hasValue(1);assertThat(objectWrites).hasValue(2);
        assertThat(NarrowContentPipelineTestSupport.legacyCapabilitiesDispatchContent()).isFalse();
    }
    @Test void oversizedRowBeyondSmokePageFailsBeforeContentCompletionWithoutReplay()throws Exception{
        pressurePayload=fixture("feed","late-oversize");original();
        assertThatThrownBy(()->NarrowContentPipelineTestSupport.execute(jdbc,tx,workers,guard,owner,PIPELINE_PROFILE,e->{})).isInstanceOf(RuntimeException.class);
        Path command=contentCommand(owner.attemptId());var result=SourceJson.parse(Files.readAllBytes(command.resolveSibling("result.json")));
        assertThat(result.get("status").stringValue()).isEqualTo("FAILED");assertThat(result.get("code").stringValue()).isEqualTo("RESOURCE_LIMIT");
        var c=SourceJson.parse(Files.readAllBytes(command),1024*1024);
        Path nativeRoot=Path.of(c.get("content").get("root").stringValue()).resolve("v1/processor_revision=1/publication="+c.get("prepared").get("fingerprint").stringValue());
        assertThat(nativeRoot.resolve("complete.tmp")).doesNotExist();assertThat(nativeRoot.resolve("complete.json")).doesNotExist();
        assertThat(count("content_pipeline_checkpoints")).isEqualTo(12);assertThat(count("content_pipeline_completions")).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='CONTENT'",Long.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());
        var before=filesystem();var beforeRows=allRows();int reads=objectRequests.get();var observed=inspect(owner.attemptId(),false);assertThat(observed.get("catalogRows").intValue()).isEqualTo(12);
        assertThat(observed.get("catalogMaxBytes").intValue()).isEqualTo(61441);assertThat(observed.get("oversizedCatalogRows").intValue()).isEqualTo(1);assertThat(observed.get("oversizedFirstRank").intValue()).isGreaterThan(10);
        assertThatThrownBy(()->NarrowContentPipelineTestSupport.execute(jdbc,tx,workers,guard,owner,PIPELINE_PROFILE,e->{})).isInstanceOf(RuntimeException.class);
        assertThat(filesystem()).isEqualTo(before);assertThat(objectRequests).hasValue(reads);assertThat(requests).hasValue(1);assertThat(objectWrites).hasValue(2);
        assertThat(allRows()).isEqualTo(beforeRows);
        assertThat(NarrowContentPipelineTestSupport.legacyCapabilitiesDispatchContent()).isFalse();System.out.println("CONTENT_PRESSURE_REJECTION "+observed);
    }
    @Test void fullPressurePrefixSafelyEndsAfterWorkerDeathAndSurvivesDatabaseRestart()throws Exception{
        pressurePayload=fixture("feed","61440,61440");objectError=null;UUID run=queue("pressure-crash"),attempt=UUID.randomUUID();
        Path descriptor=temp.resolve("pressure-binding.json"),signal=temp.resolve("pressure-signal"),log=temp.resolve("pressure-worker.log");Files.write(descriptor,binding.descriptor());
        var builder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx512m","-XX:ActiveProcessorCount=2","-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                "platform.contentapi.execution.NarrowContentPipelineCrashTestProcess",descriptor.toString(),CONTENT_PROFILE,run.toString(),attempt.toString(),PIPELINE_PROFILE,"RECHECK_CONTENT","BEFORE_SUCCESS",signal.toString()).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().put("DDIP_TEST_DB_URL",url);builder.environment().put("DDIP_TEST_DB_USER",user);builder.environment().put("DDIP_TEST_DB_PASSWORD",password);
        var child=builder.start();var descendants=new LinkedHashSet<ProcessHandle>();
        try{
            long deadline=System.nanoTime()+TimeUnit.MINUTES.toNanos(10);while(!Files.exists(signal) && child.isAlive() && System.nanoTime()<deadline){child.descendants().forEach(descendants::add);Thread.sleep(100);}
            assertThat(Files.exists(signal)).withFailMessage("Pressure worker did not reach boundary: %s",Files.readString(log)).isTrue();
            assertThat(Files.readString(signal)).isEqualTo("RECHECK_CONTENT:BEFORE_SUCCESS");child.descendants().forEach(descendants::add);assertThat(descendants).noneMatch(ProcessHandle::isAlive);
            child.destroyForcibly();assertThat(child.waitFor(30,TimeUnit.SECONDS)).isTrue();
        }finally{
            child.descendants().forEach(descendants::add);
            if(child.isAlive()){child.destroyForcibly();child.waitFor(30,TimeUnit.SECONDS);}
            // Exact disposable test processes only. Cleanup is not proof of operator quiescence.
            for(var p:descendants)if(p.isAlive())p.destroyForcibly();
            for(var p:descendants)if(p.isAlive())p.onExit().get(30,TimeUnit.SECONDS);
        }
        assertThat(count("content_pipeline_checkpoints")).isEqualTo(15);assertThat(count("content_pipeline_completions")).isZero();
        guard=InstallationHostGuard.acquire(binding);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);session=workers.register(guard,CONTENT_PROFILE,digest(CONTENT_PROFILE),NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,CONTENT_PROFILE));
        assertThat(session.status()).isEqualTo("RECOVERY_ONLY");var before=filesystem();int reads=objectRequests.get();
        var assessment=NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,run,attempt);assertThat(assessment.authorization()).isFalse();assertThat(assessment.integrity()).isEqualTo("CURRENT_BYTES_AND_INVENTORY_MATCH");
        var observed=inspect(attempt,true);counts(observed,2);assertThat(observed.get("catalogMaxBytes").intValue()).isEqualTo(61440);
        long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);
        var request=SourceJson.object().put("action","RECONCILE").put("attemptId",attempt.toString()).put("quiescenceConfirmed",true);
        UUID control=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL).request(ControlCommands.parse(run.toString(),SourceJson.canonical(request),"pressure-terminal","\""+version+"\"")).get("controlId").stringValue());
        var terminal=new NarrowContentTerminalTestSupport(jdbc,tx,INSTALL);UUID terminalId=UUID.randomUUID();var target=terminal.inspect(guard,run,attempt,control,UUID.randomUUID(),terminalId);
        assertThat(terminal.finish(guard,terminal.qualify(guard,target,Instant.now(),""))).isEqualTo("COMMITTED");
        var receipt=jdbc.queryForMap("SELECT target_bytes,review_bytes,snapshot_bytes FROM ddip_management.content_original_terminal_receipts WHERE terminal_id=?",terminalId);
        var sizes=SourceJson.object();for(String field:List.of("target","review","snapshot")){byte[] raw=(byte[])receipt.get(field+"_bytes");int cap=field.equals("snapshot")?65536:8192;assertThat(raw.length).isLessThanOrEqualTo(cap);assertThat(SourceJson.canonical(SourceJson.parse(raw,cap))).isEqualTo(raw);sizes.put(field+"Bytes",raw.length);}
        assertThat(ContentTerminalSqlTestSupport.assertFrozenBindings(jdbc,terminalId)).isPositive();
        var rows=allRows();guard.close();guard=null;NarrowContentRestartTestSupport.restart(jdbc,attempt);
        assertThat(allRows()).isEqualTo(rows);assertThat(new NarrowContentTerminalTestSupport(jdbc,tx,INSTALL).lookup(terminalId)).isEqualTo("CONFIRMED_TERMINAL");
        var lookup=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx128m","-XX:ActiveProcessorCount=2","-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),"platform.contentapi.execution.NarrowContentTerminalLookupTestProcess",url,user,password,INSTALL.toString(),terminalId.toString()).redirectErrorStream(true).start();
        try{assertThat(lookup.waitFor(30,TimeUnit.SECONDS)).isTrue();assertThat(lookup.exitValue()).withFailMessage(new String(lookup.getInputStream().readAllBytes(),java.nio.charset.StandardCharsets.UTF_8)).isZero();}
        finally{if(lookup.isAlive()){lookup.destroyForcibly();lookup.waitFor(10,TimeUnit.SECONDS);}}
        assertThat(allRows()).isEqualTo(rows);
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        assertThat(filesystem()).isEqualTo(before);assertThat(objectRequests).hasValue(reads);assertThat(requests).hasValue(1);assertThat(objectWrites).hasValue(2);assertThat(NarrowContentPipelineTestSupport.legacyCapabilitiesDispatchContent()).isFalse();
        System.out.println("CONTENT_PRESSURE_TERMINAL "+sizes);
    }
    private Map<String,List<String>> allRows(){var rows=new TreeMap<String,List<String>>();for(String table:jdbc.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='ddip_management' ORDER BY tablename",String.class))rows.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.\""+table.replace("\"","\"\"")+"\" t ORDER BY to_jsonb(t)::text",String.class));return rows;}
}
