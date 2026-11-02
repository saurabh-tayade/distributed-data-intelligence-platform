package platform.contentapi.management;

import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.sql.*;
import java.lang.reflect.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import platform.contentapi.execution.ManagementTransactions;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.management.SourceFixtures.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL read model + real loopback HTTP. Future states are SQL fixtures only, never worker execution. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RunHistoryIT extends RunApiFixtures {
    RunHistoryRepository history(){return new RunHistoryRepository(jdbc,tx,new SourceCursor(KEY));}
    String run(){return submit(content(),"run-"+UUID.randomUUID()).get("runId").stringValue();}
    UUID uuid(String id){return UUID.fromString(id);}
    List<String> ids(JsonNode data){var out=new ArrayList<String>();for(var row:data)out.add(row.get("runId").stringValue());return out;}
    record Response(int status,JsonNode json,HttpHeaders headers,int size) { }
    Response real(String path)throws Exception{return real(path,null);}
    Response real(String path,String originHeader)throws Exception {
        start();var builder=HttpRequest.newBuilder(URI.create(origin+path)).timeout(Duration.ofSeconds(10)).GET();if(originHeader!=null)builder.header("Origin",originHeader);
        try(var client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()){var r=client.send(builder.build(),HttpResponse.BodyHandlers.ofByteArray());return new Response(r.statusCode(),SourceJson.JSON.readTree(r.body()),r.headers(),r.body().length);}
    }
    void error(Response r,int status,String code){assertThat(r.status()).isEqualTo(status);assertThat(r.json().get("code").stringValue()).isEqualTo(code);assertThat(r.json().propertyNames()).containsExactlyInAnyOrder("apiVersion","code","message","correlationId","retryable");assertThat(new String(SourceJson.canonical(r.json()),java.nio.charset.StandardCharsets.UTF_8)).doesNotContain(password,url,"private","PRIVATE","jdbc:","metadata.json");}
    String next(Response r){return r.json().get("meta").get("nextCursor").stringValue();}
    JsonNode data(Response r){assertThat(r.status()).isEqualTo(200);assertThat(r.json().propertyNames()).containsExactlyInAnyOrder("apiVersion","data","meta");assertThat(r.headers().firstValue("Cache-Control")).hasValue("no-store");assertThat(r.size()).isLessThanOrEqualTo(1048576);return r.json().get("data");}
    @Test void emptyHistoryAndQueuedDetailHaveNoInventedEvidence()throws Exception {
        var empty=real(RUNS);assertThat(data(empty).isEmpty()).isTrue();assertThat(empty.json().get("meta").get("limit").intValue()).isEqualTo(20);assertThat(empty.json().get("meta").get("hasMore").booleanValue()).isFalse();assertThat(empty.json().get("meta").get("nextCursor").isNull()).isTrue();
        String id=run();var detail=data(real(RUNS+"/"+id));assertThat(detail.propertyNames()).containsExactlyInAnyOrder("runId","productId","pipeline","state","currentStage","rowVersion","requestedAt","finishedAt","sourceRevisions","inputArtifactIds","attempts","latestControl","result","counts","outcomeCode","retryEligibility");assertThat(detail.get("attempts").get("count").stringValue()).isEqualTo("0");for(String k:List.of("currentStage","finishedAt","latestControl","result","counts"))assertThat(detail.get(k).isNull()).isTrue();assertThat(detail.get("attempts").get("currentAttemptId").isNull()).isTrue();assertThat(data(real(RUNS+"/"+id+"/attempts")).isEmpty()).isTrue();var events=data(real(RUNS+"/"+id+"/events"));assertThat(events.size()).isEqualTo(1);assertThat(events.get(0).get("action").stringValue()).isEqualTo("RUN_SUBMITTED");assertThat(events.get(0).get("beforeState").isNull()).isTrue();assertThat(count("attempts")).isZero();
    }
    @Test void descendingOrderProductSourceStateAndCombinedFilters() {
        String first=run(),second=submit(reliability(false),"reliability").get("runId").stringValue(),last=run();jdbc.update("UPDATE ddip_management.runs SET state='FAILED',finished_at=now(),outcome_code='INPUT_INVALID',row_version=row_version+1 WHERE run_id=?",uuid(first));
        assertThat(ids(history().runs(null,null,null,20,null).data())).containsExactly(last,second,first);assertThat(ids(history().runs(CONTENT,null,null,20,null).data())).containsExactly(last,first);assertThat(ids(history().runs(null,"demo-feed",null,20,null).data())).containsExactly(last,first);assertThat(ids(history().runs(null,null,"FAILED",20,null).data())).containsExactly(first);assertThat(ids(history().runs(CONTENT,"demo-feed","QUEUED",20,null).data())).containsExactly(last);assertThat(history().runs(RELIABILITY,"demo-feed",null,20,null).data().isEmpty()).isTrue();
    }
    @Test void sourceAndInputJoinsNeverDuplicateRuns() {
        String id=run();tx.required(()->{byte[] b=SourceJson.canonical(rss());jdbc.update("INSERT INTO ddip_management.sources(product_id,source_id,profile_family,current_revision,enabled) VALUES (?,'another','rss-atom-v1',1,true)",CONTENT);jdbc.update("INSERT INTO ddip_management.source_revisions(product_id,source_id,revision,display_name,config_version,configuration_bytes,configuration_sha256,actor) VALUES (?,'another',1,'Another',1,?,?,'local-operator')",CONTENT,b,SourceJson.sha(b));jdbc.update("INSERT INTO ddip_management.run_sources VALUES (?,?,'another',1,0,true)",uuid(id),CONTENT);return null;});
        assertThat(ids(history().runs(null,null,null,20,null).data())).containsExactly(id);assertThat(ids(history().runs(null,"another",null,20,null).data())).containsExactly(id);assertThat(ids(history().runs(null,"demo-feed",null,20,null).data())).containsExactly(id);
    }
    void historyRows(int count){byte[] b=bytes("{}");String hash=SourceJson.sha(b);jdbc.update("INSERT INTO ddip_management.runs(run_id,product_id,pipeline,pipeline_version,principal_scope,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,state,finished_at) SELECT gen_random_uuid(),'content-intelligence','CONTENT_RSS_V1',1,'local-operator','history-'||n,?::bytea,?::text,?::bytea,?::text,?::text,'FAILED',clock_timestamp() FROM generate_series(1,?) n",b,hash,b,hash,CONTENT_PROFILE,count);}
    @Test void defaultBoundaryMaximumAndStaticTraversal()throws Exception {
        historyRows(205);var page=real(RUNS);assertThat(data(page).size()).isEqualTo(20);assertThat(page.json().get("meta").get("hasMore").booleanValue()).isTrue();var ids=new ArrayList<>(ids(data(page)));String cursor=next(page);int pages=1;
        while(cursor!=null){page=real(RUNS+"?limit=100&cursor="+cursor);ids.addAll(ids(data(page)));cursor=page.json().get("meta").get("nextCursor").isNull()?null:next(page);pages++;}assertThat(ids.size()).isEqualTo(205);assertThat(new HashSet<>(ids)).hasSize(205);assertThat(pages).isEqualTo(3);var expected=jdbc.queryForList("SELECT run_id FROM ddip_management.runs ORDER BY submission_seq DESC",UUID.class).stream().map(UUID::toString).toList();assertThat(ids).isEqualTo(expected);
    }
    @Test void newArrivalIsExcludedAndStateMembershipIsNotSnapshotIsolation() {
        String a=run(),b=run(),c=run();var first=history().runs(null,null,"QUEUED",1,null);assertThat(ids(first.data())).containsExactly(c);String newer=run();jdbc.update("UPDATE ddip_management.runs SET state='FAILED',finished_at=now(),row_version=row_version+1 WHERE run_id=?",uuid(b));var second=history().runs(null,null,"QUEUED",10,first.nextCursor());assertThat(ids(second.data())).containsExactly(a);assertThat(ids(second.data())).doesNotContain(newer,b);assertThat(count("runs")).isEqualTo(4);
    }
    @Test void cursorsBindFiltersEndpointAndRunAndRejectTampering()throws Exception {
        historyRows(3);var page=real(RUNS+"?limit=1");String token=next(page);error(real(RUNS+"?state=FAILED&cursor="+token),400,"INVALID_ARGUMENT");error(real(RUNS+"?cursor=x"+token.substring(1)),400,"INVALID_ARGUMENT");String id=jdbc.queryForObject("SELECT run_id FROM ddip_management.runs LIMIT 1",UUID.class).toString();error(real(RUNS+"/"+id+"/events?cursor="+token),400,"INVALID_ARGUMENT");
        event(id);event(id);var events=real(RUNS+"/"+id+"/events?limit=1");String eventToken=next(events);String other=run();error(real(RUNS+"/"+other+"/events?cursor="+eventToken),400,"INVALID_ARGUMENT");error(real(RUNS+"/"+id+"/attempts?cursor="+eventToken),400,"INVALID_ARGUMENT");
    }
    @Test void expiredAndChangedSigningKeysFail()throws Exception {
        historyRows(2);var old=new RunHistoryRepository(jdbc,tx,new SourceCursor(KEY),Clock.offset(Clock.systemUTC(),Duration.ofDays(-2)));String token=old.runs(null,null,null,1,null).nextCursor();assertThatThrownBy(()->history().runs(null,null,null,1,token)).hasMessage("INVALID_ARGUMENT");error(real(RUNS+"?cursor="+token),400,"INVALID_ARGUMENT");String valid=history().runs(null,null,null,1,null).nextCursor();var changed=new RunHistoryRepository(jdbc,tx,new SourceCursor(Base64.getUrlEncoder().withoutPadding().encodeToString("01234567890123456789012345678901".getBytes(java.nio.charset.StandardCharsets.US_ASCII))));assertThatThrownBy(()->changed.runs(null,null,null,1,valid)).hasMessage("INVALID_ARGUMENT");
    }
    @Test void cursorSurvivesApiRestartWithSameKey()throws Exception {
        historyRows(5);var first=real(RUNS+"?limit=2");String token=next(first);var before=data(real(RUNS+"?limit=2&cursor="+token));close();var after=data(real(RUNS+"?limit=2&cursor="+token));assertThat(after).isEqualTo(before);assertThat(ids(after)).doesNotContainAnyElementsOf(ids(data(first)));
    }
    @ParameterizedTest @ValueSource(strings={"limit=0","limit=101","limit=-1","limit=1.0","limit=01","limit=","limit=1&limit=2","productId=unknown","sourceId=../x","state=waiting","state=QUEUED&state=FAILED","unknown=x","cursor=","cursor=x"}) void invalidQueries(String query)throws Exception {error(real(RUNS+"?"+query),400,"INVALID_ARGUMENT");}
    @Test void unknownIdsAndStrictSubresourceQueries()throws Exception {
        String id=UUID.randomUUID().toString();for(String suffix:List.of("","/attempts","/events"))error(real(RUNS+"/"+id+suffix),404,"NOT_FOUND");for(String suffix:List.of("","/attempts","/events"))error(real(RUNS+"/bad"+suffix),400,"INVALID_ARGUMENT");id=run();error(real(RUNS+"/"+id+"/attempts?state=FAILED"),400,"INVALID_ARGUMENT");error(real(RUNS+"/"+id+"/events?limit=1&limit=2"),400,"INVALID_ARGUMENT");error(real(RUNS+"/"+id+"?limit=1"),400,"INVALID_ARGUMENT");
    }
    void event(String id){jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,action,before_state,after_state,safe_code,details_bytes) VALUES ('local-operator',?,'RUN_FAILED','QUEUED','FAILED','INPUT_INVALID',?)",uuid(id),bytes("{\"private\":\"PRIVATE /private/secret\"}"));}
    @Test void eventsAreRealOrderedScopedAndExactAboveJavascriptIntegerRange()throws Exception {
        String id=run(),other=run();jdbc.execute("ALTER TABLE ddip_management.audit_events ALTER COLUMN event_seq RESTART WITH 9007199254740993");event(id);event(other);event(id);var rows=data(real(RUNS+"/"+id+"/events"));assertThat(rows.size()).isEqualTo(3);assertThat(rows.get(1).get("eventSeq").stringValue()).isEqualTo("9007199254740993");assertThat(rows.get(2).get("eventSeq").stringValue()).isEqualTo("9007199254740995");assertThat(rows.get(0).get("action").stringValue()).isEqualTo("RUN_SUBMITTED");assertThat(new String(SourceJson.canonical(rows))).doesNotContain("PRIVATE","private","SOURCE_CREATED","correlationId");for(var r:rows)assertThat(r.get("timestamp").stringValue()).matches(".*\\.[0-9]{6}Z");
    }
    UUID worker(){return worker(CONTENT_PROFILE);}
    UUID worker(String profile){UUID instance=UUID.randomUUID();String hash=jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,profile);jdbc.update("INSERT INTO ddip_management.worker_instances(instance_id,installation_id,execution_profile_id,profile_sha256,host_binding_sha256,pid,process_started_at,heartbeat_at,status) VALUES (?,?,?,?,?,1,now(),now(),'STOPPED')",instance,INSTALL,profile,hash,HASH);return instance;}
    UUID attempt(String run,UUID instance,int number,String state){UUID id=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at,outcome_code) VALUES (?,?,?,'EXECUTE',?,?::bigint,?,clock_timestamp(),?)",id,uuid(run),number,instance,number,state,state.equals("SUCCEEDED")?"SUCCEEDED":state.equals("CANCELLED")?"CANCELLED":state.equals("INTERRUPTED")?"INTERRUPTED":"INPUT_INVALID");return id;}
    void stage(UUID attempt,String stage,String state,int ordinal){jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256,intended_namespace,command_address,command_sha256,child_pid,child_started_at,child_instance_id,finished_at) VALUES (?,?,?,?,?,?,'private/namespace','private/command',?,123,now(),?,now())",attempt,stage,ordinal,UUID.randomUUID(),state,HASH,HASH,UUID.randomUUID());}
    @Test void attemptsReadOnlyPersistedStagesCountsAndHighwater()throws Exception {
        String run=run();UUID instance=worker();UUID a=attempt(run,instance,1,"FAILED");stage(a,"CHECK_INPUTS","VERIFIED",0);stage(a,"SILVER","FAILED",0);attempt(run,instance,2,"INTERRUPTED");var page=real(RUNS+"/"+run+"/attempts?limit=1");var rows=data(page);assertThat(rows.size()).isEqualTo(1);assertThat(rows.get(0).get("attemptId").stringValue()).isEqualTo(a.toString());assertThat(rows.get(0).get("attemptNo").intValue()).isEqualTo(1);assertThat(rows.get(0).get("stageCount").stringValue()).isEqualTo("2");attempt(run,instance,3,"FAILED");var rest=data(real(RUNS+"/"+run+"/attempts?cursor="+next(page)));assertThat(rest.size()).isEqualTo(1);assertThat(rest.get(0).get("attemptNo").intValue()).isEqualTo(2);assertThat(new String(SourceJson.canonical(rows))).doesNotContain("private","command","pid","instance","namespace");assertThat(data(real(RUNS+"/"+run)).get("attempts").get("count").stringValue()).isEqualTo("3");
    }
    @ParameterizedTest @ValueSource(strings={"FAILED","CANCELLED","CANCEL_REQUESTED","RUNNING","RECOVERY_REQUIRED"}) void laterStatesAreOnlyDatabaseFixtures(String state)throws Exception {
        String run=run();UUID instance=worker();boolean active=Set.of("CANCEL_REQUESTED","RUNNING","RECOVERY_REQUIRED").contains(state);UUID attempt;
        if(active){attempt=UUID.randomUUID();UUID ownerAttempt=attempt;tx.required(()->{
            jdbc.queryForList("SELECT installation_id FROM ddip_management.coordinator FOR UPDATE");jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state) VALUES (?,?,1,'EXECUTE',?,1,'RUNNING')",ownerAttempt,uuid(run),instance);
            jdbc.update("UPDATE ddip_management.worker_instances SET status=?,row_version=row_version+1 WHERE instance_id=?",state.equals("RECOVERY_REQUIRED")?"RECOVERY_ONLY":"READY",instance);
            jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=?,owner_instance_id=?,epoch=1,heartbeat_at=now(),mode=?,row_version=row_version+1",ownerAttempt,instance,state.equals("RECOVERY_REQUIRED")?"RECOVERY_ONLY":"READY");
            jdbc.update("UPDATE ddip_management.runs SET state=?,current_stage='CHECK_INPUTS',current_attempt_id=?,row_version=row_version+1 WHERE run_id=?",state,ownerAttempt,uuid(run));stage(ownerAttempt,"CHECK_INPUTS",state.equals("RECOVERY_REQUIRED")?"UNKNOWN":"RUNNING",0);return null;});
        }else {attempt=attempt(run,instance,1,state);jdbc.update("UPDATE ddip_management.runs SET state=?,current_stage='CHECK_INPUTS',current_attempt_id=?,row_version=row_version+1,finished_at=now() WHERE run_id=?",state,attempt,uuid(run));}
        var value=data(real(RUNS+"/"+run));assertThat(value.get("state").stringValue()).isEqualTo(state);assertThat(value.get("attempts").get("currentAttemptId").stringValue()).isEqualTo(attempt.toString());assertThat(value.get("result").isNull()).isTrue();assertThat(value.get("counts").isNull()).isTrue();
    }
    @Test void resultIsHistoricalVerifiedNativeIdentityAndCountsRemainExact()throws Exception {
        String run=run();UUID a=attempt(run,worker(),1,"SUCCEEDED"),artifact=UUID.randomUUID();byte[] bytes=bytes("{\"private\":\"PRIVATE /private/secret\"}");jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at) VALUES (?,'CONTENT',?,1,1,?,'content-CONTENT','v1/publication',?,?, '2026-10-21T00:00:00.123456Z')",artifact,CONTENT,HASH,bytes,SourceJson.sha(bytes));stage(a,"VERIFY_RESULT","VERIFIED",0);jdbc.update("INSERT INTO ddip_management.stage_artifacts VALUES (?,'VERIFY_RESULT',0,'RESULT',0,?)",a,artifact);jdbc.update("UPDATE ddip_management.runs SET state='SUCCEEDED',current_attempt_id=?,result_artifact_id=?,finished_at=now(),counts_bytes=?,row_version=row_version+1 WHERE run_id=?",a,artifact,SourceJson.canonical(SourceJson.object().put("rows","9007199254740993")),uuid(run));
        var value=data(real(RUNS+"/"+run));assertThat(value.get("result").get("nativeFingerprint").stringValue()).isEqualTo(HASH);assertThat(value.get("result").get("verificationStatus").stringValue()).isEqualTo("VERIFIED_AT_EXECUTION");assertThat(value.get("result").get("verifiedAt").stringValue()).isEqualTo("2026-10-21T00:00:00.123456Z");assertThat(value.get("counts").get("rows").stringValue()).isEqualTo("9007199254740993");assertThat(data(real(RUNS)).get(0).get("result")).isEqualTo(value.get("result"));assertThat(new String(SourceJson.canonical(value))).doesNotContain("PRIVATE","/private/","evidence","configuration");
    }
    @Test void latestControlUsesPersistedIdentityAndNoActionEndpointsAppear()throws Exception {
        String run=run();byte[] b=bytes("{}");UUID control=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL','private-key',?,?,0,'REQUESTED','local-operator')",control,uuid(run),b,SourceJson.sha(b));var detail=data(real(RUNS+"/"+run));assertThat(detail.get("latestControl").get("controlId").stringValue()).isEqualTo(control.toString());assertThat(detail.get("latestControl").get("appliedAt").isNull()).isTrue();assertThat(new String(SourceJson.canonical(detail))).doesNotContain("private-key","expectedVersion","quiescenceConfirmed");
    }
    @Test void readsDoNotChangeRunsAuditsSourcesOrCoordinatorAndDoNotLockOwnership()throws Exception {
        String run=run();UUID a=attempt(run,worker(),1,"INTERRUPTED");stage(a,"CHECK_INPUTS","UNKNOWN",0);var before=jdbc.queryForList("SELECT run_id,state,row_version FROM ddip_management.runs");long audits=count("audit_events");var slot=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");var snapshots=stageReadSnapshots();var locked=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var pool=Executors.newSingleThreadExecutor()){var owner=pool.submit(()->tx.required(()->{jdbc.queryForList("SELECT installation_id FROM ddip_management.coordinator FOR UPDATE");jdbc.queryForList("SELECT attempt_id FROM ddip_management.attempts FOR UPDATE");jdbc.queryForList("SELECT checkpoint_id FROM ddip_management.stage_invocations FOR UPDATE");locked.countDown();try{release.await(15,TimeUnit.SECONDS);}catch(InterruptedException ex){throw new RuntimeException(ex);}return null;}));assertThat(locked.await(5,TimeUnit.SECONDS)).isTrue();for(String path:List.of(RUNS,RUNS+"/"+run,RUNS+"/"+run+"/attempts",RUNS+"/"+run+"/events",stages(run,a)))assertThat(real(path).status()).isEqualTo(200);release.countDown();owner.get(5,TimeUnit.SECONDS);}finally{release.countDown();}
        assertThat(jdbc.queryForList("SELECT run_id,state,row_version FROM ddip_management.runs")).isEqualTo(before);assertThat(count("audit_events")).isEqualTo(audits);assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(slot);
        assertThat(stageReadSnapshots()).isEqualTo(snapshots);
    }
    Map<String,List<String>> stageReadSnapshots(){var result=new LinkedHashMap<String,List<String>>();for(String table:List.of("runs","attempts","stage_invocations","stage_artifacts","sources","artifacts","controls","worker_instances","coordinator","audit_events"))result.put(table,jdbc.queryForList("SELECT row_to_json(t)::text FROM ddip_management."+table+" t ORDER BY row_to_json(t)::text",String.class));return result;}
    @Test void databaseOutageReturnsSafe503InsteadOfEmptyHistory()throws Exception {
        String run=run();UUID a=attempt(run,worker(),1,"FAILED");start();admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS false");admin.queryForList("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=? AND pid<>pg_backend_pid()",database);try{for(String path:List.of(RUNS,RUNS+"/"+run,RUNS+"/"+run+"/attempts",RUNS+"/"+run+"/events",stages(run,a)))error(real(path),503,"MANAGEMENT_UNAVAILABLE");}finally{admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS true");}
    }
    @Test void hostOriginAndGetWithoutMutationHeaders()throws Exception {
        assertThat(real(RUNS).status()).isEqualTo(200);error(real(RUNS,"https://evil.example"),403,"LOCAL_BOUNDARY_REJECTED");start();try(var client=HttpClient.newHttpClient()){var r=client.send(HttpRequest.newBuilder(URI.create("http://localhost:"+port+RUNS)).GET().build(),HttpResponse.BodyHandlers.ofByteArray());assertThat(r.statusCode()).isEqualTo(403);}
        String run=run();UUID a=attempt(run,worker(),1,"FAILED");assertThat(real(stages(run,a)).status()).isEqualTo(200);error(real(stages(run,a),"https://evil.example"),403,"LOCAL_BOUNDARY_REJECTED");
        try(var client=HttpClient.newHttpClient()){var r=client.send(HttpRequest.newBuilder(URI.create("http://localhost:"+port+stages(run,a))).GET().build(),HttpResponse.BodyHandlers.ofByteArray());assertThat(r.statusCode()).isEqualTo(403);}
    }
    @Test void capturedSourcesStayHistoricalAfterRevisionAndReadOnlyRestart()throws Exception {
        String run=run();var before=data(real(RUNS+"/"+run));sources.execute(SourceCommands.command(SourceCommands.Action.REVISE,CONTENT,"demo-feed",revise("new",rss().put("feedEndpointId",ENDPOINT2)),"new","\"0\""),"opaque");close();assertThat(data(real(RUNS+"/"+run))).isEqualTo(before);assertThat(data(real(RUNS+"?sourceId=demo-feed")).size()).isEqualTo(1);
    }
    @Test void largeMetadataHistoryUsesBoundedIndexedProjectionsWithoutNPlusOne()throws Exception {
        historyRows(5000);byte[] blob=bytes("{\"private\":\""+"x".repeat(2*1024*1024)+"\"}");byte[] empty=bytes("{}");UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO ddip_management.runs(run_id,product_id,pipeline,pipeline_version,principal_scope,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,state) VALUES (?,'content-intelligence','CONTENT_RSS_V1',1,'local-operator','large',?,?,?,?,?,'QUEUED')",id,empty,SourceJson.sha(empty),blob,SourceJson.sha(blob),CONTENT_PROFILE);jdbc.execute("ANALYZE ddip_management.runs");
        var sql=new ArrayList<String>();var ds=new AbstractDataSource(){public Connection getConnection()throws SQLException{Connection c=jdbc.getDataSource().getConnection();return(Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,m,args)->{try{if(m.getName().equals("prepareStatement"))sql.add((String)args[0]);var value=m.invoke(c,args);if(m.getName().equals("createStatement")){Statement statement=(Statement)value;return Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Statement.class},(p,method,arguments)->{try{if(method.getName().equals("executeQuery") && arguments!=null)sql.add((String)arguments[0]);return method.invoke(statement,arguments);}catch(InvocationTargetException e){throw e.getCause();}});}return value;}catch(InvocationTargetException ex){throw ex.getCause();}});}public Connection getConnection(String u,String p)throws SQLException{return getConnection();}};
        var q=new JdbcTemplate(ds);var reader=new RunHistoryRepository(q,new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(ds))),new SourceCursor(KEY));var page=reader.runs(null,null,null,100,null);assertThat(page.data().size()).isEqualTo(100);assertThat(sql).hasSize(2);assertThat(sql.getLast()).contains("ORDER BY r.submission_seq DESC LIMIT ?").doesNotContain("SELECT *","evidence_bytes","captured_selection","normalized_request","FOR UPDATE","OFFSET");
        String plan=jdbc.queryForObject("EXPLAIN (FORMAT JSON) "+sql.getLast(),String.class,5001L,101);assertThat(plan).contains("runs_submission_seq_key");assertThat(data(real(RUNS+"?limit=100")).size()).isEqualTo(100);assertThat(data(real(RUNS+"/"+id)).get("state").stringValue()).isEqualTo("QUEUED");
        UUID attempt=attempt(id.toString(),worker(),1,"FAILED");for(int i=0;i<101;i++)stage(attempt,"FETCH_SOURCE","FAILED",i);sql.clear();
        var stages=reader.stages(id.toString(),attempt.toString(),100,null);assertThat(stages.data().size()).isEqualTo(100);assertThat(sql).hasSize(3);assertThat(sql.getLast()).endsWith("ORDER BY s.stage COLLATE \"C\",s.item_ordinal LIMIT ?");
        for(String query:sql)assertThat(query).doesNotContain("SELECT *","FOR UPDATE","OFFSET","count(*)","command_address","result_address","child_pid","evidence_bytes","captured_selection","details_bytes");
        sql.clear();assertThat(reader.stages(id.toString(),attempt.toString(),100,stages.nextCursor()).data().size()).isEqualTo(1);assertThat(sql).hasSize(2);assertThat(sql.getLast()).contains("(s.stage COLLATE \"C\",s.item_ordinal)>(?,?)");
    }
    @Test void eventsHighwaterExcludeNewEventsAndUnknownPublicClassificationsFailClosed()throws Exception {
        String id=run();event(id);event(id);var first=real(RUNS+"/"+id+"/events?limit=1");event(id);var next=real(RUNS+"/"+id+"/events?cursor="+next(first));assertThat(data(next).size()).isEqualTo(2);
        jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,action,safe_code,details_bytes) VALUES ('local-operator',?,'PRIVATE_ACTION','PRIVATE_SECRET',?)",uuid(id),bytes("{}"));error(real(RUNS+"/"+id+"/events"),500,"INTERNAL_FAILURE");
    }
    @Test void malformedCountsNeverExposePrivateKeysAndVerificationFlagAloneIsInsufficient()throws Exception {
        String id=run();jdbc.update("UPDATE ddip_management.runs SET counts_bytes=?,row_version=row_version+1 WHERE run_id=?",bytes("{\"privateCredential\":\"123\"}"),uuid(id));error(real(RUNS+"/"+id),500,"INTERNAL_FAILURE");
        UUID artifact=UUID.randomUUID();byte[] e=bytes("{}");jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at) VALUES (?,'CONTENT',?,1,1,?,'content-CONTENT','v1/forged',?,?,now())",artifact,CONTENT,HASH,e,SourceJson.sha(e));jdbc.update("UPDATE ddip_management.runs SET state='SUCCEEDED',finished_at=now(),counts_bytes=NULL,result_artifact_id=?,row_version=row_version+1 WHERE run_id=?",artifact,uuid(id));error(real(RUNS+"/"+id),500,"INTERNAL_FAILURE");
    }
    @Test void databaseRestartPreservesCursorAndPersistedInspectionWhenRequested()throws Exception {
        historyRows(4);var first=real(RUNS+"?limit=1");String cursor=next(first);var expected=data(real(RUNS+"?limit=2&cursor="+cursor));String id=expected.get(0).get("runId").stringValue();event(id);var events=data(real(RUNS+"/"+id+"/events"));close();
        var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",OffsetDateTime.class);String signal=System.getProperty("ddip.test.history-restart-signal");if(signal!=null){Files.writeString(Path.of(signal),"READY");long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);while(!Files.exists(Path.of(signal+".done")) && System.nanoTime()<end)Thread.sleep(100);assertThat(Files.exists(Path.of(signal+".done"))).isTrue();assertThat(jdbc.queryForObject("SELECT pg_postmaster_start_time()",OffsetDateTime.class)).isAfter(started);}
        assertThat(data(real(RUNS+"?limit=2&cursor="+cursor))).isEqualTo(expected);assertThat(data(real(RUNS+"/"+id+"/events"))).isEqualTo(events);
    }

    @Test void oversizedEncodingReturnsOnlyFixedErrorOverRealHttp()throws Exception {
        start(platform.historyfixture.OversizedInspectionConfiguration.class);var response=real(RUNS);error(response,500,"INTERNAL_FAILURE");assertThat(response.size()).isLessThan(1024);assertThat(response.json().has("data")).isFalse();
    }
    @Test void largeVersionsAndControlTiesRemainExactAndDeterministic()throws Exception {
        byte[] b=bytes("{}");UUID id=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.runs(run_id,product_id,pipeline,pipeline_version,principal_scope,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,state,row_version,requested_at) VALUES (?,'content-intelligence','CONTENT_RSS_V1',1,'local-operator','big-version',?,?,?,?,?,'QUEUED',9007199254740993,'2026-10-21T00:00:00.654321Z')",id,b,SourceJson.sha(b),b,SourceJson.sha(b),CONTENT_PROFILE);
        UUID a=UUID.fromString("60000000-0000-4000-8000-000000000001"),z=UUID.fromString("60000000-0000-4000-8000-000000000002");for(var c:List.of(a,z))jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester,requested_at) VALUES (?,?,'CANCEL',?,?,?,0,'REQUESTED','local-operator','2026-10-21T00:00:00.123456Z')",c,id,c.toString(),b,SourceJson.sha(b));var data=data(real(RUNS+"/"+id));assertThat(data.get("rowVersion").stringValue()).isEqualTo("9007199254740993");assertThat(data.get("requestedAt").stringValue()).isEqualTo("2026-10-21T00:00:00.654321Z");assertThat(data.get("latestControl").get("controlId").stringValue()).isEqualTo(z.toString());
    }
    @Test void runEventsKeepAttemptContextAndRefuseMismatchedHistory()throws Exception {
        String id=run(),other=run();UUID attempt=attempt(id,worker(),1,"FAILED");byte[] b=bytes("{}");jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,action,safe_code,details_bytes) VALUES ('local-operator',?,?,'ATTEMPT_FINISHED','INPUT_INVALID',?)",uuid(id),attempt,b);var events=data(real(RUNS+"/"+id+"/events"));assertThat(events.get(1).get("context").get("attemptId").stringValue()).isEqualTo(attempt.toString());jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,action,safe_code,details_bytes) VALUES ('local-operator',?,?,'ATTEMPT_FINISHED','INPUT_INVALID',?)",uuid(other),attempt,b);error(real(RUNS+"/"+other+"/events"),500,"INTERNAL_FAILURE");
    }

    @Test void preflightAuthorizationAndResourceFailureAreSafePublicEvents()throws Exception {
        String id=run();UUID attempt=attempt(id,worker(),1,"RUNNING");
        jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,action,before_state,after_state,safe_code,details_bytes) VALUES ('local-operator',?,?,'STAGE_LAUNCH_AUTHORIZED','INTENT','INTENT','STARTED',?)",uuid(id),attempt,bytes("{}"));
        jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,action,before_state,after_state,safe_code,details_bytes) VALUES ('local-operator',?,?,'STAGE_FAILED','RUNNING','FAILED','RESOURCE_LIMIT',?)",uuid(id),attempt,bytes("{}"));
        var events=data(real(RUNS+"/"+id+"/events"));
        assertThat(events.get(1).get("action").stringValue()).isEqualTo("STAGE_LAUNCH_AUTHORIZED");
        assertThat(events.get(2).get("code").stringValue()).isEqualTo("RESOURCE_LIMIT");
        assertThat(events.get(2).get("context").get("attemptId").stringValue()).isEqualTo(attempt.toString());
        assertThat(new String(SourceJson.canonical(events),java.nio.charset.StandardCharsets.UTF_8)).doesNotContain("command.json","selection.json","evidence.json",url,password);
    }

    @Test void boundedAttemptPageIncludesPersistedReconciliationKind()throws Exception {
        String id=run();UUID instance=worker();for(int n=1;n<20;n++)attempt(id,instance,n,"FAILED");UUID reconcile=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at,outcome_code) VALUES (?,?,20,'RECONCILE',?,20,'FAILED',now(),'INPUT_UNAVAILABLE')",reconcile,uuid(id),instance);var page=real(RUNS+"/"+id+"/attempts?limit=100");var rows=data(page);assertThat(rows.size()).isEqualTo(20);assertThat(rows.get(19).get("kind").stringValue()).isEqualTo("RECONCILE");assertThat(rows.get(19).get("stageCount").stringValue()).isEqualTo("0");assertThat(page.json().get("meta").get("hasMore").booleanValue()).isFalse();
    }
    @Test void eventPageLimitUsesOneLookaheadAndNoDuplicates()throws Exception {
        String id=run();for(int n=0;n<100;n++)event(id);assertThat(data(real(RUNS+"/"+id+"/events")).size()).isEqualTo(20);var first=real(RUNS+"/"+id+"/events?limit=100");assertThat(data(first).size()).isEqualTo(100);assertThat(first.json().get("meta").get("hasMore").booleanValue()).isTrue();var second=real(RUNS+"/"+id+"/events?cursor="+next(first));assertThat(data(second).size()).isEqualTo(1);assertThat(Long.parseLong(data(second).get(0).get("eventSeq").stringValue())).isGreaterThan(Long.parseLong(data(first).get(99).get("eventSeq").stringValue()));
    }

    String stages(String run,UUID attempt){return RUNS+"/"+run+"/attempts/"+attempt+"/stages";}
    UUID persistedStage(UUID attempt,String name,int ordinal,String state,String started,String finished,String outcome,JsonNode counts) {
        UUID checkpoint=UUID.randomUUID();
        jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256,intended_identity,intended_namespace,command_address,command_sha256,result_address,result_sha256,child_pid,child_started_at,child_instance_id,started_at,finished_at,outcome_code,counts_bytes) VALUES (?,?,?,?,?,?,'PRIVATE','private/namespace','private/command',?,'private/proof',?,123,now(),?,?::timestamptz,?::timestamptz,?,?)",attempt,name,ordinal,checkpoint,state,HASH,HASH,HASH,UUID.randomUUID(),started,finished,outcome,counts==null?null:SourceJson.canonical(counts));
        return checkpoint;
    }
    @Test void completedReliabilityStageProjectionUsesPersistedMetadataOnly()throws Exception {
        String run=submit(reliability(false),"stage-reliability").get("runId").stringValue();UUID a=attempt(run,worker(JSON_PROFILE),1,"SUCCEEDED");
        String start="2026-11-01T10:00:00.123456Z",finish="2026-11-01T10:00:01.654321Z";
        UUID check=persistedStage(a,"CHECK_INPUTS",0,"VERIFIED",start,finish,"VERIFIED",SourceJson.object().put("files","2").put("bytes","100").put("rows","10"));
        persistedStage(a,"PUBLISH_RELIABILITY",0,"VERIFIED",start,finish,"VERIFIED",null);
        persistedStage(a,"VERIFY_RESULT",0,"VERIFIED",start,finish,"VERIFIED",SourceJson.object().put("events","9007199254740993").put("windows","2"));
        UUID artifact=UUID.randomUUID();byte[] evidence=bytes("{\"private\":\"PRIVATE\"}");
        jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at,native_format) VALUES (?,'RELIABILITY_V2',?,2,2,?,'reliability-RELIABILITY','v2/private',?,?,now(),'JSONL')",artifact,RELIABILITY,HASH,evidence,SourceJson.sha(evidence));
        jdbc.update("INSERT INTO ddip_management.stage_artifacts VALUES (?,'VERIFY_RESULT',0,'RESULT',0,?)",a,artifact);
        jdbc.update("UPDATE ddip_management.runs SET state='SUCCEEDED',current_stage='VERIFY_RESULT',current_attempt_id=?,result_artifact_id=?,finished_at=?::timestamptz,outcome_code='SUCCEEDED',row_version=row_version+1 WHERE run_id=?",a,artifact,finish,uuid(run));
        var response=real(stages(run,a));var rows=data(response);assertThat(rows.size()).isEqualTo(3);
        assertThat(rows.get(0).get("checkpointId").stringValue()).isEqualTo(check.toString());
        assertThat(rows.get(0).get("counts").get("rows").stringValue()).isEqualTo("10");assertThat(rows.get(1).get("counts").isNull()).isTrue();assertThat(rows.get(2).get("counts").get("events").stringValue()).isEqualTo("9007199254740993");
        for(int i=0;i<3;i++) {
            var row=rows.get(i);assertThat(row.propertyNames()).containsExactlyInAnyOrder("attemptId","stage","itemOrdinal","checkpointId","state","startedAt","finishedAt","outcomeCode","counts");
            assertThat(row.get("stage").stringValue()).isEqualTo(List.of("CHECK_INPUTS","PUBLISH_RELIABILITY","VERIFY_RESULT").get(i));assertThat(row.get("attemptId").stringValue()).isEqualTo(a.toString());assertThat(row.get("itemOrdinal").intValue()).isZero();
            assertThat(row.get("state").stringValue()).isEqualTo("VERIFIED");assertThat(row.get("startedAt").stringValue()).isEqualTo(start);assertThat(row.get("finishedAt").stringValue()).isEqualTo(finish);assertThat(row.get("outcomeCode").stringValue()).isEqualTo("VERIFIED");
        }
        assertThat(new String(SourceJson.canonical(response.json()),java.nio.charset.StandardCharsets.UTF_8)).doesNotContain("private","PRIVATE","command","pid","namespace","evidence","host","proof",password,url);
        assertThat(data(real(RUNS+"/"+run)).get("result").get("verificationStatus").stringValue()).isEqualTo("VERIFIED_AT_EXECUTION");assertThat(data(real(RUNS+"/"+run+"/attempts")).get(0).get("stageCount").stringValue()).isEqualTo("3");
        close();assertThat(data(real(stages(run,a)))).isEqualTo(rows);
    }
    @ParameterizedTest @ValueSource(strings={"INTENT","RUNNING","UNKNOWN","FAILED","VERIFIED"})
    void stageStatesAndNullableTimestampsRemainHistorical(String state)throws Exception {
        String run=run();UUID a=attempt(run,worker(),1,"INTERRUPTED");boolean terminal=Set.of("FAILED","VERIFIED").contains(state);
        String started=state.equals("INTENT")?null:"2026-11-01T10:00:00.000001Z",finished=terminal?"2026-11-01T10:00:01.000002Z":null;
        String code=state.equals("INTENT")?null:state.equals("VERIFIED")?"VERIFIED":"RECOVERY_REQUIRED";
        persistedStage(a,"SILVER",3,state,started,finished,code,SourceJson.object().put("accepted",9007199254740993L).put("rejected",0).put("privateCredential","PRIVATE"));
        var row=data(real(stages(run,a))).get(0);assertThat(row.get("state").stringValue()).isEqualTo(state);assertThat(row.get("itemOrdinal").intValue()).isEqualTo(3);
        assertThat(row.get("startedAt").isNull()).isEqualTo(started==null);if(started!=null)assertThat(row.get("startedAt").stringValue()).isEqualTo(started);
        assertThat(row.get("finishedAt").isNull()).isEqualTo(finished==null);if(finished!=null)assertThat(row.get("finishedAt").stringValue()).isEqualTo(finished);
        assertThat(row.get("outcomeCode").isNull()).isEqualTo(code==null);if(code!=null)assertThat(row.get("outcomeCode").stringValue()).isEqualTo(code);
        assertThat(row.get("counts")).isEqualTo(SourceJson.object().put("accepted","9007199254740993").put("rejected","0"));
    }
    @Test void originalResumeAndReconciliationStagesKeepTheirActualOwners()throws Exception {
        String run=run();UUID w=worker(),original=attempt(run,w,1,"INTERRUPTED");
        UUID checkpoint=persistedStage(original,"PREPARE_GOLD",0,"VERIFIED",null,"2026-11-01T10:00:00Z","VERIFIED",null);
        UUID reconcile=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at,outcome_code) VALUES (?,?,2,'RECONCILE',?,2,'SUCCEEDED',now(),'RECOVERED_CHECKPOINT')",reconcile,uuid(run),w);
        persistedStage(reconcile,"VERIFY_RESULT",0,"VERIFIED",null,"2026-11-01T10:00:01Z","PUBLICATION_INVALID",null);persistedStage(reconcile,"VERIFY_RESULT",1,"VERIFIED",null,"2026-11-01T10:00:02Z","VERIFIED",null);
        UUID resumed=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at,outcome_code,reused_checkpoint_id) VALUES (?,?,3,'EXECUTE',?,3,'FAILED',now(),'INPUT_CHANGED',?)",resumed,uuid(run),w,checkpoint);
        persistedStage(resumed,"PUBLISH_GOLD",0,"FAILED",null,"2026-11-01T10:00:03Z","INPUT_CHANGED",null);
        assertThat(data(real(stages(run,original))).size()).isEqualTo(1);assertThat(data(real(stages(run,original))).get(0).get("checkpointId").stringValue()).isEqualTo(checkpoint.toString());
        var recoveryRows=data(real(stages(run,reconcile)));assertThat(recoveryRows.size()).isEqualTo(2);assertThat(recoveryRows.get(0).get("outcomeCode").stringValue()).isEqualTo("PUBLICATION_INVALID");assertThat(recoveryRows.get(1).get("itemOrdinal").intValue()).isEqualTo(1);
        var resumedRows=data(real(stages(run,resumed)));assertThat(resumedRows.size()).isEqualTo(1);assertThat(resumedRows.get(0).get("attemptId").stringValue()).isEqualTo(resumed.toString());assertThat(resumedRows.get(0).get("stage").stringValue()).isEqualTo("PUBLISH_GOLD");
        assertThat(data(real(RUNS+"/"+run+"/attempts")).get(1).get("kind").stringValue()).isEqualTo("RECONCILE");
    }
    @Test void emptyStagesAndInvalidOrMismatchedIdentitiesAreSafe()throws Exception {
        String run=run(),other=run();UUID a=attempt(run,worker(),1,"FAILED");var empty=real(stages(run,a));assertThat(data(empty).isEmpty()).isTrue();assertThat(empty.json().get("meta").get("limit").intValue()).isEqualTo(20);assertThat(empty.json().get("meta").get("hasMore").booleanValue()).isFalse();assertThat(empty.json().get("meta").get("nextCursor").isNull()).isTrue();
        stage(a,"CHECK_INPUTS","FAILED",0);
        for(String path:List.of(stages(other,a),stages(UUID.randomUUID().toString(),a),stages(run,UUID.randomUUID())))error(real(path),404,"NOT_FOUND");
        error(real(stages("bad",a)),400,"INVALID_ARGUMENT");error(real(RUNS+"/"+run+"/attempts/bad/stages"),400,"INVALID_ARGUMENT");
        error(real(stages(run,a).replace(a.toString(),a.toString().toUpperCase(Locale.ROOT))),400,"INVALID_ARGUMENT");
    }
    @Test void stagePagesAreBoundedOrderedAndUseIdentityHighwaterWithoutDuplicates()throws Exception {
        String run=run();UUID a=attempt(run,worker(),1,"INTERRUPTED");
        for(int i=204;i>=0;i--)stage(a,"FETCH_SOURCE","UNKNOWN",i);stage(a,"CHECK_INPUTS","VERIFIED",0);
        var first=real(stages(run,a));assertThat(data(first).size()).isEqualTo(20);assertThat(data(first).get(0).get("stage").stringValue()).isEqualTo("CHECK_INPUTS");assertThat(first.json().get("meta").propertyNames()).containsExactlyInAnyOrder("correlationId","limit","hasMore","nextCursor");
        stage(a,"VERIFY_RESULT","VERIFIED",0); // Beyond the first page's persisted identity high-water.
        var ids=new ArrayList<String>();for(var row:data(first))ids.add(row.get("checkpointId").stringValue());String cursor=next(first);
        while(cursor!=null){var page=real(stages(run,a)+"?limit=100&cursor="+cursor);var rows=data(page);assertThat(rows.size()).isLessThanOrEqualTo(100);for(var row:rows)ids.add(row.get("checkpointId").stringValue());cursor=page.json().get("meta").get("nextCursor").isNull()?null:next(page);}
        assertThat(ids).hasSize(206).doesNotHaveDuplicates();assertThat(ids).isEqualTo(jdbc.queryForList("SELECT checkpoint_id FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage<>'VERIFY_RESULT' ORDER BY stage COLLATE \"C\",item_ordinal",UUID.class,a).stream().map(UUID::toString).toList());
        var max=real(stages(run,a)+"?limit=100");assertThat(data(max).size()).isEqualTo(100);assertThat(max.json().get("meta").get("hasMore").booleanValue()).isTrue();
    }
    @Test void stageCursorsRejectOtherRunsAttemptsEndpointsTamperingExpiryAndKeyChanges()throws Exception {
        String run=run(),other=run();UUID w=worker(),a=attempt(run,w,1,"FAILED"),b=attempt(run,w,2,"FAILED"),foreign=attempt(other,w,1,"FAILED");
        for(UUID attempt:List.of(a,b,foreign)){stage(attempt,"CHECK_INPUTS","VERIFIED",0);stage(attempt,"VERIFY_RESULT","FAILED",0);}
        String token=next(real(stages(run,a)+"?limit=1"));
        for(String path:List.of(stages(run,b),stages(other,foreign),RUNS+"/"+run+"/events",RUNS+"/"+run+"/attempts"))error(real(path+"?cursor="+token),400,"INVALID_ARGUMENT");
        error(real(stages(other,a)+"?cursor="+token),404,"NOT_FOUND");
        String tampered=(token.startsWith("A")?"B":"A")+token.substring(1);error(real(stages(run,a)+"?cursor="+tampered),400,"INVALID_ARGUMENT");
        String old=new RunHistoryRepository(jdbc,tx,new SourceCursor(KEY),Clock.offset(Clock.systemUTC(),Duration.ofDays(-2))).stages(run,a.toString(),1,null).nextCursor();error(real(stages(run,a)+"?cursor="+old),400,"INVALID_ARGUMENT");
        var changed=new RunHistoryRepository(jdbc,tx,new SourceCursor(Base64.getUrlEncoder().withoutPadding().encodeToString("01234567890123456789012345678901".getBytes(java.nio.charset.StandardCharsets.US_ASCII))));assertThatThrownBy(()->changed.stages(run,a.toString(),1,token)).hasMessage("INVALID_ARGUMENT");
        String attemptToken=next(real(RUNS+"/"+run+"/attempts?limit=1"));error(real(stages(run,a)+"?cursor="+attemptToken),400,"INVALID_ARGUMENT");
        var rest=data(real(stages(run,a)+"?cursor="+token));close();assertThat(data(real(stages(run,a)+"?cursor="+token))).isEqualTo(rest);
    }
    @ParameterizedTest @ValueSource(strings={"limit=0","limit=101","limit=-1","limit=1.0","limit=01","limit=","limit=1&limit=2","state=VERIFIED","unknown=x","cursor=","cursor=x","cursor=x&cursor=y"})
    void invalidStageQueries(String query)throws Exception {String run=run();UUID a=attempt(run,worker(),1,"FAILED");error(real(stages(run,a)+"?"+query),400,"INVALID_ARGUMENT");}
    @Test void unknownStageOutcomeAndMalformedAllowlistedCountsFailClosed()throws Exception {
        String run=run();UUID a=attempt(run,worker(),1,"FAILED");persistedStage(a,"CHECK_INPUTS",0,"FAILED",null,"2026-11-01T10:00:00Z","PRIVATE_SECRET",null);error(real(stages(run,a)),500,"INTERNAL_FAILURE");
        UUID b=attempt(run,worker(),2,"FAILED");persistedStage(b,"SILVER",0,"FAILED",null,"2026-11-01T10:00:00Z","INPUT_INVALID",SourceJson.object().put("rows","PRIVATE_SECRET"));error(real(stages(run,b)),500,"INTERNAL_FAILURE");
    }

}
