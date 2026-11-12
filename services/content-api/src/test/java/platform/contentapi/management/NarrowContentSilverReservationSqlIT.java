package platform.contentapi.management;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.management.SourceFixtures.*;
import static org.assertj.core.api.Assertions.*;

/** SQL independently rejects altered dependency assertions even when all caller hashes are updated. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentSilverReservationSqlIT extends NarrowContentSilverReservationFixtures {
    static final String INSERT="INSERT INTO ddip_management.content_silver_reservations(operation_id,bronze_operation_id,attempt_id,checkpoint_id,command_bytes,command_sha256,reservation_bytes,reservation_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
    Object[] row;boolean versionNineteen;
    @Override void migrateFixtureSchema(){
        if(!versionNineteen){super.migrateFixtureSchema();return;}
        org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").target("19").load().migrate();
        new JdbcCoordinatorRepository(jdbc,tx,INSTALL).initialize();
        historicalSubmissionForSchema(19);
    }
    @AfterEach void resetSchemaTarget(){versionNineteen=false;}
    void capture(){
        original();var j=new JdbcTemplate(jdbc.getDataSource()){@Override public int update(String sql,Object... args){
            if(sql.equals(INSERT)){row=args.clone();row[4]=((byte[])args[4]).clone();row[6]=((byte[])args[6]).clone();throw new IllegalStateException("CAPTURED");}return super.update(sql,args);}};
        var t=transactions(j);var w=WorkerTestSupport.enabled(j,t,INSTALL);
        assertThatThrownBy(()->chain(j,t,w,b->{},b->{})).hasMessage("CAPTURED");assertThat(row).isNotNull();noSilver();
    }
    void insert(){jdbc.update(INSERT,row);}
    void rebuildHashes(){
        row[5]=SourceJson.sha((byte[])row[4]);var reservation=(ObjectNode)SourceJson.parse((byte[])row[6]);reservation.put("commandSha256",(String)row[5]);row[6]=SourceJson.canonical(reservation);row[7]=SourceJson.sha((byte[])row[6]);
    }
    @Test void publishingCandidateWithoutIndependentCheckpointCannotReserve(){
        original();NarrowContentBronzeLaunchTestSupport.chain(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,bronzeOperation,BRONZE_PROFILE,b->{});
        byte[] bytes=SourceJson.canonical(SourceJson.object());
        row=new Object[]{silverOperation,bronzeOperation,owner.attemptId(),UUID.randomUUID(),bytes,SourceJson.sha(bytes),bytes,SourceJson.sha(bytes),
                jdbc.queryForObject("SELECT row_version FROM ddip_management.coordinator",Long.class),
                jdbc.queryForObject("SELECT row_version FROM ddip_management.worker_instances WHERE instance_id=?",Long.class,owner.worker().instanceId()),owner.runVersion(),owner.attemptVersion()};
        assertThatThrownBy(this::insert).hasStackTraceContaining("CONTENT_SILVER_BRONZE_REQUIRED");noSilver();
    }
    @Test void exactGeneratedReservationIsAcceptedOnceAndImmutable()throws Exception{
        capture();var files=filesystem();int calls=objectRequests.get();insert();assertThat(count("content_silver_reservations")).isEqualTo(1);assertThat(silverFacts().nativeLaunchCompatible()).isFalse();
        assertThatThrownBy(this::insert).isInstanceOf(RuntimeException.class);row[0]=UUID.randomUUID();assertThatThrownBy(this::insert).isInstanceOf(RuntimeException.class);
        for(String sql:List.of("UPDATE ddip_management.content_silver_reservations SET created_at=created_at","DELETE FROM ddip_management.content_silver_reservations","TRUNCATE ddip_management.content_silver_reservations CASCADE"))assertThatThrownBy(()->jdbc.execute(sql)).isInstanceOf(RuntimeException.class);
        assertThat(filesystem()).isEqualTo(files);assertThat(objectRequests).hasValue(calls);retained();
    }
    @ParameterizedTest @ValueSource(strings={"bronze-operation","attempt","checkpoint","same-bronze","same-fetch","cv","wv","rv","av"})
    void rowMustMatchOriginalDependencyAndFence(String field){
        capture();switch(field){case "bronze-operation"->row[1]=UUID.randomUUID();case "attempt"->row[2]=UUID.randomUUID();case "checkpoint"->row[3]=UUID.randomUUID();case "same-bronze"->row[0]=bronzeOperation;case "same-fetch"->row[0]=operation;case "cv"->row[8]=((Number)row[8]).longValue()+1;case "wv"->row[9]=((Number)row[9]).longValue()+1;case "rv"->row[10]=((Number)row[10]).longValue()+1;case "av"->row[11]=((Number)row[11]).longValue()+1;}
        assertThatThrownBy(this::insert).isInstanceOf(RuntimeException.class);noSilver();
    }
    @ParameterizedTest @ValueSource(strings={"receipt","anchor","anchor-stamp","candidate","bronze-command","bronze-reservation","fetch-ingestion","fetch-time","fetch-completion","runtime","silver-binding","temp-binding","control-binding","credential","credential-stamp","inspection","launchable","unknown","mode"})
    void callerCannotRewriteCanonicalCommandAndRehash(String field){
        capture();var n=(ObjectNode)SourceJson.parse((byte[])row[4]);
        switch(field){
            case "receipt"->((ObjectNode)n.get("bronze").get("receipt")).put("ingestionId",UUID.randomUUID().toString());
            case "anchor"->((ObjectNode)n.get("bronze").get("verified")).put("candidateSha256","0".repeat(64));
            case "anchor-stamp"->((ObjectNode)n.get("bronze").get("verifiedStamp")).put("identity","1:2");
            case "candidate"->((ObjectNode)n.get("bronze")).put("candidateSha256","0".repeat(64));
            case "bronze-command"->((ObjectNode)n.get("bronze")).put("commandSha256","0".repeat(64));
            case "bronze-reservation"->((ObjectNode)n.get("bronze")).put("reservationSha256","0".repeat(64));
            case "fetch-ingestion"->((ObjectNode)n.get("fetch")).put("ingestionId",UUID.randomUUID().toString());
            case "fetch-time"->((ObjectNode)n.get("fetch")).put("ingestedAt","2026-01-01T00:00:00Z");
            case "fetch-completion"->((ObjectNode)n.get("fetch")).put("completionSha256","0".repeat(64));
            case "runtime"->((ObjectNode)n.get("runtime")).put("registeredLedgerVersion",2);
            case "silver-binding","temp-binding","control-binding"->((ObjectNode)n.get("bindings").get(field.split("-")[0])).put("configurationSha256","0".repeat(64));
            case "credential"->((ObjectNode)n.get("credential")).put("sha256","0".repeat(64));
            case "credential-stamp"->((ObjectNode)n.get("credential").get("stamp")).put("identity","1:2");
            case "inspection"->((ObjectNode)n.get("requiresInspection")).put("checkpointId",n.get("bronze").get("checkpointId").stringValue());
            case "launchable"->n.put("nativeLaunchCompatible",true);case "unknown"->n.put("unknown",true);case "mode"->n.put("mode","PUBLISH");
        }
        row[4]=SourceJson.canonical(n);rebuildHashes();assertThatThrownBy(this::insert).isInstanceOf(RuntimeException.class);noSilver();
    }
    @ParameterizedTest @ValueSource(strings={"duplicate","float","oversized","hash","reservation"})
    void malformedOrMismatchedBytesFailClosed(String change){
        capture();String raw=new String((byte[])row[4],StandardCharsets.UTF_8);
        if(change.equals("duplicate"))raw=raw.replace("\"silverReservationCommandVersion\":1","\"silverReservationCommandVersion\":1,\"silverReservationCommandVersion\":1");
        if(change.equals("float"))raw=raw.replace("\"silverReservationCommandVersion\":1","\"silverReservationCommandVersion\":1.0");
        if(change.equals("oversized"))raw=" ".repeat(65536)+raw;
        row[4]=raw.getBytes(StandardCharsets.UTF_8);rebuildHashes();
        if(change.equals("hash"))row[5]="0".repeat(64);
        if(change.equals("reservation")){var n=(ObjectNode)SourceJson.parse((byte[])row[6]);n.put("bronzeOperationId",UUID.randomUUID().toString());row[6]=SourceJson.canonical(n);row[7]=SourceJson.sha((byte[])row[6]);}
        assertThatThrownBy(this::insert).isInstanceOf(RuntimeException.class);noSilver();
    }
    @ParameterizedTest @ValueSource(strings={"run","attempt","worker","coordinator","stop","cancel","deadline"})
    void sqlRechecksOwnerAndControls(String change){capture();changeOwner(change);assertThatThrownBy(this::insert).isInstanceOf(RuntimeException.class);noSilver();}
    @ParameterizedTest @ValueSource(strings={"ISOLATION_REPEATABLE_READ","ISOLATION_SERIALIZABLE"})
    void snapshotIsolationRejected(String isolation){capture();var t=new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource()));t.setIsolationLevelName(isolation);assertThatThrownBy(()->t.execute(s->{insert();return null;})).isInstanceOf(RuntimeException.class);noSilver();}
    @Test void coordinatorLockSerializesReservationAgainstCancellation()throws Exception{
        capture();var locked=new CountDownLatch(1);var release=new CountDownLatch(1);var started=new CountDownLatch(1);
        try(var pool=Executors.newVirtualThreadPerTaskExecutor()){
            var control=pool.submit(()->tx.required(()->{jdbc.queryForObject("SELECT singleton_id FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE",Integer.class);locked.countDown();try{assertThat(release.await(10,TimeUnit.SECONDS)).isTrue();}catch(InterruptedException ex){throw new RuntimeException(ex);}cancel();return null;}));
            assertThat(locked.await(10,TimeUnit.SECONDS)).isTrue();var reservation=pool.submit(()->{started.countDown();assertThatThrownBy(this::insert).isInstanceOf(RuntimeException.class);});
            assertThat(started.await(10,TimeUnit.SECONDS)).isTrue();Thread.sleep(150);assertThat(reservation.isDone()).isFalse();release.countDown();control.get(10,TimeUnit.SECONDS);reservation.get(10,TimeUnit.SECONDS);
        }finally{release.countDown();}noSilver();
    }
    @Test void populatedVersionNineteenUpgradePreservesCheckpointAndAddsNoReservation()throws Exception{
        versionNineteen=true;closeServer();closeObjectServer();setup();verificationProfile();original();
        var checkpoint=NarrowContentBronzeCompletionTestSupport.chain(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,bronzeOperation,BRONZE_PROFILE,VERIFY_PROFILE,b->{});
        var tables=jdbc.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='ddip_management' AND tablename<>'flyway_schema_history' ORDER BY tablename",String.class);
        var before=new TreeMap<String,List<String>>();for(String table:tables)before.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));
        var files=filesystem();var migrations=jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history WHERE version IS NOT NULL ORDER BY installed_rank");
        var migration=org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load();
        assertThat(migration.migrate().migrationsExecuted).isEqualTo(3);new SchemaLifecycle(migration,jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(false);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE content_terminal_policy IS NOT NULL",Long.class)).isZero();
        for(String table:tables){
            String historical=table.equals("attempts")?"(to_jsonb(t)-'content_terminal_policy')":"to_jsonb(t)";
            assertThat(jdbc.queryForList("SELECT "+historical+"::text FROM ddip_management."+table+" t ORDER BY "+historical+"::text",String.class)).as(table).isEqualTo(before.get(table));
        }
        assertThat(filesystem()).isEqualTo(files);assertThat(NarrowContentBronzeCompletionTestSupport.inspect(jdbc,tx,bronzeOperation)).contains(checkpoint);
        assertThat(jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history WHERE version IS NOT NULL AND version NOT IN ('20','21','22','23') ORDER BY installed_rank")).isEqualTo(migrations);noSilver();
    }
}
