package platform.contentapi.management;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.execution.NarrowContentAdmissionTestSupport.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Shared isolated database/admission fixtures. Runtime identities remain synthetic. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class NarrowContentAdmissionFixtures extends RunApiFixtures {
    static final String RUNTIME="narrow-runtime";
    final Map<String,Path> roots=new TreeMap<>();
    InstallationHostGuard.Binding binding;
    InstallationHostGuard guard;
    WorkerRepository workers;
    WorkerRepository.Session session;
    WorkerRepository.Owner owner;

    @Override void storage(String id,String product,String role) {
        try {
            Path base=Files.createTempDirectory(temp,"admission-").toRealPath();ObjectNode configuration;
            if(role.equals("CONTROL")) {
                var host=OwnershipFixtures.host(base);
                var named=new InstallationHostGuard.Binding(id,INSTALL,host.root(),host.rootIdentity(),host.guardIdentity(),host.installationIdentity());
                configuration=(ObjectNode)SourceJson.parse(named.descriptor());roots.put(id,named.root());
                if(product.equals(CONTENT))binding=named;
            } else {
                var identity=InstallationHostGuard.identity(base);
                configuration=SourceJson.object().put("importStorageVersion",1).put("root",base.toString())
                        .put("rootDevice",Long.toString(identity.device())).put("rootInode",Long.toString(identity.inode()));
                roots.put(id,base);
            }
            byte[] bytes=SourceJson.canonical(configuration);
            jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,bytes,SourceJson.sha(bytes),INSTALL);
        } catch(Exception ex) {throw new IllegalStateException(ex);}
    }
    @Override void profile(String id,boolean content,String source) {
        if(!content) {super.profile(id,false,source);return;}
        // Immutable identity metadata only. This is intentionally not a native-qualified runtime.
        byte[] runtime=SourceJson.canonical(SourceJson.object().put("contentRuntimeVersion",2).put("fixture","metadata-only-no-native-execution"));
        jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",RUNTIME,runtime,SourceJson.sha(runtime));
        byte[] profile=SourceJson.canonical(RunFixtures.profile(true,source).put("runProfileVersion",2).put("contentRuntimeId",RUNTIME));
        jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,profile,SourceJson.sha(profile));
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.storage_bindings WHERE product=?",id,CONTENT);
    }
    @AfterEach void releaseGuard() {if(guard!=null){guard.close();guard=null;}}
    UUID queue(String key) {
        var request=content();request.withArray("retainedSilverArtifactIds").removeAll();
        return UUID.fromString(submit(request,key).get("runId").stringValue());
    }
    void ready() {
        guard=InstallationHostGuard.acquire(binding);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        session=workers.register(guard,CONTENT_PROFILE,digest(CONTENT_PROFILE),"a".repeat(64));
    }
    String digest(String profile) {return jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,profile);}
    WorkerRepository.Owner claimed() {UUID run=queue("narrow");ready();owner=claim(workers,guard,session,UUID.randomUUID(),run).owner();assertThat(owner).isNotNull();return owner;}
    Facts prepare() {return NarrowContentAdmissionTestSupport.prepare(jdbc,tx,workers,guard,owner);}
    Facts inspect() {return NarrowContentAdmissionTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId());}
    Path attemptRoot() {return binding.root().resolve("runs/"+owner.runId()+"/attempts/"+owner.attemptId());}
    List<String> phases() {return jdbc.queryForList("SELECT phase FROM ddip_management.content_admission_milestones ORDER BY phase",String.class);}
    String admissionHash() {return jdbc.queryForObject("SELECT admission_sha256 FROM ddip_management.content_attempt_admissions WHERE attempt_id=?",String.class,owner.attemptId());}
    void noExecution() {
        for(String table:List.of("stage_invocations","stage_artifacts","rss_deliveries"))assertThat(count(table)).as(table).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state='SUCCEEDED' OR result_artifact_id IS NOT NULL OR current_stage IS NOT NULL",Long.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE state='RUNNING'",Long.class)).isZero();
    }
    Map<String,List<String>> metadata() {
        var snapshot=new TreeMap<String,List<String>>();
        for(String table:List.of("coordinator","worker_instances","runs","attempts","content_attempt_admissions","content_admission_milestones","audit_events","stage_invocations","stage_artifacts","rss_deliveries"))
            snapshot.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));
        return snapshot;
    }
    // Upgrade assertions alone compare the old columns, after proving V15 added only null policy.
    // Ordinary metadata snapshots deliberately keep both policy markers visible.
    Map<String,List<String>> metadataBeforeRecoveryPolicyUpgrade() {
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE content_recovery_policy IS NOT NULL",Long.class)).isZero();
        var snapshot=metadata();
        snapshot.put("attempts",jdbc.queryForList("SELECT (to_jsonb(t)-'content_recovery_policy')::text FROM ddip_management.attempts t ORDER BY (to_jsonb(t)-'content_recovery_policy')::text",String.class));
        return snapshot;
    }
    Map<String,String> filesystem() throws Exception {
        var snapshot=new TreeMap<String,String>();
        for(var entry:roots.entrySet())try(var paths=Files.walk(entry.getValue())) {
            for(Path path:paths.toList()) {
                var a=Files.readAttributes(path,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
                snapshot.put(entry.getKey()+"/"+entry.getValue().relativize(path),InstallationHostGuard.identity(path)+":"+a.lastModifiedTime()+":"
                        +PosixFilePermissions.toString(Files.getPosixFilePermissions(path,LinkOption.NOFOLLOW_LINKS))+":"
                        +(a.isDirectory()?"DIRECTORY":Base64.getEncoder().encodeToString(Files.readAllBytes(path))));
            }
        }
        return snapshot;
    }
    void cancel() {
        byte[] bytes=SourceJson.canonical(SourceJson.object());
        jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL','narrow-cancel',?,?,?,'REQUESTED','local-operator')",
                UUID.randomUUID(),owner.runId(),bytes,SourceJson.sha(bytes),owner.runVersion());
    }
    void stopAt(String point) {
        assertThatThrownBy(()->NarrowContentAdmissionTestSupport.prepare(jdbc,tx,workers,guard,owner,boundary->{
            if(boundary.equals(point))throw new IllegalStateException("TEST_CRASH_BOUNDARY");
        })).isInstanceOf(RuntimeException.class);
    }
    ObjectNode preparingReceipt() {return SourceJson.object().put("protocolVersion",1).put("admissionSha256",admissionHash());}
    ObjectNode placeholderReady() {return preparingReceipt().put("rootDevice","1").put("rootInode","1").put("lockDevice","1").put("lockInode","1").put("ledgerSha256","a".repeat(64));}
    void milestone(String phase,ObjectNode value,String hash) {
        byte[] bytes=SourceJson.canonical(value);
        jdbc.update("INSERT INTO ddip_management.content_admission_milestones(attempt_id,phase,receipt_bytes,receipt_sha256) VALUES (?,?,?,?)",owner.attemptId(),phase,bytes,hash==null?SourceJson.sha(bytes):hash);
    }

    JdbcTemplate loseCommitAcknowledgment(AtomicBoolean armed) {
        var source=new AbstractDataSource() {
            @Override public Connection getConnection() throws SQLException {
                Connection real=jdbc.getDataSource().getConnection();
                return (Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,method,args)->{
                    try {
                        Object result=method.invoke(real,args);
                        if(method.getName().equals("commit") && armed.compareAndSet(true,false))throw new SQLException("PRIVATE_COMMIT_ACK","08006");
                        return result;
                    } catch(InvocationTargetException ex) {throw ex.getCause();}
                });
            }
            @Override public Connection getConnection(String username,String password) throws SQLException {return getConnection();}
        };
        return new JdbcTemplate(source);
    }
}
