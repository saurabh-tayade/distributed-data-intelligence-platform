package platform.contentapi.execution;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.security.MessageDigest;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import platform.contentapi.importing.ImportBindings;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Explicit E5.2-D observer/orchestrator, never discovered by Surefire/Failsafe or shipped in the app.
 * No fixture inheritance, test capability, owner reconstruction, native bootstrap or pipeline call.
 * All effectful execution is via genuine HTTP submission and the unchanged packaged worker.
 */
public final class PackagedPublicContentAcceptance {
    static final class GateFailure extends IllegalStateException {GateFailure(String code){super(code);}}
    static final String FIXTURE="https://raw.githubusercontent.com/saurabh-tayade/distributed-data-intelligence-platform/6584d52153e599ce8f4a5c314e13535f2e7e7e1b/ingestion/src/test/resources/feeds/atom-public-qualification.xml";
    static final String FIXTURE_SHA="1f99a72b80837b4239189a6b02d4f143b104aae845d2f5ece77776705a66e81c";
    static final String TITLE="A paper star in an imaginary observatory";
    static final List<String> TABLES=List.of("coordinator","worker_instances","sources","source_revisions","runs","run_sources","run_inputs","attempts",
            "artifacts","artifact_dependencies","artifact_import_operations","content_attempt_admissions","content_admission_milestones",
            "content_operations","content_operation_phases","content_native_operations","content_native_launch_phases","content_fetch_completion_intents",
            "content_fetch_checkpoints","content_bronze_reservations","content_bronze_launch_phases","content_bronze_verification_phases",
            "content_bronze_checkpoints","content_silver_reservations","content_pipeline_operations","content_pipeline_phases","content_pipeline_checkpoints",
            "content_pipeline_completions","stage_invocations","stage_artifacts","rss_deliveries","controls","audit_events");
    static final List<String> REGISTRATION_TABLES=List.of("storage_bindings","execution_profiles","execution_profile_storage","command_bindings",
            "content_fetch_launch_profiles","content_bronze_launch_profiles","content_bronze_verification_profiles","content_pipeline_profiles");
    final JsonNode plan,profiles;
    final Path evidence,root;
    final String origin;
    final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).followRedirects(HttpClient.Redirect.NEVER).build();
    HikariDataSource dataSource;
    JdbcTemplate jdbc;
    ManagementTransactions tx;
    InstallationHostGuard.Binding binding;
    JsonNode storage;
    Process api,worker;
    boolean submitted;

    PackagedPublicContentAcceptance(JsonNode plan){
        this.plan=plan;profiles=plan.get("profiles");evidence=Path.of(s(plan,"evidenceDirectory"));root=Path.of(s(plan,"installationRoot"));
        origin="http://127.0.0.1:"+plan.get("apiPort").intValue();
    }
    static void need(boolean value,String code){if(!value)throw new GateFailure(code);}
    static String s(JsonNode n,String key){need(n!=null && n.has(key) && n.get(key).isString(),"INPUT_INVALID");return n.get(key).stringValue();}
    String p(String key){return s(profiles,key);}
    Path artifact(String name){return Path.of(s(plan.get("artifacts").get(name),"path"));}
    UUID installation(){return UUID.fromString(s(plan,"installationId"));}
    static byte[] bounded(Path path,int max)throws IOException{
        need(!Files.isSymbolicLink(path) && Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS),"INPUT_NOT_REGULAR");
        try(var in=Files.newInputStream(path,LinkOption.NOFOLLOW_LINKS)){byte[] raw=in.readNBytes(max+1);need(raw.length<=max,"INPUT_TOO_LARGE");return raw;}
    }
    static String hash(Path path)throws Exception{
        var digest=MessageDigest.getInstance("SHA-256");
        try(var in=Files.newInputStream(path,LinkOption.NOFOLLOW_LINKS)){byte[] bytes=new byte[65536];int read;while((read=in.read(bytes))!=-1)digest.update(bytes,0,read);}
        return HexFormat.of().formatHex(digest.digest());
    }
    static void write(Path file,byte[] raw)throws IOException{
        try(var channel=FileChannel.open(file,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS)){
            Files.setPosixFilePermissions(file,PosixFilePermissions.fromString("rw-------"));
            var buffer=java.nio.ByteBuffer.wrap(raw);while(buffer.hasRemaining())channel.write(buffer);channel.force(true);
        }
        try(var parent=FileChannel.open(file.getParent(),StandardOpenOption.READ)){parent.force(true);}
    }
    void record(String name,JsonNode value)throws IOException{write(evidence.resolve(name+".json"),SourceJson.canonical(value));}
    void pins()throws Exception{
        need(System.getProperty("os.name").equals("Linux") && Runtime.version().feature()==21
                && Runtime.version().toString().equals(s(plan.get("runtime"),"javaVersion")),"LINUX_JDK_IDENTITY_MISMATCH");
        need(Path.of(System.getProperty("java.home")).toRealPath().equals(Path.of(s(plan.get("runtime"),"javaHome"))),"JDK_HOME_MISMATCH");
        for(String key:plan.get("artifacts").propertyNames())need(hash(artifact(key)).equals(s(plan.get("artifacts").get(key),"sha256")),"ARTIFACT_CHANGED_"+key);
        for(String key:plan.get("artifacts").propertyNames()){
            Path file=artifact(key);need(file.equals(file.toRealPath()) && !file.startsWith(root) && !file.startsWith(evidence),"ARTIFACT_PATH_NOT_ISOLATED");
        }
        need(plan.get("nativeClasspath").isArray() && plan.get("nativeClasspath").size()>=2 && plan.get("nativeClasspath").size()<=128,"NATIVE_CLASSPATH_REQUIRED");
        var nativePaths=new HashSet<Path>();for(var entry:plan.get("nativeClasspath")){
            SourceJson.fields(entry,"path","sha256");Path file=Path.of(s(entry,"path"));
            need(file.equals(file.toRealPath()) && !file.startsWith(root) && !file.startsWith(evidence) && nativePaths.add(file)
                    && !file.equals(artifact("api")) && !file.equals(artifact("runner")) && hash(file).equals(s(entry,"sha256")),"NATIVE_CLASSPATH_CHANGED");
        }
        need(hash(artifact("fixture")).equals(FIXTURE_SHA)
                && Files.size(artifact("fixture"))==726 && s(plan.get("fixture"),"uri").equals(FIXTURE),"FIXTURE_MISMATCH");
        var catalog=SourceJson.object().put("catalogVersion",1);catalog.putArray("endpoints").addObject().put("endpointId",p("endpointId")).put("sourceId",p("sourceId")).put("uri",FIXTURE);
        catalog.putArray("reliabilityProfiles");catalog.putArray("storageProfiles").addObject().put("profileId",p("bronzeBindingId")).put("productId","content-intelligence").put("role","BRONZE");
        need(catalog.equals(SourceJson.parse(bounded(artifact("catalog"),65536))),"CATALOG_NOT_EXACT_REVIEWED_FIXTURE");
    }
    void authorization(String mode,byte[] raw)throws Exception{
        SourceJson.fields(plan,"planVersion","scenario","installationId","installationRoot","evidenceDirectory","database","artifacts","nativeClasspath","runtime","profiles","services","apiPort","deadlineSeconds","fixture");
        SourceJson.fields(plan.get("artifacts"),"api","ingestion","provider","runner","java","python","catalog","fixture","docker");
        need(plan.get("planVersion").isInt() && plan.get("planVersion").intValue()==1 && Set.of("complete","live-stop").contains(s(plan,"scenario"))
                && s(profiles,"pipeline").equals("CONTENT_RSS_V1") && profiles.get("runProfileVersion").isInt() && profiles.get("runProfileVersion").intValue()==2
                && profiles.get("compositionVersion").isInt() && profiles.get("compositionVersion").intValue()==2,"PUBLIC_V2_PLAN_REQUIRED");
        need(root.equals(root.toRealPath()) && evidence.equals(evidence.toRealPath()) && !root.startsWith(evidence) && !evidence.startsWith(root)
                && Files.getPosixFilePermissions(evidence).equals(PosixFilePermissions.fromString("rwx------")),"PRIVATE_ISOLATED_EVIDENCE_REQUIRED");
        need(!Files.exists(evidence.resolve("result.json")) && !Files.exists(evidence.resolve("failure.json")),"QUALIFICATION_ALREADY_ATTEMPTED");
        var approval=SourceJson.parse(bounded(evidence.resolve("approval.json"),65536));
        SourceJson.fields(approval,"approvalVersion","ownerDecision","planSha256","installationId","hostname","bootId","expiresEpochSeconds","disposable","directNativeEgress","scenario");
        long now=Instant.now().getEpochSecond();
        need(approval.get("approvalVersion").isInt() && approval.get("approvalVersion").intValue()==1
                && s(approval,"ownerDecision").matches("https://github\\.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49#issuecomment-[0-9]+")
                && s(approval,"planSha256").equals(SourceJson.sha(raw)) && s(approval,"installationId").equals(installation().toString())
                && s(approval,"scenario").equals(s(plan,"scenario")) && approval.get("disposable").isBoolean() && approval.get("disposable").booleanValue()
                && approval.get("directNativeEgress").isBoolean() && approval.get("directNativeEgress").booleanValue()
                && s(approval,"hostname").equals(Files.readString(Path.of("/proc/sys/kernel/hostname")).strip())
                && s(approval,"bootId").equals(Files.readString(Path.of("/proc/sys/kernel/random/boot_id")).strip())
                && approval.get("expiresEpochSeconds").isIntegralNumber() && approval.get("expiresEpochSeconds").longValue()>now
                && approval.get("expiresEpochSeconds").longValue()<=now+86400,"EXACT_TARGET_OWNER_APPROVAL_REQUIRED");
        var launch=SourceJson.parse(bounded(evidence.resolve("launch.json"),65536));
        need(s(launch,"mode").equals(mode) && s(launch,"planSha256").equals(SourceJson.sha(raw)),"LAUNCH_RECORD_MISMATCH");
        var arguments=java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments();
        need(arguments.size()==2 && arguments.contains("-Dloader.path="+artifact("runner"))
                && arguments.contains("-Dloader.main=platform.contentapi.execution.PackagedPublicContentAcceptance"),"UNREVIEWED_JVM_OPTIONS");
        need(System.getProperty("java.class.path").equals(artifact("api").toString())
                && Path.of(PackagedPublicContentAcceptance.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath().equals(artifact("runner")),"LAUNCH_CLASSPATH_MISMATCH");
        for(var entry:System.getenv().entrySet()){
            String key=entry.getKey(),lower=key.toLowerCase(Locale.ROOT);
            boolean refused=lower.contains("proxy") || Set.of("java_tool_options","jdk_java_options","_java_options","classpath","ld_preload","ld_library_path","pythonpath","pythonhome").contains(lower)
                    || key.startsWith("SPRING_") || key.startsWith("SERVER_") || key.startsWith("ANALYTICS_") || key.startsWith("CONTENT_ANALYTICS_") || key.startsWith("RELIABILITY_ANALYTICS_")
                    || key.startsWith("DDIP_") && !key.equals("DDIP_DB_PASSWORD");
            need(!refused || entry.getValue().isEmpty(),"INHERITED_OVERRIDE_REFUSED");
        }
    }
    void database()throws Exception{
        var db=plan.get("database");var config=new HikariConfig();config.setJdbcUrl(s(db,"url"));config.setUsername(s(db,"user"));
        config.setPassword(System.getenv("DDIP_DB_PASSWORD"));config.setMaximumPoolSize(1);config.setMinimumIdle(0);config.setReadOnly(true);
        config.setConnectionTimeout(3000);config.setValidationTimeout(1000);config.addDataSourceProperty("connectTimeout","3");config.addDataSourceProperty("socketTimeout","5");
        config.setConnectionInitSql("SET TIME ZONE 'UTC'; SET default_transaction_read_only=on; SET statement_timeout='3s'; SET lock_timeout='3s'");
        dataSource=new HikariDataSource(config);jdbc=new JdbcTemplate(dataSource);jdbc.setQueryTimeout(3);
        var template=new TransactionTemplate(new JdbcTransactionManager(dataSource));template.setReadOnly(true);template.setTimeout(3);tx=new ManagementTransactions(template);
        var coordinator=new JdbcCoordinatorRepository(jdbc,tx,installation());
        var flyway=org.flywaydb.core.Flyway.configure().dataSource(dataSource).defaultSchema("ddip_management").schemas("ddip_management")
                .locations("classpath:db/migration").cleanDisabled(true).baselineOnMigrate(false).outOfOrder(false)
                .ignoreMigrationPatterns(new String[0]).validateMigrationNaming(true).connectRetries(0).load();
        new SchemaLifecycle(flyway,jdbc,coordinator).verify(false);
        var identity=jdbc.queryForMap("SELECT current_database() AS database, (SELECT oid::text FROM pg_database WHERE datname=current_database()) AS oid, "
                +"(SELECT system_identifier::text FROM pg_control_system()) AS system_identifier, pg_postmaster_start_time()::text AS started_at, version() AS version");
        need(s(db,"systemIdentifier").equals(identity.get("system_identifier")) && s(db,"databaseOid").equals(identity.get("oid")),"DATABASE_INSTALLATION_CHANGED");
        record("database-identity",values(identity));
        binding=new ImportBindings(jdbc,tx,installation()).control(p("controlBindingId"),p("executionProfileId"));
        need(binding.root().startsWith(root),"CONTROL_OUTSIDE_INSTALLATION");
    }
    long count(String table){need(TABLES.contains(table),"TABLE_NOT_ALLOWED");return jdbc.queryForObject("SELECT count(*) FROM ddip_management."+table,Long.class);}
    Map<String,Object> slot(){return jdbc.queryForMap("SELECT * FROM ddip_management.coordinator");}
    void idle(){var c=slot();need(installation().equals(c.get("installation_id")) && "READY".equals(c.get("mode"))
                && c.get("active_import_id")==null && c.get("active_attempt_id")==null && c.get("owner_instance_id")==null,"INSTALLATION_OCCUPIED");}
    void emptyInstallation()throws Exception{
        idle();
        for(String table:TABLES)if(!Set.of("coordinator","artifacts","artifact_import_operations","audit_events").contains(table))need(count(table)==0,"INSTALLATION_NOT_FRESH_"+table);
        need(count("artifacts")==1 && count("artifact_import_operations")==1,"ONLY_ONE_PRIOR_PUBLISHER_IMPORT_ALLOWED");
        var reference=jdbc.queryForMap("SELECT * FROM ddip_management.artifacts WHERE artifact_id=?",UUID.fromString(p("publisherReferenceArtifactId")));
        need("PUBLISHER_REFERENCE".equals(reference.get("kind")) && "content-intelligence".equals(reference.get("product_id"))
                && p("publisherReferenceEvidenceSha256").equals(reference.get("evidence_sha256"))
                && SourceJson.sha((byte[])reference.get("evidence_bytes")).equals(reference.get("evidence_sha256")),"PUBLISHER_REFERENCE_MISMATCH");
        var imported=jdbc.queryForMap("SELECT * FROM ddip_management.artifact_import_operations");
        need("IMPORT".equals(imported.get("kind")) && "SUCCEEDED".equals(imported.get("state")) && "SAFE_COMPLETION".equals(imported.get("outcome_code"))
                && installation().equals(imported.get("installation_id")),"GENUINE_COMPLETED_IMPORT_REQUIRED");
        var result=SourceJson.parse((byte[])imported.get("result_bytes"));
        need(SourceJson.sha((byte[])imported.get("result_bytes")).equals(imported.get("result_sha256"))
                && result.get("artifactIds").size()==1 && result.get("artifactIds").get(0).stringValue().equals(p("publisherReferenceArtifactId")),"IMPORT_REFERENCE_CORRESPONDENCE");
        try(var guard=InstallationHostGuard.acquire(binding)){
            var registered=NarrowContentRuntimeQualification.registration(jdbc,tx,guard,p("executionProfileId"),p("executionProfileSha256"),p("deploymentSha256"));
            var startup=NarrowContentWorkerStartup.startup(jdbc,tx,guard,p("executionProfileId"),p("executionProfileSha256"),p("deploymentSha256"),p("runtimeProfileId"),()->false);
            need(startup.compositionVersion()==2 && startup.python().equals(artifact("python").toString())
                    && startup.javaHome().equals(s(plan.get("runtime"),"javaHome")) && startup.processingRoot().equals(s(plan.get("runtime"),"processingRoot")),"INSTALLED_RUNTIME_MISMATCH");
            startup.check();storage=registered.storageBindings();
            // Registration digests alone do not identify the separately approved installed JARs.
            var strict=SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.content_pipeline_profiles WHERE profile_id=?",byte[].class,p("runtimeProfileId")));
            var fetchLaunch=ContentFetchLaunchRuntime.registered(jdbc,tx,registered,binding,s(strict,"fetchLaunchProfileId"));
            var classpath=fetchLaunch.configuration().get("manifest").get("classpath");
            need(classpath.equals(plan.get("nativeClasspath")),"APPROVED_COMPLETE_CLASSPATH_NOT_INSTALLED");
            for(String name:List.of("ingestion","provider")){
                boolean found=false;for(var entry:classpath)if(s(entry,"path").equals(artifact(name).toString())
                        && s(entry,"sha256").equals(s(plan.get("artifacts").get(name),"sha256")))found=true;
                need(found,"APPROVED_JAR_NOT_INSTALLED_"+name);
            }
            for(var entry:storage){var c=entry.get("configuration");if(c.has("root"))need(Path.of(s(c,"root")).startsWith(root),"STORAGE_OUTSIDE_INSTALLATION");}
            for(var entry:storage)if(Set.of("SILVER","GOLD","CONTENT","STAGING","TEMP").contains(s(entry,"role"))){
                try(var files=Files.list(Path.of(s(entry.get("configuration"),"root")))){need(files.findAny().isEmpty(),"NATIVE_OUTPUT_ROOT_NOT_EMPTY");}
            }
            record("registered-storage",storage);record("host-binding",SourceJson.parse(binding.descriptor()));
            var chain=SourceJson.object();for(String family:List.of("FETCH","BRONZE","SILVER","GOLD","CONTENT"))chain.set(family,registered.configuration(family));
            record("registered-chain",chain);record("strict-composition",strict);record("public-launch-profile",fetchLaunch.configuration());
        }
        var referenceEvidence=platform.contentapi.importing.RegisteredImportEvidence.validate((byte[])reference.get("evidence_bytes"),(String)reference.get("evidence_sha256"));
        var referenceSelection=referenceEvidence.selection().json();
        String inspectReference="import sys,json; sys.path.insert(0,sys.argv[1]); from publisher_reference import open_snapshot\n"
                +"a=json.loads(sys.argv[3]); r=open_snapshot(sys.argv[2],a['identity']['referenceId'],a['identity']['revision'],a['fingerprint']); print(json.dumps(dict(fingerprint=r.fingerprint,rows=len(r.rows))))\n";
        record("publisher-reference-native",command(List.of(artifact("python").toString(),"-I","-B","-c",inspectReference,s(plan.get("runtime"),"processingRoot"),
                localRoot("REFERENCE"),new String(SourceJson.canonical(referenceSelection),java.nio.charset.StandardCharsets.UTF_8)),"publisher-reference-native",30));
        idle();
        record("preflight-sql",snapshot());record("preflight-files",inventory(root));
    }
    JsonNode command(List<String> command,String name,int seconds)throws Exception{
        Path output=evidence.resolve(name+".stdout"),error=evidence.resolve(name+".stderr");
        need(!Files.exists(output) && !Files.exists(error),"PROCESS_OUTPUT_EXISTS");
        var child=new ProcessBuilder(command).redirectOutput(output.toFile()).redirectError(error.toFile());environment(child,false);
        var process=child.start();
        // This supervisor only handles known read-only diagnostics, never the effectful API/worker.
        if(!process.waitFor(seconds,TimeUnit.SECONDS)){process.destroy();need(process.waitFor(10,TimeUnit.SECONDS),"READ_ONLY_DIAGNOSTIC_STOP_UNCERTAIN");throw new IllegalStateException("READ_ONLY_DIAGNOSTIC_TIMEOUT");}
        need(process.exitValue()==0,"READ_ONLY_DIAGNOSTIC_FAILED_"+name);
        return SourceJson.parse(bounded(output,4*1024*1024),4*1024*1024);
    }
    void services()throws Exception{
        var observations=SourceJson.object();
        for(String name:List.of("postgres","seaweedfs")){
            var expected=plan.get("services").get(name);String id=s(expected,"containerId");
            String projection="{\"Id\":{{json .Id}},\"Image\":{{json .Image}},\"State\":{\"Running\":{{json .State.Running}}},"
                    +"\"Config\":{\"Image\":{{json .Config.Image}},\"Labels\":{\"ddip.qualification\":{{json (index .Config.Labels \"ddip.qualification\")}},"
                    +"\"ddip.qualification.installation\":{{json (index .Config.Labels \"ddip.qualification.installation\")}}}},"
                    +"\"Mounts\":{{json .Mounts}},\"StartedAt\":{{json .State.StartedAt}},\"NetworkSettings\":{\"Ports\":{{json .NetworkSettings.Ports}}}}";
            var row=command(List.of(artifact("docker").toString(),"inspect","--type","container","--format",projection,id),"inspect-"+name,10);
            need(s(row,"Id").equals(id) && s(row,"Image").equals(s(expected,"imageId")) && row.get("State").get("Running").booleanValue(),"SERVICE_CHANGED");
            var labels=row.get("Config").get("Labels");need(labels!=null && s(labels,"ddip.qualification").equals("e5-2d")
                    && s(labels,"ddip.qualification.installation").equals(installation().toString()),"SERVICE_NOT_RESERVED");
            String image=s(row.get("Config"),"Image");
            need(image.equals(name.equals("postgres")?"postgres@sha256:3645570cccdfa447589da9f57dd740faa29b30938e861289a5574b6ca6b03826":
                    "chrislusf/seaweedfs:4.48@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d"),"UNREVIEWED_SERVICE_IMAGE");
            String containerPort=name.equals("postgres")?"5432/tcp":"8333/tcp";
            var ports=row.get("NetworkSettings").get("Ports").get(containerPort);
            need(ports!=null && ports.isArray() && ports.size()==1 && s(ports.get(0),"HostIp").equals("127.0.0.1"),"SERVICE_MUST_BIND_LOOPBACK");
            String port=s(ports.get(0),"HostPort");
            if(name.equals("postgres"))need(s(plan.get("database"),"url").startsWith("jdbc:postgresql://127.0.0.1:"+port+"/"),"POSTGRES_ENDPOINT_MISMATCH");
            else need(s(bronze(),"endpoint").equals("http://127.0.0.1:"+port),"SEAWEED_ENDPOINT_MISMATCH");
            // The Docker command requests only this projection; service environment secrets are never read.
            var service=observations.putObject(name).put("containerId",id).put("imageId",s(row,"Image")).put("imageReference",image).put("loopbackPort",port).put("startedAt",s(row,"StartedAt"));
            service.set("mounts",row.get("Mounts"));
        }
        record("service-identities",observations);
        String script="import json,sys,boto3,botocore.config,platform\n"
                +"c=json.loads(sys.argv[1]); keys=json.load(open(c['credentialsFile']))\n"
                +"s=boto3.client('s3',endpoint_url=c['endpoint'],region_name=c['region'],aws_access_key_id=keys['accessKey'],aws_secret_access_key=keys['secretKey'],config=botocore.config.Config(connect_timeout=3,read_timeout=5,retries={'total_max_attempts':1},s3={'addressing_style':'path'}))\n"
                +"r=s.list_objects_v2(Bucket=c['bucket'],MaxKeys=1); assert r.get('KeyCount',0)==0 and not r.get('IsTruncated',False)\n"
                +"print(json.dumps(dict(status='EMPTY_REAL_BUCKET',endpoint=c['endpoint'],bucket=c['bucket'],python=platform.python_version(),boto3=boto3.__version__)))\n";
        record("storage-readiness",command(List.of(artifact("python").toString(),"-I","-B","-c",script,new String(SourceJson.canonical(bronze()),java.nio.charset.StandardCharsets.UTF_8)),"storage-readiness",20));
    }
    JsonNode bronze(){for(var entry:storage)if(s(entry,"bindingId").equals(p("bronzeBindingId"))){need(s(entry,"role").equals("BRONZE"),"BRONZE_ROLE_MISMATCH");return entry.get("configuration");}throw new IllegalStateException("BRONZE_BINDING_MISSING");}
    JsonNode objects(String name)throws Exception{
        String script="import json,sys,boto3,botocore.config,hashlib\n"
                +"c=json.loads(sys.argv[1]); k=json.load(open(c['credentialsFile'])); s=boto3.client('s3',endpoint_url=c['endpoint'],region_name=c['region'],aws_access_key_id=k['accessKey'],aws_secret_access_key=k['secretKey'],config=botocore.config.Config(connect_timeout=3,read_timeout=5,retries={'total_max_attempts':1},s3={'addressing_style':'path'}))\n"
                +"r=s.list_objects_v2(Bucket=c['bucket'],MaxKeys=10); assert not r.get('IsTruncated',False); out={}\n"
                +"for o in r.get('Contents',[]):\n"
                +" v=s.get_object(Bucket=c['bucket'],Key=o['Key']); b=v['Body'].read(5242881); v['Body'].close(); assert len(b)<=5242880; out[o['Key']]=dict(sha256=hashlib.sha256(b).hexdigest(),bytes=len(b),etag=o['ETag'],modified=str(o['LastModified']),contentType=v['ContentType'])\n"
                +"print(json.dumps(out,sort_keys=True))\n";
        var result=command(List.of(artifact("python").toString(),"-I","-B","-c",script,new String(SourceJson.canonical(bronze()),java.nio.charset.StandardCharsets.UTF_8)),name,90);
        record(name,result);return result;
    }
    void network()throws Exception{
        var answer=new java.util.concurrent.CompletableFuture<InetAddress[]>();
        Thread.ofVirtual().start(()->{try{answer.complete(InetAddress.getAllByName("raw.githubusercontent.com"));}catch(Throwable ex){answer.completeExceptionally(ex);}});
        var addresses=answer.get(15,TimeUnit.SECONDS);need(addresses.length>0 && addresses.length<=64,"NATIVE_DNS_ANSWER_LIMIT");
        var values=SourceJson.object().put("diagnosticOnly",true).put("tls","NOT_TESTED_BY_DIAGNOSTIC");var list=values.putArray("answers");boolean ipv4=false;
        for(var address:addresses){list.add(address.getHostAddress());if(address instanceof Inet4Address)ipv4=true;}
        need(ipv4,"NATIVE_IPV4_DNS_UNAVAILABLE");record("native-dns",values);
        record("network-environment",SourceJson.object().put("os",System.getProperty("os.name")).put("architecture",System.getProperty("os.arch"))
                .put("observedAt",Instant.now().toString()).put("javaRuntime",Runtime.version().toString()).put("javaVendor",System.getProperty("java.vendor"))
                .put("resolvConfSha256",hash(Path.of("/etc/resolv.conf")))
                .put("hostsSha256",hash(Path.of("/etc/hosts"))).put("policy","UNCHANGED_PRODUCTION_PUBLIC_V2"));
    }
    void environment(ProcessBuilder builder,boolean database){
        var env=builder.environment();env.clear();env.putAll(Map.of("PATH",s(plan.get("runtime"),"javaHome")+"/bin:/usr/bin:/bin","TZ","UTC","LANG","C.UTF-8",
                "JAVA_HOME",s(plan.get("runtime"),"javaHome"),"OPENBLAS_NUM_THREADS","1","OMP_NUM_THREADS","1"));
        if(database)env.putAll(Map.of("DDIP_DB_URL",s(plan.get("database"),"url"),"DDIP_DB_USER",s(plan.get("database"),"user"),
                "DDIP_DB_PASSWORD",System.getenv("DDIP_DB_PASSWORD"),"DDIP_INSTALLATION_ID",installation().toString()));
    }
    Process packaged(String role)throws Exception{
        pins();var command=new ArrayList<>(List.of(artifact("java").toString(),"-jar",artifact("api").toString(),"--ddip.role="+role,
                "--spring.config.location=classpath:/application.properties","--spring.main.banner-mode=off","--logging.level.root=WARN"));
        if(role.equals("worker")){
            command.addAll(List.of("--spring.main.web-application-type=none","--logging.level.platform.contentapi.execution.WorkerIdleLifecycle=DEBUG"));
            for(var entry:Map.of("execution-profile-id",p("executionProfileId"),"control-binding-id",p("controlBindingId"),"deployment-sha256",p("deploymentSha256"),
                    "runtime-profile-id",p("runtimeProfileId"),"python",artifact("python").toString(),"java-home",s(plan.get("runtime"),"javaHome"),
                    "processing-root",s(plan.get("runtime"),"processingRoot"),"poll-millis","250").entrySet())command.add("--ddip.worker."+entry.getKey()+"="+entry.getValue());
        }else{
            byte[] secret=new byte[32];new java.security.SecureRandom().nextBytes(secret);
            command.addAll(List.of("--server.address=127.0.0.1","--server.port="+plan.get("apiPort").intValue(),"--ddip.management.catalog="+artifact("catalog"),
                    "--ddip.management.cursor-key="+Base64.getUrlEncoder().withoutPadding().encodeToString(secret),"--ddip.management.origins="+origin,
                    "--content.analytics.python-executable="+artifact("python"),"--content.analytics.processing-root="+s(plan.get("runtime"),"processingRoot"),
                    "--content.analytics.root="+localRoot("CONTENT"),"--content.analytics.gold-root="+localRoot("GOLD"),"--content.analytics.reference-root="+localRoot("REFERENCE"),
                    "--content.analytics.publication="));
        }
        Path log=evidence.resolve(role+".log");need(!Files.exists(log),"PROCESS_LOG_EXISTS");
        var builder=new ProcessBuilder(command).directory(root.toFile()).redirectErrorStream(true).redirectOutput(log.toFile());environment(builder,true);
        var process=builder.start();record(role+"-process",process(process.toHandle()));return process;
    }
    String localRoot(String role){for(var entry:storage)if(s(entry,"role").equals(role))return s(entry.get("configuration"),"root");throw new IllegalStateException("STORAGE_ROLE_MISSING");}
    static ObjectNode process(ProcessHandle process){var n=SourceJson.object().put("pid",Long.toString(process.pid())).put("alive",process.isAlive());
        process.info().startInstant().ifPresent(value->n.put("startedAt",value.toString()));return n;}
    JsonNode request(String method,String path,JsonNode body,String key,int expected)throws Exception{
        var builder=HttpRequest.newBuilder(URI.create(origin+path)).timeout(Duration.ofSeconds(30));
        if(body==null)builder.GET();else builder.header("Origin",origin).header("X-DDIP-Local-Request","1").header("Content-Type","application/json")
                .header("Idempotency-Key",key).method(method,HttpRequest.BodyPublishers.ofByteArray(SourceJson.canonical(body)));
        // One application invocation. Never resend a mutation after an exception or lost acknowledgment.
        var response=http.send(builder.build(),HttpResponse.BodyHandlers.ofInputStream());byte[] raw;
        try(var stream=response.body()){raw=stream.readNBytes(4*1024*1024+1);}need(raw.length<=4*1024*1024,"HTTP_RESPONSE_LIMIT");
        need(response.statusCode()==expected,"HTTP_STATUS_"+response.statusCode());return SourceJson.parse(raw,4*1024*1024);
    }
    void awaitApi()throws Exception{
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);
        while(System.nanoTime()<until && api.isAlive()){
            // Only read-only readiness requests may repeat.
            try{request("GET","/api/management/v1/capabilities",null,null,200);need(ownsListener(api.toHandle(),plan.get("apiPort").intValue()),"API_LISTENER_NOT_OWNED");return;}
            catch(java.net.ConnectException ex){Thread.sleep(100);}
        }throw new IllegalStateException("PACKAGED_API_STARTUP_FAILED");
    }
    static boolean ownsListener(ProcessHandle process,int port)throws Exception{
        var inodes=new HashSet<String>();
        for(String file:List.of("/proc/net/tcp","/proc/net/tcp6"))for(String line:Files.readAllLines(Path.of(file))){
            var parts=line.trim().split("\\s+");if(parts.length>9 && parts[1].endsWith(String.format(":%04X",port)) && parts[3].equals("0A"))inodes.add("socket:["+parts[9]+"]");
        }
        try(var entries=Files.list(Path.of("/proc/"+process.pid()+"/fd"))){return entries.anyMatch(path->{try{return inodes.contains(Files.readSymbolicLink(path).toString());}catch(IOException ex){return false;}});}
    }
    ObjectNode submission(){var n=SourceJson.object().put("pipeline","CONTENT_RSS_V1").put("executionProfileId",p("executionProfileId"));
        n.putArray("fetchSources").addObject().put("sourceId",p("sourceId")).put("revision",1);n.putArray("retainedSilverArtifactIds");return n.put("publisherReferenceArtifactId",p("publisherReferenceArtifactId"));}
    UUID submit(String key)throws Exception{submitted=true;var response=request("POST","/api/management/v1/runs",submission(),key,202);return UUID.fromString(s(response.get("data"),"runId"));}
    void source()throws Exception{
        var body=SourceJson.object().put("productId","content-intelligence").put("sourceId",p("sourceId")).put("displayName","Reviewed immutable public Atom qualification").put("enabled",true);
        body.putObject("config").put("configVersion",1).put("profile","rss-atom-v1").put("dataOrigin","publisher").put("feedEndpointId",p("endpointId"))
                .putNull("registeredProfileSha256").put("inputStorageProfileId",p("bronzeBindingId"));
        submitted=true;record("source-api",request("POST","/api/management/v1/sources",body,"qualification-source",201));
        record("source-revision-api",request("GET","/api/management/v1/sources/content-intelligence/"+p("sourceId")+"/revisions",null,null,200));
    }
    void awaitSuccess(UUID run)throws Exception{
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(plan.get("deadlineSeconds").intValue());
        while(System.nanoTime()<until){
            need(worker.isAlive(),"WORKER_EXITED_RETAIN_OCCUPANCY");
            String state=jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run);
            if(state.equals("SUCCEEDED")){idle();return;}
            need(Set.of("QUEUED","RUNNING").contains(state),"RUN_NOT_SUCCESSFUL_"+state);Thread.sleep(250);
        }throw new IllegalStateException("RUN_DEADLINE_AMBIGUOUS");
    }
    long logSize()throws IOException{return Files.size(evidence.resolve("worker.log"));}
    void awaitAcknowledgedIdle(long offset)throws Exception{
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
        while(System.nanoTime()<until){need(worker.isAlive(),"WORKER_EXITED");
            byte[] raw=bounded(evidence.resolve("worker.log"),4*1024*1024);
            String log=new String(raw,java.nio.charset.StandardCharsets.UTF_8);
            need(!log.contains("WORKER_OWNERSHIP_UNCERTAIN") && !log.contains("WORKER_STOP_UNCERTAIN"),"WORKER_UNCERTAIN");
            String fresh=new String(Arrays.copyOfRange(raw,(int)Math.min(offset,raw.length),raw.length),java.nio.charset.StandardCharsets.UTF_8);
            if(fresh.contains("WORKER_QUEUE_IDLE")){idle();return;}Thread.sleep(100);
        }throw new IllegalStateException("IDLE_ACKNOWLEDGMENT_NOT_OBSERVED");
    }
    byte[] frozen(UUID run){return jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE run_id=?",byte[].class,run);}
    void verifyFrozen(UUID run,byte[] frozen){
        need(Arrays.equals(frozen,frozen(run)),"FROZEN_SELECTION_CHANGED");var selected=SourceJson.parse(frozen,256*1024);
        need(selected.get("sources").size()==1 && selected.get("sources").get(0).get("revision").intValue()==1
                && s(selected.get("sources").get(0),"sourceId").equals(p("sourceId"))
                && s(selected.get("sources").get(0).get("endpoint"),"uri").equals(FIXTURE)
                && s(selected.get("executionProfile"),"profileId").equals(p("executionProfileId"))
                && s(selected.get("executionProfile"),"configurationSha256").equals(p("executionProfileSha256")),"FROZEN_SOURCE_OR_PROFILE_MISMATCH");
        var reference=RssContentInputs.reference(selected);need(s(reference,"artifactId").equals(p("publisherReferenceArtifactId"))
                && s(reference,"evidenceSha256").equals(p("publisherReferenceEvidenceSha256")),"FROZEN_PUBLISHER_REFERENCE_CHANGED");
    }
    void verify(UUID run,byte[] frozen)throws Exception{
        verifyFrozen(run,frozen);
        var row=jdbc.queryForMap("SELECT * FROM ddip_management.runs WHERE run_id=?",run);
        UUID attempt=(UUID)row.get("current_attempt_id"),artifact=(UUID)row.get("result_artifact_id");need(artifact!=null,"RESULT_MISSING");
        var a=jdbc.queryForMap("SELECT * FROM ddip_management.attempts WHERE attempt_id=?",attempt);
        need("SUCCEEDED".equals(a.get("state")) && ((Number)a.get("attempt_no")).intValue()==1 && "EXECUTE".equals(a.get("kind")),"NOT_ORIGINAL_SUCCESS");
        need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_admission_milestones WHERE attempt_id=?",Long.class,attempt)==2,"PREPARING_READY_MISSING");
        var operations=request("GET","/api/management/v1/runs/"+run+"/attempts/"+attempt+"/native-operations?limit=100",null,null,200);
        var steps=operations.get("data");need(steps.size()==17,"SEVENTEEN_OPERATIONS_REQUIRED");
        for(int i=0;i<17;i++)need(steps.get(i).get("stepIndex").intValue()==i && !steps.get(i).get("checkpointedAt").isNull(),"NATIVE_ORDER_OR_CHECKPOINT");
        var detail=request("GET","/api/management/v1/runs/"+run,null,null,200);
        need(s(detail.get("data").get("result"),"artifactId").equals(artifact.toString())
                && s(detail.get("data").get("result"),"verificationStatus").equals("VERIFIED_AT_EXECUTION")
                && s(detail.get("data").get("counts"),"articles").equals("1"),"READ_API_RESULT_MISMATCH");
        record("run-"+run+"-operations",operations);record("run-"+run+"-api",detail);
        UUID fetch=jdbc.queryForObject("SELECT operation_id FROM ddip_management.content_native_operations WHERE attempt_id=?",UUID.class,attempt);
        need(NarrowContentFetchCompletion.inspect(jdbc,tx,fetch).isPresent(),"FETCH_CHECKPOINT_NOT_VERIFIED");
        var checkpoint=jdbc.queryForMap("SELECT * FROM ddip_management.content_fetch_checkpoints WHERE operation_id=?",fetch);
        need(((Number)checkpoint.get("payload_length")).intValue()==726 && FIXTURE_SHA.equals(checkpoint.get("payload_sha256")),"PUBLIC_PAYLOAD_MISMATCH");
        Path payload=binding.root().resolve((String)checkpoint.get("journal_address")).resolve("payload.xml");
        need(Files.size(payload)==726 && hash(payload).equals(FIXTURE_SHA),"RETAINED_PAYLOAD_MISMATCH");
        var pipelineProofs=jdbc.queryForList("SELECT p.*,o.command_bytes,o.command_sha256 FROM ddip_management.content_pipeline_checkpoints p JOIN ddip_management.content_pipeline_operations o USING(operation_id) WHERE o.attempt_id=? ORDER BY o.step_index",attempt);
        need(pipelineProofs.size()==15,"NATIVE_PIPELINE_PROOFS_MISSING");
        for(var proof:pipelineProofs){
            need(SourceJson.sha((byte[])proof.get("proof_bytes")).equals(proof.get("proof_sha256"))
                    && SourceJson.sha((byte[])proof.get("command_bytes")).equals(proof.get("command_sha256")),"PIPELINE_PROOF_CHANGED");
        }
        var completed=jdbc.queryForMap("SELECT * FROM ddip_management.content_pipeline_completions WHERE attempt_id=?",attempt);
        need(artifact.equals(completed.get("artifact_id")) && SourceJson.sha((byte[])completed.get("proof_bytes")).equals(completed.get("proof_sha256")),"FINAL_PROOF_MISMATCH");
        var registered=jdbc.queryForMap("SELECT * FROM ddip_management.artifacts WHERE artifact_id=?",artifact);
        var nativeEvidence=SourceJson.parse((byte[])registered.get("evidence_bytes"),3*1024*1024);
        need(SourceJson.sha((byte[])registered.get("evidence_bytes")).equals(registered.get("evidence_sha256")),"ARTIFACT_EVIDENCE_CHANGED");
        need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifact_dependencies WHERE artifact_id=?",Long.class,artifact)==2
                && jdbc.queryForObject("SELECT dependency_artifact_id FROM ddip_management.artifact_dependencies WHERE artifact_id=? AND role='PUBLISHER_REFERENCE'",UUID.class,artifact).toString().equals(p("publisherReferenceArtifactId")),"DEPENDENCY_CORRESPONDENCE");
        String query="import sys,json; sys.path.insert(0,sys.argv[1]); from content_analytics import open_publication\n"
                +"with open_publication(sys.argv[2],sys.argv[3],gold_root=sys.argv[4],reference_root=sys.argv[5]) as opened:\n"
                +" r=opened.report('catalog',limit=10); assert len(r['rows'])==1 and r['rows'][0]['title']==sys.argv[6]; print(json.dumps(r,default=str))\n";
        record("run-"+run+"-native-read",command(List.of(artifact("python").toString(),"-I","-B","-c",query,s(plan.get("runtime"),"processingRoot"),
                localRoot("CONTENT"),s(nativeEvidence,"nativeFingerprint"),localRoot("GOLD"),localRoot("REFERENCE"),TITLE),"native-read-"+run,120));
        need(count("stage_invocations")==0 && count("stage_artifacts")==0 && count("rss_deliveries")==0,"LEGACY_HISTORY_WRITTEN");
    }
    JsonNode values(Object value)throws IOException{
        if(value instanceof Map<?,?> map){var n=SourceJson.object();for(var entry:map.entrySet())n.set(entry.getKey().toString(),values(entry.getValue()));return n;}
        if(value instanceof byte[] raw){String sha=SourceJson.sha(raw);Path file=evidence.resolve("sql-bytes-"+sha+".bin");
            if(Files.exists(file)){need(Arrays.equals(raw,bounded(file,4*1024*1024)),"SQL_BYTE_EVIDENCE_CHANGED");}else write(file,raw);
            return SourceJson.object().put("sha256",sha).put("bytes",raw.length).put("file",file.getFileName().toString());}
        var n=SourceJson.object();if(value==null)n.putNull("value");else if(value instanceof Boolean b)n.put("value",b);
        else if(value instanceof Number number)n.put("value",number.longValue());else n.put("value",value.toString());return n.get("value");
    }
    JsonNode snapshot()throws IOException{var result=SourceJson.object();var tables=new ArrayList<>(TABLES);tables.addAll(REGISTRATION_TABLES);
        for(String table:tables){var rows=result.putArray(table);
        var all=jdbc.queryForList("SELECT * FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text LIMIT 2001");
        need(all.size()<=2000,"SQL_EVIDENCE_ROW_LIMIT");for(var row:all)rows.add(values(row));
    }return result;}
    static JsonNode inventory(Path root)throws Exception{
        var result=SourceJson.object();long total=0;int count=0;
        need(root.equals(root.toRealPath()),"INVENTORY_ROOT_NOT_CANONICAL");
        try(var walk=Files.walk(root,32)){
            var iterator=walk.limit(20001).sorted().iterator();while(iterator.hasNext()){
                Path path=iterator.next();var before=Files.readAttributes(path,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
                need(++count<=20000 && root.relativize(path).getNameCount()<32 && !before.isSymbolicLink()
                        && (before.isDirectory() || before.isRegularFile()),"FILE_INVENTORY_UNSAFE");
                var entry=SourceJson.object().put("kind",before.isDirectory()?"DIRECTORY":"FILE").put("bytes",Long.toString(before.size()))
                        .put("modified",before.lastModifiedTime().toString()).put("identity",Objects.toString(before.fileKey()));
                if(before.isRegularFile()){
                    need(((Number)Files.getAttribute(path,"unix:nlink",LinkOption.NOFOLLOW_LINKS)).longValue()==1,"INVENTORY_HARD_LINK");
                    need(before.size()<=256L*1024*1024 && (total+=before.size())<=1024L*1024*1024,"FILE_INVENTORY_LIMIT");entry.put("sha256",hash(path));
                    var after=Files.readAttributes(path,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
                    need(before.size()==after.size() && before.lastModifiedTime().equals(after.lastModifiedTime()) && Objects.equals(before.fileKey(),after.fileKey()),"FILE_CHANGED_DURING_READ");
                }result.set(root.relativize(path).toString(),entry);
            }
        }return result;
    }
    void stopIdle()throws Exception{
        idle();long offset=logSize();awaitAcknowledgedIdle(offset);record("idle-before-stop",values(slot()));
        worker.destroy();need(worker.waitFor(60,TimeUnit.SECONDS),"IDLE_WORKER_STOP_UNCERTAIN");
        need(count("worker_instances")==1 && "STOPPED".equals(jdbc.queryForObject("SELECT status FROM ddip_management.worker_instances",String.class)),"IDLE_STOP_NOT_ACKNOWLEDGED");idle();
        api.destroy();need(api.waitFor(30,TimeUnit.SECONDS),"API_STOP_UNCERTAIN");
    }
    void complete()throws Exception{
        api=packaged("api");awaitApi();source();UUID first=submit("public-first");byte[] firstFrozen=frozen(first);verifyFrozen(first,firstFrozen);
        record("first-submission",SourceJson.object().put("runId",first.toString()).set("frozenSelection",values(firstFrozen)));
        need(submit("public-first").equals(first) && count("runs")==1 && count("attempts")==0,"DUPLICATE_SUBMISSION_CHANGED_EXECUTION");
        worker=packaged("worker");awaitSuccess(first);long offset=logSize();awaitAcknowledgedIdle(offset);verify(first,firstFrozen);
        record("first-success-sql",snapshot());
        UUID second=submit("public-second");byte[] secondFrozen=frozen(second);verifyFrozen(second,secondFrozen);
        record("second-submission",SourceJson.object().put("runId",second.toString()).set("frozenSelection",values(secondFrozen)));
        need(!first.equals(second),"SECOND_RUN_NOT_NEW");awaitSuccess(second);
        offset=logSize();awaitAcknowledgedIdle(offset);verify(second,secondFrozen);
        var owners=jdbc.queryForList("SELECT a.instance_id,a.owner_epoch FROM ddip_management.attempts a JOIN ddip_management.runs r USING(run_id) ORDER BY r.submission_seq");
        need(owners.size()==2 && owners.get(0).get("instance_id").equals(owners.get(1).get("instance_id"))
                && ((Number)owners.get(1).get("owner_epoch")).longValue()==((Number)owners.get(0).get("owner_epoch")).longValue()+1 && count("worker_instances")==1,"ACKNOWLEDGED_SESSION_NOT_REUSED");
        var beforeObjects=objects("before-replay-objects");need(beforeObjects.size()==4,"TWO_BRONZE_OBJECT_PAIRS_REQUIRED");
        var beforeFiles=inventory(root);var before=snapshot();record("before-replay-files",beforeFiles);record("before-replay-sql",before);
        need(submit("public-first").equals(first),"COMPLETED_REPLAY_NOT_SAME_RUN");
        var after=snapshot();var afterFiles=inventory(root);record("after-replay-sql",after);record("after-replay-files",afterFiles);
        // Heartbeats are mutable coordination metadata. All execution/output evidence must remain exact.
        var immutableBefore=(ObjectNode)before.deepCopy();var immutableAfter=(ObjectNode)after.deepCopy();
        for(String key:List.of("coordinator","worker_instances")){immutableBefore.remove(key);immutableAfter.remove(key);}
        need(immutableBefore.equals(immutableAfter) && beforeFiles.equals(afterFiles) && beforeObjects.equals(objects("after-replay-objects")),"COMPLETED_REPLAY_REWROTE_EVIDENCE");
        stopIdle();record("final-sql",snapshot());record("final-files",inventory(root));
        record("result",SourceJson.object().put("status","PACKAGED_PUBLIC_COMPLETE_PASSED").put("publicDnsIpv4Tls","PASSED_VIA_PRODUCTION_CHILD")
                .put("packagedEndToEnd","PASSED").put("secondAcknowledgedSessionRun","PASSED").put("duplicateAndCompletedReplay","PASSED")
                .put("liveChildStop","NOT_RUN_SEPARATE_INSTALLATION_REQUIRED").put("operationalActivation","NOT_AUTHORIZED"));
    }
    void liveStop()throws Exception{
        api=packaged("api");awaitApi();source();UUID run=submit("public-live-stop");byte[] frozen=frozen(run);verifyFrozen(run,frozen);
        record("live-stop-submission",SourceJson.object().put("runId",run.toString()).set("frozenSelection",values(frozen)));worker=packaged("worker");
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(plan.get("deadlineSeconds").intValue());JsonNode live=null,spark=null;UUID operation=null;
        while(System.nanoTime()<until){
            need(worker.isAlive(),"WORKER_EXITED_BEFORE_STOP");
            var rows=jdbc.queryForList("SELECT o.operation_id,p.phase_bytes FROM ddip_management.content_pipeline_operations o JOIN ddip_management.content_pipeline_phases p USING(operation_id) "
                    +"WHERE o.step_index=3 AND p.phase='CHILD_RECORDED' AND EXISTS(SELECT 1 FROM ddip_management.content_pipeline_phases r WHERE r.operation_id=o.operation_id AND r.phase='RELEASE_AUTHORIZED') "
                    +"AND NOT EXISTS(SELECT 1 FROM ddip_management.content_pipeline_phases e WHERE e.operation_id=o.operation_id AND e.phase='CANDIDATE_EXITED')");
            if(rows.size()==1){var candidate=SourceJson.parse((byte[])rows.getFirst().get("phase_bytes")).get("child");
                var child=ProcessHandle.of(Long.parseLong(s(candidate,"pid")));if(child.isPresent() && child.get().isAlive()
                        && child.get().info().startInstant().map(t->t.truncatedTo(ChronoUnit.MICROS).equals(Instant.parse(s(candidate,"startedAt")).truncatedTo(ChronoUnit.MICROS))).orElse(false)
                        && worker.descendants().anyMatch(d->d.pid()==child.get().pid())){
                    // RELEASE_AUTHORIZED alone precedes stdin delivery. A real Spark JVM below the
                    // recorded Python child independently establishes that native work has started.
                    var descendant=child.get().descendants().filter(d->d.isAlive()
                            && d.info().command().map(c->Path.of(c).equals(artifact("java"))).orElse(false)
                            && d.info().commandLine().orElse("").contains("org.apache.spark.deploy.SparkSubmit")).findFirst();
                    if(descendant.isPresent()){live=candidate;spark=process(descendant.get());operation=(UUID)rows.getFirst().get("operation_id");break;}
                }}
            need(!"SUCCEEDED".equals(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)),"LIVE_CHILD_WINDOW_MISSED_NO_RETRY");Thread.sleep(20);
        }
        need(live!=null,"NO_LIVE_NATIVE_CHILD_OBSERVED");record("live-child-before-stop",live);record("live-stop-before-sql",snapshot());
        record("live-spark-before-stop",spark);
        var descendants=SourceJson.object().putArray("processes");worker.descendants().forEach(child->descendants.add(process(child)));record("descendants-before-stop",descendants);
        record("intentional-worker-stop",SourceJson.object().put("runId",run.toString()).put("operationId",operation.toString()).put("signal","SIGTERM")
                .put("safeEnd","NOT_AUTHORIZED").put("quiescence","REQUIRES_FRESH_OPERATOR_REVIEW"));
        var originalChild=ProcessHandle.of(Long.parseLong(s(live,"pid")));Instant originalStart=Instant.parse(s(live,"startedAt")).truncatedTo(ChronoUnit.MICROS);
        need(originalChild.isPresent() && originalChild.get().isAlive() && originalChild.get().info().startInstant()
                .map(t->t.truncatedTo(ChronoUnit.MICROS).equals(originalStart)).orElse(false),"LIVE_CHILD_WINDOW_MISSED_NO_RETRY");
        var originalSpark=ProcessHandle.of(Long.parseLong(s(spark,"pid")));Instant sparkStart=Instant.parse(s(spark,"startedAt"));
        need(originalSpark.isPresent() && originalSpark.get().isAlive() && originalSpark.get().info().startInstant().map(sparkStart::equals).orElse(false),"LIVE_SPARK_WINDOW_MISSED_NO_RETRY");
        worker.destroy();need(worker.waitFor(60,TimeUnit.SECONDS),"LIVE_WORKER_STOP_UNCERTAIN");
        var c=slot();need(c.get("active_attempt_id")!=null && c.get("owner_instance_id")!=null && c.get("active_import_id")==null,"INTERRUPTED_OCCUPANCY_NOT_RETAINED");
        need(count("content_pipeline_completions")==0 && jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE kind='EXECUTE'",Long.class)==1,"INTERRUPTED_EXECUTION_CHANGED");
        record("live-stop-after-sql",snapshot());record("live-stop-after-files",inventory(root));
        var processes=SourceJson.object();processes.set("recordedChild",live);var observed=ProcessHandle.of(Long.parseLong(s(live,"pid")));
        processes.set("observation",observed.map(PackagedPublicContentAcceptance::process).orElseGet(()->SourceJson.object().put("present",false)));
        processes.put("quiescence","NOT_ESTABLISHED_BY_WORKER_EXIT");record("live-stop-process-observations",processes);
        Instant recordedStart=Instant.parse(s(live,"startedAt")).truncatedTo(ChronoUnit.MICROS);
        need(observed.isEmpty() || !observed.get().isAlive() || observed.get().info().startInstant()
                .map(t->!t.truncatedTo(ChronoUnit.MICROS).equals(recordedStart)).orElse(false),"RECORDED_NATIVE_CHILD_STOP_UNCERTAIN");
        // The API/services/installation are deliberately retained. No safe-end, restart, refetch or cleanup.
        record("result",SourceJson.object().put("status","LIVE_STOP_OBSERVED_REVIEW_REQUIRED").put("retainedOccupancy",true)
                .put("packagedComplete","NOT_RUN").put("reviewedQuiescence","OPEN_GATE").put("safeEnd","NOT_PERFORMED").put("operationalActivation","NOT_AUTHORIZED"));
    }
    void run(String mode)throws Exception{
        pins();database();emptyInstallation();services();network();pins();
        if(mode.equals("preflight"))record("result",SourceJson.object().put("status","PREREQUISITES_OBSERVED").put("publicHttpsTls","NOT_RUN")
                .put("packagedEndToEnd","NOT_RUN").put("operationalActivation","NOT_AUTHORIZED"));
        else if(s(plan,"scenario").equals("complete"))complete();else liveStop();
    }
    public static void main(String[] args)throws Exception{
        need(args.length==3 && Set.of("preflight","run").contains(args[0]) && args[2].matches("[0-9a-f]{64}"),"LAUNCHER_REQUIRED");
        byte[] raw=bounded(Path.of(args[1]),65536);need(SourceJson.sha(raw).equals(args[2]),"PLAN_CHANGED");
        var runner=new PackagedPublicContentAcceptance(SourceJson.parse(raw));
        try{runner.authorization(args[0],raw);runner.run(args[0]);}
        catch(Throwable ex){
            // Never follow an ambiguous API/worker outcome with cleanup or another mutation.
            var failure=SourceJson.object().put("status",runner.submitted?"FAILED_OR_AMBIGUOUS_RETAINED":"BLOCKED")
                    .put("exceptionType",ex.getClass().getName()).put("nextAction","STOP_AND_REVIEW_RETAINED_INSTALLATION")
                    .put("publicAcceptance","NOT_PASSED").put("operationalActivation","NOT_AUTHORIZED");
            if(ex instanceof GateFailure && ex.getMessage().matches("[A-Za-z0-9_-]{1,160}"))failure.put("safeCode",ex.getMessage());
            else if(ex instanceof PreflightFailure f)failure.put("safeCode",f.code().name());
            else if(ex instanceof ManagementFailure f)failure.put("safeCode",f.code().name());
            if(runner.api!=null)failure.set("api",process(runner.api.toHandle()));if(runner.worker!=null)failure.set("worker",process(runner.worker.toHandle()));
            runner.record("failure",failure);System.err.println(SourceJson.canonical(failure).length+" bytes of failure evidence retained; acceptance NOT PASSED.");
            System.exit(2);
        }finally{if(runner.dataSource!=null)runner.dataSource.close();}
    }
}
