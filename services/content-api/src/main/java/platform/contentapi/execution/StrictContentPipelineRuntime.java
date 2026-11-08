package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/** Separate installed strict child identity. It never changes the meaning of historical runtime-v2. */
final class StrictContentPipelineRuntime {
    private record Pin(InstallationHostGuard.Identity identity,FileTime modified,long size,String sha256) {}
    final String id,digest;
    final JsonNode config;
    final NarrowContentRuntimeQualification.Snapshot registered;
    private final InstallationHostGuard guard;
    private final RssContentRuntime nativeRuntime;
    private final Map<Path,Pin> pins;
    private StrictContentPipelineRuntime(String id,String digest,JsonNode config,NarrowContentRuntimeQualification.Snapshot registered,
            InstallationHostGuard guard,RssContentRuntime nativeRuntime){
        this.id=id;this.digest=digest;this.config=config.deepCopy();this.registered=registered;this.guard=guard;this.nativeRuntime=nativeRuntime;
        pins=observe();
    }
    static String protocolDigest(){return SourceJson.sha(SourceJson.canonical(SourceJson.object().put("strictContentRuntimeVersion",1)
            .put("adapter","strict_native_content").put("bootstrapVersion",2).put("commandProtocolVersion",2).put("ledgerVersion",2)));}
    static StrictContentPipelineRuntime load(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,
            NarrowContentRuntimeQualification.Snapshot registered,String id){
        need(id!=null && id.matches("[A-Za-z0-9_-]{1,128}"),RUNTIME_MISMATCH);
        var rows=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.content_pipeline_profiles WHERE profile_id=?",id));
        need(rows.size()==1,RUNTIME_MISMATCH);byte[] raw=(byte[])rows.getFirst().get("configuration_bytes");String digest=(String)rows.getFirst().get("configuration_sha256");
        need(digest.equals(SourceJson.sha(raw)),RUNTIME_MISMATCH);var c=PreflightFiles.canonical(raw,65536);
        SourceJson.fields(c,"strictContentPipelineVersion","runProfileId","runProfileSha256","contentRuntimeId","contentRuntimeSha256",
                "controlBindingId","strictRuntimeSha256","python","processingRoot","javaHome","modules","versions",
                "fetchLaunchProfileId","bronzeLaunchProfileId","bronzeVerificationProfileId");
        var frozen=registered.frozenSelection();var silver=registered.configuration("SILVER");
        need(c.get("strictContentPipelineVersion").isInt() && c.get("strictContentPipelineVersion").intValue()==1
                && c.get("runProfileId").equals(frozen.get("executionProfile").get("profileId"))
                && c.get("runProfileSha256").equals(frozen.get("executionProfile").get("configurationSha256"))
                && c.get("contentRuntimeId").stringValue().equals(registered.id(NarrowContentRuntimeQualification.Family.CONTENT))
                && c.get("contentRuntimeSha256").stringValue().equals(registered.digest("CONTENT"))
                && c.get("controlBindingId").stringValue().equals(guard.binding().id())
                && c.get("strictRuntimeSha256").stringValue().equals(protocolDigest()),RUNTIME_MISMATCH);
        for(String key:List.of("python","processingRoot","javaHome"))need(c.get(key).equals(silver.get(key)),RUNTIME_MISMATCH);
        need(c.get("versions").equals(versions()) && c.get("modules").isObject() && c.get("modules").size()>0 && c.get("modules").size()<=128,RUNTIME_MISMATCH);
        for(String key:List.of("fetchLaunchProfileId","bronzeLaunchProfileId","bronzeVerificationProfileId"))need(c.get(key).stringValue().matches("[A-Za-z0-9_-]{1,128}"),RUNTIME_MISMATCH);
        var runtime=RssContentRuntime.load(jdbc,tx,guard,registered.run(),frozen,registered.id(NarrowContentRuntimeQualification.Family.CONTENT));
        StrictFetchLaunchRuntime.load(jdbc,tx,guard,registered,c.get("fetchLaunchProfileId").stringValue());
        StrictBronzeLaunchRuntime.load(jdbc,tx,guard,registered,c.get("bronzeLaunchProfileId").stringValue());
        StrictBronzeVerificationRuntime.load(jdbc,tx,guard,registered,c.get("bronzeVerificationProfileId").stringValue());
        return new StrictContentPipelineRuntime(id,digest,c,registered,guard,runtime);
    }
    static JsonNode versions(){return SourceJson.object().put("python","3.12.15").put("pyspark","4.0.2").put("py4j","0.10.9.9")
            .put("boto3","1.43.114").put("botocore","1.43.114").put("duckdb","1.4.5").put("java","21");}
    /** All required imports, actual interpreter/package versions and full closure must pass before FETCH. */
    void probe(Runnable fence){
        check();fence.run();
        String script="import sys,json; sys.path.insert(0,sys.argv[1]); import strict_native_content as n; "
                +"print(json.dumps({'versions':n.runtime_probe(),'modules':list(n.MODULES)},sort_keys=True,separators=(',',':')),end='')";
        var observed=ReadOnlyVerifierProcess.receiptReader().run(command(script),environment(),guard.binding().root(),Duration.ofSeconds(30),new ReadOnlyVerifierProcess.Observer(){
            public void beforeStart(){check();fence.run();}public void started(InstallationHostGuard.ProcessIdentity child){fence.run();}public void heartbeat(){check();fence.run();}
        });
        need(observed.exitCode()==0 && observed.stderrBytes()==0 && observed.descendants().isEmpty() && observed.stdout()!=null,RUNTIME_MISMATCH);
        var result=PreflightFiles.canonical(observed.stdout(),65536);SourceJson.fields(result,"versions","modules");
        var actual=result.get("versions");need(actual.isObject(),RUNTIME_MISMATCH);
        var expected=(tools.jackson.databind.node.ObjectNode)versions().deepCopy();expected.remove("java");
        need(actual.equals(expected),RUNTIME_MISMATCH);
        var names=new TreeSet<String>();for(var name:result.get("modules"))need(names.add(name.stringValue()),RUNTIME_MISMATCH);
        need(names.equals(new TreeSet<>(config.get("modules").propertyNames())),RUNTIME_MISMATCH);
        check();fence.run();
    }
    void check(){nativeRuntime.check();need(pins.equals(observe()),INPUT_CHANGED);}
    private Map<Path,Pin> observe(){
        guard.checkHeld(guard.binding().installationId());var result=new LinkedHashMap<Path,Pin>();
        try{
            Path processing=Path.of(config.get("processingRoot").stringValue());need(processing.isAbsolute() && processing.equals(processing.toRealPath()),RUNTIME_MISMATCH);
            for(String name:config.get("modules").propertyNames()){
                need(name.matches("[a-z][a-z0-9_]*\\.py"),RUNTIME_MISMATCH);
                result.put(processing.resolve(name),pin(processing.resolve(name),1024*1024,config.get("modules").get(name).stringValue()));
            }
            for(Path executable:List.of(Path.of(config.get("python").stringValue()),Path.of(config.get("javaHome").stringValue()).resolve("bin/java"))){
                need(executable.isAbsolute() && Files.isExecutable(executable),RUNTIME_MISMATCH);Path real=executable.toRealPath();result.put(real,pin(real,64*1024*1024,null));
            }
            return Map.copyOf(result);
        }catch(PreflightFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    private static Pin pin(Path p,int max,String expected)throws java.io.IOException{
        var identity=InstallationHostGuard.identity(p);var modified=Files.getLastModifiedTime(p,LinkOption.NOFOLLOW_LINKS);byte[] bytes=PreflightFiles.read(p,max,false);String sha=SourceJson.sha(bytes);
        need((expected==null || expected.equals(sha)) && identity.equals(InstallationHostGuard.identity(p)) && modified.equals(Files.getLastModifiedTime(p,LinkOption.NOFOLLOW_LINKS)),INPUT_CHANGED);
        return new Pin(identity,modified,bytes.length,sha);
    }
    List<String> command(){return command("import sys; sys.path.insert(0,sys.argv[1]); from strict_native_content import main; sys.argv=sys.argv[:1]; sys.exit(main())");}
    private List<String> command(String script){return List.of(config.get("python").stringValue(),"-I","-B","-X","utf8","-X","pycache_prefix=/dev/null","-c",script,config.get("processingRoot").stringValue());}
    Map<String,String> environment(){return nativeRuntime.gold.silver.environment();}
    int seconds(String family){return family.equals("BRONZE")?180:registered.configuration(family).get("seconds").intValue();}
}
