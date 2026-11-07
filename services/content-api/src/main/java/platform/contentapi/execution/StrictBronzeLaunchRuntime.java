package platform.contentapi.execution;

import java.io.File;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/** Separate immutable launch identity. Historical Bronze v2 remains a ledger-v1 adapter. */
final class StrictBronzeLaunchRuntime {
    static final String MAIN="platform.ingestion.managed.StrictManagedBronze";
    private record FileObservation(InstallationHostGuard.Identity identity,FileTime modified,long size,String sha256) { }
    private final String id,digest;
    private final JsonNode configuration,frozen;
    private final InstallationHostGuard guard;
    private final Map<Path,FileObservation> installed;
    private StrictBronzeLaunchRuntime(String id,String digest,JsonNode configuration,JsonNode frozen,InstallationHostGuard guard){
        this.id=id;this.digest=digest;this.configuration=configuration.deepCopy();this.frozen=frozen.deepCopy();this.guard=guard;
        installed=observe();
    }
    static StrictBronzeLaunchRuntime load(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,
                                        NarrowContentRuntimeQualification.Snapshot registered,String id){
        need(id!=null && id.matches("[A-Za-z0-9_-]{1,128}"),RUNTIME_MISMATCH);
        var row=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.content_bronze_launch_profiles WHERE launch_profile_id=?",id));
        need(row.size()==1,RUNTIME_MISMATCH);
        byte[] bytes=(byte[])row.getFirst().get("configuration_bytes");String digest=(String)row.getFirst().get("configuration_sha256");
        need(SourceJson.sha(bytes).equals(digest),RUNTIME_MISMATCH);
        var c=PreflightFiles.canonical(bytes,65536);validate(c,registered,guard.binding());
        return new StrictBronzeLaunchRuntime(id,digest,c,registered.frozenSelection(),guard);
    }
    /** Separate protocol identity, never an installed-byte attestation on its own. */
    static String protocolDigest(String javaRuntime){
        return SourceJson.sha(SourceJson.canonical(SourceJson.object().put("strictBronzeRuntimeVersion",1)
                .put("adapter",MAIN).put("javaRuntime",javaRuntime)));
    }
    static void validate(JsonNode c,NarrowContentRuntimeQualification.Snapshot registered,InstallationHostGuard.Binding binding){
        try{
            SourceJson.fields(c,"strictBronzeLaunchVersion","bronzeRuntimeId","bronzeRuntimeSha256","runProfileId","runProfileSha256",
                    "controlBindingId","storageBindingId","storageBindingSha256","javaHome","manifest","deploymentSha256","strictRuntimeSha256","testEndpoint");
            var bronze=registered.configuration("BRONZE");var fetch=registered.configuration("FETCH");
            var expected=SourceJson.object().put("strictBronzeLaunchVersion",1)
                    .put("bronzeRuntimeId",registered.id(NarrowContentRuntimeQualification.Family.BRONZE))
                    .put("bronzeRuntimeSha256",registered.digest("BRONZE"));
            for(String key:List.of("runProfileId","runProfileSha256","controlBindingId","storageBindingId","storageBindingSha256"))expected.set(key,bronze.get(key));
            expected.set("javaHome",fetch.get("javaHome"));
            var manifest=((ObjectNode)fetch.get("manifest").deepCopy()).put("adapter",MAIN);
            expected.set("manifest",manifest);expected.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest)))
                    .put("strictRuntimeSha256",protocolDigest(manifest.get("javaRuntime").stringValue())).put("testEndpoint",true);
            need(Arrays.equals(SourceJson.canonical(c),SourceJson.canonical(expected)) && fetch.get("testEndpoint").booleanValue()
                    && binding.id().equals(c.get("controlBindingId").stringValue()),RUNTIME_MISMATCH);
            JsonNode storage=null;
            for(var entry:registered.frozenSelection().get("storageBindings"))if(entry.get("bindingId").equals(bronze.get("storageBindingId")))storage=entry.get("configuration");
            need(storage!=null,RUNTIME_MISMATCH);URI uri=URI.create(storage.get("endpoint").stringValue());
            need("http".equals(uri.getScheme()) && Set.of("127.0.0.1","[::1]").contains(uri.getHost())
                    && uri.getRawUserInfo()==null && uri.getRawQuery()==null && uri.getRawFragment()==null
                    && (uri.getRawPath().isEmpty() || uri.getRawPath().equals("/")) && uri.getPort()>0 && uri.getPort()<=65535,RUNTIME_MISMATCH);
        }catch(PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    void check(){need(installed.equals(observe()),INPUT_CHANGED);}
    private Map<Path,FileObservation> observe(){
        guard.checkHeld(guard.binding().installationId());
        try{
            Path home=Path.of(configuration.get("javaHome").stringValue());
            need(Runtime.version().feature()==21 && home.isAbsolute() && home.equals(home.toRealPath())
                    && home.equals(Path.of(System.getProperty("java.home")).toRealPath())
                    && configuration.get("manifest").get("javaRuntime").stringValue().equals(Runtime.version().toString()),RUNTIME_MISMATCH);
            var result=new LinkedHashMap<Path,FileObservation>();
            Path executable=home.resolve("bin/java");need(Files.isExecutable(executable),RUNTIME_MISMATCH);
            result.put(executable,observation(executable,64*1024*1024,null));
            var cp=configuration.get("manifest").get("classpath");need(cp.isArray() && cp.size()>0 && cp.size()<=128,RUNTIME_MISMATCH);
            long total=0;
            for(var entry:cp){
                SourceJson.fields(entry,"path","sha256");Path path=Path.of(entry.get("path").stringValue());
                need(path.isAbsolute() && path.normalize().equals(path) && path.toString().endsWith(".jar") && !result.containsKey(path),RUNTIME_MISMATCH);
                var observation=observation(path,(int)Math.min(64L*1024*1024,256L*1024*1024-total),entry.get("sha256").stringValue());
                total+=observation.size();need(total<=256L*1024*1024,RESOURCE_LIMIT);result.put(path,observation);
            }
            for(Path path:result.keySet())for(var storage:frozen.get("storageBindings")){
                var c=storage.get("configuration");
                if(c.has("root"))need(!path.startsWith(Path.of(c.get("root").stringValue())),RUNTIME_MISMATCH);
                if(c.has("credentialsFile"))need(!path.equals(Path.of(c.get("credentialsFile").stringValue())),RUNTIME_MISMATCH);
            }
            PreflightFiles.privateEntry(guard.binding().root(),true);
            need(Files.getFileStore(guard.binding().root()).getUsableSpace()>=1024L*1024*1024,RESOURCE_LIMIT);
            guard.checkHeld(guard.binding().installationId());return Map.copyOf(result);
        }catch(PreflightFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    private static FileObservation observation(Path path,int maximum,String expected)throws java.io.IOException{
        var id=InstallationHostGuard.identity(path);var modified=Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS);
        byte[] bytes=PreflightFiles.read(path,maximum,false);String hash=SourceJson.sha(bytes);
        need((expected==null || hash.equals(expected)) && id.equals(InstallationHostGuard.identity(path))
                && modified.equals(Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS)),INPUT_CHANGED);
        return new FileObservation(id,modified,bytes.length,hash);
    }
    String id(){return id;}String digest(){return digest;}
    String protocolDigest(){return configuration.get("strictRuntimeSha256").stringValue();}
    List<String> command(){var cp=new ArrayList<String>();for(var entry:configuration.get("manifest").get("classpath"))cp.add(entry.get("path").stringValue());
        return List.of(Path.of(configuration.get("javaHome").stringValue(),"bin/java").toString(),"-Xmx128m","-cp",String.join(File.pathSeparator,cp),MAIN);}
    Map<String,String> environment(){return Map.of("PATH","/usr/bin:/bin","TZ","UTC","LANG","C.UTF-8");}
    @Override public String toString(){return "StrictBronzeLaunchRuntime[registered, loopback only]";}
}
