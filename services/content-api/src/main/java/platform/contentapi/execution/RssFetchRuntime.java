package platform.contentapi.execution;

import java.io.File;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Prebuilt Java-only D1 profile. No runtime builds, ambient classpath, URL override or dispatch bean. */
final class RssFetchRuntime {
    static final String MAIN="platform.ingestion.managed.ManagedRssFetch";
    private final InstallationHostGuard guard;private final JsonNode configuration;private final String digest;
    private RssFetchRuntime(InstallationHostGuard guard,JsonNode c,String hash){this.guard=guard;configuration=c.deepCopy();digest=hash;}
    static RssFetchRuntime load(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,WorkerRepository.Owner owner,String id,JsonNode frozen) {
        return load(jdbc,tx,guard,owner.worker().profileId(),owner.worker().profileDigest(),owner.worker().deploymentDigest(),id,frozen);
    }
    static RssFetchRuntime load(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,String profileId,String profileDigest,String deploymentDigest,String id,JsonNode frozen) {
        try {
            need(id!=null && id.matches("[A-Za-z0-9_-]{1,128}"),RUNTIME_MISMATCH);
            var rows=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",id));need(rows.size()==1,RUNTIME_MISMATCH);
            String hash=(String)rows.getFirst().get("configuration_sha256");var c=FrozenRunSelection.canonical((byte[])rows.getFirst().get("configuration_bytes"),hash,65536);
            SourceJson.fields(c,"rssFetchRuntimeVersion","pipeline","runProfileId","runProfileSha256","deploymentSha256","javaHome","manifest","controlBindingId","testEndpoint","httpTimeoutMillis");
            need(c.get("rssFetchRuntimeVersion").isIntegralNumber() && c.get("rssFetchRuntimeVersion").canConvertToInt() && c.get("rssFetchRuntimeVersion").intValue()==1 && c.get("pipeline").stringValue().equals("CONTENT_RSS_V1") && c.get("runProfileId").stringValue().equals(profileId) && c.get("runProfileSha256").stringValue().equals(profileDigest) && c.get("deploymentSha256").stringValue().equals(deploymentDigest) && SourceJson.sha(SourceJson.canonical(c.get("manifest"))).equals(deploymentDigest),RUNTIME_MISMATCH);
            need(frozen.get("pipeline").stringValue().equals("CONTENT_RSS_V1") && frozen.get("executionProfile").get("configurationSha256").stringValue().equals(profileDigest),RUNTIME_MISMATCH);
            need(c.get("controlBindingId").stringValue().equals(guard.binding().id()) && c.get("testEndpoint").isBoolean() && c.get("httpTimeoutMillis").isIntegralNumber() && c.get("httpTimeoutMillis").canConvertToInt() && c.get("httpTimeoutMillis").intValue()>0 && c.get("httpTimeoutMillis").intValue()<=30000,RUNTIME_MISMATCH);
            need(tx.required(()->jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.execution_profile_storage p JOIN ddip_management.storage_bindings b USING(binding_id) WHERE p.profile_id=? AND b.binding_id=? AND b.installation_id=? AND b.role='CONTROL' AND b.configuration_bytes=? AND b.configuration_sha256=?)",Boolean.class,id,guard.binding().id(),guard.binding().installationId(),guard.binding().descriptor(),guard.binding().digest())),RUNTIME_MISMATCH);
            boolean control=false;
            for(var binding:frozen.get("storageBindings")){
                var descriptor=binding.get("configuration");
                if(binding.get("role").stringValue().equals("CONTROL")){need(binding.get("bindingId").stringValue().equals(guard.binding().id()) && Arrays.equals(SourceJson.canonical(descriptor),guard.binding().descriptor()),RUNTIME_MISMATCH);control=true;}
                else if(descriptor.has("root")){Path root=Path.of(descriptor.get("root").stringValue());need(root.isAbsolute() && !root.startsWith(guard.binding().root()) && !guard.binding().root().startsWith(root),RUNTIME_MISMATCH);}
            }
            need(control,RUNTIME_MISMATCH);var runtime=new RssFetchRuntime(guard,c,hash);runtime.check();return runtime;
        }catch(PreflightFailure | ManagementFailure ex){throw ex;}catch(RuntimeException ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    void check() {
        guard.checkHeld(guard.binding().installationId());
        try {
            Path javaHome=Path.of(configuration.get("javaHome").stringValue());
            need(Runtime.version().feature()==21 && javaHome.isAbsolute() && javaHome.equals(javaHome.toRealPath()) && javaHome.equals(Path.of(System.getProperty("java.home")).toRealPath()),RUNTIME_MISMATCH);
            var manifest=configuration.get("manifest");SourceJson.fields(manifest,"adapter","javaRuntime","classpath");
            need(manifest.get("adapter").stringValue().equals(MAIN) && manifest.get("javaRuntime").stringValue().equals(Runtime.version().toString()) && manifest.get("classpath").isArray() && manifest.get("classpath").size()>0 && manifest.get("classpath").size()<=128,RUNTIME_MISMATCH);
            Set<Path> paths=new HashSet<>();long total=0;
            for(var entry:manifest.get("classpath")){
                SourceJson.fields(entry,"path","sha256");Path path=Path.of(entry.get("path").stringValue());
                need(path.isAbsolute() && path.toString().endsWith(".jar") && !path.startsWith(guard.binding().root()) && paths.add(path),RUNTIME_MISMATCH);
                byte[] bytes=PreflightFiles.read(path,64*1024*1024,false);total+=bytes.length;need(total<=256L*1024*1024 && SourceJson.sha(bytes).equals(entry.get("sha256").stringValue()),RUNTIME_MISMATCH);
            }
            PreflightFiles.privateEntry(guard.binding().root(),true);
            need(Files.getFileStore(guard.binding().root()).getUsableSpace()>=1024L*1024*1024,RESOURCE_LIMIT);
        }catch(PreflightFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    void endpoint(JsonNode selected) {
        try {
            var e=selected.get("endpoint");SourceJson.fields(e,"endpointId","sourceId","uri");
            need(SourceJson.sha(SourceJson.canonical(e)).equals(selected.get("endpointSha256").stringValue()) && e.get("sourceId").equals(selected.get("sourceId")) && e.get("endpointId").equals(selected.get("configuration").get("feedEndpointId")),RUNTIME_MISMATCH);
            URI uri=URI.create(e.get("uri").stringValue());String host=uri.getHost()==null?"":uri.getHost().toLowerCase(Locale.ROOT);
            need(e.get("uri").stringValue().length()<=2048 && Set.of("http","https").contains(uri.getScheme().toLowerCase(Locale.ROOT)) && uri.getUserInfo()==null && uri.getQuery()==null && uri.getFragment()==null && uri.getPort()!=0 && uri.getPort()<=65535,RUNTIME_MISMATCH);
            boolean test=configuration.get("testEndpoint").booleanValue();
            need(test?Set.of("127.0.0.1","[::1]").contains(host):host.matches("[a-z0-9][a-z0-9.-]*\\.[a-z][a-z0-9.-]*") && !host.endsWith(".localhost") && !host.endsWith(".local") && !host.endsWith(".internal"),RUNTIME_MISMATCH);
        }catch(PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    List<String> command(Path descriptor){return command(descriptor,MAIN);}
    List<String> bronzeCommand(Path descriptor){return command(descriptor,"platform.ingestion.managed.ManagedBronze");}
    private List<String> command(Path descriptor,String main){var cp=new ArrayList<String>();for(var entry:configuration.get("manifest").get("classpath"))cp.add(entry.get("path").stringValue());return List.of(Path.of(configuration.get("javaHome").stringValue(),"bin/java").toString(),"-Xmx128m","-cp",String.join(File.pathSeparator,cp),main,descriptor.toString());}
    Map<String,String> environment(){return Map.of("PATH","/usr/bin:/bin","TZ","UTC","LANG","C.UTF-8");}
    String digest(){return digest;}
    String javaRuntime(){return configuration.get("manifest").get("javaRuntime").stringValue();}
    boolean testEndpoint(){return configuration.get("testEndpoint").booleanValue();}
    int timeoutMillis(){return configuration.get("httpTimeoutMillis").intValue();}
    @Override public String toString(){return "RssFetchRuntime[redacted]";}
}
