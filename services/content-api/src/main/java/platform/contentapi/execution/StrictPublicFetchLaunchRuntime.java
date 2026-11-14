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

/** Public IPv4-only policy is a separate immutable launch identity, never a v1 reinterpretation. */
final class StrictPublicFetchLaunchRuntime implements ContentFetchLaunchRuntime {
    static final String MAIN="platform.ingestion.managed.StrictPublicManagedRssFetch";
    private record FileObservation(InstallationHostGuard.Identity identity,FileTime modified,long size,String sha256) { }
    private final String id,digest;
    private final JsonNode configuration,storage;
    private final InstallationHostGuard guard;
    private final Map<Path,FileObservation> installed;
    private StrictPublicFetchLaunchRuntime(String id,String digest,JsonNode configuration,JsonNode storage,InstallationHostGuard guard){
        this.id=id;this.digest=digest;this.configuration=configuration.deepCopy();this.storage=storage.deepCopy();this.guard=guard;
        installed=observe();
    }
    static StrictPublicFetchLaunchRuntime load(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,
                                        NarrowContentRuntimeQualification.Snapshot registered,String id){
        need(id!=null && id.matches("[A-Za-z0-9_-]{1,128}"),RUNTIME_MISMATCH);
        var row=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.content_fetch_launch_profiles WHERE launch_profile_id=?",id));
        need(row.size()==1,RUNTIME_MISMATCH);
        byte[] bytes=(byte[])row.getFirst().get("configuration_bytes");String digest=(String)row.getFirst().get("configuration_sha256");
        need(SourceJson.sha(bytes).equals(digest),RUNTIME_MISMATCH);
        var c=PreflightFiles.canonical(bytes,65536);validate(c,registered,guard.binding());
        return new StrictPublicFetchLaunchRuntime(id,digest,c,registered.registration().storageBindings(),guard);
    }
    static final String POLICY_ID="ddip-public-feed-pinned-v1";
    static final String PROVIDER="platform.ingestion.managed.PublicFetchResolverProvider";
    static final String ADDRESS_POLICY_SHA256="85959d534b478bf1c8194e99f49a2c860f9fe9864978ed7118496913767cefcd";
    static JsonNode networkPolicy(){
        return SourceJson.object().put("policyVersion",1).put("policyId",POLICY_ID).put("resolverProvider",PROVIDER)
                .put("maxResolvedAddresses",64).put("addressPolicyVersion",1).put("addressPolicySha256",ADDRESS_POLICY_SHA256)
                .put("addressFamily","IPV4_ONLY").put("httpVersion","HTTP_1_1").put("maxApplicationInvocations",1)
                .put("transportRetryPolicy","JDK_BOUNDED_PINNED_V1").put("redirects",false).put("proxies",false);
    }
    static String networkPolicyDigest(){return SourceJson.sha(SourceJson.canonical(networkPolicy()));}
    /** Pure frozen-source correspondence. No DNS, current-catalog substitution, or installed file access. */
    static void validate(JsonNode c,NarrowContentRuntimeQualification.Snapshot registered,InstallationHostGuard.Binding binding){
        validateRegistration(c,registered.registration(),binding);
        try{
            var sources=registered.frozenSelection().get("sources");need(sources.isArray() && sources.size()==1,RUNTIME_MISMATCH);
            var selected=sources.get(0);var endpoint=selected.get("endpoint");
            SourceJson.fields(endpoint,"endpointId","sourceId","uri");
            need(endpoint.get("sourceId").equals(selected.get("sourceId"))
                    && endpoint.get("endpointId").equals(selected.get("configuration").get("feedEndpointId"))
                    && SourceJson.sha(SourceJson.canonical(endpoint)).equals(selected.get("endpointSha256").stringValue())
                    && SourceJson.sha(SourceJson.canonical(selected.get("configuration"))).equals(selected.get("configurationSha256").stringValue()),RUNTIME_MISMATCH);
            URI uri=URI.create(endpoint.get("uri").stringValue());String host=uri.getHost()==null?null:uri.getHost().toLowerCase(Locale.ROOT);
            need(endpoint.get("uri").stringValue().length()<=2048 && host!=null && host.matches("[a-z0-9][a-z0-9.-]*\\.[a-z][a-z0-9.-]*")
                    && !host.endsWith(".localhost") && !host.endsWith(".local") && !host.endsWith(".internal")
                    && Set.of("http","https").contains(uri.getScheme().toLowerCase(Locale.ROOT))
                    && uri.getRawUserInfo()==null && uri.getRawQuery()==null && uri.getRawFragment()==null
                    && uri.getPort()!=0 && uri.getPort()<=65535,RUNTIME_MISMATCH);
        }catch(PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    static void validateRegistration(JsonNode c,NarrowContentRuntimeQualification.Registration registered,InstallationHostGuard.Binding binding){
        try{
            SourceJson.fields(c,"strictFetchLaunchVersion","fetchRuntimeId","fetchRuntimeSha256","runProfileId","runProfileSha256",
                    "controlBindingId","javaHome","manifest","deploymentSha256","testEndpoint","networkPolicy","networkPolicySha256");
            var fetch=registered.configuration("FETCH");var historical=fetch.get("manifest").get("classpath");
            SourceJson.fields(c.get("manifest"),"adapter","javaRuntime","classpath");
            var cp=c.get("manifest").get("classpath");
            need(cp.isArray() && historical.isArray() && cp.size()==historical.size()+1 && cp.size()<=128,RUNTIME_MISMATCH);
            Set<String> paths=new HashSet<>();
            for(int i=0;i<cp.size();i++){
                var entry=cp.get(i);SourceJson.fields(entry,"path","sha256");
                need(entry.get("path").isString() && entry.get("sha256").isString()
                        && entry.get("sha256").stringValue().matches("[0-9a-f]{64}"),RUNTIME_MISMATCH);
                Path path=Path.of(entry.get("path").stringValue());
                need(path.isAbsolute() && path.normalize().equals(path) && path.toString().equals(entry.get("path").stringValue()) && path.toString().endsWith(".jar")
                        && paths.add(path.toString()) && (i==historical.size() || entry.equals(historical.get(i))),RUNTIME_MISMATCH);
            }
            var expected=SourceJson.object().put("strictFetchLaunchVersion",2)
                    .put("fetchRuntimeId",registered.id(NarrowContentRuntimeQualification.Family.FETCH))
                    .put("fetchRuntimeSha256",registered.digest("FETCH"));
            for(String key:List.of("runProfileId","runProfileSha256","controlBindingId","javaHome"))expected.set(key,fetch.get(key));
            var manifest=((ObjectNode)fetch.get("manifest").deepCopy()).put("adapter",MAIN);manifest.set("classpath",cp);
            expected.set("manifest",manifest);expected.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest))).put("testEndpoint",false);
            expected.set("networkPolicy",networkPolicy());expected.put("networkPolicySha256",networkPolicyDigest());
            need(Arrays.equals(SourceJson.canonical(c),SourceJson.canonical(expected))
                    && fetch.get("testEndpoint").isBoolean() && !fetch.get("testEndpoint").booleanValue()
                    && binding.id().equals(c.get("controlBindingId").stringValue()),RUNTIME_MISMATCH);
        }catch(PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    /** Read-only installation check; registration is not accepted by the run-bound loader. */
    static Runnable startupCheck(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,
            NarrowContentRuntimeQualification.Registration registered,String id){
        need(id!=null && id.matches("[A-Za-z0-9_-]{1,128}"),RUNTIME_MISMATCH);
        var rows=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.content_fetch_launch_profiles WHERE launch_profile_id=?",id));
        need(rows.size()==1,RUNTIME_MISMATCH);var row=rows.getFirst();byte[] raw=(byte[])row.get("configuration_bytes");String digest=(String)row.get("configuration_sha256");
        need(SourceJson.sha(raw).equals(digest),RUNTIME_MISMATCH);var c=PreflightFiles.canonical(raw,65536);
        validateRegistration(c,registered,guard.binding());
        var installed=new StrictPublicFetchLaunchRuntime(id,digest,c,registered.storageBindings(),guard);
        return installed::check;
    }
    public void check(){need(installed.equals(observe()),INPUT_CHANGED);}
    private Map<Path,FileObservation> observe(){
        guard.checkHeld(guard.binding().installationId());
        try{
            Path home=Path.of(configuration.get("javaHome").stringValue());
            need(Runtime.version().feature()==21 && home.isAbsolute() && home.equals(home.toRealPath())
                    && home.equals(Path.of(System.getProperty("java.home")).toRealPath())
                    && configuration.get("manifest").get("javaRuntime").stringValue().equals(Runtime.version().toString()),RUNTIME_MISMATCH);
            var result=new LinkedHashMap<Path,FileObservation>();
            Path executable=home.resolve("bin/java");need(Files.isExecutable(executable),RUNTIME_MISMATCH);
            result.put(executable,observation(executable,64*1024*1024,null,null));
            var cp=configuration.get("manifest").get("classpath");need(cp.isArray() && cp.size()>0 && cp.size()<=128,RUNTIME_MISMATCH);
            long total=0;
            int index=0;
            for(var entry:cp){
                SourceJson.fields(entry,"path","sha256");Path path=Path.of(entry.get("path").stringValue());
                need(path.isAbsolute() && path.normalize().equals(path) && path.toString().equals(entry.get("path").stringValue()) && path.toString().endsWith(".jar") && !result.containsKey(path),RUNTIME_MISMATCH);
                var observation=observation(path,(int)Math.min(64L*1024*1024,256L*1024*1024-total),entry.get("sha256").stringValue(),index++==cp.size()-1);
                total+=observation.size();need(total<=256L*1024*1024,RESOURCE_LIMIT);result.put(path,observation);
            }
            for(Path path:result.keySet())for(var storage:this.storage){
                var c=storage.get("configuration");
                if(c.has("root"))need(!path.startsWith(Path.of(c.get("root").stringValue())),RUNTIME_MISMATCH);
                if(c.has("credentialsFile"))need(!path.equals(Path.of(c.get("credentialsFile").stringValue())),RUNTIME_MISMATCH);
            }
            PreflightFiles.privateEntry(guard.binding().root(),true);
            need(Files.getFileStore(guard.binding().root()).getUsableSpace()>=1024L*1024*1024,RESOURCE_LIMIT);
            guard.checkHeld(guard.binding().installationId());return Map.copyOf(result);
        }catch(PreflightFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    private static FileObservation observation(Path path,int maximum,String expected,Boolean provider)throws java.io.IOException{
        var id=InstallationHostGuard.identity(path);var modified=Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS);
        byte[] bytes=PreflightFiles.read(path,maximum,false);String hash=SourceJson.sha(bytes);
        need((expected==null || hash.equals(expected)) && id.equals(InstallationHostGuard.identity(path))
                && modified.equals(Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS)),INPUT_CHANGED);
        if(provider!=null)checkJar(bytes,provider,true);
        return new FileObservation(id,modified,bytes.length,hash);
    }
    private static final String SERVICE="META-INF/services/java.net.spi.InetAddressResolverProvider";
    private static final Set<String> PROVIDER_CLASSES=Set.of("platform/ingestion/managed/PublicFetchResolverProvider.class",
            "platform/ingestion/managed/PublicFetchResolverProvider$State.class","platform/ingestion/managed/PublicFetchResolverProvider$1.class",
            "platform/ingestion/managed/PublicFetchAddressPolicy.class");
    /** Inspect the already hashed bounded bytes. Never loads provider code in the parent JVM. */
    static void checkJar(byte[] bytes,boolean provider,boolean closedClasspath){
        try(var jar=new java.util.zip.ZipInputStream(new java.io.ByteArrayInputStream(bytes))){
            Set<String> entries=new HashSet<>();Set<String> classes=new HashSet<>();boolean service=false;long inflated=0;byte[] buffer=new byte[8192];
            for(java.util.zip.ZipEntry entry;(entry=jar.getNextEntry())!=null;){
                String name=entry.getName();need(entries.add(name) && entries.size()<=100000,RUNTIME_MISMATCH);
                if(entry.isDirectory()){ /* Drain even directory payloads under the same inflation bounds. */ }
                else if(name.equals(SERVICE)){
                    need(provider && !service,RUNTIME_MISMATCH);service=true;
                    need(Arrays.equals(jar.readNBytes(1025),(PROVIDER+"\n").getBytes(java.nio.charset.StandardCharsets.UTF_8)),RUNTIME_MISMATCH);
                }else if(name.endsWith(".class")){
                    if(provider){need(PROVIDER_CLASSES.contains(name),RUNTIME_MISMATCH);classes.add(name);}
                    else need(!name.contains("PublicFetchResolverProvider") && !name.endsWith("/PublicFetchAddressPolicy.class"),RUNTIME_MISMATCH);
                }else if(name.equals("META-INF/MANIFEST.MF")){
                    byte[] manifest=jar.readNBytes(65537);need(manifest.length<=65536,RUNTIME_MISMATCH);
                    if(closedClasspath)need(new java.util.jar.Manifest(new java.io.ByteArrayInputStream(manifest)).getMainAttributes().getValue("Class-Path")==null,RUNTIME_MISMATCH);
                }else if(provider)need(name.matches("META-INF/maven/[^/]+/[^/]+/(pom.properties|pom.xml)"),RUNTIME_MISMATCH);
                long entryBytes=0;
                for(int count;(count=jar.read(buffer))!=-1;){
                    entryBytes+=count;inflated+=count;
                    need(entryBytes<=64L*1024*1024 && inflated<=256L*1024*1024,RESOURCE_LIMIT);
                }
                jar.closeEntry();
            }
            need(!entries.isEmpty() && (!provider || (service && classes.equals(PROVIDER_CLASSES))),RUNTIME_MISMATCH);
        }catch(PreflightFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    public String id(){return id;}public String digest(){return digest;}
    public List<String> command(){var cp=new ArrayList<String>();for(var entry:configuration.get("manifest").get("classpath"))cp.add(entry.get("path").stringValue());
        return List.of(Path.of(configuration.get("javaHome").stringValue(),"bin/java").toString(),"-Xmx128m","-Dddip.publicFetch.policy="+POLICY_ID,"-cp",String.join(File.pathSeparator,cp),MAIN);}
    public Map<String,String> environment(){return Map.of("PATH","/usr/bin:/bin","TZ","UTC","LANG","C.UTF-8");}
    @Override public String toString(){return "StrictPublicFetchLaunchRuntime[registered, bounded public IPv4 only]";}
}
