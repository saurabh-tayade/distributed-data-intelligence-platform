package platform.contentapi.execution;

import java.nio.file.Path;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.FrozenRunSelection;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/**
 * SQL-only correspondence of registered configurations to an independently supplied READY identity.
 * Not installed-artifact verification, READY reconstruction, ownership, or permission to launch.
 * In particular the registered v2 adapters implement ledger v1, not narrow READY ledger v2.
 * Configuration correspondence performs no journal, credential, filesystem, network, or process access.
 * The guard overload first uses the existing read-only READY reader, including ledger inspection.
 */
final class NarrowContentRuntimeQualification {
    enum Family { CONTENT, GOLD, SILVER, BRONZE, FETCH }
    private record Registered(String id,String digest,byte[] bytes) {
        JsonNode configuration(){return SourceJson.parse(bytes);}
    }
    /** Immutable registered description only. Contains no selected source, run, attempt or READY identity. */
    static final class Registration {
        private final Map<Family,Registered> chain;
        private final Registered profile;
        private final byte[] storage;
        private Registration(Map<Family,Registered> chain,Registered profile,JsonNode storage){this.chain=Map.copyOf(chain);this.profile=profile;this.storage=SourceJson.canonical(storage);}
        String profileId(){return profile.id();}
        String profileDigest(){return profile.digest();}
        JsonNode storageBindings(){return SourceJson.parse(storage,256*1024);}
        String id(Family family){return chain.get(family).id();}
        String digest(String family){return chain.get(Family.valueOf(family)).digest();}
        JsonNode configuration(String family){return chain.get(Family.valueOf(family)).configuration();}
    }
    static final class Snapshot {
        private final Map<Family,Registered> chain;
        private final Registration registration;
        private final byte[] frozen;
        private final UUID run,attempt;
        private Snapshot(Registration registration,JsonNode frozen,NarrowContentEvidenceContext.Expected ready){this.registration=registration;this.chain=registration.chain;this.frozen=SourceJson.canonical(frozen);run=ready.run();attempt=ready.attempt();}
        Registration registration(){return registration;}
        UUID run(){return run;}
        UUID attempt(){return attempt;}
        JsonNode frozenSelection(){return SourceJson.parse(frozen,256*1024);}
        String digest(String family){return digest(Family.valueOf(family));}
        JsonNode configuration(String family){return configuration(Family.valueOf(family));}
        String id(Family family){return chain.get(Objects.requireNonNull(family)).id();}
        String digest(Family family){return chain.get(Objects.requireNonNull(family)).digest();}
        JsonNode configuration(Family family){return chain.get(Objects.requireNonNull(family)).configuration();}
        int ledgerVersion(){return 1;}
        boolean nativeLaunchCompatible(){return false;}
        boolean installedArtifactsVerified(){return false;}
        @Override public String toString(){return "NarrowContentRuntimeQualification[configuration correspondence only; non-launchable]";}
    }
    /** Production entry: reconstruct and inspect recorded READY before configuration correspondence. */
    static Snapshot inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt){
        return inspect(jdbc,tx,NarrowContentAdmission.inspectReady(jdbc,tx,guard,run,attempt));
    }
    /** Correspondence seam: the caller must independently establish the supplied READY provenance. */
    static Snapshot inspect(JdbcTemplate jdbc,ManagementTransactions tx,NarrowContentEvidenceContext.Expected ready){
        need(jdbc!=null && tx!=null && ready!=null,INPUT_INVALID);
        need(ready.kind()==NarrowContentEvidenceContext.PlanKind.ORIGINAL,RUNTIME_MISMATCH);
        return tx.required(()->{
            try{return read(jdbc,ready);}
            catch(PreflightFailure ex){throw ex;}
            catch(org.springframework.dao.DataAccessException ex){throw ex;}
            catch(RuntimeException ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
        });
    }
    private static Snapshot read(JdbcTemplate j,NarrowContentEvidenceContext.Expected ready){
        var row=one(j.queryForList("SELECT captured_selection,selection_sha256,execution_profile_id FROM ddip_management.runs WHERE run_id=?",ready.run()));
        need(ready.frozen().selectionSha256().equals(row.get("selection_sha256")),RUNTIME_MISMATCH);
        var frozen=FrozenRunSelection.canonical((byte[])row.get("captured_selection"),(String)row.get("selection_sha256"),256*1024);
        ContentEvidenceProfile.selection(frozen);
        var ep=frozen.get("executionProfile");
        SourceJson.fields(ep,"profileId","configurationSha256","configuration");
        String profileId=text(ep,"profileId");
        need(profileId.equals(row.get("execution_profile_id")) && ready.frozen().profileSha256().equals(text(ep,"configurationSha256")),RUNTIME_MISMATCH);
        Registered registeredProfile=load(j,profileId);
        need(registeredProfile.digest().equals(ready.frozen().profileSha256()) && registeredProfile.configuration().equals(ep.get("configuration")),RUNTIME_MISMATCH);
        var profile=registeredProfile.configuration();String deployment=text(profile.get("runtime"),"deploymentSha256");
        FrozenRunSelection.profile(registeredProfile.bytes(),registeredProfile.digest(),deployment);
        integer(profile,"runProfileVersion",2,2);
        need(ready.source().equals(text(frozen.get("sources").get(0),"sourceId")),RUNTIME_MISMATCH);
        var registration=correspondence(j,registeredProfile,frozen.get("storageBindings"),ready.host(),ready.frozen().runtimeSha256());
        need(text(frozen.get("sources").get(0).get("configuration"),"inputStorageProfileId").equals(text(registration.configuration("BRONZE"),"storageBindingId")),RUNTIME_MISMATCH);
        return new Snapshot(registration,frozen,ready);
    }
    private static Registration correspondence(JdbcTemplate j,Registered registeredProfile,JsonNode storageBindings,InstallationHostGuard.Binding host,String runtimeHash){
        String profileId=registeredProfile.id();var profile=registeredProfile.configuration();String deployment=text(profile.get("runtime"),"deploymentSha256");
        Map<String,JsonNode> bindings=new HashMap<>();Set<Path> roots=new HashSet<>();
        need(storageBindings.isArray() && storageBindings.size()<=128,RUNTIME_MISMATCH);
        for(var b:storageBindings){
            SourceJson.fields(b,"bindingId","productId","role","configurationSha256","configuration");
            need(bindings.put(text(b,"bindingId"),b)==null,RUNTIME_MISMATCH);
            need(SourceJson.sha(SourceJson.canonical(b.get("configuration"))).equals(text(b,"configurationSha256")),RUNTIME_MISMATCH);
            membership(j,host,profileId,b);
            if(b.get("configuration").has("root")){
                Path root=absolute(b.get("configuration"),"root");
                for(Path other:roots)need(!root.startsWith(other) && !other.startsWith(root),RUNTIME_MISMATCH);
                roots.add(root);
            }
        }
        var control=binding(bindings,host.id(),"CONTROL");
        need(Arrays.equals(SourceJson.canonical(control.get("configuration")),host.descriptor())
                && text(control,"configurationSha256").equals(host.digest()),RUNTIME_MISMATCH);
        var chain=new EnumMap<Family,Registered>(Family.class);Set<String> ids=new HashSet<>();
        String id=text(profile,"contentRuntimeId");String expectedHash=runtimeHash;
        for(Family family:Family.values()){
            need(ids.add(id),RUNTIME_MISMATCH);var registered=load(j,id);
            need(registered.digest().equals(expectedHash),RUNTIME_MISMATCH);chain.put(family,registered);
            var c=registered.configuration();validate(family,c,profileId,registeredProfile.digest(),deployment);
            if(family!=Family.FETCH){String next=switch(family){case CONTENT->"gold";case GOLD->"silver";case SILVER->"bronze";case BRONZE->"fetch";default->throw new AssertionError();};id=text(c,next+"RuntimeId");expectedHash=text(c,next+"RuntimeSha256");}
        }
        var content=chain.get(Family.CONTENT).configuration();var gold=chain.get(Family.GOLD).configuration();
        var silver=chain.get(Family.SILVER).configuration();var bronze=chain.get(Family.BRONZE).configuration();var fetch=chain.get(Family.FETCH).configuration();
        need(text(bronze,"controlBindingId").equals(host.id()) && text(fetch,"controlBindingId").equals(host.id()),RUNTIME_MISMATCH);
        var storage=binding(bindings,text(bronze,"storageBindingId"),"BRONZE");
        var storageConfig=storage.get("configuration");
        SourceJson.fields(storageConfig,"importBronzeVersion","endpoint","region","bucket","credentialsFile");
        integer(storageConfig,"importBronzeVersion",1,1);
        var endpoint=java.net.URI.create(text(storageConfig,"endpoint"));
        need("http".equals(endpoint.getScheme()) && Set.of("127.0.0.1","localhost","[::1]").contains(endpoint.getHost())
                && endpoint.getPort()>0 && endpoint.getPort()<=65535 && endpoint.getUserInfo()==null
                && endpoint.getQuery()==null && endpoint.getFragment()==null && Set.of("","/").contains(endpoint.getPath()),RUNTIME_MISMATCH);
        need(text(storageConfig,"region").matches("[A-Za-z0-9_-]{1,128}") && !text(storageConfig,"bucket").isBlank(),RUNTIME_MISMATCH);
        absolute(storageConfig,"credentialsFile");
        need(text(storage,"configurationSha256").equals(text(bronze,"storageBindingSha256")),RUNTIME_MISMATCH);
        for(Family family:Family.values()){
            var r=chain.get(family);membership(j,host,r.id(),control);
            switch(family){
                case CONTENT->{member(j,host,r,bindings,content,"referenceBindingId","REFERENCE");member(j,host,r,bindings,content,"contentBindingId","CONTENT");member(j,host,r,bindings,gold,"goldBindingId","GOLD");member(j,host,r,bindings,silver,"tempBindingId","TEMP");}
                case GOLD->{member(j,host,r,bindings,gold,"goldBindingId","GOLD");member(j,host,r,bindings,silver,"outputBindingId","SILVER");member(j,host,r,bindings,silver,"tempBindingId","TEMP");}
                case SILVER->{member(j,host,r,bindings,silver,"outputBindingId","SILVER");member(j,host,r,bindings,silver,"tempBindingId","TEMP");}
                case BRONZE->membership(j,host,r.id(),storage);
                case FETCH->{ }
            }
        }
        Path processing=absolute(silver,"processingRoot");for(Path root:roots)need(!root.startsWith(processing) && !processing.startsWith(root),RUNTIME_MISMATCH);
        return new Registration(chain,registeredProfile,storageBindings);
    }
    private static void validate(Family family,JsonNode c,String profile,String profileHash,String deployment){
        switch(family){
            case CONTENT->{SourceJson.fields(c,"contentRuntimeVersion","goldRuntimeId","goldRuntimeSha256","referenceBindingId","contentBindingId","modules","seconds");integer(c,"contentRuntimeVersion",2,2);modules(c,RssContentRuntime.MODULES);}
            case GOLD->{SourceJson.fields(c,"goldRuntimeVersion","silverRuntimeId","silverRuntimeSha256","goldBindingId","modules","duckdb","seconds","python");integer(c,"goldRuntimeVersion",2,2);need("1.4.5".equals(text(c,"duckdb")),RUNTIME_MISMATCH);absolute(c,"python");modules(c,RssGoldRuntime.MODULES);}
            case SILVER->{SourceJson.fields(c,"silverRuntimeVersion","bronzeRuntimeId","bronzeRuntimeSha256","runProfileSha256","deploymentSha256","python","javaHome","processingRoot","modules","versions","outputBindingId","tempBindingId","seconds");integer(c,"silverRuntimeVersion",2,2);links(c,profileHash,deployment);absolute(c,"python");absolute(c,"javaHome");absolute(c,"processingRoot");modules(c,RssSilverRuntime.MODULES);need(c.get("versions").equals(SourceJson.object().put("python","3.12.15").put("pyspark","4.0.2").put("boto3","1.43.114").put("java","21")),RUNTIME_MISMATCH);}
            case BRONZE->{SourceJson.fields(c,"bronzeRuntimeVersion","adapter","fetchRuntimeId","fetchRuntimeSha256","runProfileId","runProfileSha256","deploymentSha256","controlBindingId","storageBindingId","storageBindingSha256");integer(c,"bronzeRuntimeVersion",2,2);links(c,profileHash,deployment);need(profile.equals(text(c,"runProfileId")) && "platform.ingestion.managed.BoundedManagedBronze".equals(text(c,"adapter")),RUNTIME_MISMATCH);}
            case FETCH->{SourceJson.fields(c,"rssFetchRuntimeVersion","pipeline","runProfileId","runProfileSha256","deploymentSha256","javaHome","manifest","controlBindingId","testEndpoint","httpTimeoutMillis");integer(c,"rssFetchRuntimeVersion",2,2);links(c,profileHash,deployment);absolute(c,"javaHome");need(profile.equals(text(c,"runProfileId")) && "CONTENT_RSS_V1".equals(text(c,"pipeline")) && c.get("testEndpoint").isBoolean(),RUNTIME_MISMATCH);integer(c,"httpTimeoutMillis",1,30000);var manifest=c.get("manifest");SourceJson.fields(manifest,"adapter","javaRuntime","classpath");need("platform.ingestion.managed.BoundedManagedRssFetch".equals(text(manifest,"adapter")) && !text(manifest,"javaRuntime").isBlank() && SourceJson.sha(SourceJson.canonical(manifest)).equals(deployment),RUNTIME_MISMATCH);need(manifest.get("classpath").isArray() && manifest.get("classpath").size()>0 && manifest.get("classpath").size()<=128,RUNTIME_MISMATCH);Set<Path> paths=new HashSet<>();for(var entry:manifest.get("classpath")){SourceJson.fields(entry,"path","sha256");Path path=absolute(entry,"path");need(path.toString().endsWith(".jar") && paths.add(path) && text(entry,"sha256").matches("[0-9a-f]{64}"),RUNTIME_MISMATCH);}}
        }
        if(Set.of(Family.CONTENT,Family.GOLD,Family.SILVER).contains(family))integer(c,"seconds",1,900);
    }
    private static void modules(JsonNode c,List<String> legacy){var names=ContentEvidenceProfile.modules(legacy,2);SourceJson.fields(c.get("modules"),names.toArray(String[]::new));for(String name:names)need(text(c.get("modules"),name).matches("[0-9a-f]{64}"),RUNTIME_MISMATCH);}
    private static void links(JsonNode c,String profile,String deployment){need(profile.equals(text(c,"runProfileSha256")) && deployment.equals(text(c,"deploymentSha256")),RUNTIME_MISMATCH);}
    private static void member(JdbcTemplate j,InstallationHostGuard.Binding host,Registered r,Map<String,JsonNode> bindings,JsonNode c,String field,String role){membership(j,host,r.id(),binding(bindings,text(c,field),role));}
    private static JsonNode binding(Map<String,JsonNode> bindings,String id,String role){var b=bindings.get(id);need(b!=null && role.equals(text(b,"role")),RUNTIME_MISMATCH);return b;}
    private static void membership(JdbcTemplate j,InstallationHostGuard.Binding host,String profile,JsonNode b){need(Boolean.TRUE.equals(j.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.execution_profile_storage p JOIN ddip_management.storage_bindings b USING(binding_id) WHERE p.profile_id=? AND b.binding_id=? AND b.installation_id=? AND b.role=? AND b.product=? AND b.configuration_sha256=? AND b.configuration_bytes=?)",Boolean.class,profile,text(b,"bindingId"),host.installationId(),text(b,"role"),text(b,"productId"),text(b,"configurationSha256"),SourceJson.canonical(b.get("configuration")))),RUNTIME_MISMATCH);}
    private static Registered load(JdbcTemplate j,String id){need(id!=null && id.matches("[A-Za-z0-9_-]{1,128}"),RUNTIME_MISMATCH);var r=one(j.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",id));String hash=(String)r.get("configuration_sha256");var c=FrozenRunSelection.canonical((byte[])r.get("configuration_bytes"),hash,65536);return new Registered(id,hash,SourceJson.canonical(c));}
    private static Map<String,Object> one(List<Map<String,Object>> rows){need(rows.size()==1,RUNTIME_MISMATCH);return rows.getFirst();}
    private static String text(JsonNode n,String key){return SourceJson.text(n.get(key));}
    private static void integer(JsonNode n,String key,int min,int max){var v=n.get(key);need(v.isIntegralNumber() && v.canConvertToInt() && v.intValue()>=min && v.intValue()<=max,RUNTIME_MISMATCH);}
    private static Path absolute(JsonNode n,String key){Path p=Path.of(text(n,key));need(p.isAbsolute() && p.equals(p.normalize()),RUNTIME_MISMATCH);return p;}
    private NarrowContentRuntimeQualification(){}
}
