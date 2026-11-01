package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Reviewed fixed Content adapter, extending the exact D4 runtime and original storage bindings. */
final class RssContentRuntime {
    static final List<String> MODULES=List.of("managed_content.py","content_dependencies.py","content_publication.py","content_processing.py","content_analytics.py","publisher_reference.py");
    final JsonNode config,reference,content;final RssGoldRuntime gold;final String digest;
    private RssContentRuntime(JsonNode c,JsonNode r,JsonNode out,RssGoldRuntime g,String sha){config=c;reference=r;content=out;gold=g;digest=sha;}
    static RssContentRuntime load(JdbcTemplate j,ManagementTransactions tx,InstallationHostGuard guard,UUID run,JsonNode frozen,String id){
        var rows=tx.required(()->j.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",id));need(rows.size()==1,RUNTIME_MISMATCH);var row=rows.getFirst();String sha=(String)row.get("configuration_sha256");
        var c=FrozenRunSelection.canonical((byte[])row.get("configuration_bytes"),sha,65536);SourceJson.fields(c,"contentRuntimeVersion","goldRuntimeId","goldRuntimeSha256","referenceBindingId","contentBindingId","modules","seconds");
        need(c.get("contentRuntimeVersion").isIntegralNumber() && c.get("contentRuntimeVersion").canConvertToInt() && Set.of(1,2).contains(c.get("contentRuntimeVersion").intValue()) && c.get("seconds").isIntegralNumber() && c.get("seconds").canConvertToInt() && c.get("seconds").intValue()>0 && c.get("seconds").intValue()<=900,RUNTIME_MISMATCH);
        ContentEvidenceProfile.runtime(j,tx,frozen,"CONTENT",id,c.get("contentRuntimeVersion").intValue());
        var gold=RssGoldRuntime.load(j,tx,guard,run,frozen,c.get("goldRuntimeId").stringValue());need(gold.digest.equals(c.get("goldRuntimeSha256").stringValue()),RUNTIME_MISMATCH);
        JsonNode reference=null,content=null;for(var b:frozen.get("storageBindings")){if(b.get("bindingId").equals(c.get("referenceBindingId"))){need(b.get("role").stringValue().equals("REFERENCE"),RUNTIME_MISMATCH);reference=b.get("configuration");}if(b.get("bindingId").equals(c.get("contentBindingId"))){need(b.get("role").stringValue().equals("CONTENT"),RUNTIME_MISMATCH);content=b.get("configuration");}}
        need(reference!=null && content!=null,RUNTIME_MISMATCH);
        for(String binding:List.of(c.get("referenceBindingId").stringValue(),c.get("contentBindingId").stringValue(),gold.config.get("goldBindingId").stringValue(),gold.silver.config.get("tempBindingId").stringValue(),guard.binding().id()))need(tx.required(()->j.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.execution_profile_storage p JOIN ddip_management.storage_bindings b USING(binding_id) WHERE p.profile_id=? AND b.binding_id=? AND b.installation_id=?)",Boolean.class,id,binding,guard.binding().installationId())),RUNTIME_MISMATCH);
        var runtime=new RssContentRuntime(c,reference,content,gold,sha);runtime.check();return runtime;
    }
    void check(){gold.check();try{var modules=ContentEvidenceProfile.modules(MODULES,config.get("contentRuntimeVersion").intValue());SourceJson.fields(config.get("modules"),modules.toArray(String[]::new));Path processing=Path.of(gold.silver.config.get("processingRoot").stringValue());for(String m:modules)need(SourceJson.sha(PreflightFiles.read(processing.resolve(m),1024*1024,false)).equals(config.get("modules").get(m).stringValue()),RUNTIME_MISMATCH);
        var roots=new ArrayList<Path>();for(var b:List.of(reference,content,gold.gold,gold.silver.binding())){Path root=Path.of(b.get("root").stringValue());var identity=InstallationHostGuard.identity(root);need(root.isAbsolute() && root.equals(root.toRealPath()) && Long.toString(identity.device()).equals(b.get("rootDevice").stringValue()) && Long.toString(identity.inode()).equals(b.get("rootInode").stringValue()),INPUT_CHANGED);need(!root.startsWith(processing) && !processing.startsWith(root),RUNTIME_MISMATCH);for(Path other:roots)need(!root.startsWith(other) && !other.startsWith(root),RUNTIME_MISMATCH);roots.add(root);}need(Files.getFileStore(roots.get(1)).getUsableSpace()>=1024L*1024*1024,RESOURCE_LIMIT);
    }catch(PreflightFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}}
    JsonNode binding(JsonNode value){var n=SourceJson.object();for(String k:List.of("root","rootDevice","rootInode"))n.set(k,value.get(k));return n;}
    List<String> command(Path path){if(config.get("contentRuntimeVersion").intValue()==2)return ContentEvidenceProfile.command(gold.silver.config,"CONTENT",path);return List.of(gold.config.get("python").stringValue(),"-I","-B","-X","utf8",Path.of(gold.silver.config.get("processingRoot").stringValue()).resolve("managed_content.py").toString(),path.toString());}
}
