package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Fixed additive Gold adapter profile; delegates established Python/Spark/host binding checks to D3. */
final class RssGoldRuntime {
    static final List<String> MODULES=List.of("managed_gold.py","gold.py","gold_publication.py","gold_reader.py","gold_analytics.py","content_metadata.py");
    final JsonNode config,gold;final RssSilverRuntime silver;final String digest;
    RssGoldRuntime(JsonNode c,JsonNode gold,RssSilverRuntime silver,String digest){config=c;this.gold=gold;this.silver=silver;this.digest=digest;}
    static RssGoldRuntime load(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,JsonNode frozen,String id){
        var rows=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",id));need(rows.size()==1,RUNTIME_MISMATCH);var row=rows.getFirst();
        String digest=(String)row.get("configuration_sha256");var c=FrozenRunSelection.canonical((byte[])row.get("configuration_bytes"),digest,65536);SourceJson.fields(c,"goldRuntimeVersion","silverRuntimeId","silverRuntimeSha256","goldBindingId","modules","duckdb","seconds","python");
        need(c.get("goldRuntimeVersion").isIntegralNumber() && c.get("goldRuntimeVersion").canConvertToInt() && Set.of(1,2).contains(c.get("goldRuntimeVersion").intValue()) && c.get("duckdb").stringValue().equals("1.4.5") && c.get("seconds").isIntegralNumber() && c.get("seconds").canConvertToInt() && c.get("seconds").intValue()>0 && c.get("seconds").intValue()<=900,RUNTIME_MISMATCH);
        ContentEvidenceProfile.runtime(jdbc,tx,frozen,"GOLD",id,c.get("goldRuntimeVersion").intValue());
        var profiles=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",c.get("silverRuntimeId").stringValue()));need(profiles.size()==1,RUNTIME_MISMATCH);var sr=profiles.getFirst();need(c.get("silverRuntimeSha256").stringValue().equals(sr.get("configuration_sha256")),RUNTIME_MISMATCH);var sc=FrozenRunSelection.canonical((byte[])sr.get("configuration_bytes"),(String)sr.get("configuration_sha256"),65536);
        String bronzeSha=tx.required(()->jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,sc.get("bronzeRuntimeId").stringValue()));
        need(bronzeSha.equals(sc.get("bronzeRuntimeSha256").stringValue()),RUNTIME_MISMATCH);
        var silver=RssSilverRuntime.load(jdbc,tx,guard,frozen,bronzeSha,c.get("silverRuntimeId").stringValue());JsonNode gold=null;
        for(var b:frozen.get("storageBindings"))if(b.get("bindingId").equals(c.get("goldBindingId"))){need(b.get("role").stringValue().equals("GOLD"),RUNTIME_MISMATCH);gold=b.get("configuration");}
        need(gold!=null,RUNTIME_MISMATCH);
        for(String binding:List.of(c.get("goldBindingId").stringValue(),guard.binding().id(),silver.config.get("outputBindingId").stringValue(),silver.config.get("tempBindingId").stringValue()))need(tx.required(()->jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.execution_profile_storage p JOIN ddip_management.storage_bindings b USING(binding_id) WHERE p.profile_id=? AND b.binding_id=? AND b.installation_id=?)",Boolean.class,id,binding,guard.binding().installationId())),RUNTIME_MISMATCH);
        var runtime=new RssGoldRuntime(c,gold,silver,digest);runtime.check();return runtime;
    }
    void check(){silver.check();try{
        Path python=Path.of(config.get("python").stringValue());need(python.isAbsolute() && Files.isExecutable(python),RUNTIME_MISMATCH);
        var modules=ContentEvidenceProfile.modules(MODULES,config.get("goldRuntimeVersion").intValue());SourceJson.fields(config.get("modules"),modules.toArray(String[]::new));Path processing=Path.of(silver.config.get("processingRoot").stringValue());for(String module:modules)need(SourceJson.sha(PreflightFiles.read(processing.resolve(module),1024*1024,false)).equals(config.get("modules").get(module).stringValue()),RUNTIME_MISMATCH);
        Path root=Path.of(gold.get("root").stringValue());var identity=InstallationHostGuard.identity(root);need(root.isAbsolute() && root.equals(root.toRealPath()) && Long.toString(identity.device()).equals(gold.get("rootDevice").stringValue()) && Long.toString(identity.inode()).equals(gold.get("rootInode").stringValue()),INPUT_CHANGED);
        need(!root.startsWith(processing) && !processing.startsWith(root) && Files.getFileStore(root).getUsableSpace()>=1024L*1024*1024,RESOURCE_LIMIT);
    }catch(PreflightFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}}
    JsonNode binding(){var n=SourceJson.object();for(String k:List.of("root","rootDevice","rootInode"))n.set(k,gold.get(k));return n;}
    List<String> command(Path p){if(config.get("goldRuntimeVersion").intValue()==2)return ContentEvidenceProfile.command(silver.config,"GOLD",p);return List.of(config.get("python").stringValue(),"-I","-B","-X","utf8",Path.of(silver.config.get("processingRoot").stringValue()).resolve("managed_gold.py").toString(),p.toString());}
}
