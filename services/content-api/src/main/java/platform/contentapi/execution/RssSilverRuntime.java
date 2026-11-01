package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Additive reviewed internal runtime; neither RunProfile nor native product fingerprints change. */
final class RssSilverRuntime {
    static final List<String> MODULES=List.of("managed_silver.py","bronze_to_silver.py","silver.py","silver_inputs.py","silver_import_qualification.py","artifact_import_verifier.py","publication_metadata.py");
    final JsonNode config,output,scratch;final String digest;final InstallationHostGuard guard;
    private RssSilverRuntime(JsonNode config,String digest,JsonNode output,JsonNode scratch,InstallationHostGuard guard){this.config=config;this.digest=digest;this.output=output;this.scratch=scratch;this.guard=guard;}
    static RssSilverRuntime load(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,RssBronzeContext bronze,String id){
        return load(jdbc,tx,guard,bronze.frozen,bronze.runtimeSha,id);
    }
    /** Runtime qualification is independent of delivery completion, including pre-journal recovery. */
    static RssSilverRuntime load(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,JsonNode frozen,String bronzeSha,String id){
        try {
            var rows=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",id));need(rows.size()==1,RUNTIME_MISMATCH);
            var row=rows.getFirst();String sha=(String)row.get("configuration_sha256");var c=FrozenRunSelection.canonical((byte[])row.get("configuration_bytes"),sha,65536);
            SourceJson.fields(c,"silverRuntimeVersion","bronzeRuntimeId","bronzeRuntimeSha256","runProfileSha256","deploymentSha256","python","javaHome","processingRoot","modules","versions","outputBindingId","tempBindingId","seconds");
            need(c.get("silverRuntimeVersion").isIntegralNumber() && c.get("silverRuntimeVersion").canConvertToInt() && Set.of(1,2).contains(c.get("silverRuntimeVersion").intValue()) && c.get("bronzeRuntimeSha256").stringValue().equals(bronzeSha) && c.get("runProfileSha256").equals(frozen.get("executionProfile").get("configurationSha256")) && c.get("deploymentSha256").equals(frozen.get("executionProfile").get("configuration").get("runtime").get("deploymentSha256")),RUNTIME_MISMATCH);
            ContentEvidenceProfile.runtime(jdbc,tx,frozen,"SILVER",id,c.get("silverRuntimeVersion").intValue());
            need(c.get("seconds").isIntegralNumber() && c.get("seconds").canConvertToInt() && c.get("seconds").intValue()>0 && c.get("seconds").intValue()<=900,RUNTIME_MISMATCH);
            JsonNode output=null,scratch=null;Set<Path> roots=new HashSet<>();
            for(var b:frozen.get("storageBindings")){
                if(b.get("configuration").has("root")){Path root=Path.of(b.get("configuration").get("root").stringValue());for(Path other:roots)need(!root.startsWith(other) && !other.startsWith(root),RUNTIME_MISMATCH);roots.add(root);}
                if(b.get("bindingId").equals(c.get("outputBindingId"))){need(b.get("role").stringValue().equals("SILVER"),RUNTIME_MISMATCH);output=b.get("configuration");}
                if(b.get("bindingId").equals(c.get("tempBindingId"))){need(b.get("role").stringValue().equals("TEMP"),RUNTIME_MISMATCH);scratch=b.get("configuration");}
            }
            need(output!=null && scratch!=null,RUNTIME_MISMATCH);
            for(String binding:List.of(c.get("outputBindingId").stringValue(),c.get("tempBindingId").stringValue(),guard.binding().id()))need(tx.required(()->jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.execution_profile_storage p JOIN ddip_management.storage_bindings b USING(binding_id) WHERE p.profile_id=? AND b.binding_id=? AND b.installation_id=?)",Boolean.class,id,binding,guard.binding().installationId())),RUNTIME_MISMATCH);
            var runtime=new RssSilverRuntime(c,sha,output,scratch,guard);runtime.check();return runtime;
        }catch(PreflightFailure | ManagementFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    void check(){
        guard.checkHeld(guard.binding().installationId());
        try {
            Path processing=Path.of(config.get("processingRoot").stringValue()),python=Path.of(config.get("python").stringValue()),java=Path.of(config.get("javaHome").stringValue());
            need(processing.isAbsolute() && processing.equals(processing.toRealPath()) && python.isAbsolute() && Files.isExecutable(python) && java.equals(Path.of(System.getProperty("java.home")).toRealPath()) && Runtime.version().feature()==21,RUNTIME_MISMATCH);
            need(config.get("versions").equals(SourceJson.object().put("python","3.12.15").put("pyspark","4.0.2").put("boto3","1.43.114").put("java","21")),RUNTIME_MISMATCH);
            var modules=ContentEvidenceProfile.modules(MODULES,config.get("silverRuntimeVersion").intValue());SourceJson.fields(config.get("modules"),modules.toArray(String[]::new));for(String module:modules)need(SourceJson.sha(PreflightFiles.read(processing.resolve(module),1024*1024,false)).equals(config.get("modules").get(module).stringValue()),RUNTIME_MISMATCH);
            for(var binding:List.of(output,scratch)){
                Path root=Path.of(binding.get("root").stringValue());var identity=InstallationHostGuard.identity(root);
                need(root.isAbsolute() && root.equals(root.toRealPath()) && Long.toString(identity.device()).equals(binding.get("rootDevice").stringValue()) && Long.toString(identity.inode()).equals(binding.get("rootInode").stringValue()),INPUT_CHANGED);
                need(!processing.startsWith(root) && !root.startsWith(processing),RUNTIME_MISMATCH);
                need(Files.getFileStore(root).getUsableSpace()>=1024L*1024*1024,RESOURCE_LIMIT);
            }
            PreflightFiles.privateEntry(Path.of(scratch.get("root").stringValue()),true);
        }catch(PreflightFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    JsonNode binding(){var n=SourceJson.object();for(String key:List.of("root","rootDevice","rootInode"))n.set(key,output.get(key));return n;}
    JsonNode nativeRuntime(){return SourceJson.object().put("javaHome",config.get("javaHome").stringValue()).put("scratchRoot",scratch.get("root").stringValue()).put("scratchDevice",scratch.get("rootDevice").stringValue()).put("scratchInode",scratch.get("rootInode").stringValue());}
    List<String> command(Path path){if(config.get("silverRuntimeVersion").intValue()==2)return ContentEvidenceProfile.command(config,"SILVER",path);return List.of(config.get("python").stringValue(),"-I","-B","-X","utf8",Path.of(config.get("processingRoot").stringValue()).resolve("managed_silver.py").toString(),path.toString());}
    Map<String,String> environment(){return Map.of("PATH","/usr/bin:/bin","TZ","UTC","LANG","C.UTF-8","JAVA_HOME",config.get("javaHome").stringValue(),"OPENBLAS_NUM_THREADS","1","OMP_NUM_THREADS","1");}
}
