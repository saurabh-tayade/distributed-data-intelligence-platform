package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Execution-only descriptor. RunProfile v1 stays unchanged; frozen deployment SHA binds this manifest. */
public final class ReliabilityRuntime {
    public static final List<String> MODULES=List.of("reliability_execution_preflight.py","artifact_import_verifier.py","publication_metadata.py",
        "reliability_format_inputs.py","reliability_format_metadata.py","reliability_format_parquet.py","reliability_inputs.py","reliability_metadata.py",
        "service_reference.py","reliability_format_publication.py","reliability_format_processing.py","reliability_format_analytics.py");
    private final String id,digest;private final JsonNode config,manifest,bindings;
    private final Path python,processing,javaHome;
    private ReliabilityRuntime(String id,String digest,JsonNode config,JsonNode manifest,JsonNode bindings) {
        this.id=id;this.digest=digest;this.config=config.deepCopy();this.manifest=manifest.deepCopy();this.bindings=bindings.deepCopy();
        python=Path.of(config.get("python").stringValue());processing=Path.of(config.get("processingRoot").stringValue());javaHome=Path.of(config.get("javaHome").stringValue());
    }
    public static ReliabilityRuntime load(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,WorkerRepository.Owner owner,String id,JsonNode frozen) {
        try {
            need(id!=null && id.matches("[A-Za-z0-9_-]{1,128}"),RUNTIME_MISMATCH);
            var rows=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",id));
            need(rows.size()==1,RUNTIME_MISMATCH);byte[] raw=(byte[])rows.getFirst().get("configuration_bytes");String hash=(String)rows.getFirst().get("configuration_sha256");
            var c=FrozenRunSelection.canonical(raw,hash,65536);
            SourceJson.fields(c,"executionRuntimeVersion","pipeline","runProfileId","runProfileSha256","python","javaHome","processingRoot","deploymentSha256","deploymentRevision","bindings","verificationSeconds");
            need(integer(c,"executionRuntimeVersion")==1 && text(c,"pipeline").equals("RELIABILITY_V2") && text(c,"runProfileId").equals(owner.worker().profileId()) && text(c,"runProfileSha256").equals(owner.worker().profileDigest()) && text(c,"deploymentSha256").equals(owner.worker().deploymentDigest()) && text(c,"deploymentRevision").matches("[0-9a-f]{40}") && integer(c,"verificationSeconds")>=1 && integer(c,"verificationSeconds")<=900,RUNTIME_MISMATCH);
            var expected=frozen.get("executionProfile");need(expected.get("profileId").stringValue().equals(c.get("runProfileId").stringValue()) && expected.get("configurationSha256").stringValue().equals(c.get("runProfileSha256").stringValue()) && frozen.get("pipeline").stringValue().equals("RELIABILITY_V2"),RUNTIME_MISMATCH);
            var refs=c.get("bindings");SourceJson.fields(refs,"batch","reference","output","staging","temp","control");
            var resolved=SourceJson.object();Set<Path> roots=new HashSet<>();
            for(String role:List.of("batch","reference","output","staging","temp","control")) {
                String binding=text(refs,role);need(binding.matches("[A-Za-z0-9_-]{1,128}"),RUNTIME_MISMATCH);
                String storageRole=switch(role){case "batch"->"BATCH";case "reference"->"REFERENCE";case "output"->"RELIABILITY";case "staging"->"STAGING";case "temp"->"TEMP";default->"CONTROL";};
                var matched=new ArrayList<JsonNode>();for(var row:frozen.get("storageBindings"))if(row.get("bindingId").stringValue().equals(binding))matched.add(row);
                need(matched.size()==1 && matched.getFirst().get("role").stringValue().equals(storageRole),RUNTIME_MISMATCH);
                var descriptor=matched.getFirst().get("configuration");need(SourceJson.sha(SourceJson.canonical(descriptor)).equals(matched.getFirst().get("configurationSha256").stringValue()),RUNTIME_MISMATCH);
                if(role.equals("control"))need(binding.equals(guard.binding().id()) && Arrays.equals(SourceJson.canonical(descriptor),guard.binding().descriptor()),RUNTIME_MISMATCH);
                else {SourceJson.fields(descriptor,"importStorageVersion","root","rootDevice","rootInode");need(integer(descriptor,"importStorageVersion")==1,RUNTIME_MISMATCH);}
                var n=resolved.putObject(role);for(String key:List.of("root","rootDevice","rootInode"))n.put(key,text(descriptor,key));
                Path path=Path.of(text(descriptor,"root"));need(path.isAbsolute() && path.equals(path.normalize()),RUNTIME_MISMATCH);roots.add(path);
                boolean associated=tx.required(()->jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.execution_profile_storage p JOIN ddip_management.storage_bindings b USING(binding_id) WHERE p.profile_id=? AND b.binding_id=? AND b.installation_id=? AND b.role=? AND b.configuration_sha256=?)",Boolean.class,id,binding,guard.binding().installationId(),storageRole,matched.getFirst().get("configurationSha256").stringValue()));
                need(associated,RUNTIME_MISMATCH);
            }
            need(roots.size()==6,RUNTIME_MISMATCH);for(Path a:roots)for(Path b:roots)if(!a.equals(b))need(!a.startsWith(b),RUNTIME_MISMATCH);
            for(var input:frozen.get("inputs"))need(input.get("storageBindingId").stringValue().equals(text(refs,input.get("role").stringValue().equals("RELIABILITY_BATCH")?"batch":"reference")),RUNTIME_MISMATCH);
            Path processing=Path.of(text(c,"processingRoot"));need(processing.isAbsolute() && processing.equals(processing.toRealPath()),RUNTIME_MISMATCH);
            for(Path root:roots)need(!processing.startsWith(root) && !root.startsWith(processing),RUNTIME_MISMATCH);
            byte[] deployment=PreflightFiles.read(processing.resolve("e5-reliability-deployment.json"),65536,false);
            need(SourceJson.sha(deployment).equals(text(c,"deploymentSha256")),RUNTIME_MISMATCH);
            var manifest=PreflightFiles.canonical(deployment,65536);SourceJson.fields(manifest,"deploymentRevision","versions","spark","modules");
            need(manifest.get("deploymentRevision").stringValue().equals(c.get("deploymentRevision").stringValue()),RUNTIME_MISMATCH);
            var runtime=new ReliabilityRuntime(id,hash,c,manifest,resolved);runtime.check();return runtime;
        }catch(PreflightFailure | ManagementFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    public void check() {
        try {
            need(Runtime.version().feature()==21 && javaHome.isAbsolute() && javaHome.toRealPath().equals(Path.of(System.getProperty("java.home")).toRealPath()) && Files.isExecutable(javaHome.resolve("bin/java")) && python.isAbsolute() && Files.isRegularFile(python) && Files.isExecutable(python) && processing.equals(processing.toRealPath()),RUNTIME_MISMATCH);
            need(Arrays.equals(SourceJson.canonical(manifest),PreflightFiles.read(processing.resolve("e5-reliability-deployment.json"),65536,false)),RUNTIME_MISMATCH);
            SourceJson.fields(manifest.get("modules"),MODULES.toArray(String[]::new));
            var versions=SourceJson.object().put("java","21").put("python","3.12.15").put("pyspark","4.0.2").put("duckdb","1.4.5");
            need(versions.equals(manifest.get("versions")),RUNTIME_MISMATCH);
            var spark=SourceJson.object().put("master","local[2]").put("shuffle_partitions",8).put("default_parallelism",8).put("driver_memory_mib",1024).put("executor_memory_mib",1024).put("output_partitions",4).put("event_log_enabled",false);
            need(spark.equals(manifest.get("spark")),RUNTIME_MISMATCH);
            for(String name:MODULES)need(SourceJson.sha(PreflightFiles.read(processing.resolve(name),1024*1024,false)).equals(text(manifest.get("modules"),name)),RUNTIME_MISMATCH);
            for(String role:bindings.propertyNames()) {
                var b=bindings.get(role);Path root=Path.of(text(b,"root"));var identity=InstallationHostGuard.identity(root);
                need(root.equals(root.toRealPath()) && Files.isDirectory(root,LinkOption.NOFOLLOW_LINKS) && Files.isReadable(root) && Long.toString(identity.device()).equals(text(b,"rootDevice")) && Long.toString(identity.inode()).equals(text(b,"rootInode")),INPUT_CHANGED);
                if(Set.of("staging","temp","control").contains(role))PreflightFiles.privateEntry(root,true);
            }
        }catch(PreflightFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    public void admission() {
        check();for(String role:List.of("output","staging","temp","control"))try{Path root=Path.of(text(bindings.get(role),"root"));need(Files.isWritable(root),INPUT_UNAVAILABLE);need(Files.getFileStore(root).getUsableSpace()>=1024L*1024*1024,RESOURCE_LIMIT);}catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
    JsonNode json(){var n=SourceJson.object().put("profileId",id).put("profileSha256",digest);n.set("configuration",config.deepCopy());return n;}
    JsonNode bindings(){return bindings.deepCopy();}
    List<String> command(Path descriptor){return List.of(python.toString(),"-I","-B","-X","utf8",processing.resolve("reliability_execution_preflight.py").toString(),descriptor.toString());}
    Path scratch(){return Path.of(text(bindings.get("temp"),"root"));}
    Map<String,String> environment(){return Map.of("PATH","/usr/bin:/bin","TZ","UTC","LANG","C.UTF-8","JAVA_HOME",javaHome.toString(),"TMPDIR",scratch().toString(),"OPENBLAS_NUM_THREADS","1","OMP_NUM_THREADS","1","MKL_NUM_THREADS","1");}
    int seconds(){return integer(config,"verificationSeconds");}
    String digest(){return digest;}
    private static int integer(JsonNode n,String key){var v=n.get(key);need(v!=null && v.isIntegralNumber() && v.canConvertToInt(),RUNTIME_MISMATCH);return v.intValue();}
    private static String text(JsonNode n,String key){try{return SourceJson.text(n.get(key));}catch(RuntimeException ex){throw new PreflightFailure(RUNTIME_MISMATCH);}}
    @Override public String toString(){return "ReliabilityRuntime[redacted]";}
}
