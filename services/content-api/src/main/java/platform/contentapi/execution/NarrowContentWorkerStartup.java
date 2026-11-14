package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.function.BooleanSupplier;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/** Run-independent, read-only installation readiness. Never a READY receipt or launch authority. */
final class NarrowContentWorkerStartup {
    private record Pin(InstallationHostGuard.Identity identity,FileTime modified,long size,String digest) {}
    static final class Configuration {
        private final NarrowContentRuntimeQualification.Registration registered;
        private final JsonNode strict;
        private final InstallationHostGuard guard;
        private final BooleanSupplier stopping;
        private final List<Runnable> children;
        private final Map<Path,Pin> pins;
        private Configuration(NarrowContentRuntimeQualification.Registration registered,JsonNode strict,
                InstallationHostGuard guard,BooleanSupplier stopping,List<Runnable> children){
            this.registered=registered;this.strict=strict.deepCopy();this.guard=guard;this.stopping=stopping;
            this.children=List.copyOf(children);pins=observe();
        }
        int compositionVersion(){return strict.get("strictContentPipelineVersion").intValue();}
        String python(){return strict.get("python").stringValue();}
        String javaHome(){return strict.get("javaHome").stringValue();}
        String processingRoot(){return strict.get("processingRoot").stringValue();}
        void check(){
            need(!stopping.getAsBoolean() && !Thread.currentThread().isInterrupted(),INTERRUPTED);
            guard.checkHeld(guard.binding().installationId());
            for(var child:children)child.run();
            need(pins.equals(observe()),INPUT_CHANGED);
        }
        private Map<Path,Pin> observe(){
            need(!stopping.getAsBoolean() && !Thread.currentThread().isInterrupted(),INTERRUPTED);
            guard.checkHeld(guard.binding().installationId());
            try{
                Path processing=Path.of(processingRoot()),python=Path.of(python()),javaRoot=Path.of(javaHome());
                need(processing.isAbsolute() && processing.equals(processing.toRealPath()) && javaRoot.isAbsolute()
                        && javaRoot.equals(javaRoot.toRealPath()) && javaRoot.equals(Path.of(System.getProperty("java.home")).toRealPath())
                        && Runtime.version().feature()==21 && python.isAbsolute() && Files.isExecutable(python),RUNTIME_MISMATCH);
                need(registered.configuration("GOLD").get("python").stringValue().equals(python())
                        && registered.configuration("FETCH").get("javaHome").stringValue().equals(javaHome()),RUNTIME_MISMATCH);
                var result=new LinkedHashMap<Path,Pin>();
                for(String family:List.of("CONTENT","GOLD","SILVER"))modules(result,processing,registered.configuration(family).get("modules"));
                modules(result,processing,strict.get("modules"));
                for(Path executable:List.of(python,javaRoot.resolve("bin/java"))){
                    need(Files.isExecutable(executable),RUNTIME_MISMATCH);Path real=executable.toRealPath();result.put(real,pin(real,64*1024*1024,null));
                }
                for(var binding:registered.storageBindings()){
                    var c=binding.get("configuration");if(!c.has("root"))continue;
                    Path root=Path.of(c.get("root").stringValue());var identity=InstallationHostGuard.identity(root);
                    need(root.isAbsolute() && root.equals(root.toRealPath())
                            && Long.toString(identity.device()).equals(c.get("rootDevice").stringValue())
                            && Long.toString(identity.inode()).equals(c.get("rootInode").stringValue()),INPUT_CHANGED);
                    need(!root.startsWith(processing) && !processing.startsWith(root),RUNTIME_MISMATCH);
                    for(Path file:result.keySet())need(!file.startsWith(root),RUNTIME_MISMATCH);
                    need(Files.getFileStore(root).getUsableSpace()>=1024L*1024*1024,RESOURCE_LIMIT);
                    if(Set.of("TEMP","CONTROL").contains(binding.get("role").stringValue()))PreflightFiles.privateEntry(root,true);
                }
                for(var binding:registered.storageBindings())if(binding.get("configuration").has("credentialsFile")){
                    Path credentials=Path.of(binding.get("configuration").get("credentialsFile").stringValue());
                    need(!result.containsKey(credentials),RUNTIME_MISMATCH);
                    PreflightFiles.privateEntry(credentials,false);
                    need(Files.getPosixFilePermissions(credentials).equals(java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")),RUNTIME_MISMATCH);
                    // Validate the exact bytes being pinned, not an earlier filesystem read.
                    // Keep only the digest/identity, never the credential payload.
                    result.put(credentials,pin(credentials,8192,null,bytes->{
                        var values=SourceJson.parse(bytes,8192);SourceJson.fields(values,"accessKey","secretKey");
                        for(String key:List.of("accessKey","secretKey"))need(values.get(key).isString() && !values.get(key).stringValue().isBlank()
                                && values.get(key).stringValue().getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=4096
                                && values.get(key).stringValue().codePoints().noneMatch(Character::isISOControl),RUNTIME_MISMATCH);
                    }));
                    PreflightFiles.privateEntry(credentials,false);
                    need(Files.getPosixFilePermissions(credentials).equals(java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")),RUNTIME_MISMATCH);
                }
                guard.checkHeld(guard.binding().installationId());return Map.copyOf(result);
            }catch(PreflightFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
        }
        private static void modules(Map<Path,Pin> pins,Path root,JsonNode modules)throws java.io.IOException{
            for(String name:modules.propertyNames()){
                need(name.matches("[a-z][a-z0-9_]*\\.py"),RUNTIME_MISMATCH);
                Path path=root.resolve(name);Pin observed=pin(path,1024*1024,modules.get(name).stringValue());
                Pin previous=pins.putIfAbsent(path,observed);need(previous==null || previous.equals(observed),INPUT_CHANGED);
            }
        }
        private static Pin pin(Path path,int max,String expected)throws java.io.IOException{
            return pin(path,max,expected,bytes->{});
        }
        private static Pin pin(Path path,int max,String expected,java.util.function.Consumer<byte[]> validate)throws java.io.IOException{
            var identity=InstallationHostGuard.identity(path);var modified=Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS);
            byte[] bytes=PreflightFiles.read(path,max,false);String digest=SourceJson.sha(bytes);
            validate.accept(bytes);
            need((expected==null || expected.equals(digest)) && identity.equals(InstallationHostGuard.identity(path))
                    && modified.equals(Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS)),INPUT_CHANGED);
            return new Pin(identity,modified,bytes.length,digest);
        }
        @Override public String toString(){return "NarrowContentWorkerStartup[read-only readiness; non-authorizing]";}
    }
    static Configuration startup(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,
            String profile,String digest,String deployment,String runtimeId,BooleanSupplier stopping){
        need(jdbc!=null && tx!=null && guard!=null && stopping!=null,INPUT_INVALID);
        need(!stopping.getAsBoolean() && !Thread.currentThread().isInterrupted(),INTERRUPTED);
        try{
            var registration=NarrowContentRuntimeQualification.registration(jdbc,tx,guard,profile,digest,deployment);
            // Reuse ordinary storage readiness, including private local credentials. No object
            // request or native inspection is performed, and no run-bound selection is invented.
            new platform.contentapi.importing.ImportBindings(jdbc,tx,guard.binding().installationId()).checkWorkerStorage(profile,"content-intelligence");
            var rows=tx.required(()->jdbc.queryForList("SELECT profile_id,configuration_bytes,configuration_sha256 FROM ddip_management.content_pipeline_profiles WHERE convert_from(configuration_bytes,'UTF8')::jsonb->>'runProfileId'=? LIMIT 2",profile));
            need(rows.size()==1 && Objects.equals(runtimeId,rows.getFirst().get("profile_id")),RUNTIME_MISMATCH);
            var row=rows.getFirst();byte[] raw=(byte[])row.get("configuration_bytes");
            need(SourceJson.sha(raw).equals(row.get("configuration_sha256")),RUNTIME_MISMATCH);var strict=PreflightFiles.canonical(raw,65536);
            // Ordinary startup rejects historical loopback before probes or registration.
            need(strict.get("strictContentPipelineVersion").isInt()
                    && strict.get("strictContentPipelineVersion").intValue()==2,RUNTIME_MISMATCH);
            StrictContentPipelineRuntime.validateRegistration(strict,registration,guard);
            StrictContentPipelineRuntime.validateComposition(jdbc,tx,strict,registration,guard.binding());
            var children=List.of(
                    ContentFetchLaunchRuntime.startupCheck(jdbc,tx,guard,registration,strict.get("fetchLaunchProfileId").stringValue()),
                    StrictBronzeLaunchRuntime.startupCheck(jdbc,tx,guard,registration,strict.get("bronzeLaunchProfileId").stringValue()),
                    StrictBronzeVerificationRuntime.startupCheck(jdbc,tx,guard,registration,strict.get("bronzeVerificationProfileId").stringValue()));
            var result=new Configuration(registration,strict,guard,stopping,children);
            var environment=Map.of("PATH","/usr/bin:/bin","TZ","UTC","LANG","C.UTF-8","JAVA_HOME",result.javaHome(),"OPENBLAS_NUM_THREADS","1","OMP_NUM_THREADS","1");
            StrictContentPipelineRuntime.probe(strict,environment,guard,result::check,result::check,()->{},stopping);
            result.check();return result;
        }catch(PreflightFailure ex){throw ex;}catch(org.springframework.dao.DataAccessException ex){throw ex;}
        catch(RuntimeException ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    private NarrowContentWorkerStartup(){}
}
