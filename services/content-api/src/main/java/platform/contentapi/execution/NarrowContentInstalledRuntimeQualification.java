package platform.contentapi.execution;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/** Inactive installed-file observations. No command, owner, sink, continuation or launch capability. */
final class NarrowContentInstalledRuntimeQualification {
    static final int JAR_BYTES=64*1024*1024, MODULE_BYTES=1024*1024;
    static final long CLASSPATH_BYTES=256L*1024*1024, FREE_BYTES=1024L*1024*1024;
    private static final Set<String> SPACE_ROLES=Set.of("CONTROL","SILVER","TEMP","GOLD","CONTENT");
    enum Boundary { INSTALLED_INSPECTED }
    @FunctionalInterface interface Space { long available(Path root) throws IOException; }
    record FileIdentity(InstallationHostGuard.Identity identity,long bytes,FileTime modified) { }
    record Artifact(String kind,String name,String sha256,FileIdentity file) { }
    record Root(String bindingId,String role,InstallationHostGuard.Identity identity,Long usableBytes) { }
    record Executable(String role,FileIdentity file) { }
    /** Detached, immutable observations. In particular this does not upgrade the v1 native ledger. */
    record Installed(List<Artifact> artifacts,List<Root> roots,List<Executable> executables) {
        Installed { artifacts=List.copyOf(artifacts);roots=List.copyOf(roots);executables=List.copyOf(executables); }
    }
    static final class Snapshot {
        private final NarrowContentEvidenceContext.Expected ready;
        private final NarrowContentRuntimeQualification.Snapshot correspondence;
        private final Installed installed;
        private Snapshot(NarrowContentEvidenceContext.Expected ready,NarrowContentRuntimeQualification.Snapshot correspondence,Installed installed){this.ready=ready;this.correspondence=correspondence;this.installed=installed;}
        NarrowContentEvidenceContext.Expected ready(){return ready;}
        NarrowContentRuntimeQualification.Snapshot correspondence(){return correspondence;}
        List<Artifact> artifacts(){return installed.artifacts();}
        List<Root> roots(){return installed.roots();}
        List<Executable> executables(){return installed.executables();}
        boolean installedArtifactsVerified(){return true;}
        boolean runtimeVersionsVerified(){return false;}
        boolean strictSinkCompatible(){return false;}
        boolean nativeLaunchCompatible(){return false;}
        @Override public String toString(){return "NarrowContentInstalledRuntimeQualification[observations only; non-launchable]";}
    }
    static Snapshot inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt){return inspect(jdbc,tx,guard,run,attempt,ignored->{});}
    static Snapshot inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt,Consumer<Boundary> boundary){
        need(jdbc!=null && tx!=null && guard!=null && run!=null && attempt!=null && boundary!=null,INPUT_INVALID);
        // READY reconstruction must run outside a surrounding SQL transaction. Files are never read
        // while a metadata transaction is held, and no caller-supplied expectation establishes READY.
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,run,attempt);
        var registered=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);
        var installed=observe(registered,guard,root->Files.getFileStore(root).getUsableSpace());
        boundary.accept(Boundary.INSTALLED_INSPECTED);
        // A second bounded pass rejects drift during inspection; neither pass grants future safety.
        var rechecked=observe(registered,guard,root->Files.getFileStore(root).getUsableSpace());
        need(installed.artifacts().equals(rechecked.artifacts()) && installed.executables().equals(rechecked.executables()),INPUT_CHANGED);
        need(sameRoots(installed.roots(),rechecked.roots()),INPUT_CHANGED);
        var current=NarrowContentAdmission.inspectReady(jdbc,tx,guard,run,attempt);
        need(ready.equals(current),RUNTIME_MISMATCH);
        var registration=NarrowContentRuntimeQualification.inspect(jdbc,tx,current);
        need(registered.frozenSelection().equals(registration.frozenSelection()),RUNTIME_MISMATCH);
        for(var family:NarrowContentRuntimeQualification.Family.values())need(registered.id(family).equals(registration.id(family))
                && registered.digest(family).equals(registration.digest(family)) && registered.configuration(family).equals(registration.configuration(family)),RUNTIME_MISMATCH);
        guard.checkHeld(guard.binding().installationId());
        return new Snapshot(ready,registered,rechecked);
    }
    /** Filesystem-only test seam. It cannot construct a READY-qualified Snapshot. */
    static Installed observe(NarrowContentRuntimeQualification.Snapshot registered,InstallationHostGuard guard,Space space){
        need(registered!=null && guard!=null && space!=null,INPUT_INVALID);
        guard.checkHeld(guard.binding().installationId());
        try {
            var frozen=registered.frozenSelection();var silver=registered.configuration("SILVER");
            var gold=registered.configuration("GOLD");var fetch=registered.configuration("FETCH");
            Path processing=directory(path(silver,"processingRoot"));
            List<Path> paths=new ArrayList<>();List<Root> roots=new ArrayList<>();Set<Path> credentials=new HashSet<>();
            for(var binding:frozen.get("storageBindings")){
                var c=binding.get("configuration");String role=text(binding,"role");
                if(role.equals("BRONZE")){credentials.add(path(c,"credentialsFile"));continue;}
                need(c.has("root"),RUNTIME_MISMATCH);
                if(role.equals("CONTROL"))need(text(binding,"bindingId").equals(guard.binding().id()) && Arrays.equals(SourceJson.canonical(c),guard.binding().descriptor()),RUNTIME_MISMATCH);
                else {SourceJson.fields(c,"importStorageVersion","root","rootDevice","rootInode");need(c.get("importStorageVersion").isIntegralNumber() && c.get("importStorageVersion").canConvertToInt() && c.get("importStorageVersion").intValue()==1,RUNTIME_MISMATCH);}
                Path root=directory(path(c,"root"));var identity=InstallationHostGuard.identity(root);
                need(Long.toString(identity.device()).equals(text(c,"rootDevice")) && Long.toString(identity.inode()).equals(text(c,"rootInode")),INPUT_CHANGED);
                separate(root,processing);for(Path other:paths)separate(root,other);paths.add(root);
                if(Set.of("CONTROL","TEMP","STAGING").contains(role))PreflightFiles.privateEntry(root,true);
                Long usable=null;if(SPACE_ROLES.contains(role)){usable=space.available(root);need(usable>=FREE_BYTES,RESOURCE_LIMIT);}
                roots.add(new Root(text(binding,"bindingId"),role,identity,usable));
            }
            need(!credentials.isEmpty(),RUNTIME_MISMATCH);
            roots.add(new Root("PROCESSING","PROCESSING",InstallationHostGuard.identity(processing),null));
            // Metadata only: the Bronze credential path is never stat'ed, resolved or opened.
            var executables=new ArrayList<Executable>();
            Path currentJava=Path.of(System.getProperty("java.home")).toRealPath();
            need(Runtime.version().feature()==21 && text(fetch.get("manifest"),"javaRuntime").equals(Runtime.version().toString()),RUNTIME_MISMATCH);
            for(var entry:Map.of("FETCH_JAVA",fetch,"SILVER_JAVA",silver).entrySet()){
                Path java=directory(path(entry.getValue(),"javaHome"));need(java.equals(currentJava),RUNTIME_MISMATCH);
                executables.add(executable(entry.getKey(),java.resolve("bin/java"),paths,credentials));
            }
            // Existing v2 Gold and Content commands use SILVER_PYTHON. GOLD_PYTHON is a separate
            // declared executable check, not evidence that it is used by those v2 commands.
            executables.add(executable("SILVER_PYTHON",path(silver,"python"),paths,credentials));
            executables.add(executable("GOLD_PYTHON",path(gold,"python"),paths,credentials));
            executables.sort(Comparator.comparing(Executable::role));
            var artifacts=new ArrayList<Artifact>();long total=0;int ordinal=0;
            for(var entry:fetch.get("manifest").get("classpath")){
                Path file=path(entry,"path");for(Path root:paths)need(!file.startsWith(root),RUNTIME_MISMATCH);
                need(!credentials.contains(file),RUNTIME_MISMATCH);
                var observation=artifact("JAR",Integer.toString(ordinal++),file,text(entry,"sha256"),(int)Math.min(JAR_BYTES,CLASSPATH_BYTES-total));
                total=Math.addExact(total,observation.file().bytes());need(total<=CLASSPATH_BYTES,RESOURCE_LIMIT);artifacts.add(observation);
            }
            var modules=new TreeMap<String,String>();
            for(String family:List.of("SILVER","GOLD","CONTENT")){
                var c=registered.configuration(family).get("modules");
                for(String name:c.propertyNames()){String hash=text(c,name),old=modules.putIfAbsent(name,hash);need(old==null || old.equals(hash),RUNTIME_MISMATCH);}
            }
            for(var module:modules.entrySet()){
                Path file=processing.resolve(module.getKey());need(!credentials.contains(file),RUNTIME_MISMATCH);
                artifacts.add(artifact("MODULE",module.getKey(),file,module.getValue(),MODULE_BYTES));
            }
            guard.checkHeld(guard.binding().installationId());return new Installed(artifacts,roots,executables);
        }catch(PreflightFailure | ImportOwnershipFailure ex){throw ex;}
        catch(NoSuchFileException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        catch(IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        catch(RuntimeException ex){throw new PreflightFailure(RUNTIME_MISMATCH);}
    }
    private static Artifact artifact(String kind,String name,Path path,String hash,int maximum) throws IOException {
        FileIdentity before=fileIdentity(path);byte[] bytes=PreflightFiles.read(path,maximum,false);FileIdentity after=fileIdentity(path);
        need(before.equals(after) && before.bytes()==bytes.length,INPUT_CHANGED);need(SourceJson.sha(bytes).equals(hash),RUNTIME_MISMATCH);
        return new Artifact(kind,name,hash,after);
    }
    private static FileIdentity fileIdentity(Path path) throws IOException {
        var a=Files.readAttributes(path,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
        return new FileIdentity(InstallationHostGuard.identity(path),a.size(),a.lastModifiedTime());
    }
    private static Executable executable(String role,Path declared,List<Path> roots,Set<Path> credentials) throws IOException {
        need(!credentials.contains(declared),RUNTIME_MISMATCH);Path real=declared.toRealPath();need(!credentials.contains(real),RUNTIME_MISMATCH);
        for(Path root:roots)need(!declared.startsWith(root) && !real.startsWith(root),RUNTIME_MISMATCH);
        need(Files.isRegularFile(real,LinkOption.NOFOLLOW_LINKS) && Files.isExecutable(declared),RUNTIME_MISMATCH);
        return new Executable(role,fileIdentity(real));
    }
    private static boolean sameRoots(List<Root> a,List<Root> b){
        if(a.size()!=b.size())return false;
        for(int i=0;i<a.size();i++)if(!a.get(i).bindingId().equals(b.get(i).bindingId()) || !a.get(i).role().equals(b.get(i).role()) || !a.get(i).identity().equals(b.get(i).identity()))return false;
        return true;
    }
    private static Path directory(Path path) throws IOException {need(path.equals(path.toRealPath()) && Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS),RUNTIME_MISMATCH);return path;}
    private static void separate(Path a,Path b){need(!a.startsWith(b) && !b.startsWith(a),RUNTIME_MISMATCH);}
    private static String text(JsonNode n,String field){return SourceJson.text(n.get(field));}
    private static Path path(JsonNode n,String field){Path p=Path.of(text(n,field));need(p.isAbsolute() && p.equals(p.normalize()),RUNTIME_MISMATCH);return p;}
    private NarrowContentInstalledRuntimeQualification() { }
}
