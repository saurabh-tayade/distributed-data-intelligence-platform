package platform.contentapi.execution;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.PreflightFailure.*;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/** Monotonic private-tree reservations. Never a native-data quota or a dispatch capability. */
final class ContentEvidenceBudget {
    static final long BYTES=64L*1024*1024;
    static final int ENTRIES=4096,INVENTORY=1024*1024,LEDGER=256*1024;
    static final String MANIFEST=".content-evidence.json",LOCK=".content-evidence.lock";
    private static final String PENDING=MANIFEST+".preparing";
    private static final Map<Path,Object> LOCAL_LOCKS=new ConcurrentHashMap<>();
    private static final Set<Path> REQUIRED=ConcurrentHashMap.newKeySet();
    record Totals(long actualBytes,int actualEntries,long reservedBytes,int reservedEntries,int inventoryMaximum){}
    enum StoreBoundary { PREFIX_CREATED, PREFIX_WRITTEN, PREFIX_FORCED, RENAMED, DIRECTORY_FORCED }
    interface StoreFault { void at(StoreBoundary boundary); }
    private static final StoreFault NO_STORE_FAULT=boundary->{};

    /** Fresh narrow preparation only. The caller created this root and lock, never an adopted tree. */
    static void initializeFresh(InstallationHostGuard guard,UUID run,UUID attempt,
                                InstallationHostGuard.Identity rootIdentity,
                                InstallationHostGuard.Identity lockIdentity,StoreFault fault){
        need(guard!=null && run!=null && attempt!=null && rootIdentity!=null && lockIdentity!=null && fault!=null,INPUT_INVALID);
        guard.checkHeld(guard.binding().installationId());
        Path root=guard.binding().root().resolve("runs/"+run+"/attempts/"+attempt);
        locked(root,()->{
            need(rootIdentity.equals(InstallationHostGuard.identity(root))
                    && lockIdentity.equals(InstallationHostGuard.identity(root.resolve(LOCK))),INPUT_CHANGED);
            try(var children=Files.list(root)){
                need(children.allMatch(child->child.getFileName().toString().equals(LOCK)),INPUT_CHANGED);
            }catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
            var n=SourceJson.object().put("version",1).put("treeKind","ATTEMPT").put("runId",run.toString()).put("attemptId",attempt.toString())
                    .put("rootDevice",Long.toString(rootIdentity.device())).put("rootInode",Long.toString(rootIdentity.inode()));
            n.putArray("directories").add("");n.putObject("files");store(root,n,fault);return null;
        });
    }

    static void initialize(InstallationHostGuard guard,UUID run,UUID attempt){
        initialize(guard,run,attempt,guard.binding().root().resolve("runs/"+run+"/attempts/"+attempt),"ATTEMPT");
    }
    static void initializePackets(InstallationHostGuard guard,UUID run,UUID control){
        initialize(guard,run,control,guard.binding().root().resolve("recovery/"+control),"PACKETS");
    }
    private static void initialize(InstallationHostGuard guard,UUID run,UUID attempt,Path root,String kind){
        guard.checkHeld(guard.binding().installationId());
        REQUIRED.add(root);
        try{
            Path p=guard.binding().root();
            for(Path segment:guard.binding().root().relativize(root)){
                p=p.resolve(segment);try{Files.createDirectory(p,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));PreparedCommand.forceDirectory(p.getParent());}catch(FileAlreadyExistsException ignored){}
                PreflightFiles.privateEntry(p,true);
            }
            synchronized(LOCAL_LOCKS.computeIfAbsent(root,k->new Object())){
                if(!Files.exists(root.resolve(LOCK),LinkOption.NOFOLLOW_LINKS)){
                    try(var children=Files.list(root)){need(children.findAny().isEmpty(),INPUT_CHANGED);}
                    try(var channel=FileChannel.open(root.resolve(LOCK),Set.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))){channel.force(true);}
                    PreparedCommand.forceDirectory(root);
                }
                locked(root,()->{
                    if(Files.exists(root.resolve(MANIFEST),LinkOption.NOFOLLOW_LINKS)){var current=read(root);need(current.get("runId").stringValue().equals(run.toString()) && current.get("attemptId").stringValue().equals(attempt.toString()) && current.get("treeKind").stringValue().equals(kind),INPUT_CHANGED);validate(root,current);return null;}
                    // A partial ledger is retained and refused, never adopted or overwritten.
                    need(!Files.exists(root.resolve(PENDING),LinkOption.NOFOLLOW_LINKS),INPUT_CHANGED);
                    try(var children=Files.list(root)){need(children.allMatch(child->child.getFileName().toString().equals(LOCK)),INPUT_CHANGED);}catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
                    var id=InstallationHostGuard.identity(root);
                    var n=SourceJson.object().put("version",1).put("treeKind",kind).put("runId",run.toString()).put("attemptId",attempt.toString())
                            .put("rootDevice",Long.toString(id.device())).put("rootInode",Long.toString(id.inode()));
                    n.putArray("directories").add("");n.putObject("files");store(root,n);return null;
                });
            }
        }catch(PreflightFailure ex){throw ex;}catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
    static Path root(Path directory){
        for(Path p=directory;p!=null;p=p.getParent()){
            if(REQUIRED.contains(p) || Files.exists(p.resolve(LOCK),LinkOption.NOFOLLOW_LINKS) || Files.exists(p.resolve(MANIFEST),LinkOption.NOFOLLOW_LINKS))return p;
        }
        return null;
    }
    static boolean enabled(Path directory){return root(directory)!=null;}
    static Map<String,Integer> slots(String stage){
        int s=65536,q=512*1024,l=1024*1024;var slots=new LinkedHashMap<String,Integer>();
        slots.put("command.json",Set.of("STORE_BRONZE","INSPECT_BRONZE","SILVER","VERIFY_SILVER","RECHECK_SILVER").contains(stage)?s:l);
        slots.put("result.json",s);slots.put("verified.json",s);
        if(!Set.of("STORE_BRONZE","INSPECT_BRONZE").contains(stage))slots.put("child.json",s);
        if(!stage.equals("SILVER"))slots.put("evidence.json",Set.of("STORE_BRONZE","INSPECT_BRONZE").contains(stage)?s:Set.of("VERIFY_SILVER","RECHECK_SILVER").contains(stage)?q:l);
        if(Set.of("PUBLISH_GOLD","VERIFY_RESULT","VERIFY_SILVER").contains(stage))slots.put("selection.json",stage.equals("VERIFY_SILVER")?q:l);
        return Map.copyOf(slots);
    }
    static void reserve(Path directory,Map<String,Integer> slots){
        Path root=root(directory);if(root==null)return;
        locked(root,()->{
            var n=read(root);validate(root,n);var next=(ObjectNode)n.deepCopy();
            var directories=new TreeSet<String>();for(var d:n.get("directories"))directories.add(d.stringValue());
            String rel=relative(root,directory);Path p=Path.of(rel);
            while(!p.toString().isEmpty()){directories.add(p.toString());p=p.getParent();if(p==null)break;}
            var files=(ObjectNode)next.get("files");boolean changed=directories.size()!=n.get("directories").size();
            for(var slot:slots.entrySet()){
                need(slot.getKey().matches("[a-z][a-z0-9.-]{0,63}") && slot.getValue()>0 && slot.getValue()<=PreflightFiles.LARGE,INPUT_INVALID);
                for(String name:List.of(slot.getKey(),slot.getKey()+".preparing")){
                    String address=rel.isEmpty()?name:rel+"/"+name;
                    if(files.has(address))need(files.get(address).intValue()==slot.getValue(),INPUT_CHANGED);
                    else{files.put(address,slot.getValue());changed=true;}
                }
            }
            var ds=next.putArray("directories");directories.forEach(ds::add);
            // A version-2 plan is complete before effects: existing exact slots may be
            // consumed, but even an empty unplanned directory cannot extend its inventory.
            need(n.get("version").intValue()!=2 || !changed,RESOURCE_LIMIT);
            limits(next);if(changed)store(root,next);return null;
        });
    }
    /** Atomically install one complete fixed plan. Never retrofit a populated legacy tree. */
    static void reservePlan(ContentOperationPlan plan){
        reservePlan(plan,NO_STORE_FAULT);
    }
    static void reservePlan(ContentOperationPlan plan,StoreFault fault){
        need(plan!=null && fault!=null,INPUT_INVALID);
        Path directory=plan.root(),root=root(directory);need(root!=null && root.equals(directory),INPUT_INVALID);
        locked(root,()->{
            var n=read(root);validate(root,n);
            need(n.get("treeKind").stringValue().equals("ATTEMPT") && (n.get("version").intValue()==2 || n.get("files").isEmpty() && n.get("directories").size()==1 && n.get("directories").get(0).stringValue().isEmpty()),INPUT_CHANGED);
            var next=(ObjectNode)n.deepCopy();plan.apply(next);var total=limits(next);
            need(total.inventoryMaximum()<=128*1024,RESOURCE_LIMIT);
            if(n.get("version").intValue()==2)need(Arrays.equals(SourceJson.canonical(n),SourceJson.canonical(next)),INPUT_CHANGED);
            else store(root,next,fault);
            return null;
        });
    }
    static void write(Path directory,Path target,Path temporary,byte[] bytes,Runnable action){
        Path root=root(directory);if(root==null){action.run();return;}
        locked(root,()->{
            var n=read(root);validate(root,n);
            for(Path path:List.of(target,temporary)){
                String rel=relative(root,path);need(n.get("files").has(rel) && bytes.length<=n.get("files").get(rel).intValue(),RESOURCE_LIMIT);
            }
            action.run();validate(root,n);return null;
        });
    }
    static Totals totals(Path directory){Path root=root(directory);need(root!=null,INPUT_INVALID);return locked(root,()->validate(root,read(root)));}
    static Totals inspect(Path directory){Path root=root(directory);need(root!=null,INPUT_INVALID);return locked(root,()->validate(root,read(root,false)));}
    /** Explicit narrow read: never discover a root, initialize state or fall back to legacy. */
    static Totals inspect(NarrowContentEvidenceContext context,InstallationHostGuard guard,
                          NarrowContentEvidenceContext.FrozenIdentities independentlySupplied){
        return inspect(context,guard,independentlySupplied,()->{});
    }
    /** Restricted native FETCH scope for launch or read-only candidate verification. A result
     * prefix is an active/uncertain write, never READY, success or new-invocation permission.
     * Candidate verification additionally requires the exact final five-file inventory. */
    static Totals inspectFetch(NarrowContentEvidenceContext context,InstallationHostGuard guard,Path directory,boolean released){
        need(directory!=null && directory.getParent().getParent().getParent().equals(context.root()),INPUT_INVALID);
        return inspect(context,guard,context.frozen(),()->{
            Set<String> names=released?Set.of("command.json","result.json","result.json.preparing","payload.xml","payload.xml.preparing",
                    "metadata.json","metadata.json.preparing","descriptor.json","descriptor.json.preparing"):Set.of("command.json");
            try(var walk=Files.walk(context.root(),4)){
                for(Path p:walk.limit(ENTRIES+1).toList()){
                    if(Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS))need(Set.of(context.root(),directory.getParent().getParent(),directory.getParent(),directory).contains(p),INPUT_CHANGED);
                    else if(p.getParent().equals(context.root()))need(Set.of(MANIFEST,LOCK).contains(p.getFileName().toString()),INPUT_CHANGED);
                    else need(p.getParent().equals(directory) && names.contains(p.getFileName().toString()),INPUT_CHANGED);
                }
            }catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        });
    }
    /** Only completed FETCH and the single original STORE_BRONZE/0 slot. Never a recovery reader. */
    static Totals inspectBronzeLaunch(NarrowContentEvidenceContext context,InstallationHostGuard guard,Path directory,boolean released){
        need(context!=null && directory!=null && directory.equals(context.originalDirectory("STORE_BRONZE",0)),INPUT_INVALID);
        Path fetch=context.originalDirectory("FETCH_SOURCE",0);
        return inspect(context,guard,context.frozen(),()->{
            Set<Path> directories=Set.of(context.root(),fetch.getParent().getParent(),fetch.getParent(),fetch,directory.getParent(),directory);
            var fetchNames=new HashSet<>(RssJournalFiles.CANDIDATE);fetchNames.add("complete.json");
            Set<String> bronzeNames=released?Set.of("command.json","evidence.json","evidence.json.preparing","result.json","result.json.preparing"):Set.of("command.json");
            try(var walk=Files.walk(context.root(),4)){
                for(Path p:walk.limit(ENTRIES+1).toList()){
                    if(Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS))need(directories.contains(p),INPUT_CHANGED);
                    else if(p.getParent().equals(context.root()))need(Set.of(MANIFEST,LOCK).contains(p.getFileName().toString()),INPUT_CHANGED);
                    else if(p.getParent().equals(fetch))need(fetchNames.contains(p.getFileName().toString()),INPUT_CHANGED);
                    else need(p.getParent().equals(directory) && bronzeNames.contains(p.getFileName().toString()),INPUT_CHANGED);
                }
            }catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        });
    }
    /** Dedicated checkpoint scope: completed FETCH, exact candidate slots and the existing parent anchor. Earlier readers stay unchanged. */
    static Totals inspectBronzeCompletion(NarrowContentEvidenceContext context,InstallationHostGuard guard,Path directory){
        return inspect(context,guard,context.frozen(),bronzeCompletionScope(context,directory));
    }
    /** The caller already holds the original ledger write lock; never reacquire it. */
    static Totals inspectBronzeCompletionSnapshot(NarrowContentEvidenceContext context,InstallationHostGuard guard,Path directory){
        try{return inspectSnapshot(context,guard,context.frozen(),bronzeCompletionScope(context,directory));}
        catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
    private static Runnable bronzeCompletionScope(NarrowContentEvidenceContext context,Path directory){
        need(context!=null && directory!=null && directory.equals(context.originalDirectory("STORE_BRONZE",0)),INPUT_INVALID);
        Path fetch=context.originalDirectory("FETCH_SOURCE",0);
        return ()->{
            Set<Path> directories=Set.of(context.root(),fetch.getParent().getParent(),fetch.getParent(),fetch,directory.getParent(),directory);
            var fetchNames=new HashSet<>(RssJournalFiles.CANDIDATE);fetchNames.add("complete.json");
            Set<String> bronzeNames=Set.of("command.json","evidence.json","result.json","verified.json","verified.json.preparing");
            try(var walk=Files.walk(context.root(),4)){
                for(Path p:walk.limit(ENTRIES+1).toList()){
                    if(Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS))need(directories.contains(p),INPUT_CHANGED);
                    else if(p.getParent().equals(context.root()))need(Set.of(MANIFEST,LOCK).contains(p.getFileName().toString()),INPUT_CHANGED);
                    else if(p.getParent().equals(fetch))need(fetchNames.contains(p.getFileName().toString()),INPUT_CHANGED);
                    else need(p.getParent().equals(directory) && bronzeNames.contains(p.getFileName().toString()),INPUT_CHANGED);
                }
            }catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        };
    }
    /** Completed FETCH scope only; it does not relax launch or candidate inspection. */
    static Totals inspectFetchCompletion(NarrowContentEvidenceContext context,InstallationHostGuard guard,Path directory){
        return inspect(context,guard,context.frozen(),fetchCompletionScope(context,directory));
    }
    /** Optimistic read-only observation: no lock wait or channel that could disturb another writer's lock. */
    static Totals inspectFetchRecovery(NarrowContentEvidenceContext context,InstallationHostGuard guard,Path directory){
        try{return inspectSnapshot(context,guard,context.frozen(),fetchCompletionScope(context,directory));}
        catch(PreflightFailure ex){throw ex;}
        catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        catch(RuntimeException ex){throw new PreflightFailure(INPUT_CHANGED);}
    }
    private static Runnable fetchCompletionScope(NarrowContentEvidenceContext context,Path directory){
        need(directory!=null && directory.getParent().getParent().getParent().equals(context.root()),INPUT_INVALID);
        return ()->{
            var names=new HashSet<>(RssJournalFiles.CANDIDATE);names.add("complete.json");
            try(var walk=Files.walk(context.root(),4)){
                for(Path p:walk.limit(ENTRIES+1).toList()){
                    if(Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS))need(Set.of(context.root(),directory.getParent().getParent(),directory.getParent(),directory).contains(p),INPUT_CHANGED);
                    else if(p.getParent().equals(context.root()))need(Set.of(MANIFEST,LOCK).contains(p.getFileName().toString()),INPUT_CHANGED);
                    else need(p.getParent().equals(directory) && names.contains(p.getFileName().toString()),INPUT_CHANGED);
                }
            }catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        };
    }
    private static Totals inspect(NarrowContentEvidenceContext context,InstallationHostGuard guard,
                                  NarrowContentEvidenceContext.FrozenIdentities independentlySupplied,Runnable scope){
        need(context!=null,INPUT_INVALID);Path root=context.root();
        synchronized(LOCAL_LOCKS.computeIfAbsent(root,k->new Object())){
            try{
                context.check(guard,independentlySupplied);
                try(var channel=FileChannel.open(root.resolve(LOCK),StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS)){
                    context.check(guard,independentlySupplied);
                    try(var lock=channel.lock()){
                        // Recheck pathname identities after waiting; the lock is not an identity seal.
                        return inspectSnapshot(context,guard,independentlySupplied,scope);
                    }
                }
            }catch(PreflightFailure ex){throw ex;}
            catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
            catch(RuntimeException ex){throw new PreflightFailure(INPUT_CHANGED);}
        }
    }
    private static Totals inspectSnapshot(NarrowContentEvidenceContext context,InstallationHostGuard guard,
                                          NarrowContentEvidenceContext.FrozenIdentities independentlySupplied,Runnable scope)throws java.io.IOException{
        context.check(guard,independentlySupplied);Path root=context.root();
        need(!Files.exists(root.resolve(PENDING),LinkOption.NOFOLLOW_LINKS),INPUT_CHANGED);
        PreflightFiles.privateEntry(root.resolve(MANIFEST),false);
        var identity=InstallationHostGuard.identity(root.resolve(MANIFEST));
        var modified=Files.getLastModifiedTime(root.resolve(MANIFEST),LinkOption.NOFOLLOW_LINKS);
        byte[] bytes=PreflightFiles.read(root.resolve(MANIFEST),LEDGER,true);
        var value=parse(root,bytes);context.ledger(bytes,value);
        var result=validate(root,value);scope.run();
        byte[] after=PreflightFiles.read(root.resolve(MANIFEST),LEDGER,true);
        need(Arrays.equals(bytes,after) && identity.equals(InstallationHostGuard.identity(root.resolve(MANIFEST)))
                && modified.equals(Files.getLastModifiedTime(root.resolve(MANIFEST),LinkOption.NOFOLLOW_LINKS))
                && !Files.exists(root.resolve(PENDING),LinkOption.NOFOLLOW_LINKS),INPUT_CHANGED);
        context.check(guard,independentlySupplied);
        return result;
    }
    static void require(InstallationHostGuard guard,UUID run,UUID attempt){
        Path root=guard.binding().root().resolve("runs/"+run+"/attempts/"+attempt);REQUIRED.add(root);inspect(root);
    }
    private static String relative(Path root,Path path){
        need(path.isAbsolute() && path.normalize().equals(path) && path.startsWith(root),INPUT_INVALID);
        String rel=root.relativize(path).toString();need(rel.isEmpty() || Path.of(rel).getNameCount()<=4,RESOURCE_LIMIT);return rel;
    }
    private static ObjectNode read(Path root){
        return read(root,true);
    }
    private static ObjectNode read(Path root,boolean writing){
        if(writing)need(!Files.exists(root.resolve(PENDING),LinkOption.NOFOLLOW_LINKS),INPUT_CHANGED);
        return parse(root,PreflightFiles.read(root.resolve(MANIFEST),LEDGER,true));
    }
    private static ObjectNode parse(Path root,byte[] bytes){
        var n=PreflightFiles.canonical(bytes,LEDGER);
        need(n.get("version")!=null && n.get("version").isIntegralNumber() && n.get("version").canConvertToInt() && Set.of(1,2).contains(n.get("version").intValue()),INPUT_INVALID);
        if(n.get("version").intValue()==2){
            SourceJson.fields(n,"version","treeKind","runId","attemptId","rootDevice","rootInode","directories","files","reservationPolicy","planKind","sourceId");
            need(n.get("reservationPolicy").isString() && n.get("reservationPolicy").stringValue().equals(NarrowContentProfile.POLICY),INPUT_INVALID);
            ContentOperationPlan.validate(root,n);
        }else SourceJson.fields(n,"version","treeKind","runId","attemptId","rootDevice","rootInode","directories","files");
        need(n.get("directories").isArray() && n.get("files").isObject(),INPUT_INVALID);
        var id=InstallationHostGuard.identity(root);
        need(n.get("rootDevice").stringValue().equals(Long.toString(id.device())) && n.get("rootInode").stringValue().equals(Long.toString(id.inode()))
                && root.endsWith(prefix(n)),INPUT_CHANGED);
        limits(n);return (ObjectNode)n;
    }
    private static Totals limits(JsonNode n){
        long bytes=2L*LEDGER;int entries=n.get("directories").size()+n.get("files").size()+3;
        for(var d:n.get("directories")){
            need(d.isString(),INPUT_INVALID);String path=d.stringValue();
            need(path.isEmpty() || !path.startsWith("/") && !path.startsWith("..") && Path.of(path).normalize().toString().equals(path) && Path.of(path).getNameCount()<4,RESOURCE_LIMIT);
        }
        for(String name:n.get("files").propertyNames()){
            var maximum=n.get("files").get(name);need(maximum.isIntegralNumber() && maximum.canConvertToInt() && maximum.intValue()>0 && maximum.intValue()<=PreflightFiles.LARGE,INPUT_INVALID);
            need(!name.startsWith("/") && Path.of(name).normalize().toString().equals(name) && !name.startsWith("..") && Path.of(name).getNameCount()<=4,INPUT_INVALID);
            bytes+=maximum.intValue();
        }
        var inventory=SourceJson.object().put("processEvidenceIncomplete",true);var list=inventory.putArray("files");String prefix=prefix(n);
        for(var d:n.get("directories"))inventoryEntry(list.addObject(),prefix+(d.stringValue().isEmpty()?"":"/"+d.stringValue()),true);
        for(String f:n.get("files").propertyNames())inventoryEntry(list.addObject(),prefix+"/"+f,false);
        for(String f:List.of(MANIFEST,PENDING,LOCK))inventoryEntry(list.addObject(),prefix+"/"+f,false);
        int maximum=SourceJson.canonical(inventory).length;
        need(bytes<=BYTES && entries<=ENTRIES && maximum<=INVENTORY && SourceJson.canonical(n).length<=LEDGER,RESOURCE_LIMIT);
        if(n.get("version").intValue()==2)need(maximum<=128*1024,RESOURCE_LIMIT);
        return new Totals(0,0,bytes,entries,maximum);
    }
    private static void inventoryEntry(ObjectNode f,String address,boolean directory){
        f.put("address",address).put("device","9".repeat(20)).put("inode","9".repeat(20)).put("modifiedAt","x".repeat(64)).put("directory",directory);
        if(!directory)f.put("length","9".repeat(20)).put("sha256","f".repeat(64));
    }
    private static String prefix(JsonNode n){
        String run=UUID.fromString(n.get("runId").stringValue()).toString(),attempt=UUID.fromString(n.get("attemptId").stringValue()).toString();
        need(Set.of("ATTEMPT","PACKETS").contains(n.get("treeKind").stringValue()),INPUT_INVALID);
        return n.get("treeKind").stringValue().equals("ATTEMPT")?"runs/"+run+"/attempts/"+attempt:"recovery/"+attempt;
    }
    private static Totals validate(Path root,JsonNode n){
        var reserved=limits(n);Set<String> directories=new HashSet<>();for(var d:n.get("directories"))need(d.isString() && directories.add(d.stringValue()),INPUT_INVALID);
        long bytes=0;int count=0;
        try(var walk=Files.walk(root,4)){
            for(Path p:walk.limit(ENTRIES+1).toList()){
                count++;need(count<=ENTRIES,RESOURCE_LIMIT);var a=Files.readAttributes(p,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);PreflightFiles.privateEntry(p,a.isDirectory());
                String rel=relative(root,p);need(a.lastModifiedTime().toString().length()<=64,RESOURCE_LIMIT);
                if(a.isDirectory())need(directories.contains(rel) && (rel.isEmpty() || Path.of(rel).getNameCount()<4),INPUT_CHANGED);
                else{
                    int cap=rel.equals(LOCK)?0:List.of(MANIFEST,PENDING).contains(rel)?LEDGER:n.get("files").has(rel)?n.get("files").get(rel).intValue():-1;
                    need(cap>=0 && a.size()<=cap,INPUT_CHANGED);bytes+=a.size();need(bytes<=BYTES,RESOURCE_LIMIT);
                }
            }
        }catch(PreflightFailure ex){throw ex;}catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        return new Totals(bytes,count,reserved.reservedBytes(),reserved.reservedEntries(),reserved.inventoryMaximum());
    }
    private static void store(Path root,JsonNode n){
        store(root,n,NO_STORE_FAULT);
    }
    private static void store(Path root,JsonNode n,StoreFault fault){
        byte[] bytes=SourceJson.canonical(n);need(bytes.length<=LEDGER,RESOURCE_LIMIT);
        try{
            try(var channel=FileChannel.open(root.resolve(PENDING),Set.of(StandardOpenOption.WRITE,StandardOpenOption.CREATE_NEW,LinkOption.NOFOLLOW_LINKS),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))){
                fault.at(StoreBoundary.PREFIX_CREATED);
                var b=ByteBuffer.wrap(bytes);while(b.hasRemaining())channel.write(b);fault.at(StoreBoundary.PREFIX_WRITTEN);
                channel.force(true);fault.at(StoreBoundary.PREFIX_FORCED);
            }
            need(Arrays.equals(bytes,PreflightFiles.read(root.resolve(PENDING),LEDGER,true)),INPUT_CHANGED);
            Files.move(root.resolve(PENDING),root.resolve(MANIFEST),StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
            fault.at(StoreBoundary.RENAMED);PreparedCommand.forceDirectory(root);fault.at(StoreBoundary.DIRECTORY_FORCED);
        }catch(FileAlreadyExistsException ex){throw new PreflightFailure(INPUT_CHANGED);}
        catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
    private static <T>T locked(Path root,Supplier<T> action){
        synchronized(LOCAL_LOCKS.computeIfAbsent(root,k->new Object())){
            PreflightFiles.privateEntry(root,true);PreflightFiles.privateEntry(root.resolve(LOCK),false);
            try(var channel=FileChannel.open(root.resolve(LOCK),StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);var lock=channel.lock()){
                return action.get();
            }catch(PreflightFailure ex){throw ex;}catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}catch(RuntimeException ex){throw new PreflightFailure(INPUT_CHANGED);}
        }
    }
    private ContentEvidenceBudget(){}
}
