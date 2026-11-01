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
            limits(next);if(changed)store(root,next);return null;
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
        var n=PreflightFiles.canonical(PreflightFiles.read(root.resolve(MANIFEST),LEDGER,true),LEDGER);
        SourceJson.fields(n,"version","treeKind","runId","attemptId","rootDevice","rootInode","directories","files");
        need(n.get("version").intValue()==1 && n.get("directories").isArray() && n.get("files").isObject(),INPUT_INVALID);
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
        byte[] bytes=SourceJson.canonical(n);need(bytes.length<=LEDGER,RESOURCE_LIMIT);
        try{
            try(var channel=FileChannel.open(root.resolve(PENDING),Set.of(StandardOpenOption.WRITE,StandardOpenOption.CREATE_NEW,LinkOption.NOFOLLOW_LINKS),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))){
                var b=ByteBuffer.wrap(bytes);while(b.hasRemaining())channel.write(b);channel.force(true);
            }
            need(Arrays.equals(bytes,PreflightFiles.read(root.resolve(PENDING),LEDGER,true)),INPUT_CHANGED);
            Files.move(root.resolve(PENDING),root.resolve(MANIFEST),StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);PreparedCommand.forceDirectory(root);
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
