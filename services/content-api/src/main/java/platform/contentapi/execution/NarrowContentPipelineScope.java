package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.NarrowContentOperationRouting.Operation;

/** Ordered post-anchor scope. Completed bytes stay pinned while only authorized ancestors may grow. */
final class NarrowContentPipelineScope {
    private record Pin(InstallationHostGuard.Identity identity,FileTime modified,long size,String sha256) {}
    record Observation(byte[] bytes,Pin pin){
        Observation{bytes=bytes.clone();}
        @Override public byte[] bytes(){return bytes.clone();}
    }
    private final NarrowContentEvidenceContext context;
    private final InstallationHostGuard guard;
    private final Map<Path,Pin> files=new LinkedHashMap<>();
    private final Map<Path,InstallationHostGuard.Identity> directories=new LinkedHashMap<>();
    private Path active;
    private Operation operation;
    NarrowContentPipelineScope(NarrowContentEvidenceContext.Expected ready,InstallationHostGuard guard){
        this.context=new NarrowContentEvidenceContext(ready);this.guard=guard;
        // The original live Bronze callback must perform its retained-pins check immediately before this transfer.
        ContentEvidenceBudget.inspectBronzeCompletion(context,guard,context.originalDirectory("STORE_BRONZE",0));
        try(var walk=Files.walk(context.root(),4)){
            for(Path p:walk.toList())if(Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS))directories.put(p,InstallationHostGuard.identity(p));else files.put(p,pin(p));
        }catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        inspect(false);
    }
    Path start(Operation op){
        need(active==null && op.ordinal()>=2 && op.ordinal()<=16,INPUT_INVALID);
        Path path=context.originalDirectory(op.ledgerStage,op.ledgerOrdinal);need(Files.notExists(path,LinkOption.NOFOLLOW_LINKS),INPUT_CHANGED);
        operation=op;active=path;inspect(false);return path;
    }
    void directoryReady(){
        need(active!=null,INPUT_INVALID);
        for(Path p=active;p.startsWith(context.root());p=p.getParent()){
            PreflightFiles.privateEntry(p,true);var identity=InstallationHostGuard.identity(p);
            var prior=directories.putIfAbsent(p,identity);need(prior==null || prior.equals(identity),INPUT_CHANGED);
        }
        inspect(false);
    }
    void inspect(boolean released){
        context.inspect(guard,context.frozen());
        for(var item:directories.entrySet()){PreflightFiles.privateEntry(item.getKey(),true);need(item.getValue().equals(InstallationHostGuard.identity(item.getKey())),INPUT_CHANGED);}
        for(var item:files.entrySet())need(item.getValue().equals(pin(item.getKey())),INPUT_CHANGED);
        var allowedDirectories=new HashSet<>(directories.keySet());
        if(active!=null)for(Path p=active;p.startsWith(context.root());p=p.getParent())allowedDirectories.add(p);
        try(var walk=Files.walk(context.root(),4)){
            var all=walk.limit(222).toList();need(all.size()<=221,RESOURCE_LIMIT);
            for(Path p:all){
                if(Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS)){need(allowedDirectories.contains(p),INPUT_CHANGED);PreflightFiles.privateEntry(p,true);}
                else if(!files.containsKey(p)){
                    need(active!=null && p.getParent().equals(active),INPUT_CHANGED);String name=p.getFileName().toString();
                    var slots=ContentEvidenceBudget.slots(operation.ledgerStage);
                    if(!released)need(name.equals("command.json"),INPUT_CHANGED);
                    else need(slots.containsKey(name) || name.endsWith(".preparing") && slots.containsKey(name.substring(0,name.length()-10)),INPUT_CHANGED);
                }
            }
        }catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
    String complete(Map<String,Observation> expected){
        need(active!=null && expected.containsKey("command.json") && expected.containsKey("result.json"),INPUT_INVALID);inspect(true);
        try(var list=Files.list(active)){need(list.map(p->p.getFileName().toString()).collect(java.util.stream.Collectors.toSet()).equals(expected.keySet()),INPUT_CHANGED);}
        catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        var inventory=SourceJson.object();
        for(String name:new TreeSet<>(expected.keySet())){
            Path p=active.resolve(name);Pin pin=pin(p);need(pin.equals(expected.get(name).pin()),INPUT_CHANGED);files.put(p,pin);
            inventory.putObject(name).put("sha256",pin.sha256()).put("bytes",pin.size()).put("device",Long.toString(pin.identity().device()))
                    .put("inode",Long.toString(pin.identity().inode())).put("modified",pin.modified().toString());
        }
        for(Path p=active;p.startsWith(context.root());p=p.getParent())directories.putIfAbsent(p,InstallationHostGuard.identity(p));
        active=null;operation=null;inspect(false);return SourceJson.sha(SourceJson.canonical(inventory));
    }
    private static Pin pin(Path p){return observe(p,p.getFileName().toString().equals("payload.xml")?5*1024*1024:1024*1024).pin();}
    static Observation observe(Path p,int maximum){
        try{
            PreflightFiles.privateEntry(p,false);var id=InstallationHostGuard.identity(p);var modified=Files.getLastModifiedTime(p,LinkOption.NOFOLLOW_LINKS);
            byte[] raw=PreflightFiles.read(p,maximum,true);
            need(id.equals(InstallationHostGuard.identity(p)) && modified.equals(Files.getLastModifiedTime(p,LinkOption.NOFOLLOW_LINKS)) && Files.size(p)==raw.length,INPUT_CHANGED);
            return new Observation(raw,new Pin(id,modified,raw.length,SourceJson.sha(raw)));
        }catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
}
