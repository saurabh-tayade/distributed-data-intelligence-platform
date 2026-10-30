package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.RecoveryFailure.*;
import static platform.contentapi.execution.RecoveryFailure.Code.*;

/** Bounded capture of the exact original private attempt, including launch-gap child sidecars. */
final class ContentRecoveryFiles {
    static RecoveryFiles.Capture capture(InstallationHostGuard guard,UUID run,UUID attempt,List<Map<String,Object>> stages){
        var n=SourceJson.object();var files=n.putArray("files");var hints=new HashSet<Long>();
        Path root=guard.binding().root().resolve("runs/"+run+"/attempts/"+attempt);
        try{
            // An intent may precede creation of its private directory.
            try{Files.readAttributes(root,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);}catch(NoSuchFileException ex){need(stages.stream().noneMatch(s->s.get("command_address")!=null || s.get("result_address")!=null),EVIDENCE_UNAVAILABLE);return new RecoveryFiles.Capture(n,Set.of());}
            try(var walk=Files.walk(root,4)){
                var paths=walk.limit(4097).sorted().toList();need(paths.size()<=4096,EVIDENCE_UNAVAILABLE);
                long total=0;
                for(Path p:paths){var a=Files.readAttributes(p,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);PreflightFiles.privateEntry(p,a.isDirectory());var id=InstallationHostGuard.identity(p);
                    var f=files.addObject().put("address",guard.binding().root().relativize(p).toString()).put("device",Long.toString(id.device())).put("inode",Long.toString(id.inode())).put("modifiedAt",a.lastModifiedTime().toString()).put("directory",a.isDirectory());
                    if(a.isDirectory())continue;
                    total+=a.size();need(total<=64L*1024*1024,EVIDENCE_UNAVAILABLE);
                    byte[] raw=PreflightFiles.read(p,p.getFileName().toString().equals("payload.xml")?RssJournalFiles.PAYLOAD:PreflightFiles.LARGE,true);
                    var after=Files.readAttributes(p,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);need(a.fileKey().equals(after.fileKey()) && a.size()==after.size() && a.lastModifiedTime().equals(after.lastModifiedTime()),EVIDENCE_CHANGED);
                    f.put("length",Long.toString(raw.length)).put("sha256",SourceJson.sha(raw));
                    if(Set.of("child.json","evidence.json","result.json").contains(p.getFileName().toString())){
                        try{var e=SourceJson.parse(raw,PreflightFiles.LARGE);if(p.getFileName().toString().equals("child.json"))hints.add(Long.parseLong(e.get("pid").stringValue()));else if(e.has("cleanup") && e.get("cleanup").has("pid"))hints.add(Long.parseLong(e.get("cleanup").get("pid").stringValue()));}catch(RuntimeException ex){need(p.getFileName().toString().equals("evidence.json"),EVIDENCE_CHANGED);}
                    }
                }
            }
            for(var s:stages)for(String kind:List.of("command","result"))if(s.get(kind+"_address")!=null){String address=(String)s.get(kind+"_address");need(address.startsWith("runs/"+run+"/attempts/"+attempt+"/"),EVIDENCE_CHANGED);byte[] raw=RecoveryFiles.readPrivate(guard,address,PreflightFiles.LARGE);need(SourceJson.sha(raw).equals(s.get(kind+"_sha256")),EVIDENCE_CHANGED);}
            need(SourceJson.canonical(n).length<=RecoveryFiles.MAXIMUM,EVIDENCE_UNAVAILABLE);return new RecoveryFiles.Capture(n,Set.copyOf(hints));
        }catch(RecoveryFailure ex){throw ex;}catch(PreflightFailure ex){throw new RecoveryFailure(ex.code()==PreflightFailure.Code.INPUT_UNAVAILABLE?EVIDENCE_UNAVAILABLE:EVIDENCE_CHANGED);}catch(java.io.IOException ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}
    }
    private ContentRecoveryFiles(){}
}
