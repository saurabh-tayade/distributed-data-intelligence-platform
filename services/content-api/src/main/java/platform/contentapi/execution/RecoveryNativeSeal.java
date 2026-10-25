package platform.contentapi.execution;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** Fresh readable byte/identity seal, never an analytical verifier or native-format replacement. */
final class RecoveryNativeSeal {
    private static final long MAX_BYTES=8L*1024*1024*1024;
    private final Map<String,JsonNode> entries=new TreeMap<>();
    private long bytes;private final long deadline=System.nanoTime()+Duration.ofSeconds(60).toNanos();
    private Path publication;
    static RecoveryNativeSeal capture(ReliabilityRuntime runtime,JsonNode pack,String fingerprint) {
        var result=new RecoveryNativeSeal();runtime.admission();var bindings=runtime.bindings();
        for(var input:pack.get("bundle").get("capturedSelection").get("inputs")) {
            String role=input.get("role").stringValue().equals("RELIABILITY_BATCH")?"batch":"reference";
            result.target(Path.of(bindings.get(role).get("root").stringValue()),input.get("relativeAddress").stringValue());
        }
        Path output=Path.of(bindings.get("output").get("root").stringValue());
        result.publication=output.resolve("v2/processor_revision=2/publication="+fingerprint);
        result.target(output,output.relativize(result.publication).toString());runtime.admission();return result;
    }
    private void target(Path root,String address) {
        Path relative=Path.of(address);need(!relative.isAbsolute() && relative.normalize().equals(relative) && !address.contains("..") && !address.contains("\\"),EVIDENCE_CHANGED);
        try {
            Path p=root;parent(p);
            for(Path segment:relative){p=p.resolve(segment);if(!p.equals(root.resolve(relative)))parent(p);}
            tree(p,0);
        }catch(IOException | SecurityException ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}
    }
    private void parent(Path p)throws IOException {
        try {var a=attributes(p);need(a.isDirectory() && p.equals(p.toRealPath()),EVIDENCE_CHANGED);var id=InstallationHostGuard.identity(p);put(p,SourceJson.object().put("device",Long.toString(id.device())).put("inode",Long.toString(id.inode())).put("mode",Files.getAttribute(p,"unix:mode",LinkOption.NOFOLLOW_LINKS).toString()));}
        catch(NoSuchFileException ex){put(p,SourceJson.object().put("absent",true));}
    }
    private void tree(Path p,int depth)throws IOException {
        bounded();need(depth<=8,EVIDENCE_UNAVAILABLE);BasicFileAttributes before;
        try{before=attributes(p);}catch(NoSuchFileException ex){put(p,SourceJson.object().put("absent",true));return;}
        need(!before.isSymbolicLink() && (before.isDirectory() || before.isRegularFile()) && p.equals(p.toRealPath()),EVIDENCE_CHANGED);
        var node=stamp(p,before);var initial=node.deepCopy();
        if(before.isDirectory()) {
            List<String> names=names(p);var listed=node.putArray("names");names.forEach(listed::add);put(p,node);
            for(String name:names)tree(p.resolve(name),depth+1);
            need(names.equals(names(p)),EVIDENCE_CHANGED);
        }else {
            need(((Number)Files.getAttribute(p,"unix:nlink",LinkOption.NOFOLLOW_LINKS)).longValue()==1,EVIDENCE_CHANGED);
            bytes=Math.addExact(bytes,before.size());need(bytes<=MAX_BYTES,EVIDENCE_UNAVAILABLE);
            try {
                var digest=MessageDigest.getInstance("SHA-256");long count=0;
                try(var input=Files.newInputStream(p,LinkOption.NOFOLLOW_LINKS)){byte[] buffer=new byte[65536];int n;while((n=input.read(buffer))!=-1){bounded();count+=n;need(count<=before.size(),EVIDENCE_CHANGED);digest.update(buffer,0,n);}}
                need(count==before.size(),EVIDENCE_CHANGED);node.put("sha256",HexFormat.of().formatHex(digest.digest()));put(p,node);
            }catch(java.security.NoSuchAlgorithmException ex){throw new IllegalStateException();}
        }
        need(stamp(p,attributes(p)).equals(initial),EVIDENCE_CHANGED);
    }
    private BasicFileAttributes attributes(Path p)throws IOException{return Files.readAttributes(p,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);}
    private tools.jackson.databind.node.ObjectNode stamp(Path p,BasicFileAttributes a)throws IOException {
        var id=InstallationHostGuard.identity(p);return SourceJson.object().put("device",Long.toString(id.device())).put("inode",Long.toString(id.inode())).put("size",Long.toString(a.size())).put("modified",a.lastModifiedTime().toString()).put("changed",Files.getAttribute(p,"unix:ctime",LinkOption.NOFOLLOW_LINKS).toString()).put("mode",Files.getAttribute(p,"unix:mode",LinkOption.NOFOLLOW_LINKS).toString());
    }
    private List<String> names(Path p)throws IOException {var result=new ArrayList<String>();try(var stream=Files.newDirectoryStream(p)){for(Path child:stream){bounded();need(result.size()<8192,EVIDENCE_UNAVAILABLE);result.add(child.getFileName().toString());}}Collections.sort(result);return result;}
    private void bounded(){need(System.nanoTime()<deadline && entries.size()<8192,EVIDENCE_UNAVAILABLE);}
    private void put(Path p,JsonNode node){bounded();entries.put(p.toString(),node);}
    String digest(){var n=SourceJson.object();entries.forEach(n::set);return SourceJson.sha(SourceJson.canonical(n));}
    boolean same(RecoveryNativeSeal other){return entries.equals(other.entries);}
    /** Native PUBLICATION_INVALID sometimes includes I/O. Require a separate positive byte fault. */
    boolean demonstrablyInvalid(String fingerprint) {
        try {
            JsonNode manifest=entries.get(publication.resolve("inputs.json").toString());
            if(manifest!=null && manifest.has("sha256") && !manifest.get("sha256").stringValue().equals(fingerprint))return true;
            Path marker=publication.resolve("complete.json");var entry=entries.get(marker.toString());
            if(entry==null || !entry.has("sha256"))return false;
            byte[] raw=PreflightFiles.read(marker,1024*1024,false);need(SourceJson.sha(raw).equals(entry.get("sha256").stringValue()),EVIDENCE_CHANGED);
            JsonNode complete;try{complete=SourceJson.parse(raw,1024*1024);}catch(RuntimeException ex){return true;}
            if(!complete.has("files") || !complete.get("files").isObject())return true;
            for(var list:complete.get("files")) {
                if(!list.isArray())return true;
                for(var file:list) {
                    if(!file.has("path") || !file.get("path").isString() || !file.has("sha256") || !file.has("byte_length"))return true;
                    String name=file.get("path").stringValue();if(!name.matches("[A-Za-z0-9_.=-]+/[A-Za-z0-9_.=-]+") || name.contains(".."))return true;
                    var current=entries.get(publication.resolve(name).toString());
                    if(current==null || !current.has("sha256") || !current.get("sha256").equals(file.get("sha256")) || !current.get("size").stringValue().equals(file.get("byte_length").asString()))return true;
                }
            }
            return false;
        }catch(RecoveryFailure ex){throw ex;}catch(PreflightFailure ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}catch(RuntimeException ex){return false;}
    }
    @Override public String toString(){return "RecoveryNativeSeal[redacted]";}
}
