package platform.contentapi.execution;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.time.Instant;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Restricted D1 evidence under CONTROL. No repair, cleanup, namespace discovery or native writes. */
final class RssJournalFiles {
    static final int SMALL=65536,PAYLOAD=5*1024*1024;
    static final Set<String> CANDIDATE=Set.of("command.json","payload.xml","metadata.json","descriptor.json","result.json");
    record Evidence(String address,byte[] descriptorBytes,JsonNode descriptor,String completionSha,JsonNode completionStamp) {
        Evidence {descriptorBytes=descriptorBytes.clone();descriptor=descriptor.deepCopy();completionStamp=completionStamp.deepCopy();}
        @Override public byte[] descriptorBytes(){return descriptorBytes.clone();}
        @Override public JsonNode descriptor(){return descriptor.deepCopy();}
        @Override public JsonNode completionStamp(){return completionStamp.deepCopy();}
        byte[] checkpointReference(){var n=SourceJson.object().put("completionSha256",completionSha);n.set("completionStamp",completionStamp);n.set("bronze",descriptor.get("expected"));return SourceJson.canonical(n);}
        @Override public String toString(){return "RssJournalEvidence[redacted]";}
    }
    final InstallationHostGuard guard;final String address;final Path directory;
    RssJournalFiles(InstallationHostGuard guard,WorkerRepository.FetchStage stage,boolean create) {
        this(guard,stage.owner().runId(),stage.owner().attemptId(),stage.checkpoint(),stage.source(),stage.namespace(),create);
    }
    /** Historical reference opening never fabricates or returns an execution Owner token. */
    RssJournalFiles(InstallationHostGuard guard,UUID run,UUID attempt,UUID checkpoint,String source,String namespace,boolean create) {
        this.guard=guard;address=namespace;directory=guard.binding().root().resolve(address);
        need(source!=null && source.matches("[a-z0-9][a-z0-9_-]{0,127}") && address.equals("runs/"+run+"/attempts/"+attempt+"/fetch-source/"+source+"/"+checkpoint),INPUT_INVALID);
        if(create)ContentEvidenceBudget.reserve(directory,Map.of("command.json",SMALL,"payload.xml",PAYLOAD,"metadata.json",SMALL,"descriptor.json",SMALL,"result.json",SMALL,"complete.json",SMALL));
        try {
            checkGuard();Path path=guard.binding().root();String[] parts=address.split("/");
            for(int i=0;i<parts.length;i++){
                path=path.resolve(parts[i]);
                if(create){if(i==parts.length-1)Files.createDirectory(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
                    else try{Files.createDirectory(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));}catch(FileAlreadyExistsException ignored){ }}
                privateEntry(path,true);
            }
        }catch(FileAlreadyExistsException ex){throw new PreflightFailure(INPUT_CHANGED);}
        catch(IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
    void checkGuard(){guard.checkHeld(guard.binding().installationId());}
    void write(String name,byte[] bytes) {
        need(Set.of("command.json","complete.json").contains(name) && bytes.length<=SMALL,INPUT_INVALID);checkGuard();privateEntry(directory,true);
        if(ContentEvidenceBudget.enabled(directory)){PreparedCommand.writeOnce(directory,name,bytes,SMALL);return;}
        try(var file=FileChannel.open(directory.resolve(name),Set.of(StandardOpenOption.WRITE,StandardOpenOption.CREATE_NEW,LinkOption.NOFOLLOW_LINKS),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))){
            var buffer=ByteBuffer.wrap(bytes);while(buffer.hasRemaining())file.write(buffer);file.force(true);
        }catch(FileAlreadyExistsException ex){throw new PreflightFailure(INPUT_CHANGED);}catch(IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        need(Arrays.equals(bytes,read(name)),INPUT_CHANGED);
    }
    byte[] read(String name){need(CANDIDATE.contains(name) || name.equals("complete.json"),INPUT_INVALID);checkGuard();privateEntry(directory,true);privateEntry(directory.resolve(name),false);return PreflightFiles.read(directory.resolve(name),name.equals("payload.xml")?PAYLOAD:SMALL,true);}
    private static void privateEntry(Path path,boolean directory){PreflightFiles.privateEntry(path,directory);try{need(Files.getPosixFilePermissions(path).equals(PosixFilePermissions.fromString(directory?"rwx------":"rw-------")),INPUT_INVALID);}catch(IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}}
    private void membership(boolean complete){
        checkGuard();Set<String> names=new HashSet<>();try(var entries=Files.list(directory)){entries.forEach(p->names.add(p.getFileName().toString()));}catch(IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        var expected=new HashSet<>(CANDIDATE);if(complete)expected.add("complete.json");need(names.equals(expected),INPUT_CHANGED);
    }
    JsonNode validateCandidate(byte[] command) {
        membership(false);return validate(command);
    }
    String failedRetrieval(byte[] command) {
        try {
            checkGuard();Set<String> names=new HashSet<>();try(var entries=Files.list(directory)){entries.forEach(p->names.add(p.getFileName().toString()));}
            need(names.equals(Set.of("command.json","result.json")) && Arrays.equals(command,read("command.json")),RESULT_INVALID);
            var c=SourceJson.parse(command);var r=SourceJson.parse(read("result.json"));SourceJson.fields(r,"protocolVersion","runId","attemptId","stage","itemOrdinal","status","code","evidenceId","counts");
            for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))need(r.get(key).equals(c.get(key)),RESULT_INVALID);
            need(r.get("status").stringValue().equals("FAILED") && r.get("evidenceId").isNull() && r.get("counts").isObject() && r.get("counts").isEmpty(),RESULT_INVALID);
            String code=r.get("code").stringValue();need(Set.of("FETCH_HTTP","FETCH_TRANSPORT","FETCH_PAYLOAD_TOO_LARGE","FETCH_INTERRUPTED","FETCH_DISABLED").contains(code),RESULT_INVALID);return code;
        }catch(PreflightFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RESULT_INVALID);}
    }
    private JsonNode validate(byte[] command) {
        try {
            need(Arrays.equals(command,read("command.json")),INPUT_CHANGED);var c=SourceJson.parse(command);
            var d=SourceJson.parse(read("descriptor.json"));
            SourceJson.fields(d,"journalVersion","commandSha256","runId","attemptId","ownerEpoch","stage","itemOrdinal","checkpointId","sourceId","revision","capturedConfigSha256","endpointSha256","runtimeSha256","ingestionId","ingestedAt","httpStatus","contentType","declaredContentLength","payloadByteLength","payloadSha256","expected");
            need(d.get("journalVersion").isIntegralNumber() && d.get("journalVersion").canConvertToInt() && d.get("journalVersion").intValue()==1 && d.get("commandSha256").stringValue().equals(SourceJson.sha(command)),INPUT_INVALID);
            for(String key:List.of("runId","attemptId","ownerEpoch","stage","itemOrdinal","checkpointId","sourceId","revision","capturedConfigSha256","endpointSha256","runtimeSha256"))need(d.get(key).equals(c.get(key)),INPUT_CHANGED);
            String ingestion=d.get("ingestionId").stringValue(),instant=d.get("ingestedAt").stringValue();
            need(UUID.fromString(ingestion).toString().equals(ingestion) && Instant.parse(instant).toString().equals(instant) && instant.matches("[0-9]{4}-.*Z"),INPUT_INVALID);
            need(d.get("httpStatus").isIntegralNumber() && d.get("httpStatus").canConvertToInt() && d.get("httpStatus").intValue()>=200 && d.get("httpStatus").intValue()<300,INPUT_INVALID);
            byte[] payload=read("payload.xml"),metadata=read("metadata.json");
            need(d.get("payloadByteLength").isIntegralNumber() && d.get("payloadByteLength").canConvertToInt() && d.get("payloadByteLength").longValue()==payload.length && SourceJson.sha(payload).equals(d.get("payloadSha256").stringValue()),INPUT_CHANGED);
            var e=d.get("expected");SourceJson.fields(e,"payloadKey","metadataKey","metadataByteLength","metadataSha256");
            String prefix="bronze/rss/source_id="+Base64.getUrlEncoder().withoutPadding().encodeToString(c.get("sourceId").stringValue().getBytes(java.nio.charset.StandardCharsets.UTF_8))+"/ingestion_date="+Instant.parse(instant).atOffset(java.time.ZoneOffset.UTC).toLocalDate()+"/ingestion_id="+ingestion+"/";
            need(e.get("payloadKey").stringValue().equals(prefix+"payload.xml") && e.get("metadataKey").stringValue().equals(prefix+"metadata.json") && e.get("metadataByteLength").isIntegralNumber() && e.get("metadataByteLength").longValue()==metadata.length && SourceJson.sha(metadata).equals(e.get("metadataSha256").stringValue()),INPUT_CHANGED);
            var m=SourceJson.parse(metadata);SourceJson.fields(m,"schemaVersion","ingestionId","sourceId","sourceUri","ingestedAt","httpStatus","contentType","declaredContentLength","payloadByteLength","payloadSha256","payloadKey");
            need(m.get("schemaVersion").intValue()==1 && m.get("sourceUri").equals(c.get("feedUri")) && m.get("payloadKey").equals(e.get("payloadKey")),INPUT_INVALID);
            for(String key:List.of("ingestionId","sourceId","ingestedAt","httpStatus","contentType","declaredContentLength","payloadByteLength","payloadSha256"))need(m.get(key).equals(d.get(key)),INPUT_CHANGED);
            need((d.get("contentType").isNull() || d.get("contentType").isString()) && (d.get("declaredContentLength").isNull() || d.get("declaredContentLength").isIntegralNumber() && d.get("declaredContentLength").canConvertToLong() && d.get("declaredContentLength").longValue()>=0),INPUT_INVALID);
            var r=SourceJson.parse(read("result.json"));SourceJson.fields(r,"protocolVersion","runId","attemptId","stage","itemOrdinal","status","code","evidenceId","counts");
            for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))need(r.get(key).equals(c.get(key)),RESULT_INVALID);
            need(r.get("status").stringValue().equals("OK") && r.get("code").isNull() && r.get("evidenceId").equals(c.get("checkpointId")) && r.get("counts").isObject() && r.get("counts").isEmpty(),RESULT_INVALID);
            return d;
        }catch(PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new PreflightFailure(INPUT_INVALID);}
    }
    Evidence complete(byte[] command) {
        validateCandidate(command);var marker=SourceJson.object().put("journalCompletionVersion",1).put("directoryIdentity",identity(directory));
        var files=marker.putArray("files");for(String name:new TreeSet<>(CANDIDATE)){byte[] raw=read(name);var f=files.addObject().put("name",name).put("byteLength",raw.length).put("sha256",SourceJson.sha(raw));f.set("stamp",stamp(directory.resolve(name)));}
        byte[] raw=SourceJson.canonical(marker);write("complete.json",raw);return reopen(command,SourceJson.sha(raw));
    }
    Evidence reopen(byte[] command,String completionSha) {
        membership(true);byte[] completion=read("complete.json");need(SourceJson.sha(completion).equals(completionSha),INPUT_CHANGED);
        var marker=PreflightFiles.canonical(completion,SMALL);SourceJson.fields(marker,"journalCompletionVersion","directoryIdentity","files");
        need(marker.get("journalCompletionVersion").intValue()==1 && marker.get("directoryIdentity").stringValue().equals(identity(directory)) && marker.get("files").isArray() && marker.get("files").size()==CANDIDATE.size(),INPUT_CHANGED);
        Set<String> seen=new HashSet<>();for(var f:marker.get("files")){
            SourceJson.fields(f,"name","byteLength","sha256","stamp");String name=f.get("name").stringValue();need(CANDIDATE.contains(name) && seen.add(name),INPUT_INVALID);
            byte[] bytes=read(name);need(f.get("byteLength").longValue()==bytes.length && f.get("sha256").stringValue().equals(SourceJson.sha(bytes)) && Arrays.equals(SourceJson.canonical(f.get("stamp")),SourceJson.canonical(stamp(directory.resolve(name)))),INPUT_CHANGED);
        }
        JsonNode d=validate(command);membership(true);return new Evidence(address,read("descriptor.json"),d,completionSha,stamp(directory.resolve("complete.json")));
    }
    private static String identity(Path p){var id=InstallationHostGuard.identity(p);return id.device()+":"+id.inode();}
    private static JsonNode stamp(Path p){try{var attrs=Files.readAttributes(p,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);return SourceJson.object().put("identity",identity(p)).put("modified",attrs.lastModifiedTime().toString()).put("size",attrs.size());}catch(IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}}
    @Override public String toString(){return "RssJournalFiles[redacted]";}
}
