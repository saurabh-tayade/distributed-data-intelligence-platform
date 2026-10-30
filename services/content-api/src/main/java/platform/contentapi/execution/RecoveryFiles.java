package platform.contentapi.execution;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** Exact private operational sidecars only. No native-root traversal, output qualification or publication. */
final class RecoveryFiles {
    static final int MAXIMUM=1024*1024;
    record Retained(byte[] bytes,InstallationHostGuard.Identity identity,String modifiedAt) {
        @Override public String toString(){return "RecoveryFile[redacted]";}
    }
    record Capture(JsonNode inventory,Set<Long> descendantPids) { }
    static Capture capture(InstallationHostGuard guard,UUID run,UUID attempt,long epoch,String selectionSha,JsonNode runtime,List<Map<String,Object>> stages) {
        if(runtime.has("contentRecoveryRuntimeVersion"))return ContentRecoveryFiles.capture(guard,run,attempt,stages);
        var result=SourceJson.object();var files=result.putArray("files");Set<Long> hints=new HashSet<>();
        try {
            for(var stage:stages) {
                String name=(String)stage.get("stage");need(Set.of("CHECK_INPUTS","PUBLISH_RELIABILITY","VERIFY_RESULT").contains(name),RUNTIME_MISMATCH);
                String base="runs/"+run+"/attempts/"+attempt+"/"+name.toLowerCase(Locale.ROOT).replace('_','-')+"/"+stage.get("checkpoint_id");
                String command=(String)stage.get("command_address"),anchor=(String)stage.get("result_address");
                need(command==null || command.equals(base+"/command.json"),EVIDENCE_CHANGED);
                need(anchor==null || anchor.equals(base+"/verified.json"),EVIDENCE_CHANGED);
                for(String file:List.of("command.json","selection.json","retained.json","result.json","evidence.json","verified.json")) {
                    String address=base+"/"+file;Path path=resolve(guard,address);
                    boolean intended=file.equals("command.json") && (stage.get("intended_identity")!=null || stage.get("child_pid")!=null);
                    boolean required=address.equals(command) || address.equals(anchor) || intended;
                    if(!present(path)){need(!required,EVIDENCE_UNAVAILABLE);files.addObject().put("address",address).put("present",false);continue;}
                    var before=Files.readAttributes(path,java.nio.file.attribute.BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);var identity=InstallationHostGuard.identity(path);
                    byte[] raw=PreflightFiles.read(path,Set.of("selection.json","retained.json","evidence.json").contains(file)?PreflightFiles.LARGE:PreflightFiles.SMALL,true);String sha=SourceJson.sha(raw);
                    var after=Files.readAttributes(path,java.nio.file.attribute.BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);need(Objects.equals(before.fileKey(),after.fileKey()) && before.lastModifiedTime().equals(after.lastModifiedTime()) && before.size()==after.size(),EVIDENCE_CHANGED);
                    files.addObject().put("address",address).put("present",true).put("length",Long.toString(raw.length)).put("sha256",sha).put("device",Long.toString(identity.device())).put("inode",Long.toString(identity.inode())).put("modifiedAt",after.lastModifiedTime().toString());
                    if(address.equals(command))need(sha.equals(stage.get("command_sha256")),EVIDENCE_CHANGED);
                    if(address.equals(anchor))need(sha.equals(stage.get("result_sha256")),EVIDENCE_CHANGED);
                    if(file.equals("command.json")) {
                        var c=PreflightFiles.canonical(raw,PreflightFiles.SMALL);
                        need(c.get("protocolVersion").intValue()==1 && c.get("runId").stringValue().equals(run.toString()) && c.get("attemptId").stringValue().equals(attempt.toString()) && c.get("checkpointId").stringValue().equals(stage.get("checkpoint_id").toString()) && c.get("stage").stringValue().equals(name) && c.get("ownerEpoch").stringValue().equals(Long.toString(epoch)) && c.get("capturedConfigSha256").stringValue().equals(selectionSha) && c.get("runtime").equals(runtime),EVIDENCE_CHANGED);
                        Object intendedSha=name.equals("CHECK_INPUTS")?stage.get("intended_identity"):stage.get("input_evidence_sha256");need(intendedSha==null || sha.equals(intendedSha),EVIDENCE_CHANGED);
                    }
                    if(file.equals("evidence.json")) {
                        // Only a process hint is inspected; no native publication/result validity is inferred.
                        JsonNode evidence=null;try{evidence=SourceJson.parse(raw,PreflightFiles.LARGE);}catch(RuntimeException ignored){need(Set.of("INTENT","RUNNING","UNKNOWN").contains(stage.get("state")),EVIDENCE_CHANGED);}
                        if(evidence!=null && evidence.has("cleanup") && evidence.get("cleanup").has("pid")) {
                            String pid=evidence.get("cleanup").get("pid").stringValue();need(pid.matches("[1-9][0-9]{0,18}"),PROCESS_UNRESOLVED);long value=Long.parseLong(pid);need(value>0,PROCESS_UNRESOLVED);hints.add(value);
                        }
                    }
                }
            }
            need(SourceJson.canonical(result).length<=MAXIMUM,EVIDENCE_UNAVAILABLE);return new Capture(result,Set.copyOf(hints));
        }catch(RecoveryFailure ex){throw ex;}catch(PreflightFailure ex){throw new RecoveryFailure(ex.code()==PreflightFailure.Code.INPUT_UNAVAILABLE?EVIDENCE_UNAVAILABLE:EVIDENCE_CHANGED);}catch(IOException ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}catch(RuntimeException ex){throw new RecoveryFailure(EVIDENCE_CHANGED);}
    }
    private static Path resolve(InstallationHostGuard guard,String address) {
        Path p=guard.binding().root();String[] segments=address.split("/");
        for(int i=0;i<segments.length;i++) {
            need(!segments[i].isEmpty() && !segments[i].equals(".") && !segments[i].equals("..") && !segments[i].contains("\\"),EVIDENCE_CHANGED);p=p.resolve(segments[i]);
            if(i<segments.length-1 && present(p))PreflightFiles.privateEntry(p,true);
        }
        need(p.normalize().startsWith(guard.binding().root()),EVIDENCE_CHANGED);return p;
    }
    private static boolean present(Path p) {
        try{Files.readAttributes(p,java.nio.file.attribute.BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);return true;}
        catch(NoSuchFileException ex){return false;}catch(IOException | SecurityException ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}
    }
    static String write(InstallationHostGuard guard,UUID control,UUID qualification,byte[] bytes) {
        need(bytes.length<=MAXIMUM,EVIDENCE_UNAVAILABLE);String address="recovery/"+control+"/"+qualification+"/qualification.json";
        try {
            Path p=guard.binding().root();for(String segment:List.of("recovery",control.toString(),qualification.toString())){p=p.resolve(segment);try{Files.createDirectory(p,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));}catch(FileAlreadyExistsException ignored){ }PreflightFiles.privateEntry(p,true);}
            try(var channel=FileChannel.open(p.resolve("qualification.json"),Set.of(StandardOpenOption.WRITE,StandardOpenOption.CREATE_NEW,LinkOption.NOFOLLOW_LINKS),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))){var b=ByteBuffer.wrap(bytes);while(b.hasRemaining())channel.write(b);channel.force(true);}
            need(Arrays.equals(bytes,read(guard,address)),EVIDENCE_CHANGED);return address;
        }catch(RecoveryFailure ex){throw ex;}catch(IOException | RuntimeException ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}
    }
    static byte[] read(InstallationHostGuard guard,String address) {
        try{return PreflightFiles.read(resolve(guard,address),MAXIMUM,true);}catch(RecoveryFailure ex){throw ex;}catch(RuntimeException ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}
    }
    static byte[] readPrivate(InstallationHostGuard guard,String address,int maximum) {
        need(maximum>0 && maximum<=PreflightFiles.LARGE,INVALID_REQUEST);return PreflightFiles.read(resolve(guard,address),maximum,true);
    }
    /** Captures the packet's identity across a read and subsequent authorization, without creating anything. */
    static Retained retained(InstallationHostGuard guard,String address) {
        try {
            Path path=resolve(guard,address);
            var before=Files.readAttributes(path,java.nio.file.attribute.BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
            var identity=InstallationHostGuard.identity(path);byte[] bytes=PreflightFiles.read(path,MAXIMUM,true);
            var after=Files.readAttributes(path,java.nio.file.attribute.BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
            need(Objects.equals(before.fileKey(),after.fileKey()) && before.size()==after.size() && before.lastModifiedTime().equals(after.lastModifiedTime()),EVIDENCE_CHANGED);
            return new Retained(bytes,identity,after.lastModifiedTime().toString());
        }catch(RecoveryFailure ex){throw ex;}catch(PreflightFailure ex){throw new RecoveryFailure(ex.code()==PreflightFailure.Code.INPUT_UNAVAILABLE?EVIDENCE_UNAVAILABLE:EVIDENCE_CHANGED);}catch(IOException ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}catch(RuntimeException ex){throw new RecoveryFailure(EVIDENCE_CHANGED);}
    }
    private RecoveryFiles(){ }
}
