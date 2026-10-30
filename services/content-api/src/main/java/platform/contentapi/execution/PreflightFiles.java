package platform.contentapi.execution;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Fixed private sidecars under the held installation CONTROL root. Never native publication roots. */
final class PreflightFiles {
    static final int SMALL=65536,LARGE=16*1024*1024;
    final Path directory;final String address;
    PreflightFiles(InstallationHostGuard guard,UUID run,UUID attempt,UUID checkpoint) {
        this(guard,run,attempt,checkpoint,"CHECK_INPUTS");
    }
    PreflightFiles(InstallationHostGuard guard,UUID run,UUID attempt,UUID checkpoint,String stage) {
        need(Set.of("CHECK_INPUTS","PUBLISH_RELIABILITY","VERIFY_RESULT","STORE_BRONZE","INSPECT_BRONZE","SILVER","VERIFY_SILVER","INSPECT_SILVER","RECHECK_SILVER","PREPARE_GOLD","PUBLISH_GOLD","PUBLISH_CONTENT").contains(stage),INPUT_INVALID);
        address="runs/"+run+"/attempts/"+attempt+"/"+stage.toLowerCase(Locale.ROOT).replace('_','-')+"/"+checkpoint;
        try {
            Path p=guard.binding().root();
            for(String segment:address.split("/")) {
                p=p.resolve(segment);
                try{Files.createDirectory(p,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));}catch(FileAlreadyExistsException ignored){ }
                privateEntry(p,true);
            }
            directory=p;
        }catch(IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
    static void privateEntry(Path p,boolean directory) {
        try {
            need(p.isAbsolute() && p.equals(p.toRealPath()) && (directory?Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS):Files.isRegularFile(p,LinkOption.NOFOLLOW_LINKS)),INPUT_INVALID);
            need(Files.getOwner(p).equals(Files.getOwner(Path.of(System.getProperty("user.home")))) && Files.getPosixFilePermissions(p).stream().noneMatch(v->v.name().startsWith("GROUP_") || v.name().startsWith("OTHERS_")),INPUT_INVALID);
            if(!directory)need(((Number)Files.getAttribute(p,"unix:nlink",LinkOption.NOFOLLOW_LINKS)).longValue()==1,INPUT_INVALID);
        }catch(IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
    static byte[] read(Path p,int maximum,boolean privateFile) {
        try {
            if(privateFile)privateEntry(p,false);
            else need(p.equals(p.toRealPath()) && Files.isRegularFile(p,LinkOption.NOFOLLOW_LINKS) && ((Number)Files.getAttribute(p,"unix:nlink",LinkOption.NOFOLLOW_LINKS)).longValue()==1,RUNTIME_MISMATCH);
            var before=Files.readAttributes(p,java.nio.file.attribute.BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
            need(before.size()<=maximum,RESOURCE_LIMIT);
            byte[] bytes;try(var stream=Files.newInputStream(p,LinkOption.NOFOLLOW_LINKS)){bytes=stream.readNBytes(maximum+1);}
            need(bytes.length<=maximum,RESOURCE_LIMIT);
            var after=Files.readAttributes(p,java.nio.file.attribute.BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
            need(before.size()==bytes.length && after.size()==before.size() && before.lastModifiedTime().equals(after.lastModifiedTime()) && Objects.equals(before.fileKey(),after.fileKey()),INPUT_CHANGED);
            return bytes;
        }catch(NoSuchFileException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        catch(IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
    static JsonNode canonical(byte[] bytes,int limit) {
        try{var n=SourceJson.parse(bytes,limit);need(Arrays.equals(bytes,SourceJson.canonical(n)),INPUT_INVALID);return n;}
        catch(PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new PreflightFailure(INPUT_INVALID);}
    }
    void write(String name,byte[] bytes,int maximum) {
        need(Set.of("command.json","selection.json","verified.json","retained.json","evidence.json","child.json").contains(name) && bytes.length<=maximum,INPUT_INVALID);
        privateEntry(directory,true);
        try(var channel=FileChannel.open(directory.resolve(name),Set.of(StandardOpenOption.WRITE,StandardOpenOption.CREATE_NEW,LinkOption.NOFOLLOW_LINKS),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))) {
            var buffer=ByteBuffer.wrap(bytes);while(buffer.hasRemaining())channel.write(buffer);channel.force(true);
        }catch(FileAlreadyExistsException ex){throw new PreflightFailure(INPUT_CHANGED);}
        catch(IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        need(Arrays.equals(read(directory.resolve(name),maximum,true),bytes),INPUT_CHANGED);
    }
    byte[] read(String name,int maximum){need(Set.of("command.json","selection.json","result.json","evidence.json","verified.json","retained.json","child.json").contains(name),INPUT_INVALID);privateEntry(directory,true);return read(directory.resolve(name),maximum,true);}
    @Override public String toString(){return "PreflightFiles[redacted]";}
}
