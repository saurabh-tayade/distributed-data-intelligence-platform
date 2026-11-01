package platform.contentapi.execution;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import platform.contentapi.management.SourceJson;
import static platform.contentapi.execution.PreflightFailure.*;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/** A forced, immutable command reference. Preparing it grants no execution authority. */
final class PreparedCommand {
    final String address,sha256;
    private final Path path;
    private final Object fileKey;
    private final long length;
    private final java.nio.file.attribute.FileTime modified;

    private PreparedCommand(Path path,String address,byte[] bytes)throws java.io.IOException{
        this.path=path;this.address=address;sha256=SourceJson.sha(bytes);length=bytes.length;
        var a=Files.readAttributes(path,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
        fileKey=a.fileKey();modified=a.lastModifiedTime();
    }
    static PreparedCommand prepare(InstallationHostGuard guard,Path directory,byte[] bytes,int maximum){
        guard.checkHeld(guard.binding().installationId());
        PreflightFiles.canonical(bytes,maximum);
        Path path=directory.resolve("command.json");
        // A forced leaf cannot make a newly created parent entry durable on its own.
        try{for(Path p=directory;p.startsWith(guard.binding().root()) && !p.equals(guard.binding().root());p=p.getParent()){PreflightFiles.privateEntry(p,true);forceDirectory(p.getParent());}}
        catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        writeOnce(directory,"command.json",bytes,maximum);
        try{return new PreparedCommand(path,guard.binding().root().relativize(path).toString(),bytes);}
        catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
    void verify(InstallationHostGuard guard,WorkerRepository.Owner owner,String namespace,String digest){
        guard.checkHeld(owner.worker().installationId());
        need(address.equals(namespace+"/command.json") && sha256.equals(digest)
                && address.startsWith("runs/"+owner.runId()+"/attempts/"+owner.attemptId()+"/"),INPUT_CHANGED);
        try{
            var a=Files.readAttributes(path,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
            need(Objects.equals(a.fileKey(),fileKey) && a.size()==length && a.lastModifiedTime().equals(modified),INPUT_CHANGED);
            need(SourceJson.sha(PreflightFiles.read(path,(int)length,true)).equals(sha256),INPUT_CHANGED);
        }catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
    /** Same-directory atomic publication; an interrupted prefix remains visibly uncommitted. */
    static void writeOnce(Path directory,String name,byte[] bytes,int maximum){
        need(bytes.length<=maximum && name.matches("[a-z][a-z0-9.-]{0,63}"),RESOURCE_LIMIT);
        PreflightFiles.privateEntry(directory,true);
        Path target=directory.resolve(name),temporary=directory.resolve(name+".preparing");
        ContentEvidenceBudget.write(directory,target,temporary,bytes,()->{
            try{
                need(!Files.exists(target,LinkOption.NOFOLLOW_LINKS),INPUT_CHANGED);
                try(var channel=FileChannel.open(temporary,Set.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))){
                    var b=ByteBuffer.wrap(bytes);while(b.hasRemaining())channel.write(b);channel.force(true);
                }
                need(Arrays.equals(bytes,PreflightFiles.read(temporary,maximum,true)),INPUT_CHANGED);
                // Compliant writers hold the same tree lock; neither target nor prefix is replaced.
                need(!Files.exists(target,LinkOption.NOFOLLOW_LINKS),INPUT_CHANGED);
                Files.move(temporary,target,StandardCopyOption.ATOMIC_MOVE);
                forceDirectory(directory);
                need(Arrays.equals(bytes,PreflightFiles.read(target,maximum,true)),INPUT_CHANGED);
            }catch(FileAlreadyExistsException ex){throw new PreflightFailure(INPUT_CHANGED);}
            catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        });
    }
    static void forceDirectory(Path directory)throws java.io.IOException{
        try(var channel=FileChannel.open(directory,StandardOpenOption.READ)){channel.force(true);}
    }
    @Override public String toString(){return "PreparedCommand[redacted]";}
}
