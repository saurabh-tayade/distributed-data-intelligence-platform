package platform.ingestion.managed;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;

/** Fixed bounded adapter sink. Legacy entry points do not acquire this protocol. */
final class ReservedEvidence implements AutoCloseable {
    private static final String LEDGER=".content-evidence.json",LOCK=".content-evidence.lock";
    private static final ObjectMapper JSON=new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final ThreadLocal<ReservedEvidence> CURRENT=new ThreadLocal<>();
    private final Path root;
    private final String device,inode;
    private ReservedEvidence(Path root)throws Exception{
        this.root=root;ManagedRssFetch.privateEntry(root,true);ManagedRssFetch.privateEntry(root.resolve(LOCK),false);device=attribute(root,"unix:dev");inode=attribute(root,"unix:ino");
        try(var channel=FileChannel.open(root.resolve(LOCK),StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);var lock=channel.lock()){check(lock.isValid());manifest();}
    }
    static ReservedEvidence open(Path command)throws Exception{
        Path root=command.getParent();int depth=0;
        while(root!=null && !Files.exists(root.resolve(LOCK),LinkOption.NOFOLLOW_LINKS) && depth++<4)root=root.getParent();
        check(root!=null && Files.exists(root.resolve(LOCK),LinkOption.NOFOLLOW_LINKS) && CURRENT.get()==null);
        var sink=new ReservedEvidence(root);CURRENT.set(sink);return sink;
    }
    void checkActive(){check(CURRENT.get()==this);}
    static boolean active(){return CURRENT.get()!=null;}
    static void write(Path target,byte[] bytes)throws java.io.IOException{
        try{CURRENT.get().put(target,bytes);}catch(java.io.IOException ex){throw ex;}catch(Exception ex){throw new java.io.IOException("EVIDENCE_UNAVAILABLE");}
    }
    private JsonNode manifest()throws Exception{
        ManagedRssFetch.privateEntry(root,true);ManagedRssFetch.privateEntry(root.resolve(LOCK),false);
        check(device.equals(attribute(root,"unix:dev")) && inode.equals(attribute(root,"unix:ino")) && !Files.exists(root.resolve(LEDGER+".preparing"),LinkOption.NOFOLLOW_LINKS));
        var n=JSON.readTree(ManagedRssFetch.read(root.resolve(LEDGER),256*1024));
        check(n.path("version").isInt() && n.get("version").intValue()==1 && device.equals(n.path("rootDevice").asText()) && inode.equals(n.path("rootInode").asText())
                && root.endsWith("runs/"+UUID.fromString(n.path("runId").asText())+"/attempts/"+UUID.fromString(n.path("attemptId").asText())) && n.path("files").isObject() && n.path("directories").isArray());
        long maximum=512L*1024;for(var value:n.get("files")){check(value.isInt() && value.intValue()>0 && value.intValue()<=16*1024*1024);maximum+=value.intValue();}
        check(maximum<=64L*1024*1024 && n.get("files").size()+n.get("directories").size()+3<=4096);return n;
    }
    private void put(Path target,byte[] bytes)throws Exception{
        check(target.isAbsolute() && target.normalize().equals(target) && target.startsWith(root));ManagedRssFetch.privateEntry(target.getParent(),true);
        Path temporary=target.resolveSibling(target.getFileName()+".preparing");
        try(var channel=FileChannel.open(root.resolve(LOCK),StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);var lock=channel.lock()){
            check(lock.isValid());var n=manifest();for(Path path:List.of(target,temporary)){var cap=n.get("files").get(root.relativize(path).toString());check(cap!=null && bytes.length<=cap.intValue() && !Files.exists(path,LinkOption.NOFOLLOW_LINKS));}
            try(var output=FileChannel.open(temporary,Set.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))){
                var buffer=ByteBuffer.wrap(bytes);while(buffer.hasRemaining())output.write(buffer);output.force(true);
            }
            check(Arrays.equals(bytes,ManagedRssFetch.read(temporary,Math.max(bytes.length,1))) && !Files.exists(target,LinkOption.NOFOLLOW_LINKS));
            Files.move(temporary,target,StandardCopyOption.ATOMIC_MOVE);try(var directory=FileChannel.open(target.getParent(),StandardOpenOption.READ)){directory.force(true);}
            check(Arrays.equals(bytes,ManagedRssFetch.read(target,Math.max(bytes.length,1))));
        }
    }
    private static String attribute(Path p,String key)throws Exception{return Files.getAttribute(p,key,LinkOption.NOFOLLOW_LINKS).toString();}
    private static void check(boolean value){if(!value)throw new IllegalArgumentException("EVIDENCE_UNAVAILABLE");}
    @Override public void close(){CURRENT.remove();}
}
