package platform.ingestion.managed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ReservedEvidenceTest {
    @TempDir Path temp;
    Path root,directory,command;ObjectNode manifest;
    static final ObjectMapper JSON=new ObjectMapper();
    @BeforeEach void setup()throws Exception{
        var run=UUID.randomUUID();var attempt=UUID.randomUUID();root=temp.toRealPath().resolve("runs/"+run+"/attempts/"+attempt);directory=root.resolve("store-bronze/"+UUID.randomUUID());Files.createDirectories(directory);
        for(Path p=directory;p.startsWith(temp.toRealPath());p=p.getParent())Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rwx------"));
        Files.createFile(root.resolve(".content-evidence.lock"),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        command=directory.resolve("command.json");Files.writeString(command,"{}");Files.setPosixFilePermissions(command,PosixFilePermissions.fromString("rw-------"));
        manifest=JSON.createObjectNode().put("version",1).put("treeKind","ATTEMPT").put("runId",run.toString()).put("attemptId",attempt.toString()).put("rootDevice",Files.getAttribute(root,"unix:dev").toString()).put("rootInode",Files.getAttribute(root,"unix:ino").toString());
        manifest.putArray("directories").add("").add("store-bronze").add(root.relativize(directory).toString());var files=manifest.putObject("files");
        for(String name:new String[]{"evidence.json","result.json"})for(String suffix:new String[]{"",".preparing"})files.put(root.relativize(directory.resolve(name+suffix)).toString(),4);
        ledger();
    }
    void ledger()throws Exception{Files.write(root.resolve(".content-evidence.json"),JSON.writeValueAsBytes(manifest));Files.setPosixFilePermissions(root.resolve(".content-evidence.json"),PosixFilePermissions.fromString("rw-------"));}
    @Test void exactCapacityAndFirstRejectedByte()throws Exception{
        try(var sink=ReservedEvidence.open(command)){sink.checkActive();ManagedRssFetch.write(directory.resolve("evidence.json"),new byte[4]);assertThrows(java.io.IOException.class,()->ManagedRssFetch.write(directory.resolve("result.json"),new byte[5]));}
        assertEquals(4,Files.size(directory.resolve("evidence.json")));assertFalse(Files.exists(directory.resolve("result.json.preparing")));assertFalse(ReservedEvidence.active());
    }
    @Test void unreservedAndRepeatedWritesAreRefused()throws Exception{
        try(var sink=ReservedEvidence.open(command)){sink.checkActive();ManagedRssFetch.write(directory.resolve("evidence.json"),new byte[4]);assertThrows(java.io.IOException.class,()->ManagedRssFetch.write(directory.resolve("evidence.json"),new byte[4]));assertThrows(java.io.IOException.class,()->ManagedRssFetch.write(directory.resolve("unreserved"),new byte[1]));}
        assertEquals(4,Files.size(directory.resolve("evidence.json")));
    }
    @Test void partialWriteIsRetainedAndNeverOverwritten()throws Exception{
        Path prefix=directory.resolve("result.json.preparing");Files.write(prefix,new byte[2]);Files.setPosixFilePermissions(prefix,PosixFilePermissions.fromString("rw-------"));
        try(var sink=ReservedEvidence.open(command)){sink.checkActive();assertThrows(java.io.IOException.class,()->ManagedRssFetch.write(directory.resolve("result.json"),new byte[4]));}
        assertEquals(2,Files.size(prefix));assertFalse(Files.exists(directory.resolve("result.json")));
    }
    @Test void missingCorruptOrPendingLedgerCannotOpenSink()throws Exception{
        Files.writeString(root.resolve(".content-evidence.json"),"{");assertThrows(Exception.class,()->ReservedEvidence.open(command));ledger();Files.delete(root.resolve(".content-evidence.json"));assertThrows(Exception.class,()->ReservedEvidence.open(command));ledger();Files.writeString(root.resolve(".content-evidence.json.preparing"),"{");assertThrows(Exception.class,()->ReservedEvidence.open(command));assertFalse(ReservedEvidence.active());
    }
    @Test void aggregateBytesAndEntriesAreRefused()throws Exception{
        var files=manifest.putObject("files");for(int i=0;i<4;i++)files.put("x"+i,16*1024*1024);ledger();assertThrows(Exception.class,()->ReservedEvidence.open(command));files=manifest.putObject("files");for(int i=0;i<4096;i++)files.put("x"+i,1);ledger();assertThrows(Exception.class,()->ReservedEvidence.open(command));
    }
    @Test void changedRootIdentityCannotOpenSink()throws Exception{manifest.put("rootInode","0");ledger();assertThrows(Exception.class,()->ReservedEvidence.open(command));}
    @Test void anotherJvmMustWaitForTheSameReservationLock()throws Exception{
        String cp=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));var childBuilder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",cp,Writer.class.getName(),command.toString());
        try(var channel=FileChannel.open(root.resolve(".content-evidence.lock"),StandardOpenOption.WRITE);var lock=channel.lock()){
            assertTrue(lock.isValid());var child=childBuilder.start();
            try{assertFalse(child.waitFor(200,TimeUnit.MILLISECONDS));assertFalse(Files.exists(directory.resolve("result.json")));lock.release();assertTrue(child.waitFor(10,TimeUnit.SECONDS));assertEquals(0,child.exitValue());}finally{child.destroyForcibly();child.waitFor(10,TimeUnit.SECONDS);}
        }
        assertEquals(4,Files.size(directory.resolve("result.json")));
    }
    public static final class Writer {
        private Writer(){}
        public static void main(String[] args)throws Exception{Path command=Path.of(args[0]);try(var sink=ReservedEvidence.open(command)){sink.checkActive();ManagedRssFetch.write(command.resolveSibling("result.json"),new byte[4]);}}
    }
}
