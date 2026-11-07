package platform.ingestion.managed;

import com.sun.security.auth.module.UnixSystem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Inactive, explicitly bound ORIGINAL ledger-v2 STORE_BRONZE sink. Does not discover,
 * reserve, repair or adopt evidence. Locks serialize cooperating Java threads and
 * POSIX processes, not arbitrary same-user adversaries. Paths are checked before
 * and after acquiring locks; a pathname is never treated as an identity seal.
 * The original plan is reconstructed independently and parity-tested against the
 * unchanged FETCH sink; this class does not widen the earlier sink's writer scope.
 */
final class StrictBronzeEvidence {
    static final String POLICY = "ddip-content-narrow-v1";
    static final String MANIFEST = ".content-evidence.json", LOCK = ".content-evidence.lock";
    private static final int LEDGER_LIMIT = 256 * 1024, BYTE_LIMIT = 64 * 1024 * 1024;
    private static final int ENTRY_LIMIT = 221, INVENTORY_LIMIT = 128 * 1024;
    private static final Object LOCAL_LOCK = new Object();
    private static final Map<String,Integer> COUNTS = Map.ofEntries(
            Map.entry("FETCH_SOURCE",1), Map.entry("STORE_BRONZE",1), Map.entry("INSPECT_BRONZE",5),
            Map.entry("SILVER",1), Map.entry("VERIFY_SILVER",1), Map.entry("RECHECK_SILVER",1),
            Map.entry("PREPARE_GOLD",1), Map.entry("PUBLISH_GOLD",1), Map.entry("VERIFY_RESULT",2),
            Map.entry("CHECK_INPUTS",4), Map.entry("PUBLISH_CONTENT",1));
    private static final Set<String> WRITABLE = Set.of("evidence.json","result.json");

    record Identity(long device, long inode) {
        @Override public String toString() { return "Identity[redacted]"; }
    }
    record Frozen(String selectionSha256, String profileSha256, String runtimeSha256) {
        @Override public String toString() { return "Frozen[redacted]"; }
    }
    record Writer(String installationId, String workerInstanceId, long epoch, String runId,
                  String attemptId, String stage, int ordinal, String checkpointId, String commandSha256) {
        @Override public String toString() { return "Writer[redacted]"; }
    }
    record Expected(String installationId, String policy, String planKind, String runId,
                    String originalAttemptId, String sourceId, Path controlRoot, Identity controlIdentity,
                    Identity rootIdentity, Identity lockIdentity, String ledgerSha256, Frozen frozen, Writer writer) {
        @Override public String toString() { return "Expected[redacted]"; }
    }
    record Current(String policy, Frozen frozen, Writer writer) {
        @Override public String toString() { return "Current[redacted]"; }
    }
    static final class Failure extends IOException {
        private static final long serialVersionUID = 1L;
        Failure(String code) { super(code); }
    }
    enum Boundary { MARKER_CREATED, MARKER_FORCED, MARKER_DIRECTORY_FORCED, EVIDENCE_LINKED, RESULT_LINKED }
    @FunctionalInterface interface Fault { void at(Boundary boundary) throws IOException; }
    @FunctionalInterface interface CheckedAction<T> { T run() throws Exception; }
    @FunctionalInterface private interface IoAction<T> { T run() throws IOException; }
    private record Plan(Set<String> directories, Map<String,Integer> files, byte[] ledger) { }
    private record Read(byte[] bytes, Map<String,Object> stamp) { }

    private final Expected expected;
    private final Current current;
    private final Path root, directory;
    private final Plan plan;
    private final Fault fault;
    private Thread sessionThread;
    private FileChannel markerChannel;
    private Map<String,Object> markerStamp;
    private Map<String,Object> commandStamp, ledgerStamp;
    private Identity operationIdentity, operationParentIdentity;

    StrictBronzeEvidence(Expected expected, Current current) throws IOException {
        this(expected,current,boundary-> { });
    }
    StrictBronzeEvidence(Expected expected, Current current, Fault fault) throws IOException {
        need(fault!=null,"INPUT_INVALID"); this.fault=fault;
        validateExpectations(expected,current);
        this.expected=expected; this.current=current;
        root=expected.controlRoot().resolve("runs").resolve(expected.runId()).resolve("attempts").resolve(expected.originalAttemptId());
        directory=root.resolve(address(expected,"STORE_BRONZE",0));
        plan=plan(expected);
        need(sha(plan.ledger()).equals(expected.ledgerSha256()),"INPUT_CHANGED");
        inspect();
        // Pin local parent-owned entries for this invocation, including same-byte replacement.
        operationIdentity=identity(privateEntry(directory,true));
        operationParentIdentity=identity(privateEntry(directory.getParent(),true));
        commandStamp=read(directory.resolve("command.json"),65536).stamp();
        ledgerStamp=read(root.resolve(MANIFEST),LEDGER_LIMIT).stamp();
        inspect();
    }

    static void validateExpectations(Expected expected, Current current) throws IOException {
        need(expected!=null && current!=null,"INPUT_INVALID");
        need(POLICY.equals(expected.policy()) && "ORIGINAL".equals(expected.planKind()),"INPUT_INVALID");
        uuid(expected.installationId()); uuid(expected.runId()); uuid(expected.originalAttemptId());
        need(expected.sourceId()!=null && expected.sourceId().matches("[a-z0-9][a-z0-9_-]{0,127}"),"INPUT_INVALID");
        need(expected.controlRoot()!=null && expected.controlRoot().isAbsolute()
                && expected.controlRoot().normalize().equals(expected.controlRoot()),"INPUT_INVALID");
        identityInput(expected.controlIdentity()); identityInput(expected.rootIdentity()); identityInput(expected.lockIdentity());
        hash(expected.ledgerSha256()); frozen(expected.frozen()); frozen(current.frozen());
        writer(expected,expected.writer()); writer(expected,current.writer());
        need(expected.policy().equals(current.policy()) && expected.frozen().equals(current.frozen())
                && expected.writer().equals(current.writer()),"INPUT_CHANGED");
    }
    private static void writer(Expected expected, Writer value) throws IOException {
        need(value!=null,"INPUT_INVALID");
        uuid(value.installationId()); uuid(value.workerInstanceId()); uuid(value.runId()); uuid(value.attemptId()); uuid(value.checkpointId());
        hash(value.commandSha256());
        need(value.epoch()>0 && value.installationId().equals(expected.installationId())
                && value.runId().equals(expected.runId()) && value.attemptId().equals(expected.originalAttemptId())
                && "STORE_BRONZE".equals(value.stage()) && value.ordinal()==0
                && checkpoint(value.runId(),value.attemptId(),value.stage(),value.ordinal()).equals(value.checkpointId()),"INPUT_INVALID");
    }
    private static void frozen(Frozen value) throws IOException {
        need(value!=null,"INPUT_INVALID"); hash(value.selectionSha256()); hash(value.profileSha256()); hash(value.runtimeSha256());
    }
    private static void identityInput(Identity value) throws IOException {
        need(value!=null && value.device()>=0 && value.inode()>0,"INPUT_INVALID");
    }
    private static void uuid(String value) throws IOException {
        try { need(value!=null && UUID.fromString(value).toString().equals(value),"INPUT_INVALID"); }
        catch(IllegalArgumentException ex) { throw new Failure("INPUT_INVALID"); }
    }
    private static void hash(String value) throws IOException {
        need(value!=null && value.matches("[0-9a-f]{64}"),"INPUT_INVALID");
    }
    static String checkpoint(String run, String attempt, String stage, int ordinal) throws IOException {
        uuid(run); uuid(attempt);
        need(stage!=null && COUNTS.containsKey(stage) && ordinal>=0 && ordinal<COUNTS.get(stage),"INPUT_INVALID");
        String identity=(stage.equals("VERIFY_SILVER")?"SILVER":stage)+":"+ordinal;
        return UUID.nameUUIDFromBytes((POLICY+"\n"+run+"\n"+attempt+"\n"+identity).getBytes(StandardCharsets.UTF_8)).toString();
    }
    /** Pure reconstruction; no bytes or authority are obtained from a filesystem. */
    static byte[] ledger(Expected expected) throws IOException {
        need(expected!=null,"INPUT_INVALID");
        validateExpectations(expected,new Current(expected.policy(),expected.frozen(),expected.writer()));
        return plan(expected).ledger().clone();
    }
    private static String address(Expected expected, String stage, int ordinal) throws IOException {
        return stage.toLowerCase(Locale.ROOT).replace('_','-')+"/"
                +(stage.equals("FETCH_SOURCE")?expected.sourceId()+"/":"")
                +checkpoint(expected.runId(),expected.originalAttemptId(),stage,ordinal);
    }
    private static Map<String,Integer> slots(String stage) {
        int small=65536, half=512*1024, large=1024*1024;
        if(stage.equals("FETCH_SOURCE"))return Map.of("command.json",small,"payload.xml",5*large,
                "metadata.json",small,"descriptor.json",small,"result.json",small,"complete.json",small);
        boolean bronze=Set.of("STORE_BRONZE","INSPECT_BRONZE").contains(stage);
        var slots=new TreeMap<String,Integer>();
        slots.put("command.json",bronze || Set.of("SILVER","VERIFY_SILVER","RECHECK_SILVER").contains(stage)?small:large);
        slots.put("result.json",small); slots.put("verified.json",small);
        if(!bronze)slots.put("child.json",small);
        if(!stage.equals("SILVER"))slots.put("evidence.json",bronze?small:Set.of("VERIFY_SILVER","RECHECK_SILVER").contains(stage)?half:large);
        if(Set.of("PUBLISH_GOLD","VERIFY_RESULT","VERIFY_SILVER").contains(stage))slots.put("selection.json",stage.equals("VERIFY_SILVER")?half:large);
        return slots;
    }
    private static Plan plan(Expected expected) throws IOException {
        var directories=new TreeSet<String>(); directories.add("");
        var files=new TreeMap<String,Integer>();
        for(String stage:new TreeSet<>(COUNTS.keySet()))for(int ordinal=0;ordinal<COUNTS.get(stage);ordinal++) {
            String address=address(expected,stage,ordinal), part="";
            for(String segment:address.split("/")) { part=part.isEmpty()?segment:part+"/"+segment; directories.add(part); }
            for(var slot:slots(stage).entrySet())for(String suffix:List.of("",".preparing"))files.put(address+"/"+slot.getKey()+suffix,slot.getValue());
        }
        var value=new TreeMap<String,Object>();
        value.put("version",2); value.put("treeKind","ATTEMPT"); value.put("runId",expected.runId());
        value.put("attemptId",expected.originalAttemptId()); value.put("rootDevice",Long.toString(expected.rootIdentity().device()));
        value.put("rootInode",Long.toString(expected.rootIdentity().inode())); value.put("reservationPolicy",expected.policy());
        value.put("planKind",expected.planKind()); value.put("sourceId",expected.sourceId());
        value.put("directories",directories); value.put("files",files);
        byte[] ledger=canonical(value);
        long reserved=2L*LEDGER_LIMIT; for(int cap:files.values())reserved+=cap;
        need(reserved==BYTE_LIMIT && directories.size()+files.size()+3==ENTRY_LIMIT && ledger.length<=LEDGER_LIMIT,"RESOURCE_LIMIT");
        var inventory=new ArrayList<Object>(); String prefix="runs/"+expected.runId()+"/attempts/"+expected.originalAttemptId();
        for(String name:directories)inventory.add(inventoryEntry(prefix+(name.isEmpty()?"":"/"+name),true));
        for(String name:files.keySet())inventory.add(inventoryEntry(prefix+"/"+name,false));
        for(String name:List.of(MANIFEST,MANIFEST+".preparing",LOCK))inventory.add(inventoryEntry(prefix+"/"+name,false));
        need(canonical(Map.of("processEvidenceIncomplete",true,"files",inventory)).length<=INVENTORY_LIMIT,"RESOURCE_LIMIT");
        return new Plan(Set.copyOf(directories),Map.copyOf(files),ledger);
    }
    private static Map<String,Object> inventoryEntry(String address, boolean directory) {
        var entry=new TreeMap<String,Object>(); entry.put("address",address); entry.put("device","9".repeat(20));
        entry.put("inode","9".repeat(20)); entry.put("modifiedAt","x".repeat(64)); entry.put("directory",directory);
        if(!directory) { entry.put("length","9".repeat(20)); entry.put("sha256","f".repeat(64)); }
        return entry;
    }
    private static byte[] canonical(Object value) throws IOException {
        return StrictFetchJson.canonical(node(value));
    }
    private static JsonNode node(Object value) throws IOException {
        if(value instanceof Map<?,?> map) {
            var result=StrictFetchJson.object();
            for(var entry:map.entrySet())result.set((String)entry.getKey(),node(entry.getValue()));
            return result;
        }
        if(value instanceof Collection<?> values) {
            ArrayNode result=JsonNodeFactory.instance.arrayNode();
            for(Object item:values)result.add(node(item)); return result;
        }
        if(value instanceof String text)return JsonNodeFactory.instance.textNode(text);
        if(value instanceof Integer number)return JsonNodeFactory.instance.numberNode(number);
        if(value instanceof Boolean bool)return JsonNodeFactory.instance.booleanNode(bool);
        throw new Failure("INPUT_INVALID");
    }

    Path directory() { return directory; }
    byte[] readCommand() throws IOException { return locked(this::inspectUnlocked); }
    void inspect() throws IOException { locked(()-> { inspectUnlocked(); return null; }); }
    /** Candidate readback while this invocation still owns the empty result prefix. */
    byte[] readCandidate(String name) throws IOException {
        need(Set.of("evidence.json").contains(name),"INPUT_INVALID");
        return locked(()-> {
            inspectUnlocked();
            byte[] bytes=read(directory.resolve(name),slots("STORE_BRONZE").get(name)).bytes();
            inspectUnlocked(); return bytes;
        });
    }
    void requireFresh() throws IOException { locked(()-> { freshUnlocked(); return null; }); }
    /**
     * Consume the already reserved result prefix durably before network work. The
     * shared ledger lock is released during the callback. Only this invocation
     * may finish its retained open prefix; a restart never adopts it. The marker
     * indicates possible object-store work, not success or managed execution authority.
     */
    <T> T runFresh(CheckedAction<T> action) throws Exception {
        need(action!=null,"INPUT_INVALID");
        boolean[] started={false};
        try {
            locked(()-> {
                need(sessionThread==null,"INPUT_CHANGED");
                sessionThread=Thread.currentThread(); started[0]=true; freshUnlocked();
                Path marker=directory.resolve("result.json.preparing");
                markerChannel=FileChannel.open(marker,Set.of(StandardOpenOption.READ,StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE_NEW,LinkOption.NOFOLLOW_LINKS),
                        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
                fault.at(Boundary.MARKER_CREATED);
                markerChannel.force(true); fault.at(Boundary.MARKER_FORCED);
                forceDirectory(); fault.at(Boundary.MARKER_DIRECTORY_FORCED);
                markerStamp=privateEntry(marker,false);
                need(number(markerStamp,"size")==0 && markerChannel.size()==0,"INPUT_CHANGED");
                checkPaths(); sessionThread=Thread.currentThread(); return null;
            });
            T result=action.run();
            locked(()-> { need(markerStamp==null,"INPUT_CHANGED"); inspectUnlocked(); return null; });
            return result;
        } finally {
            synchronized(LOCAL_LOCK) {
                if(started[0]) {
                    sessionThread=null; markerStamp=null;
                    if(markerChannel!=null) { try { markerChannel.close(); } finally { markerChannel=null; } }
                }
            }
        }
    }
    private void freshUnlocked() throws IOException {
        inspectUnlocked();
        try(var children=Files.newDirectoryStream(directory)) {
            for(Path child:children)need(child.getFileName().toString().equals("command.json"),"INPUT_CHANGED");
        }
    }
    private <T> T locked(IoAction<T> action) throws IOException {
        synchronized(LOCAL_LOCK) {
            try {
                Map<String,Object> before=checkPaths();
                try(var channel=FileChannel.open(root.resolve(LOCK),StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS)) {
                    need(before.equals(checkPaths()) && channel.size()==0,"INPUT_CHANGED");
                    try(var lock=channel.lock()) {
                        need(lock.isValid() && before.equals(checkPaths()),"INPUT_CHANGED");
                        try { return action.run(); } finally { need(before.equals(checkPaths()),"INPUT_CHANGED"); }
                    }
                }
            } catch(Failure ex) { throw ex; }
            catch(IOException | RuntimeException ex) { throw new Failure("EVIDENCE_UNAVAILABLE"); }
        }
    }
    private Map<String,Object> checkPaths() throws IOException {
        validateExpectations(expected,current);
        need(identity(privateEntry(expected.controlRoot(),true)).equals(expected.controlIdentity()),"INPUT_CHANGED");
        var ancestry=new LinkedHashMap<String,Identity>();
        Path path=expected.controlRoot(); ancestry.put(path.toString(),expected.controlIdentity());
        for(Path segment:path.relativize(root)) {
            path=path.resolve(segment); ancestry.put(path.toString(),identity(privateEntry(path,true)));
        }
        need(identity(privateEntry(root,true)).equals(expected.rootIdentity()),"INPUT_CHANGED");
        var lock=privateEntry(root.resolve(LOCK),false);
        need(identity(lock).equals(expected.lockIdentity()) && number(lock,"size")==0,"INPUT_CHANGED");
        if(operationIdentity!=null)need(operationIdentity.equals(identity(privateEntry(directory,true)))
                && operationParentIdentity.equals(identity(privateEntry(directory.getParent(),true))),"INPUT_CHANGED");
        var result=new HashMap<String,Object>(lock); result.put("ancestry",Map.copyOf(ancestry)); return result;
    }
    private byte[] inspectUnlocked() throws IOException {
        Read ledger=read(root.resolve(MANIFEST),LEDGER_LIMIT);
        need(Arrays.equals(ledger.bytes(),plan.ledger()) && sha(ledger.bytes()).equals(expected.ledgerSha256()),"INPUT_CHANGED");
        long total=0; int entries=0; var pending=new ArrayDeque<Path>(); pending.add(root);
        while(!pending.isEmpty()) {
            Path path=pending.removeLast(); String relative=root.relativize(path).toString();
            need(++entries<=ENTRY_LIMIT,"RESOURCE_LIMIT");
            var attributes=stamp(path); boolean isDirectory=(number(attributes,"mode")&0170000)==0040000;
            attributes=privateEntry(path,isDirectory);
            if(isDirectory) {
                need(plan.directories().contains(relative),"INPUT_CHANGED");
                try(var children=Files.newDirectoryStream(path)) {
                    for(Path child:children) { need(entries+pending.size()<ENTRY_LIMIT,"RESOURCE_LIMIT"); pending.add(child); }
                }
            } else {
                if(relative.endsWith(".preparing")) {
                    need(ownsMarker() && path.equals(directory.resolve("result.json.preparing"))
                            && markerStamp.equals(attributes) && markerChannel.size()==0,"INPUT_CHANGED");
                }
                Integer cap=relative.equals(LOCK)?0:relative.equals(MANIFEST)?LEDGER_LIMIT:plan.files().get(relative);
                need(cap!=null,"INPUT_CHANGED"); need(number(attributes,"size")<=cap,"RESOURCE_LIMIT");
                total+=number(attributes,"size"); need(total<=BYTE_LIMIT,"RESOURCE_LIMIT");
            }
        }
        Read commandRead=read(directory.resolve("command.json"),65536);
        byte[] command=commandRead.bytes();
        need(sha(command).equals(expected.writer().commandSha256())
                && (commandStamp==null || commandStamp.equals(commandRead.stamp()))
                && (ledgerStamp==null || ledgerStamp.equals(ledger.stamp())),"INPUT_CHANGED");
        Read after=read(root.resolve(MANIFEST),LEDGER_LIMIT);
        need(Arrays.equals(ledger.bytes(),after.bytes()) && ledger.stamp().equals(after.stamp()),"INPUT_CHANGED");
        checkPaths(); return command;
    }
    void write(String name, byte[] data) throws IOException {
        need(name!=null && WRITABLE.contains(name) && data!=null,"INPUT_INVALID");
        need(data.length<=slots("STORE_BRONZE").get(name),"RESOURCE_LIMIT");
        byte[] bytes=data.clone();
        locked(()-> {
            need(ownsMarker(),"INPUT_CHANGED");
            inspectUnlocked(); Path target=directory.resolve(name), temporary=directory.resolve(name+".preparing");
            if(name.equals("result.json") && ownsMarker()) {
                need(absent(target) && markerStamp.equals(privateEntry(temporary,false))
                        && markerChannel.size()==0,"INPUT_CHANGED");
                var buffer=ByteBuffer.wrap(bytes); markerChannel.position(0);
                while(buffer.hasRemaining())markerChannel.write(buffer); markerChannel.force(true);
                need(Arrays.equals(read(temporary,bytes.length).bytes(),bytes),"INPUT_CHANGED");
                need(identity(privateEntry(temporary,false)).equals(identity(markerStamp)),"INPUT_CHANGED");
                forceDirectory(); checkPaths();
                Files.createLink(target,temporary); fault.at(Boundary.RESULT_LINKED); forceDirectory(); Files.delete(temporary); forceDirectory();
                markerStamp=null;
                need(Arrays.equals(read(target,bytes.length).bytes(),bytes),"INPUT_CHANGED");
                inspectUnlocked(); return null;
            }
            need(absent(target) && absent(temporary),"INPUT_CHANGED");
            try(var output=FileChannel.open(temporary,Set.of(StandardOpenOption.WRITE,StandardOpenOption.CREATE_NEW,LinkOption.NOFOLLOW_LINKS),
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))) {
                var buffer=ByteBuffer.wrap(bytes); while(buffer.hasRemaining())output.write(buffer); output.force(true);
            }
            need(Arrays.equals(read(temporary,bytes.length).bytes(),bytes),"INPUT_CHANGED");
            forceDirectory(); checkPaths();
            // No-replace publication. A crash between link and unlink leaves nlink=2
            // and a prefix; every subsequent invocation refuses without cleanup.
            Files.createLink(target,temporary); fault.at(Boundary.EVIDENCE_LINKED); forceDirectory(); Files.delete(temporary); forceDirectory();
            need(Arrays.equals(read(target,bytes.length).bytes(),bytes),"INPUT_CHANGED");
            inspectUnlocked(); return null;
        });
    }
    private boolean ownsMarker() {
        return sessionThread==Thread.currentThread() && markerChannel!=null && markerStamp!=null;
    }
    private void forceDirectory() throws IOException {
        privateEntry(directory,true);
        try(var channel=FileChannel.open(directory,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)) { channel.force(true); }
    }
    private static boolean absent(Path path) throws IOException {
        try { stamp(path); return false; } catch(NoSuchFileException ex) { return true; }
    }
    private static Map<String,Object> stamp(Path path) throws IOException {
        return Files.readAttributes(path,"unix:dev,ino,mode,uid,gid,nlink,size,lastModifiedTime,ctime",LinkOption.NOFOLLOW_LINKS);
    }
    private static Map<String,Object> privateEntry(Path path, boolean directory) throws IOException {
        var attributes=stamp(path); long mode=number(attributes,"mode");
        need(path.isAbsolute() && path.equals(path.toRealPath())
                && (mode&0170000)==(directory?0040000:0100000) && (mode&0077)==0
                && number(attributes,"uid")==new UnixSystem().getUid()
                && (directory || number(attributes,"nlink")==1),"EVIDENCE_UNAVAILABLE");
        return attributes;
    }
    private static Identity identity(Map<String,Object> attributes) { return new Identity(number(attributes,"dev"),number(attributes,"ino")); }
    private static long number(Map<String,Object> attributes, String key) { return ((Number)attributes.get(key)).longValue(); }
    private static Read read(Path path, int cap) throws IOException {
        var before=privateEntry(path,false); need(number(before,"size")<=cap,"RESOURCE_LIMIT");
        byte[] bytes;
        try(var channel=FileChannel.open(path,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)) {
            need(before.equals(privateEntry(path,false)) && channel.size()==number(before,"size"),"INPUT_CHANGED");
            var buffer=ByteBuffer.allocate((int)number(before,"size")+1);
            while(buffer.hasRemaining() && channel.read(buffer)!=-1) { /* bounded */ }
            bytes=Arrays.copyOf(buffer.array(),buffer.position());
            need(bytes.length==number(before,"size") && bytes.length<=cap && channel.size()==bytes.length
                    && before.equals(privateEntry(path,false)),"INPUT_CHANGED");
        }
        return new Read(bytes,before);
    }
    private static String sha(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch(NoSuchAlgorithmException ex) { throw new IllegalStateException("SHA256_UNAVAILABLE"); }
    }
    private static void need(boolean value, String code) throws Failure { if(!value)throw new Failure(code); }
}
