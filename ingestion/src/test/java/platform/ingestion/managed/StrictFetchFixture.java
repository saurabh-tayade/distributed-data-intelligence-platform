package platform.ingestion.managed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** Synthetic parent expectations are fixed before reading any child command or evidence. */
final class StrictFetchFixture implements AutoCloseable {
    static final ObjectMapper JSON = new ObjectMapper();
    final HttpServer server;
    final AtomicInteger requests = new AtomicInteger();
    final Path control, root, directory;
    final ObjectNode packet, commandNode;
    final byte[] command;
    volatile byte[] body = {'<','f','e','e','d','>',0,(byte)255,'\r','\n'};
    volatile int status = 200;
    volatile String location;
    volatile java.util.concurrent.CountDownLatch entered, release;

    StrictFetchFixture(Path temporary) throws Exception {
        server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/feed",exchange->{
            requests.incrementAndGet();
            if(entered!=null)entered.countDown();
            if(release!=null)try { release.await(); } catch(InterruptedException ex) { Thread.currentThread().interrupt(); }
            try {
                exchange.getResponseHeaders().set("Content-Type","application/atom+xml; charset=UTF-8");
                if(location!=null)exchange.getResponseHeaders().set("Location",location);
                exchange.sendResponseHeaders(status,body.length);
                exchange.getResponseBody().write(body);
            } catch(java.io.IOException ignored) { /* Kill/oversize tests deliberately close the socket. */ }
            finally { exchange.close(); }
        });
        server.start();
        String installation=UUID.randomUUID().toString(), run=UUID.randomUUID().toString(), attempt=UUID.randomUUID().toString();
        String checkpoint=StrictFetchEvidence.checkpoint(run,attempt,"FETCH_SOURCE",0);
        control=temporary.toRealPath().resolve("control");
        root=control.resolve("runs/"+run+"/attempts/"+attempt);
        directory=root.resolve("fetch-source/demo-feed/"+checkpoint);
        Files.createDirectories(directory);
        for(Path p=directory; p.startsWith(control); p=p.getParent())Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rwx------"));
        privateWrite(root.resolve(".content-evidence.lock"),new byte[0]);
        String uri="http://127.0.0.1:"+server.getAddress().getPort()+"/feed";
        commandNode=JSON.createObjectNode().put("protocolVersion",1).put("runId",run).put("attemptId",attempt)
                .put("ownerEpoch",3).put("stage","FETCH_SOURCE").put("itemOrdinal",0).put("checkpointId",checkpoint)
                .put("sourceId","demo-feed").put("revision",7).put("capturedConfigSha256","a".repeat(64))
                .put("endpointSha256","b".repeat(64)).put("feedUri",uri).put("runtimeSha256","c".repeat(64))
                .put("javaRuntime",Runtime.version().toString()).put("testEndpoint",true).put("httpTimeoutMillis",10000);
        command=canonical(commandNode);
        // CONTENT and independently registered FETCH runtime digests are intentionally distinct.
        var frozen=new StrictFetchEvidence.Frozen("d".repeat(64),"e".repeat(64),"f".repeat(64));
        var writer=new StrictFetchEvidence.Writer(installation,UUID.randomUUID().toString(),3,run,attempt,"FETCH_SOURCE",0,checkpoint,sha(command));
        var expected=new StrictFetchEvidence.Expected(installation,StrictFetchEvidence.POLICY,"ORIGINAL",run,attempt,"demo-feed",control,
                identity(control),identity(root),identity(root.resolve(".content-evidence.lock")),"0".repeat(64),frozen,writer);
        byte[] ledger=StrictFetchEvidence.ledger(expected);
        privateWrite(root.resolve(".content-evidence.json"),ledger);
        privateWrite(directory.resolve("command.json"),command);
        ObjectNode e=JSON.createObjectNode().put("installationId",installation).put("policy",StrictFetchEvidence.POLICY).put("planKind","ORIGINAL")
                .put("runId",run).put("originalAttemptId",attempt).put("sourceId","demo-feed").put("controlRoot",control.toString()).put("ledgerSha256",sha(ledger));
        e.set("controlIdentity",identityJson(control)); e.set("rootIdentity",identityJson(root));e.set("lockIdentity",identityJson(root.resolve(".content-evidence.lock")));
        ObjectNode f=JSON.createObjectNode().put("selectionSha256",frozen.selectionSha256()).put("profileSha256",frozen.profileSha256()).put("runtimeSha256",frozen.runtimeSha256());
        ObjectNode w=JSON.createObjectNode().put("installationId",installation).put("workerInstanceId",writer.workerInstanceId()).put("epoch","3")
                .put("runId",run).put("attemptId",attempt).put("stage","FETCH_SOURCE").put("ordinal",0).put("checkpointId",checkpoint).put("commandSha256",sha(command));
        e.set("frozen",f);e.set("writer",w);
        ObjectNode current=JSON.createObjectNode().put("policy",StrictFetchEvidence.POLICY);
        current.set("frozen",f.deepCopy());current.set("writer",w.deepCopy());
        ObjectNode fetch=JSON.createObjectNode();
        for(String field:new String[]{"revision","capturedConfigSha256","endpointSha256","feedUri","runtimeSha256","javaRuntime","testEndpoint","httpTimeoutMillis"})fetch.set(field,commandNode.get(field).deepCopy());
        packet=JSON.createObjectNode().put("bootstrapVersion",1).put("mode","FETCH_SOURCE_CANDIDATE").put("childPid","1");
        packet.set("expected",e);packet.set("current",current);packet.set("fetch",fetch);
    }
    byte[] packet(Process child) throws Exception { packet.put("childPid",Long.toString(child.pid()));return canonical(packet); }
    static StrictFetchEvidence.Identity identity(Path path) throws Exception {
        return new StrictFetchEvidence.Identity(((Number)Files.getAttribute(path,"unix:dev")).longValue(),((Number)Files.getAttribute(path,"unix:ino")).longValue());
    }
    static ObjectNode identityJson(Path path) throws Exception {
        var i=identity(path);return JSON.createObjectNode().put("device",Long.toString(i.device())).put("inode",Long.toString(i.inode()));
    }
    static void privateWrite(Path path,byte[] bytes) throws Exception {
        Files.write(path,bytes);Files.setPosixFilePermissions(path,PosixFilePermissions.fromString("rw-------"));
    }
    static byte[] canonical(JsonNode node) throws Exception { return JSON.writeValueAsBytes(sorted(node)); }
    private static Object sorted(JsonNode n) {
        if(n.isObject()) { var result=new TreeMap<String,Object>();n.properties().forEach(e->result.put(e.getKey(),sorted(e.getValue())));return result; }
        if(n.isTextual())return n.textValue();if(n.isBoolean())return n.booleanValue();if(n.isIntegralNumber())return n.bigIntegerValue();
        if(n.isNull())return null;throw new IllegalArgumentException("test packet type");
    }
    static String sha(byte[] bytes) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
    @Override public void close() { if(release!=null)release.countDown();server.stop(0); }
}
