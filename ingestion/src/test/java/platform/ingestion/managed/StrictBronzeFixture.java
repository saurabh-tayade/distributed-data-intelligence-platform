package platform.ingestion.managed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import platform.ingestion.bronze.BronzeKeys;

/** Test-only original journal/parent bootstrap and real SDK loopback object-store fixture. */
final class StrictBronzeFixture implements AutoCloseable {
    final StrictFetchFixture fetched;
    final HttpServer server;
    final Path root,directory,credentials;
    final ObjectNode packet,command;
    final Map<String,byte[]> objects=new ConcurrentHashMap<>();
    final Map<String,String> types=new ConcurrentHashMap<>();
    final AtomicInteger gets=new AtomicInteger(),puts=new AtomicInteger(),unexpected=new AtomicInteger();
    final String payloadPath,metadataPath;
    final byte[] payload,metadata,ledger;
    volatile String error;
    volatile String failPut;
    volatile boolean acknowledgeLost;
    volatile IntConsumer beforeGet=n->{};
    volatile IntConsumer afterPut=n->{};
    volatile CountDownLatch entered,release;

    StrictBronzeFixture(Path temporary)throws Exception {
        fetched=new StrictFetchFixture(temporary);root=fetched.root;
        fetched.packet.put("childPid",Long.toString(ProcessHandle.current().pid()));
        if(StrictManagedRssFetch.execute(new ByteArrayInputStream(StrictFetchJson.canonical(fetched.packet)))!=0)throw new AssertionError("FETCH_FIXTURE");
        var marker=StrictFetchJson.object().put("journalCompletionVersion",1).put("directoryIdentity",identity(fetched.directory));
        var files=marker.putArray("files");
        for(String name:List.of("command.json","descriptor.json","metadata.json","payload.xml","result.json")){
            Path path=fetched.directory.resolve(name);byte[] bytes=Files.readAllBytes(path);
            var entry=files.addObject().put("name",name).put("byteLength",bytes.length).put("sha256",StrictFetchJson.sha(bytes));entry.set("stamp",stamp(path));
        }
        StrictFetchFixture.privateWrite(fetched.directory.resolve("complete.json"),StrictFetchJson.canonical(marker));
        var descriptor=StrictFetchJson.parse(Files.readAllBytes(fetched.directory.resolve("descriptor.json")),65536);
        var reference=StrictFetchJson.object().put("completionSha256",StrictFetchJson.sha(Files.readAllBytes(fetched.directory.resolve("complete.json"))));
        reference.set("bronze",descriptor.get("expected").deepCopy());reference.set("completionStamp",stamp(fetched.directory.resolve("complete.json")));
        var feed=ManagedRssFetch.reopen(fetched.directory,fetched.command);var keys=BronzeKeys.forFeed(feed);
        payload=feed.body();metadata=Files.readAllBytes(fetched.directory.resolve("metadata.json"));
        payloadPath="/strict-bronze/"+keys.payload();metadataPath="/strict-bronze/"+keys.metadata();
        server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/",exchange->{
            try {
                String path=exchange.getRequestURI().getPath(),method=exchange.getRequestMethod();
                if(method.equals("GET")){
                    int number=gets.incrementAndGet();beforeGet.accept(number);
                    if(entered!=null)entered.countDown();
                    if(entered!=null && release!=null)try{release.await();}catch(InterruptedException ex){Thread.currentThread().interrupt();}
                    byte[] value=objects.get(path);
                    if(error!=null || value==null){String code=error==null?"NoSuchKey":error;byte[] bytes=("<Error><Code>"+code+"</Code><Message>Synthetic</Message></Error>").getBytes(StandardCharsets.UTF_8);exchange.getResponseHeaders().set("Content-Type","application/xml");exchange.sendResponseHeaders(code.equals("AccessDenied")?403:404,bytes.length);exchange.getResponseBody().write(bytes);}
                    else{exchange.getResponseHeaders().set("Content-Type",types.getOrDefault(path,path.equals(payloadPath)?"application/octet-stream":"application/json"));exchange.sendResponseHeaders(200,value.length);exchange.getResponseBody().write(value);}
                } else if(method.equals("PUT")){
                    int number=puts.incrementAndGet();byte[] value=exchange.getRequestBody().readAllBytes();
                    if(path.equals(failPut)){byte[] bytes="<Error><Code>InternalError</Code></Error>".getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(500,bytes.length);exchange.getResponseBody().write(bytes);}
                    else{objects.put(path,value);types.put(path,exchange.getRequestHeaders().getFirst("Content-Type"));afterPut.accept(number);if(!acknowledgeLost)exchange.sendResponseHeaders(200,-1);}
                } else{unexpected.incrementAndGet();exchange.sendResponseHeaders(400,-1);}
            }catch(java.io.IOException ignored){/* Deliberately killed/ambiguous clients close the socket. */}
            finally{exchange.close();}
        });server.start();
        credentials=temporary.resolve("bronze-credentials.json");StrictFetchFixture.privateWrite(credentials,"{\"accessKey\":\"fixture-access\",\"secretKey\":\"fixture-secret\"}".getBytes(StandardCharsets.UTF_8));
        var config=StrictFetchJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:"+server.getAddress().getPort()).put("region","us-east-1").put("bucket","strict-bronze").put("credentialsFile",credentials.toString());
        var storage=StrictFetchJson.object().put("bindingId","bronze").put("bindingSha256",StrictFetchJson.sha(StrictFetchJson.canonical(config))).put("credentialsSha256",StrictFetchJson.sha(Files.readAllBytes(credentials)));storage.set("configuration",config);storage.set("credentialsStamp",stamp(credentials));
        var facts=StrictFetchJson.object().put("checkpointId",fetched.commandNode.get("checkpointId").textValue()).put("commandSha256",StrictFetchJson.sha(fetched.command)).put("descriptorSha256",StrictFetchJson.sha(Files.readAllBytes(fetched.directory.resolve("descriptor.json"))));facts.set("reference",reference);
        for(String key:List.of("revision","capturedConfigSha256","endpointSha256","runtimeSha256","feedUri"))facts.set(key,fetched.commandNode.get(key).deepCopy());
        var bronze=StrictFetchJson.object().put("runtimeSha256","1".repeat(64));bronze.set("storage",storage);bronze.set("fetch",facts);
        String run=fetched.commandNode.get("runId").textValue(),attempt=fetched.commandNode.get("attemptId").textValue();
        String checkpoint=StrictFetchEvidence.checkpoint(run,attempt,"STORE_BRONZE",0);directory=root.resolve("store-bronze/"+checkpoint);Files.createDirectories(directory);Files.setPosixFilePermissions(directory.getParent(),PosixFilePermissions.fromString("rwx------"));Files.setPosixFilePermissions(directory,PosixFilePermissions.fromString("rwx------"));
        command=StrictFetchJson.object().put("protocolVersion",1).put("runId",run).put("attemptId",attempt).put("ownerEpoch",3).put("stage","STORE_BRONZE").put("itemOrdinal",0).put("checkpointId",checkpoint).put("mode","PUBLISH").put("runtimeSha256","1".repeat(64));
        var journal=command.putObject("journal").put("directory",fetched.directory.toString()).put("commandSha256",facts.get("commandSha256").textValue()).put("descriptorSha256",facts.get("descriptorSha256").textValue());journal.set("reference",reference.deepCopy());command.set("storage",storage.deepCopy());
        var expected=fetched.packet.get("expected").deepCopy();var current=fetched.packet.get("current").deepCopy();
        for(var n:List.of(expected,current)){var writer=(ObjectNode)n.get("writer");writer.put("stage","STORE_BRONZE").put("checkpointId",checkpoint).put("commandSha256",StrictFetchJson.sha(StrictFetchJson.canonical(command)));}
        packet=StrictFetchJson.object().put("bootstrapVersion",1).put("mode","STORE_BRONZE_CANDIDATE").put("childPid","1").put("strictRuntimeSha256",StrictFetchJson.sha(StrictFetchJson.canonical(StrictManagedBronze.runtimeIdentity())));
        packet.set("expected",expected);packet.set("current",current);packet.set("bronze",bronze);
        StrictFetchFixture.privateWrite(directory.resolve("command.json"),StrictFetchJson.canonical(command));ledger=Files.readAllBytes(root.resolve(".content-evidence.json"));
    }
    byte[] packet(Process child)throws Exception{packet.put("childPid",Long.toString(child.pid()));return StrictFetchJson.canonical(packet);}
    void pair(){objects.put(payloadPath,payload.clone());objects.put(metadataPath,metadata.clone());}
    JsonNode evidence()throws Exception{return StrictFetchJson.parse(Files.readAllBytes(directory.resolve("evidence.json")),65536);}
    JsonNode result()throws Exception{return StrictFetchJson.parse(Files.readAllBytes(directory.resolve("result.json")),65536);}
    static String identity(Path path)throws Exception{return Files.getAttribute(path,"unix:dev",LinkOption.NOFOLLOW_LINKS)+":"+Files.getAttribute(path,"unix:ino",LinkOption.NOFOLLOW_LINKS);}
    static ObjectNode stamp(Path path)throws Exception{return StrictFetchJson.object().put("identity",identity(path)).put("modified",Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS).toString()).put("size",Files.size(path));}
    @Override public void close(){if(release!=null)release.countDown();server.stop(0);fetched.close();}
}
