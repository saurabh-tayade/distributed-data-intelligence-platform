package platform.ingestion.managed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import platform.ingestion.bronze.*;
import platform.ingestion.rss.*;
import static platform.ingestion.managed.StrictFetchJson.*;

/** Inactive ledger-v2 Bronze candidate child. Bootstrap correspondence is not durable authority. */
public final class StrictManagedBronze {
    static final String ERROR="STRICT_BRONZE_REJECTED";
    static final Set<String> FETCH_FILES=Set.of("command.json","payload.xml","metadata.json","descriptor.json","result.json");
    record Bootstrap(StrictBronzeEvidence.Expected expected,StrictBronzeEvidence.Current current,JsonNode bronze){
        Bootstrap{bronze=bronze.deepCopy();}
        @Override public JsonNode bronze(){return bronze.deepCopy();}
        @Override public String toString(){return "StrictBronzeBootstrap[redacted; not authority]";}
    }
    private static final ObjectMapper CREDENTIALS=new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private record Dependency(RawFeed feed,RssSource source,byte[] metadata,JsonNode descriptor,byte[] credentials){
        @Override public String toString(){return "BronzeDependency[redacted]";}
    }
    public static void main(String[] args){
        int status=2;
        try{
            need(args.length==0);
            int mode=((Number)Files.getAttribute(Path.of("/proc/self/fd/0"),"unix:mode")).intValue();
            need((mode&0170000)==0010000);status=execute(System.in);
        }catch(Exception ignored){System.err.println(ERROR);}
        System.exit(status);
    }
    /** The framing contract is identical; the packet and adapter identity are separate. */
    static byte[] receive(InputStream input)throws IOException{return StrictManagedRssFetch.receive(input);}
    /** Protocol identity only. Installed JAR/manifest qualification belongs to a future parent. */
    static ObjectNode runtimeIdentity(){return object().put("strictBronzeRuntimeVersion",1)
            .put("adapter","platform.ingestion.managed.StrictManagedBronze").put("javaRuntime",Runtime.version().toString());}
    static Bootstrap parse(byte[] packet)throws IOException{
        try{
            var n=StrictFetchJson.parse(packet,65535);
            fields(n,"bootstrapVersion","mode","childPid","expected","current","strictRuntimeSha256","bronze");
            need(n.get("bootstrapVersion").isInt() && n.get("bootstrapVersion").intValue()==1
                    && text(n,"mode").equals("STORE_BRONZE_CANDIDATE") && decimal(n,"childPid",true)==ProcessHandle.current().pid()
                    && Runtime.version().feature()==21 && text(n,"strictRuntimeSha256").equals(sha(canonical(runtimeIdentity()))));
            var e=n.get("expected");var c=n.get("current");
            fields(e,"installationId","policy","planKind","runId","originalAttemptId","sourceId","controlRoot",
                    "controlIdentity","rootIdentity","lockIdentity","ledgerSha256","frozen","writer");
            fields(c,"policy","frozen","writer");String root=text(e,"controlRoot");Path path=Path.of(root);
            need(path.isAbsolute() && path.normalize().equals(path) && path.toString().equals(root));
            var expected=new StrictBronzeEvidence.Expected(text(e,"installationId"),text(e,"policy"),text(e,"planKind"),text(e,"runId"),
                    text(e,"originalAttemptId"),text(e,"sourceId"),path,identity(e.get("controlIdentity")),identity(e.get("rootIdentity")),
                    identity(e.get("lockIdentity")),text(e,"ledgerSha256"),frozen(e.get("frozen")),writer(e.get("writer")));
            var current=new StrictBronzeEvidence.Current(text(c,"policy"),frozen(c.get("frozen")),writer(c.get("writer")));
            StrictBronzeEvidence.validateExpectations(expected,current);need(expected.writer().epoch()>0);
            var bronze=n.get("bronze");fields(bronze,"runtimeSha256","storage","fetch");hash(text(bronze,"runtimeSha256"));
            var fetch=bronze.get("fetch");fields(fetch,"checkpointId","commandSha256","descriptorSha256","reference","revision",
                    "capturedConfigSha256","endpointSha256","runtimeSha256","feedUri");
            need(text(fetch,"checkpointId").equals(StrictBronzeEvidence.checkpoint(expected.runId(),expected.originalAttemptId(),"FETCH_SOURCE",0))
                    && !text(fetch,"checkpointId").equals(expected.writer().checkpointId())
                    && fetch.get("revision").isInt() && fetch.get("revision").intValue()>0);
            for(String key:List.of("commandSha256","descriptorSha256","capturedConfigSha256","endpointSha256","runtimeSha256"))hash(text(fetch,key));
            URI source=URI.create(text(fetch,"feedUri"));need(Set.of("http","https").contains(source.getScheme()) && source.getHost()!=null
                    && source.getRawUserInfo()==null && source.getRawFragment()==null && source.getRawQuery()==null);
            reference(fetch.get("reference"));storage(bronze.get("storage"));
            return new Bootstrap(expected,current,bronze);
        }catch(RuntimeException ex){throw invalid();}
    }
    private static StrictBronzeEvidence.Identity identity(JsonNode n)throws IOException{fields(n,"device","inode");return new StrictBronzeEvidence.Identity(decimal(n,"device",false),decimal(n,"inode",true));}
    private static StrictBronzeEvidence.Frozen frozen(JsonNode n)throws IOException{fields(n,"selectionSha256","profileSha256","runtimeSha256");return new StrictBronzeEvidence.Frozen(text(n,"selectionSha256"),text(n,"profileSha256"),text(n,"runtimeSha256"));}
    private static StrictBronzeEvidence.Writer writer(JsonNode n)throws IOException{
        fields(n,"installationId","workerInstanceId","epoch","runId","attemptId","stage","ordinal","checkpointId","commandSha256");need(n.get("ordinal").isInt());
        return new StrictBronzeEvidence.Writer(text(n,"installationId"),text(n,"workerInstanceId"),decimal(n,"epoch",true),text(n,"runId"),text(n,"attemptId"),text(n,"stage"),n.get("ordinal").intValue(),text(n,"checkpointId"),text(n,"commandSha256"));
    }
    private static void hash(String value)throws IOException{need(value.matches("[0-9a-f]{64}"));}
    private static void stampShape(JsonNode n)throws IOException{
        fields(n,"identity","modified","size");need(text(n,"identity").matches("[0-9]+:[0-9]+") && !text(n,"modified").isEmpty()
                && text(n,"modified").length()<=64 && n.get("size").isIntegralNumber() && n.get("size").canConvertToLong() && n.get("size").longValue()>=0);
    }
    private static void reference(JsonNode n)throws IOException{
        fields(n,"bronze","completionSha256","completionStamp");hash(text(n,"completionSha256"));stampShape(n.get("completionStamp"));
        need(n.get("completionStamp").get("size").longValue()>0 && n.get("completionStamp").get("size").longValue()<=65536);
        var b=n.get("bronze");fields(b,"payloadKey","metadataKey","metadataByteLength","metadataSha256");
        need(!text(b,"payloadKey").isEmpty() && !text(b,"metadataKey").isEmpty() && b.get("metadataByteLength").isInt()
                && b.get("metadataByteLength").intValue()>0 && b.get("metadataByteLength").intValue()<=65536);hash(text(b,"metadataSha256"));
    }
    private static void storage(JsonNode s)throws IOException{
        fields(s,"bindingId","bindingSha256","configuration","credentialsSha256","credentialsStamp");
        need(text(s,"bindingId").matches("[A-Za-z0-9_-]{1,128}"));hash(text(s,"bindingSha256"));hash(text(s,"credentialsSha256"));
        stampShape(s.get("credentialsStamp"));need(s.get("credentialsStamp").get("size").longValue()>0 && s.get("credentialsStamp").get("size").longValue()<=8192);
        var c=s.get("configuration");fields(c,"importBronzeVersion","endpoint","region","bucket","credentialsFile");
        need(c.get("importBronzeVersion").isInt() && c.get("importBronzeVersion").intValue()==1 && sha(canonical(c)).equals(text(s,"bindingSha256")));
        URI uri=URI.create(text(c,"endpoint"));
        need(uri.getScheme().equals("http") && Set.of("127.0.0.1","[::1]").contains(uri.getHost()) && uri.getPort()>0 && uri.getPort()<=65535
                && uri.getRawUserInfo()==null && uri.getRawQuery()==null && uri.getRawFragment()==null && (uri.getRawPath().isEmpty() || uri.getRawPath().equals("/")));
        need(text(c,"region").matches("[A-Za-z0-9_-]{1,64}") && text(c,"bucket").matches("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]"));
        String value=text(c,"credentialsFile");Path path=Path.of(value);need(path.isAbsolute() && path.normalize().equals(path) && path.toString().equals(value));
    }
    static ObjectNode command(Bootstrap b)throws IOException{
        var w=b.expected().writer();var facts=b.bronze();var f=facts.get("fetch");
        var c=object().put("protocolVersion",1).put("runId",w.runId()).put("attemptId",w.attemptId()).put("ownerEpoch",w.epoch())
                .put("stage","STORE_BRONZE").put("itemOrdinal",0).put("checkpointId",w.checkpointId()).put("mode","PUBLISH").put("runtimeSha256",text(facts,"runtimeSha256"));
        c.putObject("journal").put("directory",fetchDirectory(b).toString()).put("commandSha256",text(f,"commandSha256"))
                .put("descriptorSha256",text(f,"descriptorSha256")).set("reference",f.get("reference"));c.set("storage",facts.get("storage"));return c;
    }
    private static Path fetchDirectory(Bootstrap b)throws IOException{return b.expected().controlRoot().resolve("runs/"+b.expected().runId()+"/attempts/"+b.expected().originalAttemptId()+"/fetch-source/"+b.expected().sourceId()+"/"+text(b.bronze().get("fetch"),"checkpointId"));}
    static int execute(InputStream input)throws Exception{
        var b=parse(receive(input));var sink=new StrictBronzeEvidence(b.expected(),b.current());byte[] command=sink.readCommand();
        need(Arrays.equals(command,canonical(command(b))));var d=dependency(b);sink.requireFresh();
        boolean[] interrupted={false};
        try{return sink.runFresh(()->produce(b,sink,command,d,interrupted));}
        finally{if(interrupted[0])Thread.currentThread().interrupt();}
    }
    private static int produce(Bootstrap b,StrictBronzeEvidence sink,byte[] command,Dependency original,boolean[] interrupted)throws Exception{
        // The forced prefix now means object-store work MAY have occurred, even if no GET was sent.
        recheck(b,sink,command,original,interrupted);
        var storage=b.bronze().get("storage");var config=storage.get("configuration");var secret=CREDENTIALS.readTree(original.credentials());
        var local=new LocalS3(Map.of("BRONZE_S3_ENDPOINT",text(config,"endpoint"),"BRONZE_S3_REGION",text(config,"region"),
                "BRONZE_S3_BUCKET",text(config,"bucket"),"BRONZE_S3_ACCESS_KEY",text(secret,"accessKey"),"BRONZE_S3_SECRET_KEY",text(secret,"secretKey")));
        var observation=verify(local,original);
        if(observation.state()==BronzeObjectVerifier.State.ABSENT){
            recheck(b,sink,command,original,interrupted);
            // Preserve the native writer: its own second existence check may encounter a
            // payload-only race. This is not a stronger remote compare-and-swap guarantee.
            try(var client=local.client()){new S3BronzeStorage(client,local.bucket()).write(original.source(),original.feed());}
            catch(BronzeStorageException ignored){/* Independent GETs classify partial or ambiguous effects. */}
            recheck(b,sink,command,original,interrupted);observation=verify(local,original);
        }
        if(observation.state()==BronzeObjectVerifier.State.EXACT){recheck(b,sink,command,original,interrupted);observation=verify(local,original);}
        // SDK interruptions can leave the flag set. Preserve it while allowing bounded
        // failure evidence; neither absence of evidence nor a return code proves no effects.
        boolean wasInterrupted=Thread.interrupted();if(wasInterrupted)interrupted[0]=true;
        recheck(b,sink,command,original,interrupted);
        var evidence=object().put("bronzeEvidenceVersion",1).put("commandSha256",sha(command)).put("state",observation.state().name());
        if(observation.state()==BronzeObjectVerifier.State.EXACT){
            var receipt=evidence.putObject("receipt").put("receiptVersion",1);
            for(String key:List.of("runId","attemptId","sourceId","revision","capturedConfigSha256","ingestionId","ingestedAt","payloadByteLength","payloadSha256"))receipt.set(key,original.descriptor().get(key));
            receipt.put("journalSha256",text(b.bronze().get("fetch"),"descriptorSha256")).put("storageBindingId",text(storage,"bindingId"))
                    .put("storageBindingSha256",text(storage,"bindingSha256")).put("bucket",local.bucket()).set("expected",original.descriptor().get("expected"));
            receipt.put("payloadContentType",observation.payloadContentType()).put("metadataContentType",observation.metadataContentType());
        }else evidence.putNull("receipt");
        byte[] proof=canonical(evidence);need(proof.length<=65536);sink.write("evidence.json",proof);recheck(b,sink,command,original,interrupted);
        need(Arrays.equals(proof,sink.readCandidate("evidence.json")));
        var c=StrictFetchJson.parse(command,65536);var result=object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))result.set(key,c.get(key));
        boolean exact=observation.state()==BronzeObjectVerifier.State.EXACT;result.put("status",exact?"OK":"FAILED");
        if(exact){result.putNull("code");result.set("evidenceId",c.get("checkpointId"));}else{result.put("code","BRONZE_"+observation.state().name());result.putNull("evidenceId");}
        result.putObject("counts");sink.write("result.json",canonical(result));recheck(b,sink,command,original,interrupted);return exact?0:1;
    }
    private static BronzeObjectVerifier.Observation verify(LocalS3 local,Dependency d)throws BronzeStorageException{
        try(var client=local.client()){return new BronzeObjectVerifier(client,local.bucket()).verify(d.source(),d.feed(),d.metadata());}
    }
    private static void recheck(Bootstrap b,StrictBronzeEvidence sink,byte[] command,Dependency original,boolean[] interrupted)throws Exception{
        if(Thread.interrupted())interrupted[0]=true;
        sink.inspect();need(Arrays.equals(command,sink.readCommand()));var current=dependency(b);
        need(Arrays.equals(original.credentials(),current.credentials()) && Arrays.equals(original.metadata(),current.metadata())
                && original.descriptor().equals(current.descriptor()) && Arrays.equals(original.feed().body(),current.feed().body()));sink.inspect();
    }
    private static Dependency dependency(Bootstrap b)throws Exception{
        Path directory=fetchDirectory(b);var f=b.bronze().get("fetch");var ref=f.get("reference");
        var before=treeStamps(directory);byte[] raw=stableRead(directory.resolve("command.json"),65536);need(sha(raw).equals(text(f,"commandSha256")));
        var c=StrictFetchJson.parse(raw,65536);fields(c,"protocolVersion","runId","attemptId","ownerEpoch","stage","itemOrdinal","checkpointId","sourceId",
                "revision","capturedConfigSha256","endpointSha256","feedUri","runtimeSha256","javaRuntime","testEndpoint","httpTimeoutMillis");
        need(c.get("protocolVersion").isInt() && c.get("protocolVersion").intValue()==1 && c.get("ownerEpoch").isIntegralNumber()
                && c.get("ownerEpoch").canConvertToLong() && c.get("ownerEpoch").longValue()==b.expected().writer().epoch()
                && c.get("itemOrdinal").isInt() && c.get("itemOrdinal").intValue()==0 && text(c,"stage").equals("FETCH_SOURCE")
                && text(c,"runId").equals(b.expected().runId()) && text(c,"attemptId").equals(b.expected().originalAttemptId())
                && text(c,"sourceId").equals(b.expected().sourceId()));
        for(String key:List.of("checkpointId","revision","capturedConfigSha256","endpointSha256","runtimeSha256","feedUri"))need(c.get(key).equals(f.get(key)));
        byte[] descriptor=stableRead(directory.resolve("descriptor.json"),65536);need(sha(descriptor).equals(text(f,"descriptorSha256")));var d=StrictFetchJson.parse(descriptor,65536);
        byte[] complete=stableRead(directory.resolve("complete.json"),65536);need(sha(complete).equals(text(ref,"completionSha256")) && stamp(directory.resolve("complete.json")).equals(ref.get("completionStamp")));
        var marker=StrictFetchJson.parse(complete,65536);fields(marker,"journalCompletionVersion","directoryIdentity","files");
        need(marker.get("journalCompletionVersion").isInt() && marker.get("journalCompletionVersion").intValue()==1
                && text(marker,"directoryIdentity").equals(identityText(directory)) && marker.get("files").isArray() && marker.get("files").size()==5);
        var names=new ArrayList<String>();for(var entry:marker.get("files")){
            fields(entry,"name","byteLength","sha256","stamp");String name=text(entry,"name");need(FETCH_FILES.contains(name));names.add(name);
            byte[] bytes=stableRead(directory.resolve(name),name.equals("payload.xml")?5*1024*1024:65536);
            need(entry.get("byteLength").isIntegralNumber() && entry.get("byteLength").canConvertToLong() && entry.get("byteLength").longValue()==bytes.length
                    && text(entry,"sha256").equals(sha(bytes)) && stamp(directory.resolve(name)).equals(entry.get("stamp")));
        }
        need(names.equals(new ArrayList<>(new TreeSet<>(FETCH_FILES))) && d.get("expected").equals(ref.get("bronze")));
        var result=StrictFetchJson.parse(stableRead(directory.resolve("result.json"),65536),65536);
        fields(result,"protocolVersion","runId","attemptId","stage","itemOrdinal","status","code","evidenceId","counts");
        for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))need(result.get(key).equals(c.get(key)));
        need(text(result,"status").equals("OK") && result.get("code").isNull() && result.get("evidenceId").equals(c.get("checkpointId")) && result.get("counts").isObject() && result.get("counts").isEmpty());
        RawFeed feed=ManagedRssFetch.reopen(directory,raw);need(feed.ingestionId().toString().equals(text(d,"ingestionId")));
        byte[] metadata=stableRead(directory.resolve("metadata.json"),65536);need(before.equals(treeStamps(directory)));
        var storage=b.bronze().get("storage");Path credentials=Path.of(text(storage.get("configuration"),"credentialsFile"));
        byte[] secret=stableRead(credentials,8192);need(sha(secret).equals(text(storage,"credentialsSha256")) && stamp(credentials).equals(storage.get("credentialsStamp")));
        var secrets=CREDENTIALS.readTree(secret);fields(secrets,"accessKey","secretKey");for(String key:List.of("accessKey","secretKey")){String value=text(secrets,key);need(!value.isBlank() && value.length()<=4096 && value.codePoints().noneMatch(Character::isISOControl));}
        return new Dependency(feed,new RssSource(feed.sourceId(),URI.create(text(c,"feedUri")),true),metadata,d,secret);
    }
    private static Map<Path,JsonNode> treeStamps(Path directory)throws IOException{
        ManagedRssFetch.privateEntry(directory,true);var names=new HashSet<String>();try(var children=Files.list(directory)){children.forEach(p->names.add(p.getFileName().toString()));}
        var all=new HashSet<>(FETCH_FILES);all.add("complete.json");need(names.equals(all));var result=new HashMap<Path,JsonNode>();result.put(directory,stamp(directory));
        for(String name:all){Path path=directory.resolve(name);ManagedRssFetch.privateEntry(path,false);result.put(path,stamp(path));}return Map.copyOf(result);
    }
    private static byte[] stableRead(Path path,int limit)throws IOException{var before=stamp(path);byte[] bytes=ManagedRssFetch.read(path,limit);need(before.equals(stamp(path)));return bytes;}
    private static String identityText(Path path)throws IOException{return Files.getAttribute(path,"unix:dev",LinkOption.NOFOLLOW_LINKS)+":"+Files.getAttribute(path,"unix:ino",LinkOption.NOFOLLOW_LINKS);}
    private static JsonNode stamp(Path path)throws IOException{
        // Match parsed bootstrap/marker number types as well as values. Jackson's
        // LongNode.equals(IntNode) is false even for the same small file length.
        return StrictFetchJson.parse(canonical(object().put("identity",identityText(path))
                .put("modified",Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS).toString())
                .put("size",Files.size(path))),65536);
    }
    private StrictManagedBronze(){}
}
