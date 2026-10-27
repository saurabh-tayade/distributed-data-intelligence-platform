package platform.ingestion.managed;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import java.nio.file.*;
import java.util.*;
import platform.ingestion.bronze.*;
import platform.ingestion.rss.*;

/** Fixed D2 child. Original D1 journal is read-only; inspection mode cannot call the writer. */
public final class ManagedBronze {
    private static final ObjectMapper JSON=new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private ManagedBronze(){ }
    public static void main(String[] args){int code=2;try{if(args.length==1)code=execute(Path.of(args[0]));}catch(Exception ignored){ }System.exit(code);}
    public static int execute(Path command) throws Exception {
        ManagedRssFetch.privateEntry(command.getParent(),true);byte[] raw=ManagedRssFetch.read(command,65536);
        var c=JSON.readTree(raw);fields(c,"protocolVersion","runId","attemptId","ownerEpoch","stage","itemOrdinal","checkpointId","mode","runtimeSha256","journal","storage");
        boolean publish=text(c,"mode").equals("PUBLISH");
        check(publish || text(c,"mode").equals("INSPECT"));
        check(c.path("protocolVersion").isInt() && c.get("protocolVersion").intValue()==1 && Runtime.version().feature()==21
                && text(c,"stage").equals(publish?"STORE_BRONZE":"INSPECT_BRONZE")
                && c.path("ownerEpoch").isIntegralNumber() && c.get("ownerEpoch").canConvertToLong() && c.get("ownerEpoch").longValue()>0
                && c.path("itemOrdinal").isInt() && c.get("itemOrdinal").intValue()>=0 && c.get("itemOrdinal").intValue()<8 && text(c,"runtimeSha256").matches("[0-9a-f]{64}"));
        for(String key:List.of("runId","attemptId","checkpointId"))check(UUID.fromString(text(c,key)).toString().equals(text(c,key)));
        var journal=c.get("journal");fields(journal,"directory","commandSha256","descriptorSha256","reference");
        Path directory=Path.of(text(journal,"directory"));
        byte[] original=ManagedRssFetch.read(directory.resolve("command.json"),65536);
        check(sha(original).equals(text(journal,"commandSha256")));
        var fetched=JSON.readTree(original);var descriptor=JSON.readTree(ManagedRssFetch.read(directory.resolve("descriptor.json"),65536));
        for(String key:List.of("runId","attemptId","itemOrdinal"))check(c.get(key).equals(fetched.get(key)));
        check(sha(ManagedRssFetch.read(directory.resolve("descriptor.json"),65536)).equals(text(journal,"descriptorSha256")));
        validateJournal(directory,journal.get("reference"));
        RawFeed feed=ManagedRssFetch.reopen(directory,original);
        var source=new RssSource(feed.sourceId(),java.net.URI.create(text(fetched,"feedUri")),true);
        byte[] metadata=ManagedRssFetch.read(directory.resolve("metadata.json"),65536);
        var storage=c.get("storage");fields(storage,"bindingId","bindingSha256","configuration","credentialsSha256","credentialsStamp");
        var config=storage.get("configuration");fields(config,"importBronzeVersion","endpoint","region","bucket","credentialsFile");
        check(config.path("importBronzeVersion").isInt() && config.get("importBronzeVersion").intValue()==1);
        Path credentials=Path.of(text(config,"credentialsFile"));byte[] secret=ManagedRssFetch.read(credentials,8192);
        check(sha(secret).equals(text(storage,"credentialsSha256")));checkStamp(credentials,storage.get("credentialsStamp"));
        var secrets=JSON.readTree(secret);fields(secrets,"accessKey","secretKey");
        var local=new LocalS3(Map.of("BRONZE_S3_ENDPOINT",text(config,"endpoint"),"BRONZE_S3_REGION",text(config,"region"),"BRONZE_S3_BUCKET",text(config,"bucket"),"BRONZE_S3_ACCESS_KEY",text(secrets,"accessKey"),"BRONZE_S3_SECRET_KEY",text(secrets,"secretKey")));
        BronzeObjectVerifier.Observation observed;
        try(var client=local.client()){observed=new BronzeObjectVerifier(client,local.bucket()).verify(source,feed,metadata);}
        var state=observed.state();
        if(publish && state==BronzeObjectVerifier.State.ABSENT){
            // Only a fresh, one-shot parent authorization permits these writes. Orphans require
            // explicit future retry authority; never complete a PAYLOAD_ONLY observation here.
            validateJournal(directory,journal.get("reference"));checkStamp(credentials,storage.get("credentialsStamp"));
            try(var client=local.client()){new S3BronzeStorage(client,local.bucket()).write(source,feed);}
            catch(BronzeStorageException ex){/* Actual independent GETs below classify any partial effect. */}
            try(var client=local.client()){observed=new BronzeObjectVerifier(client,local.bucket()).verify(source,feed,metadata);}
        }
        state=observed.state();
        // A fresh client reads both objects even when the pair pre-existed; no writer return value
        // or cached observation constitutes verification.
        if(state==BronzeObjectVerifier.State.EXACT)try(var client=local.client()){
            observed=new BronzeObjectVerifier(client,local.bucket()).verify(source,feed,metadata);
        }
        state=observed.state();
        validateJournal(directory,journal.get("reference"));check(Arrays.equals(secret,ManagedRssFetch.read(credentials,8192)));checkStamp(credentials,storage.get("credentialsStamp"));
        var evidence=JSON.createObjectNode().put("bronzeEvidenceVersion",1).put("commandSha256",sha(raw)).put("state",state.name());
        if(state==BronzeObjectVerifier.State.EXACT){
            var r=evidence.putObject("receipt").put("receiptVersion",1);
            for(String key:List.of("runId","attemptId","sourceId","revision","capturedConfigSha256","ingestionId","ingestedAt","payloadByteLength","payloadSha256"))r.set(key,descriptor.get(key));
            r.put("journalSha256",text(journal,"descriptorSha256")).put("storageBindingId",text(storage,"bindingId")).put("storageBindingSha256",text(storage,"bindingSha256"));
            r.put("bucket",local.bucket()).set("expected",descriptor.get("expected"));
            r.put("payloadContentType",observed.payloadContentType()).put("metadataContentType",observed.metadataContentType());
        }else evidence.putNull("receipt");
        ManagedRssFetch.write(command.getParent().resolve("evidence.json"),JSON.writeValueAsBytes(evidence));
        var result=JSON.createObjectNode();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))result.set(key,c.get(key));
        boolean exact=state==BronzeObjectVerifier.State.EXACT;result.put("status",exact?"OK":"FAILED");if(exact)result.putNull("code");else result.put("code","BRONZE_"+state.name());
        if(exact)result.set("evidenceId",c.get("checkpointId"));else result.putNull("evidenceId");result.putObject("counts");
        ManagedRssFetch.write(command.getParent().resolve("result.json"),JSON.writeValueAsBytes(result));return exact?0:1;
    }
    private static void validateJournal(Path directory,JsonNode reference)throws Exception {
        fields(reference,"bronze","completionSha256","completionStamp");
        byte[] complete=ManagedRssFetch.read(directory.resolve("complete.json"),65536);check(sha(complete).equals(text(reference,"completionSha256")));checkStamp(directory.resolve("complete.json"),reference.get("completionStamp"));
        var marker=JSON.readTree(complete);fields(marker,"journalCompletionVersion","directoryIdentity","files");
        check(marker.path("journalCompletionVersion").isInt() && marker.get("journalCompletionVersion").intValue()==1 && text(marker,"directoryIdentity").equals(identity(directory)));
        Set<String> names=new HashSet<>();try(var files=Files.list(directory)){files.forEach(p->names.add(p.getFileName().toString()));}
        check(names.equals(Set.of("command.json","payload.xml","metadata.json","descriptor.json","result.json","complete.json")) && marker.get("files").isArray() && marker.get("files").size()==5);
        var seen=new HashSet<String>();for(var f:marker.get("files")){fields(f,"name","byteLength","sha256","stamp");String name=text(f,"name");check(names.contains(name) && !name.equals("complete.json") && seen.add(name));byte[] bytes=ManagedRssFetch.read(directory.resolve(name),name.equals("payload.xml")?5*1024*1024:65536);check(f.get("byteLength").isIntegralNumber() && f.get("byteLength").canConvertToInt() && f.get("byteLength").intValue()==bytes.length && sha(bytes).equals(text(f,"sha256")));checkStamp(directory.resolve(name),f.get("stamp"));}
        var d=JSON.readTree(ManagedRssFetch.read(directory.resolve("descriptor.json"),65536));check(d.get("expected").equals(reference.get("bronze")));
    }
    private static void checkStamp(Path path,JsonNode stamp)throws Exception{fields(stamp,"identity","modified","size");check(text(stamp,"identity").equals(identity(path)) && text(stamp,"modified").equals(Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS).toString()) && stamp.get("size").isIntegralNumber() && stamp.get("size").canConvertToLong() && stamp.get("size").longValue()==Files.size(path));}
    private static String identity(Path path)throws Exception{return Files.getAttribute(path,"unix:dev",LinkOption.NOFOLLOW_LINKS)+":"+Files.getAttribute(path,"unix:ino",LinkOption.NOFOLLOW_LINKS);}
    private static String sha(byte[] bytes)throws Exception{return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));}
    private static String text(JsonNode n,String key){check(n.has(key) && n.get(key).isTextual());return n.get(key).textValue();}
    private static void fields(JsonNode n,String... keys){check(n!=null && n.isObject() && n.size()==keys.length);for(String key:keys)check(n.has(key));}
    private static void check(boolean value){if(!value)throw new IllegalArgumentException("BRONZE_EVIDENCE_INVALID");}
}
