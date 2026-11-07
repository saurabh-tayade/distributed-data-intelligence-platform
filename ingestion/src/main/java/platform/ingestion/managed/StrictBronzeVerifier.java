package platform.ingestion.managed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import platform.ingestion.bronze.BronzeObjectVerifier;
import platform.ingestion.bronze.LocalS3;
import static platform.ingestion.managed.StrictFetchJson.*;

/** Inactive GET-only independent observation. Output is not durable checkpoint authority. */
public final class StrictBronzeVerifier {
    static final String ERROR="STRICT_BRONZE_VERIFIER_REJECTED";
    record Bootstrap(StrictManagedBronze.Bootstrap facts,String candidateSha256) {
        @Override public String toString(){return "StrictBronzeVerifierBootstrap[redacted; not authority]";}
    }
    public static void main(String[] args){
        int status=2;
        try{
            need(args.length==0);
            int mode=((Number)Files.getAttribute(Path.of("/proc/self/fd/0"),"unix:mode")).intValue();
            need((mode&0170000)==0010000);status=execute(System.in,System.out);
        }catch(Exception ignored){System.err.println(ERROR);}
        System.exit(status);
    }
    static ObjectNode runtimeIdentity(){return object().put("strictBronzeVerifierRuntimeVersion",1)
            .put("adapter","platform.ingestion.managed.StrictBronzeVerifier").put("javaRuntime",Runtime.version().toString());}
    static Bootstrap parse(byte[] packet)throws IOException{
        try{
            var n=StrictFetchJson.parse(packet,65535);
            fields(n,"bootstrapVersion","mode","childPid","expected","current","strictRuntimeSha256","bronze","candidateSha256");
            need(n.get("bootstrapVersion").isInt() && n.get("bootstrapVersion").intValue()==1
                    && text(n,"mode").equals("STORE_BRONZE_VERIFY") && decimal(n,"childPid",true)==ProcessHandle.current().pid()
                    && Runtime.version().feature()==21 && text(n,"strictRuntimeSha256").equals(sha(canonical(runtimeIdentity())))
                    && text(n,"candidateSha256").matches("[0-9a-f]{64}"));
            return new Bootstrap(StrictManagedBronze.parseFacts(n),text(n,"candidateSha256"));
        }catch(RuntimeException ex){throw invalid();}
    }
    static int execute(InputStream input,OutputStream output)throws Exception{
        var b=parse(StrictManagedBronze.receive(input));var f=b.facts();
        // This reader only inspects existing slots. In particular it never calls requireFresh/runFresh/write.
        var reader=new StrictBronzeEvidence(f.expected(),f.current());
        byte[] command=reader.readCommand();need(Arrays.equals(command,canonical(StrictManagedBronze.command(f))));
        var original=StrictManagedBronze.dependency(f);
        recheck(b,reader,command,original);
        var config=f.bronze().get("storage").get("configuration");
        var secret=new ObjectMapper().readTree(original.credentials());
        var local=new LocalS3(Map.of("BRONZE_S3_ENDPOINT",text(config,"endpoint"),"BRONZE_S3_REGION",text(config,"region"),
                "BRONZE_S3_BUCKET",text(config,"bucket"),"BRONZE_S3_ACCESS_KEY",text(secret,"accessKey"),"BRONZE_S3_SECRET_KEY",text(secret,"secretKey")));
        BronzeObjectVerifier.Observation observed;
        try(var client=local.client()){
            observed=new BronzeObjectVerifier(client,local.bucket()).verify(original.source(),original.feed(),original.metadata(),
                    ()->recheck(b,reader,command,original));
        }
        // Even a non-exact observation cannot mask a changed local dependency or candidate.
        boolean interrupted=Thread.interrupted();
        try{
            recheck(b,reader,command,original);
            if(observed.state()!=BronzeObjectVerifier.State.EXACT)return 1;
            byte[] receipt=canonical(receipt(f,original,observed.payloadContentType(),observed.metadataContentType()));
            need(receipt.length>0 && receipt.length<=65536);output.write(receipt);output.flush();return 0;
        }finally{if(interrupted)Thread.currentThread().interrupt();}
    }
    private static void recheck(Bootstrap b,StrictBronzeEvidence reader,byte[] command,StrictManagedBronze.Dependency original)throws Exception{
        reader.inspect();need(Arrays.equals(command,reader.readCommand()));
        need(candidate(reader.directory(),command,b.facts(),original).equals(b.candidateSha256()));
        var current=StrictManagedBronze.dependency(b.facts());
        need(Arrays.equals(original.credentials(),current.credentials()) && Arrays.equals(original.metadata(),current.metadata())
                && original.descriptor().equals(current.descriptor()) && Arrays.equals(original.feed().body(),current.feed().body()));
        need(candidate(reader.directory(),command,b.facts(),original).equals(b.candidateSha256()));reader.inspect();
    }
    static ObjectNode receipt(StrictManagedBronze.Bootstrap b,StrictManagedBronze.Dependency d,String payloadType,String metadataType)throws IOException{
        var receipt=object().put("receiptVersion",1);
        for(String key:List.of("runId","attemptId","sourceId","revision","capturedConfigSha256","ingestionId","ingestedAt","payloadByteLength","payloadSha256"))receipt.set(key,d.descriptor().get(key));
        var storage=b.bronze().get("storage");
        receipt.put("journalSha256",text(b.bronze().get("fetch"),"descriptorSha256")).put("storageBindingId",text(storage,"bindingId"))
                .put("storageBindingSha256",text(storage,"bindingSha256")).put("bucket",text(storage.get("configuration"),"bucket"))
                .set("expected",d.descriptor().get("expected"));
        receipt.put("payloadContentType",payloadType).put("metadataContentType",metadataType);return receipt;
    }
    /** Exact parity with the parent candidate inventory. No new files, prefixes or ledger allocations. */
    static String candidate(Path directory,byte[] command,StrictManagedBronze.Bootstrap b,StrictManagedBronze.Dependency d)throws Exception{
        ManagedRssFetch.privateEntry(directory,true);var before=StrictManagedBronze.stamp(directory);
        Set<String> names=Set.of("command.json","evidence.json","result.json");
        try(var files=Files.list(directory)){need(files.map(p->p.getFileName().toString()).collect(java.util.stream.Collectors.toSet()).equals(names));}
        var inventory=object();inventory.set("directory",before);var contents=new HashMap<String,byte[]>();
        for(String name:new TreeSet<>(names)){
            Path path=directory.resolve(name);ManagedRssFetch.privateEntry(path,false);var stamp=StrictManagedBronze.stamp(path);
            byte[] bytes=StrictManagedBronze.stableRead(path,65536);need(stamp.equals(StrictManagedBronze.stamp(path)));contents.put(name,bytes);
            inventory.putObject(name).put("bytes",bytes.length).put("sha256",sha(bytes)).set("stamp",stamp);
        }
        need(Arrays.equals(command,contents.get("command.json")));
        JsonNode c=StrictFetchJson.parse(command,65536),e=StrictFetchJson.parse(contents.get("evidence.json"),65536);
        fields(e,"bronzeEvidenceVersion","commandSha256","state","receipt");
        need(e.get("bronzeEvidenceVersion").isInt() && e.get("bronzeEvidenceVersion").intValue()==1 && text(e,"commandSha256").equals(sha(command)));
        String state=text(e,"state");need(Arrays.stream(BronzeObjectVerifier.State.values()).anyMatch(s->s.name().equals(state)));
        var result=object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))result.set(key,c.get(key));
        if(state.equals("EXACT")){
            result.put("status","OK").putNull("code");result.set("evidenceId",c.get("checkpointId"));
            String type=text(e.get("receipt"),"payloadContentType");need(Set.of("application/xml","application/octet-stream").contains(type));
            need(Arrays.equals(canonical(receipt(b,d,type,"application/json")),canonical(e.get("receipt"))));
        }else{result.put("status","FAILED").put("code","BRONZE_"+state).putNull("evidenceId");need(e.get("receipt").isNull());}
        result.putObject("counts");need(Arrays.equals(canonical(result),contents.get("result.json")) && before.equals(StrictManagedBronze.stamp(directory)));
        return sha(canonical(inventory));
    }
    private StrictBronzeVerifier(){}
}
