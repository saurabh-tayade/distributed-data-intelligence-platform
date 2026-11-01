package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import platform.contentapi.execution.*;

/** Real disposable original worker JVM. Leaves its interrupted database and native bytes retained. */
public final class ContentRecoveryTestProcess extends RssContentIT {
    @Override void storage(String id,String product,String role){
        if(!role.equals("BRONZE")){super.storage(id,product,role);return;}
        byte[] raw=SourceJson.canonical(SourceJson.object().put("importBronzeVersion",1).put("endpoint",System.getenv("BRONZE_S3_ENDPOINT")).put("region",System.getenv("BRONZE_S3_REGION")).put("bucket",System.getenv("BRONZE_S3_BUCKET")).put("credentialsFile",credentials.toString()));
        jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,raw,SourceJson.sha(raw),INSTALL);
    }
    public static void main(String[] args)throws Exception{
        temp=Path.of(args[0]);var f=new ContentRecoveryTestProcess();f.database();f.setup();String stop=args.length>1?args[1]:"FINAL";f.multi=java.util.Set.of("GOLD","PARTIAL_SILVER").contains(stop);
        if(java.util.Set.of("BEFORE_FETCH","FETCH_SOURCE","BRONZE","SILVER","PARTIAL_SILVER").contains(stop)){
            if(stop.equals("BEFORE_FETCH") || stop.equals("FETCH_SOURCE")){f.ready();f.processingProfiles();if(stop.equals("FETCH_SOURCE"))f.owner=f.fetch().stage().owner();}else f.silver(stop.equals("BRONZE")?0:1);var profile=SourceJson.object().put("contentRuntimeVersion",1).put("goldRuntimeId",f.goldRuntime).put("goldRuntimeSha256",f.digest(f.goldRuntime)).put("referenceBindingId",REFS).put("contentBindingId","content-CONTENT").put("seconds",300);profile.set("modules",RssContentTestSupport.modules(Path.of("../../processing").toRealPath()));f.register(f.contentRuntime,profile);
        }else f.goldReady();
        if(java.util.Set.of("FINAL","CHECK_INPUTS","CONTENT_INTENT","PARTIAL_CONTENT").contains(stop)){
            String boundaryStage=stop.equals("CHECK_INPUTS")?"CHECK_INPUTS":"PUBLISH_CONTENT";
            String boundaryPoint=stop.equals("CONTENT_INTENT")?"INTENT":stop.equals("PARTIAL_CONTENT")?"AUTHORIZED":"ACKNOWLEDGED";
            try{f.complete((stage,boundary,owner)->{if(stage.equals(boundaryStage) && boundary.equals(boundaryPoint)){
                if(stop.equals("PARTIAL_CONTENT"))f.incompleteContent(owner);
                throw new IllegalStateException("INTERRUPTED");
            }});throw new AssertionError("Expected interruption");}
            catch(IllegalStateException ex){if(!ex.getMessage().equals("INTERRUPTED"))throw ex;}
        }
        var n=SourceJson.object().put("url",f.url).put("runId",f.owner.runId().toString()).put("runtime",f.contentRuntime).put("profile",CONTENT_PROFILE).put("database",f.database);
        n.put("endpoint",f.endpoint).put("feedPayload",java.util.Base64.getEncoder().encodeToString(f.feedPayload()));
        n.set("nextRequest",f.reliability(false));
        n.set("binding",SourceJson.parse(f.guard.binding().descriptor()));
        Path info=temp.resolve("owner.json");Files.write(info,SourceJson.canonical(n));Files.setPosixFilePermissions(info,PosixFilePermissions.fromString("rw-------"));
        System.out.println("READY");System.out.flush();System.in.read();f.release();f.closeProxy();
    }
    /** Inject an incomplete intended namespace before launch; never call or repair a publisher. */
    void incompleteContent(WorkerRepository.Owner current){
        try{
            String address=jdbc.queryForObject("SELECT command_address FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='PUBLISH_CONTENT'",String.class,current.attemptId());
            var command=SourceJson.parse(Files.readAllBytes(guard.binding().root().resolve(address)),16*1024*1024);
            Path namespace=Path.of(command.get("content").get("root").stringValue()).resolve("v1/processor_revision=1/publication="+command.get("prepared").get("fingerprint").stringValue());
            Files.createDirectories(namespace);Files.writeString(namespace.resolve("interrupted-part.bin"),"retained-incomplete-publication",StandardOpenOption.CREATE_NEW);
        }catch(Exception ex){throw new IllegalStateException(ex);}
    }
}
