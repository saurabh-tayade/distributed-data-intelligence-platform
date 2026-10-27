package platform.ingestion.managed;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.util.Base64;
import platform.ingestion.bronze.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;

/** Test classpath only: exact synthetic object seeding and independent backend snapshots. */
public final class BronzeFixture {
    private BronzeFixture(){ }
    public static void main(String[] args)throws Exception {
        var local=new LocalS3(System.getenv());var json=new ObjectMapper();
        try(var client=local.client()){
            if(args[0].equals("head")){client.headBucket(HeadBucketRequest.builder().bucket(local.bucket()).build());System.out.print("READY");return;}
            Path directory=Path.of(args[1]);byte[] command=ManagedRssFetch.read(directory.resolve("command.json"),65536);var feed=ManagedRssFetch.reopen(directory,command);var keys=BronzeKeys.forFeed(feed);
            if(args[0].equals("seed")){
                String variant=args[2];byte[] payload=feed.body(),metadata=ManagedRssFetch.read(directory.resolve("metadata.json"),65536);
                if(variant.equals("bad-payload"))payload=new byte[]{42};if(variant.equals("bad-metadata"))metadata=new byte[]{123,125};
                if(!variant.equals("metadata-only"))client.putObject(PutObjectRequest.builder().bucket(local.bucket()).key(keys.payload()).contentType(variant.equals("bad-type")?"text/plain":"application/octet-stream").build(),RequestBody.fromBytes(payload));
                if(!variant.equals("payload-only"))client.putObject(PutObjectRequest.builder().bucket(local.bucket()).key(keys.metadata()).contentType("application/json").build(),RequestBody.fromBytes(metadata));
                System.out.print("SEEDED");return;
            }
            var snapshot=json.createObjectNode();
            for(String role:new String[]{"payload","metadata"})try{
                var response=client.getObjectAsBytes(GetObjectRequest.builder().bucket(local.bucket()).key(role.equals("payload")?keys.payload():keys.metadata()).build());
                snapshot.putObject(role).put("base64",Base64.getEncoder().encodeToString(response.asByteArray())).put("contentType",response.response().contentType()).put("length",response.response().contentLength()).put("etag",response.response().eTag()).put("modified",response.response().lastModified().toString());
            }catch(S3Exception ex){if(ex.statusCode()==404 && ex.awsErrorDetails()!=null && ex.awsErrorDetails().errorCode().equals("NoSuchKey"))snapshot.putNull(role);else throw ex;}
            System.out.print(json.writeValueAsString(snapshot));
        }
    }
}
