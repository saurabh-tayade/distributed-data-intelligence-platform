package platform.ingestion.bronze;

import java.io.IOException;
import java.util.Arrays;
import platform.ingestion.rss.RawFeed;
import platform.ingestion.rss.RssSource;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Independent bounded GET verification, never writes or interprets a generic 404 as absence. */
public final class BronzeObjectVerifier {
    public enum State { EXACT, ABSENT, PAYLOAD_ONLY, METADATA_ONLY, CONFLICT, UNAVAILABLE }
    public record Observation(State state,String payloadContentType,String metadataContentType) { }
    private record ObjectRead(ObjectState state,String contentType) { }
    private enum ObjectState { EXACT, ABSENT, CONFLICT, UNAVAILABLE }
    private final S3Client client;
    private final String bucket;
    public BronzeObjectVerifier(S3Client client,String bucket){this.client=client;this.bucket=bucket;}

    public State inspect(RssSource source,RawFeed feed,byte[] metadata) throws BronzeStorageException {return verify(source,feed,metadata).state();}
    public Observation verify(RssSource source,RawFeed feed,byte[] metadata) throws BronzeStorageException {
        if(feed.body().length>5*1024*1024 || metadata.length>65536
                || !Arrays.equals(metadata,S3BronzeStorage.preparedMetadata(source,feed))) {
            throw new IllegalArgumentException("BRONZE_EXPECTATION_INVALID");
        }
        var keys=BronzeKeys.forFeed(feed);
        var payload=get(keys.payload(),feed.body(),"application/octet-stream");
        var descriptor=get(keys.metadata(),metadata,"application/json");
        State state;
        if(payload.state()==ObjectState.CONFLICT || descriptor.state()==ObjectState.CONFLICT)state=State.CONFLICT;
        else if(payload.state()==ObjectState.UNAVAILABLE || descriptor.state()==ObjectState.UNAVAILABLE)state=State.UNAVAILABLE;
        else if(payload.state()==ObjectState.EXACT && descriptor.state()==ObjectState.EXACT)state=State.EXACT;
        else if(payload.state()==ObjectState.ABSENT && descriptor.state()==ObjectState.ABSENT)state=State.ABSENT;
        else state=payload.state()==ObjectState.EXACT?State.PAYLOAD_ONLY:State.METADATA_ONLY;
        return new Observation(state,payload.contentType(),descriptor.contentType());
    }
    private ObjectRead get(String key,byte[] expected,String contentType) {
        try(var input=client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())) {
            try {
                var response=input.response();
                // SeaweedFS 4.48 may infer application/xml for payload.xml on GET when it
                // omits the original octet-stream MIME. Keep the native PUT unchanged;
                // allow only this observed mapping and retain the actual type in receipts.
                if(!(contentType.equals(response.contentType()) || contentType.equals("application/octet-stream") && "application/xml".equals(response.contentType())) || response.contentLength()==null
                        || response.contentLength()!=expected.length
                        || response.contentEncoding()!=null && !response.contentEncoding().isEmpty())return new ObjectRead(ObjectState.CONFLICT,response.contentType());
                byte[] buffer=new byte[8192];int offset=0,n;
                var digest=java.security.MessageDigest.getInstance("SHA-256");
                while((n=input.read(buffer))!=-1){
                    if(n>expected.length-offset)return new ObjectRead(ObjectState.CONFLICT,response.contentType());
                    for(int i=0;i<n;i++)if(buffer[i]!=expected[offset+i])return new ObjectRead(ObjectState.CONFLICT,response.contentType());
                    digest.update(buffer,0,n);offset+=n;
                }
                return new ObjectRead(offset==expected.length && java.util.HexFormat.of().formatHex(digest.digest()).equals(S3BronzeStorage.sha256(expected))?ObjectState.EXACT:ObjectState.CONFLICT,response.contentType());
            }finally{input.abort();} // Never drain an unexpectedly large response on close.
        }catch(S3Exception ex){
            return new ObjectRead(ex.statusCode()==404 && ex.awsErrorDetails()!=null && "NoSuchKey".equals(ex.awsErrorDetails().errorCode())?ObjectState.ABSENT:ObjectState.UNAVAILABLE,null);
        }catch(SdkException | IOException ex){return new ObjectRead(ObjectState.UNAVAILABLE,null);}
        catch(java.security.NoSuchAlgorithmException ex){throw new IllegalStateException(ex);}
    }
}
