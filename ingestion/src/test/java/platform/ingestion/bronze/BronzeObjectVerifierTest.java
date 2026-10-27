package platform.ingestion.bronze;

import java.io.*;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import static org.junit.jupiter.api.Assertions.*;
import static platform.ingestion.bronze.S3BronzeStorageTest.*;

class BronzeObjectVerifierTest {
    final Map<String,byte[]> objects=new HashMap<>();final Map<String,String> types=new HashMap<>();
    final AtomicInteger gets=new AtomicInteger(),aborts=new AtomicInteger(),readBytes=new AtomicInteger();
    byte[] metadata;BronzeKeys keys;String error;boolean failRead,overflow;
    @BeforeEach void setup()throws Exception {metadata=S3BronzeStorage.preparedMetadata(SOURCE,feed(PAYLOAD));keys=BronzeKeys.forFeed(feed(PAYLOAD));}
    void pair(){objects.put(keys.payload(),PAYLOAD);objects.put(keys.metadata(),metadata);}
    BronzeObjectVerifier.State inspect()throws Exception {return new BronzeObjectVerifier(client(),"bucket").inspect(SOURCE,feed(PAYLOAD),metadata);}
    S3Client client(){return(S3Client)Proxy.newProxyInstance(S3Client.class.getClassLoader(),new Class<?>[]{S3Client.class},(p,m,args)->{
        if(m.getName().equals("close"))return null;
        if(!m.getName().equals("getObject"))throw new AssertionError("Verifier attempted "+m.getName());
        gets.incrementAndGet();if(error!=null)throw s3Error(error,error.equals("AccessDenied")?403:404);
        String key=((GetObjectRequest)args[0]).key();byte[] value=objects.get(key);if(value==null)throw s3Error("NoSuchKey",404);
        boolean isPayload=key.equals(keys.payload());byte[] actual=overflow && isPayload?Arrays.copyOf(value,65536):value;
        var input=new ByteArrayInputStream(actual){@Override public synchronized int read(byte[] b,int off,int len){int n=super.read(b,off,len);if(n>0)readBytes.addAndGet(n);return n;}};
        InputStream stream=failRead?new InputStream(){public int read()throws IOException{throw new IOException("synthetic");}}:input;
        var response=GetObjectResponse.builder().contentType(types.getOrDefault(key,isPayload?"application/octet-stream":"application/json")).contentLength((long)value.length).build();
        return new ResponseInputStream<>(response,AbortableInputStream.create(stream,()->aborts.incrementAndGet()));
    });}
    @Test void exactPairRequiresTwoBoundedReads()throws Exception {pair();assertEquals(BronzeObjectVerifier.State.EXACT,inspect());assertEquals(2,gets.get());assertEquals(2,aborts.get());}
    @Test void provedAbsenceRequiresBothNoSuchKeyReads()throws Exception {assertEquals(BronzeObjectVerifier.State.ABSENT,inspect());assertEquals(2,gets.get());}
    @Test void payloadOnlyAndMetadataOnlyAreDistinct()throws Exception {objects.put(keys.payload(),PAYLOAD);assertEquals(BronzeObjectVerifier.State.PAYLOAD_ONLY,inspect());objects.clear();objects.put(keys.metadata(),metadata);assertEquals(BronzeObjectVerifier.State.METADATA_ONLY,inspect());}
    @ParameterizedTest @ValueSource(strings={"NotFound","NoSuchBucket","AccessDenied","Unknown"}) void genericErrorsAreNeverAbsence(String code)throws Exception {error=code;assertEquals(BronzeObjectVerifier.State.UNAVAILABLE,inspect());}
    @Test void exactPayloadWithDifferentMetadataIsConflict()throws Exception {pair();objects.put(keys.metadata(),new byte[]{123,125});assertEquals(BronzeObjectVerifier.State.CONFLICT,inspect());}
    @Test void sameLengthDifferentPayloadIsConflict()throws Exception {pair();var changed=PAYLOAD.clone();changed[0]^=1;objects.put(keys.payload(),changed);assertEquals(BronzeObjectVerifier.State.CONFLICT,inspect());}
    @Test void knownSeaweedXmlMimeFallbackIsRecorded()throws Exception {pair();types.put(keys.payload(),"application/xml");var observed=new BronzeObjectVerifier(client(),"bucket").verify(SOURCE,feed(PAYLOAD),metadata);assertEquals(BronzeObjectVerifier.State.EXACT,observed.state());assertEquals("application/xml",observed.payloadContentType());}
    @ParameterizedTest @ValueSource(strings={"text/plain","text/html","application/json"}) void unrelatedPayloadTypeIsConflict(String type)throws Exception {pair();types.put(keys.payload(),type);assertEquals(BronzeObjectVerifier.State.CONFLICT,inspect());}
    @Test void wrongMetadataTypeIsConflict()throws Exception {pair();types.put(keys.metadata(),"text/plain");assertEquals(BronzeObjectVerifier.State.CONFLICT,inspect());}
    @Test void ioFailureIsUnavailableAndStreamsAreAborted()throws Exception {pair();failRead=true;assertEquals(BronzeObjectVerifier.State.UNAVAILABLE,inspect());assertEquals(2,aborts.get());}
    @Test void oversizedStreamIsAbortedWithoutDraining()throws Exception {pair();overflow=true;assertEquals(BronzeObjectVerifier.State.CONFLICT,inspect());assertTrue(readBytes.get()<=8192+metadata.length);assertEquals(2,aborts.get());}
    @Test void suppliedExpectationMustMatchOriginalSerializer()throws Exception {pair();assertThrows(IllegalArgumentException.class,()->new BronzeObjectVerifier(client(),"bucket").inspect(SOURCE,feed(PAYLOAD),new byte[]{123,125}));assertEquals(0,gets.get());}
    @Test void emptyPayloadIsStillIndependentlyRead()throws Exception {var raw=feed(new byte[0]);metadata=S3BronzeStorage.preparedMetadata(SOURCE,raw);objects.put(keys.payload(),new byte[0]);objects.put(keys.metadata(),metadata);assertEquals(BronzeObjectVerifier.State.EXACT,new BronzeObjectVerifier(client(),"bucket").inspect(SOURCE,raw,metadata));}
}
