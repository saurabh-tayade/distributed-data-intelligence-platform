package platform.contentapi.importing;

import java.nio.file.Path;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.importing.ImportFailure.Code.*;

/** Fixed selections only. Addresses must be the exact namespace derived from the native keys. */
public record ImportCatalog(UUID operationId,String profileId,String controlBindingId,List<Entry> entries,byte[] requestBytes) {
    public static final int MAX_ARTIFACTS=16;
    public record Entry(String kind,String product,String bindingId,String address,int contract,int processor,JsonNode identity,String fingerprint) {
        public Entry {identity=identity.deepCopy();}
        @Override public JsonNode identity(){return identity.deepCopy();}
        public boolean silver(){return kind.equals("SILVER");}
        public String format(){return kind.equals("RELIABILITY_BATCH_V2")?identity.get("inputFormat").stringValue():null;}
        public String nativeIdentity() {
            var n=SourceJson.object();
            if(format()!=null)n.put("sourceId",identity.get("sourceId").stringValue()).put("batchId",identity.get("batchId").stringValue());
            else if(silver())n.put("bucketSha256",SourceJson.sha(identity.get("bronzeBucket").stringValue().getBytes(java.nio.charset.StandardCharsets.UTF_8))).put("metadataKeySha256",SourceJson.sha(identity.get("bronzeMetadataKey").stringValue().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            else n.put("referenceId",identity.get("referenceId").stringValue()).put("revision",identity.get("revision").stringValue());
            return new String(SourceJson.canonical(n),java.nio.charset.StandardCharsets.UTF_8);
        }
        public JsonNode json() {
            var n=SourceJson.object().put("kind",kind).put("productId",product).put("storageBindingId",bindingId).put("relativeAddress",address)
                    .put("contractVersion",contract).put("processorRevision",processor);
            if(!silver())n.put("fingerprint",fingerprint);n.set("identity",identity.deepCopy());return n;
        }
        public String digest(){return SourceJson.sha(SourceJson.canonical(json()));}
        @Override public String toString(){return "ImportSelection[redacted]";}
    }
    public ImportCatalog {entries=List.copyOf(entries);requestBytes=requestBytes.clone();}
    @Override public byte[] requestBytes(){return requestBytes.clone();}
    public static ImportCatalog load(Path file){return parse(ImportJson.read(file,65536));}
    public static ImportCatalog parse(byte[] raw) {
        var root=ImportJson.parse(raw,65536);ImportJson.fields(root,"catalogVersion","operationId","executionProfileId","controlBindingId","artifacts");
        ImportJson.integer(root.get("catalogVersion"),1,1);
        UUID id=ImportJson.uuid(root.get("operationId"));String profile=ImportJson.token(root.get("executionProfileId")),control=ImportJson.token(root.get("controlBindingId"));
        var items=root.get("artifacts");ImportFailure.require(items.isArray() && items.size()>0 && items.size()<=MAX_ARTIFACTS,INVALID_CATALOG);
        var entries=new ArrayList<Entry>();for(JsonNode n:items)entries.add(entry(n));
        entries.sort(Comparator.comparing(e -> new String(SourceJson.canonical(e.json()),java.nio.charset.StandardCharsets.UTF_8)));
        var nativeKeys=new HashSet<String>();var locations=new HashSet<String>();
        for(var e:entries) {
            ImportFailure.require(nativeKeys.add(e.product()+"|"+e.kind()+"|"+e.contract()+"|"+e.processor()+"|"+e.format()+"|"+e.nativeIdentity()),INVALID_CATALOG);
            ImportFailure.require(locations.add(e.bindingId()+"|"+e.address()),INVALID_CATALOG);
        }
        var normalized=root.deepCopy();var array=SourceJson.object().putArray("unused");for(var e:entries)array.add(e.json());
        ((tools.jackson.databind.node.ObjectNode)normalized).set("artifacts",array);
        byte[] request=SourceJson.canonical(normalized);ImportFailure.require(request.length<=65536,INVALID_CATALOG);
        return new ImportCatalog(id,profile,control,entries,request);
    }
    static Entry entry(JsonNode n) {
        if(n!=null && n.has("kind") && ImportJson.text(n.get("kind")).equals("SILVER"))return silverEntry(n);
        ImportJson.fields(n,"kind","productId","storageBindingId","relativeAddress","contractVersion","processorRevision","identity","fingerprint");
        String kind=ImportJson.text(n.get("kind")),product=ImportJson.text(n.get("productId")),binding=ImportJson.token(n.get("storageBindingId")),address=ImportJson.text(n.get("relativeAddress"));
        int contract=ImportJson.integer(n.get("contractVersion"),1,2),processor=ImportJson.integer(n.get("processorRevision"),1,2);
        String fingerprint=ImportJson.hash(n.get("fingerprint"));JsonNode i=n.get("identity");String expected;
        if(kind.equals("RELIABILITY_BATCH_V2")) {
            ImportFailure.require(product.equals("application-reliability") && contract==2 && processor==2,INVALID_CATALOG);
            ImportJson.fields(i,"sourceId","sourceRevision","batchId","inputFormat","registeredProfileSha256");
            String source=ImportJson.match(i.get("sourceId"),"[a-z0-9][a-z0-9_-]{0,127}"),batch=ImportJson.match(i.get("batchId"),"[a-z0-9][a-z0-9_-]{0,63}"),format=ImportJson.text(i.get("inputFormat"));
            ImportFailure.require(Set.of("JSONL","PARQUET").contains(format),INVALID_CATALOG);ImportJson.integer(i.get("sourceRevision"),1,1000);ImportJson.hash(i.get("registeredProfileSha256"));
            expected="v2/format="+format.toLowerCase(Locale.ROOT)+"/source_id="+source+"/batch_id="+batch;
        } else {
            ImportFailure.require(Set.of("PUBLISHER_REFERENCE","SERVICE_REFERENCE").contains(kind) && contract==1 && processor==1 &&
                    product.equals(kind.equals("PUBLISHER_REFERENCE")?"content-intelligence":"application-reliability"),INVALID_CATALOG);
            ImportJson.fields(i,"referenceId","revision");String ref=ImportJson.match(i.get("referenceId"),"[a-z0-9][a-z0-9_-]{0,63}"),revision=ImportJson.match(i.get("revision"),"[a-z0-9][a-z0-9_-]{0,31}");
            expected="reference_id="+ref+"/revision="+revision;
        }
        ImportFailure.require(address.equals(expected),INVALID_CATALOG);return new Entry(kind,product,binding,address,contract,processor,i,fingerprint);
    }
    private static Entry silverEntry(JsonNode n) {
        ImportJson.fields(n,"kind","productId","storageBindingId","relativeAddress","contractVersion","processorRevision","identity");
        ImportFailure.require(ImportJson.text(n.get("productId")).equals("content-intelligence"),INVALID_CATALOG);
        ImportJson.integer(n.get("contractVersion"),1,1);ImportJson.integer(n.get("processorRevision"),1,1);
        var i=n.get("identity");ImportJson.fields(i,"sourceId","ingestionId","bronzeStorageBindingId","bronzeBucket","bronzeMetadataKey","bronzePayloadKey");
        String source=ImportJson.text(i.get("sourceId"));ImportFailure.require(!source.isBlank() && source.getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=512 && source.codePoints().noneMatch(c->Character.isISOControl(c)),INVALID_CATALOG);
        String ingestion=ImportJson.uuid(i.get("ingestionId")).toString();ImportJson.token(i.get("bronzeStorageBindingId"));
        String bucket=ImportJson.match(i.get("bronzeBucket"),"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]");
        String key=ImportJson.text(i.get("bronzeMetadataKey")),payload=ImportJson.text(i.get("bronzePayloadKey"));
        String encoded=Base64.getUrlEncoder().withoutPadding().encodeToString(source.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        String prefix="bronze/rss/source_id="+encoded+"/ingestion_date=";
        ImportFailure.require(key.length()>=prefix.length()+("/ingestion_id="+ingestion+"/metadata.json").length() && key.startsWith(prefix) && key.endsWith("/ingestion_id="+ingestion+"/metadata.json"),INVALID_CATALOG);
        String date=key.substring(prefix.length(),key.length()-("/ingestion_id="+ingestion+"/metadata.json").length());
        try {ImportFailure.require(date.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}") && java.time.LocalDate.parse(date).toString().equals(date),INVALID_CATALOG);}catch(RuntimeException ex){throw new ImportFailure(INVALID_CATALOG);}
        ImportFailure.require(payload.equals(key.substring(0,key.length()-"metadata.json".length())+"payload.xml"),INVALID_CATALOG);
        String address="v1/"+SourceJson.sha(bucket.getBytes(java.nio.charset.StandardCharsets.UTF_8))+"/"+key.substring("bronze/rss/".length(),key.length()-"metadata.json".length())+"complete.json";
        ImportFailure.require(address.length()<=1024 && ImportJson.text(n.get("relativeAddress")).equals(address),INVALID_CATALOG);
        return new Entry("SILVER","content-intelligence",ImportJson.token(n.get("storageBindingId")),address,1,1,i,null);
    }
    @Override public String toString(){return "ImportCatalog[redacted]";}
}
