package platform.contentapi.importing;

import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.importing.ImportFailure.Code.RESULT_INVALID;

/** Fixed Silver protocol projection, not an alternative native Silver reader. */
final class SilverImportEvidence {
    private static void need(boolean value){ImportFailure.require(value,RESULT_INVALID);}
    private static long number(JsonNode n,long max){need(n!=null && n.isIntegralNumber() && n.canConvertToLong() && n.longValue()>=0 && n.longValue()<=max);return n.longValue();}
    private static void array(JsonNode n,int low,int high){need(n!=null && n.isArray() && n.size()>=low && n.size()<=high);}
    private static void stamp(JsonNode n,int count){array(n,count,count);for(var v:n)ImportJson.match(v,"-?[0-9]{1,20}");}
    private static String path(JsonNode n){String path=ImportJson.text(n);need(!path.startsWith("/") && !path.isEmpty() && Arrays.stream(path.split("/",-1)).noneMatch(s->s.isEmpty() || s.equals("..")) && (!path.contains("/./")));return path;}
    static void validate(ImportCatalog.Entry entry,JsonNode e) {
        ImportJson.fields(e,"evidenceVersion","native","capture","inventoryIdentity");ImportJson.integer(e.get("evidenceVersion"),2,2);
        var capture=e.get("capture");ImportJson.fields(capture,"local","bronze");var local=capture.get("local");ImportJson.fields(local,"files","directories","guards","completionBase64","analyticalFileCounts");
        var files=local.get("files");array(files,3,512);Map<String,JsonNode> byPath=new HashMap<>();long bytes=0;var counts=new HashMap<String,Integer>();counts.put("accepted",0);counts.put("rejected",0);
        var ordered=new ArrayList<JsonNode>();String previous="";
        for(var f:files){ImportJson.fields(f,"path","dataset","byteLength","sha256","stamp");String p=path(f.get("path")),dataset=ImportJson.text(f.get("dataset"));
            need(byPath.putIfAbsent(p,f)==null && Arrays.compareUnsigned(previous.getBytes(java.nio.charset.StandardCharsets.UTF_8),p.getBytes(java.nio.charset.StandardCharsets.UTF_8))<0);previous=p;
            need(Set.of("accepted","rejected","bookkeeping").contains(dataset));long size=number(f.get("byteLength"),1024L*1024*1024);bytes+=size;need(bytes<=1024L*1024*1024);ImportJson.hash(f.get("sha256"));stamp(f.get("stamp"),7);need(Long.toString(size).equals(f.get("stamp").get(2).stringValue()));
            if(!dataset.equals("bookkeeping")){need(p.startsWith(dataset+"/") && p.substring(dataset.length()+1).indexOf('/')<0 && !p.substring(dataset.length()+1).startsWith("_") && !p.substring(dataset.length()+1).startsWith(".") && p.endsWith(".parquet"));counts.compute(dataset,(k,v)->v+1);}
            else need(size<=(p.equals("complete.json")?65536:16L*1024*1024));
            if(!p.equals("complete.json")){var value=SourceJson.object().put("path",p).put("dataset",dataset).put("byteLength",size).put("sha256",f.get("sha256").stringValue());ordered.add(value);}
        }
        ImportJson.fields(local.get("analyticalFileCounts"),"accepted","rejected");for(String name:counts.keySet())need(counts.get(name)>=1 && counts.get(name)<=128 && ImportJson.integer(local.get("analyticalFileCounts").get(name),1,128)==counts.get(name));
        var directories=local.get("directories");array(directories,3,512);Set<String> directoryPaths=new HashSet<>();
        for(var d:directories){ImportJson.fields(d,"path","stamp","names");String p=path(d.get("path"));need(directoryPaths.add(p));stamp(d.get("stamp"),7);array(d.get("names"),0,512);Set<String> names=new HashSet<>();for(var n:d.get("names"))need(names.add(ImportJson.text(n)));}
        need(directoryPaths.containsAll(Set.of(".","accepted","rejected")));
        for(var d:directories){String p=d.get("path").stringValue();Set<String> children=new HashSet<>();String prefix=p.equals(".")?"":p+"/";
            for(String candidate:byPath.keySet())if(candidate.startsWith(prefix) && candidate.substring(prefix.length()).indexOf('/')<0)children.add(candidate.substring(prefix.length()));
            for(String candidate:directoryPaths)if(!candidate.equals(p) && !candidate.equals(".") && candidate.startsWith(prefix) && candidate.substring(prefix.length()).indexOf('/')<0)children.add(candidate.substring(prefix.length()));
            Set<String> reported=new HashSet<>();for(var n:d.get("names"))reported.add(n.stringValue());need(children.equals(reported));}
        String namespace=entry.address().substring(0,entry.address().length()-"/complete.json".length());String[] parts=namespace.split("/");var guards=local.get("guards");array(guards,parts.length+1,parts.length+1);String prefix=".";
        for(int n=0;n<guards.size();n++){var g=guards.get(n);ImportJson.fields(g,"path","identity");need(ImportJson.text(g.get("path")).equals(prefix));stamp(g.get("identity"),3);if(n<parts.length)prefix=n==0?parts[0]:prefix+"/"+parts[n];}
        var markerFile=byPath.get("complete.json");need(markerFile!=null);byte[] raw=Base64.getDecoder().decode(ImportJson.text(local.get("completionBase64")));need(raw.length<=65536 && raw.length==number(markerFile.get("byteLength"),65536) && SourceJson.sha(raw).equals(ImportJson.hash(markerFile.get("sha256"))));
        var marker=ImportJson.parse(raw,65536);ImportJson.fields(marker,"identity","counts");var mi=marker.get("identity");ImportJson.fields(mi,"metadata_sha256","payload_sha256","contract_version","processor_revision","bucket","metadata_key");ImportJson.integer(mi.get("contract_version"),1,1);ImportJson.integer(mi.get("processor_revision"),1,1);
        var bronze=capture.get("bronze");ImportJson.fields(bronze,"bucket","metadataKey","metadataByteLength","metadataSha256","payloadKey","payloadByteLength","payloadSha256");var selection=entry.identity();
        need(ImportJson.text(bronze.get("bucket")).equals(selection.get("bronzeBucket").stringValue()) && ImportJson.text(bronze.get("metadataKey")).equals(selection.get("bronzeMetadataKey").stringValue()) && ImportJson.text(bronze.get("payloadKey")).equals(selection.get("bronzePayloadKey").stringValue()));
        number(bronze.get("metadataByteLength"),65536);number(bronze.get("payloadByteLength"),5*1024*1024);need(ImportJson.hash(bronze.get("metadataSha256")).equals(ImportJson.hash(mi.get("metadata_sha256"))) && ImportJson.hash(bronze.get("payloadSha256")).equals(ImportJson.hash(mi.get("payload_sha256"))) && mi.get("bucket").equals(bronze.get("bucket")) && mi.get("metadata_key").equals(bronze.get("metadataKey")));
        var nativeResult=e.get("native");ImportJson.fields(nativeResult,"verifier","counts","schemaPolicy","bronzeProvenance","semanticReconstruction","sparkShutdown");
        need(ImportJson.text(nativeResult.get("verifier")).equals("silver_inputs.load_inputs") && ImportJson.text(nativeResult.get("schemaPolicy")).equals("native-nullable-schema-and-required-row-values-v1") && ImportJson.text(nativeResult.get("bronzeProvenance")).equals("all-rows-exact-v1") && nativeResult.get("semanticReconstruction").isBoolean() && !nativeResult.get("semanticReconstruction").booleanValue() && ImportJson.text(nativeResult.get("sparkShutdown")).equals("CONFIRMED"));
        ImportJson.fields(marker.get("counts"),"accepted","rejected");for(var n:marker.get("counts"))number(n,Long.MAX_VALUE);need(nativeResult.get("counts").equals(marker.get("counts")));
        var value=SourceJson.object().put("qualificationPolicy","ddip-e5-silver-import").put("qualificationRevision",1).put("productId","content-intelligence").put("contractVersion",1).put("processorRevision",1).put("sourceId",selection.get("sourceId").stringValue()).put("ingestionId",selection.get("ingestionId").stringValue());
        value.set("nativeCompletion",SourceJson.object().put("relativeAddress",entry.address()).put("byteLength",raw.length).put("sha256",SourceJson.sha(raw)));var list=value.putArray("files");for(var f:ordered)list.add(f);value.set("bronze",bronze);
        var inventory=e.get("inventoryIdentity");ImportJson.fields(inventory,"kind","sha256","value");need(ImportJson.text(inventory.get("kind")).equals("E5_SILVER_INVENTORY_V1") && Arrays.equals(SourceJson.canonical(inventory.get("value")),SourceJson.canonical(value)) && ImportJson.hash(inventory.get("sha256")).equals(SourceJson.sha(SourceJson.canonical(value))));
    }
}
