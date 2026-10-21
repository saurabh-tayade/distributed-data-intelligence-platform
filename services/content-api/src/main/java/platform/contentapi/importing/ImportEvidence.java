package platform.contentapi.importing;

import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.importing.ImportFailure.Code.RESULT_INVALID;

/** Protocol shape/consistency checks only. Native schema/provenance interpretation stays in its public reader. */
final class ImportEvidence {
    private static void need(boolean b){ImportFailure.require(b,RESULT_INVALID);}
    private static void array(JsonNode n,int low,int high){need(n!=null && n.isArray() && n.size()>=low && n.size()<=high);}
    private static long number(JsonNode n,long max){need(n!=null && n.isIntegralNumber() && n.canConvertToLong() && n.longValue()>=0 && n.longValue()<=max);return n.longValue();}
    private static void stamp(JsonNode n,int count){array(n,count,count);for(var v:n)ImportJson.match(v,"-?[0-9]{1,20}");}
    static void validate(ImportCatalog.Entry e,JsonNode evidence) {
        ImportJson.fields(evidence,"evidenceVersion","native","capture");ImportJson.integer(evidence.get("evidenceVersion"),1,1);
        var capture=evidence.get("capture");ImportJson.fields(capture,"files","directories","guards","completionBase64");
        var files=capture.get("files");boolean batch=e.format()!=null;array(files,batch?3:4,batch?18:4);Map<String,JsonNode> inventory=new HashMap<>();
        for(var f:files) {
            ImportJson.fields(f,"path","byteLength","sha256","stamp");String path=ImportJson.text(f.get("path"));
            need(path.matches("complete\\.json|manifest\\.json|reference\\.csv|vocabulary\\.json|data/[a-z0-9][a-z0-9_-]{0,63}\\.(jsonl|parquet)"));
            need(inventory.putIfAbsent(path,f)==null);long size=number(f.get("byteLength"),256L*1024*1024);ImportJson.hash(f.get("sha256"));stamp(f.get("stamp"),7);
            need(Long.toString(size).equals(f.get("stamp").get(2).stringValue()));
        }
        var expected=new HashSet<>(Set.of("complete.json","manifest.json"));
        if(batch) {for(String path:inventory.keySet())if(path.startsWith("data/")){need(path.endsWith("."+e.format().toLowerCase(Locale.ROOT)));expected.add(path);}}
        else expected.addAll(Set.of("reference.csv","vocabulary.json"));
        need(inventory.keySet().equals(expected));need(ImportJson.hash(inventory.get("manifest.json").get("sha256")).equals(e.fingerprint()));
        byte[] complete=Base64.getDecoder().decode(ImportJson.text(capture.get("completionBase64")));need(complete.length<=4096 && complete.length==number(inventory.get("complete.json").get("byteLength"),4096));
        need(SourceJson.sha(complete).equals(ImportJson.hash(inventory.get("complete.json").get("sha256"))));
        var directories=capture.get("directories");array(directories,batch?2:1,batch?2:1);Set<String> directoryPaths=new HashSet<>();
        for(var d:directories){ImportJson.fields(d,"path","stamp","names");String path=ImportJson.text(d.get("path"));need(directoryPaths.add(path));stamp(d.get("stamp"),7);array(d.get("names"),1,18);Set<String> names=new HashSet<>();for(var n:d.get("names"))need(names.add(ImportJson.text(n)));
            Set<String> expectedNames=new HashSet<>();if(path.equals("data") && batch){for(String name:inventory.keySet())if(name.startsWith("data/"))expectedNames.add(name.substring(5));}
            else if(path.equals(".")){for(String name:inventory.keySet())if(!name.contains("/"))expectedNames.add(name);if(batch)expectedNames.add("data");}
            else throw new ImportFailure(RESULT_INVALID);need(names.equals(expectedNames));}
        need(directoryPaths.equals(batch?Set.of(".","data"):Set.of(".")));
        var guards=capture.get("guards");String[] parts=e.address().split("/");array(guards,parts.length+1,parts.length+1);String path=".";
        for(int i=0;i<guards.size();i++){var g=guards.get(i);ImportJson.fields(g,"path","identity");need(ImportJson.text(g.get("path")).equals(path));stamp(g.get("identity"),3);if(i<parts.length)path=i==0?parts[0]:path+"/"+parts[i];}
        var n=evidence.get("native");String verifier=batch?"reliability_format_inputs.open_batch":e.kind().equals("PUBLISHER_REFERENCE")?"publisher_reference.open_snapshot":"service_reference.open_snapshot";
        need(ImportJson.text(n.get("verifier")).equals(verifier) && ImportJson.hash(n.get("fingerprint")).equals(e.fingerprint()));
        if(batch) {
            ImportJson.fields(n,"verifier","fingerprint","ingestionId","receivedAt","profileSha256","inventory","dependencies");ImportJson.uuid(n.get("ingestionId"));ImportJson.match(n.get("receivedAt"),"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z");
            need(ImportJson.hash(n.get("profileSha256")).equals(e.identity().get("registeredProfileSha256").stringValue()));array(n.get("inventory"),1,16);array(n.get("dependencies"),n.get("inventory").size(),n.get("inventory").size());Set<String> rawFiles=new HashSet<>();
            for(int i=0;i<n.get("inventory").size();i++) {
                var f=n.get("inventory").get(i);ImportJson.fields(f,"artifact_id","file","byte_length","sha256","record_count","physical_line_count","row_groups");
                String id=ImportJson.match(f.get("artifact_id"),"[a-z0-9][a-z0-9_-]{0,63}"),file=ImportJson.text(f.get("file"));need(file.equals("data/"+id+"."+e.format().toLowerCase(Locale.ROOT)) && rawFiles.add(file));file(inventory,f);
                long count=number(f.get("record_count"),2000000);if(e.format().equals("JSONL"))need(number(f.get("physical_line_count"),2000000)==count && f.get("row_groups").isNull());
                else {need(f.get("physical_line_count").isNull());array(f.get("row_groups"),0,64);for(var group:f.get("row_groups")){ImportJson.fields(group,"row_group_ordinal","record_start","record_count","compressed_byte_length","uncompressed_byte_length");for(var v:group)number(v,Long.MAX_VALUE);}}
                var dependency=f.deepCopy();((tools.jackson.databind.node.ObjectNode)dependency).put("role","REGISTERED_RAW");need(dependency.equals(n.get("dependencies").get(i)));
            }
            expected.removeAll(Set.of("complete.json","manifest.json"));need(rawFiles.equals(expected));
        } else {
            ImportJson.fields(n,"verifier","fingerprint","rowCount","csvSchemaVersion","validatorRevision","dependencies");long count=number(n.get("rowCount"),2000000);ImportJson.integer(n.get("csvSchemaVersion"),1,1);ImportJson.integer(n.get("validatorRevision"),1,1);array(n.get("dependencies"),2,2);
            var csv=n.get("dependencies").get(0);ImportJson.fields(csv,"role","file","byte_length","sha256","row_count");need(ImportJson.text(csv.get("role")).equals("REFERENCE_CSV") && ImportJson.text(csv.get("file")).equals("reference.csv") && number(csv.get("row_count"),2000000)==count);file(inventory,csv);
            var vocab=n.get("dependencies").get(1);ImportJson.fields(vocab,"role","file","byte_length","sha256","version");need(ImportJson.text(vocab.get("role")).equals("VOCABULARY") && ImportJson.text(vocab.get("file")).equals("vocabulary.json"));ImportJson.integer(vocab.get("version"),1,1);file(inventory,vocab);
        }
    }
    private static void file(Map<String,JsonNode> inventory,JsonNode f){var observed=inventory.get(ImportJson.text(f.get("file")));need(observed!=null && number(f.get("byte_length"),256L*1024*1024)==number(observed.get("byteLength"),256L*1024*1024) && ImportJson.hash(f.get("sha256")).equals(ImportJson.hash(observed.get("sha256"))));}
}
