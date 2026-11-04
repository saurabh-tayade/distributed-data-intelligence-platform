package platform.contentapi.execution;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.INPUT_INVALID;

/** Fixed reservation grammar. Runtime routing/adoption and restart qualification remain separate gates. */
final class ContentOperationPlan {
    private static final Map<String,Integer> ORIGINAL = Map.ofEntries(
            Map.entry("STORE_BRONZE",1), Map.entry("INSPECT_BRONZE",5),
            Map.entry("SILVER",1), Map.entry("VERIFY_SILVER",1), Map.entry("RECHECK_SILVER",1),
            Map.entry("PREPARE_GOLD",1), Map.entry("PUBLISH_GOLD",1), Map.entry("VERIFY_RESULT",2),
            Map.entry("CHECK_INPUTS",4), Map.entry("PUBLISH_CONTENT",1));
    private final Path root;
    private final UUID run,attempt;
    private final String source,kind;
    private final Map<String,Path> operations;
    private final Map<Path,Map<String,Integer>> reservations;

    private ContentOperationPlan(Path root,UUID run,UUID attempt,String source,boolean recovery){
        need(root!=null && run!=null && attempt!=null && root.isAbsolute() && root.normalize().equals(root)
                && root.endsWith(Path.of("runs",run.toString(),"attempts",attempt.toString())),INPUT_INVALID);
        this.root=root;this.run=run;this.attempt=attempt;this.source=source;kind=recovery?"RECOVERY":"ORIGINAL";
        var paths=new LinkedHashMap<String,Path>();var slots=new LinkedHashMap<Path,Map<String,Integer>>();
        if(recovery){
            for(int i=0;i<9;i++)add(paths,slots,"VERIFY_RESULT",i,null,ContentEvidenceBudget.slots("VERIFY_RESULT"));
        }else{
            need(source!=null && source.matches("[a-z0-9][a-z0-9_-]{0,127}"),INPUT_INVALID);
            add(paths,slots,"FETCH_SOURCE",0,source,Map.of("command.json",65536,"payload.xml",5*1024*1024,
                    "metadata.json",65536,"descriptor.json",65536,"result.json",65536,"complete.json",65536));
            for(String stage:new TreeSet<>(ORIGINAL.keySet()))for(int i=0;i<ORIGINAL.get(stage);i++)add(paths,slots,stage,i,null,ContentEvidenceBudget.slots(stage));
        }
        operations=Map.copyOf(paths);reservations=Map.copyOf(slots);
    }
    static ContentOperationPlan original(Path root,UUID run,UUID attempt,String source){return new ContentOperationPlan(root,run,attempt,source,false);}
    static ContentOperationPlan recovery(Path root,UUID run,UUID attempt){return new ContentOperationPlan(root,run,attempt,null,true);}
    private void add(Map<String,Path> paths,Map<Path,Map<String,Integer>> slots,String stage,int ordinal,String source,Map<String,Integer> files){
        String key=stage+":"+ordinal;
        // Existing Silver verification uses the publisher checkpoint in a distinct stage directory.
        String identity=stage.equals("VERIFY_SILVER")?"SILVER:"+ordinal:key;
        UUID checkpoint=UUID.nameUUIDFromBytes((NarrowContentProfile.POLICY+"\n"+run+"\n"+attempt+"\n"+identity).getBytes(StandardCharsets.UTF_8));
        Path path=root.resolve(stage.toLowerCase(Locale.ROOT).replace('_','-'));
        if(source!=null)path=path.resolve(source);
        path=path.resolve(checkpoint.toString());paths.put(key,path);slots.put(path,Map.copyOf(files));
    }
    Path directory(String stage,int ordinal){Path result=operations.get(stage+":"+ordinal);need(result!=null,INPUT_INVALID);return result;}
    Path root(){return root;}
    Map<Path,Map<String,Integer>> reservations(){return reservations;}
    void reserve(){ContentEvidenceBudget.reservePlan(this);}
    void apply(ObjectNode n){
        n.put("version",2).put("reservationPolicy",NarrowContentProfile.POLICY).put("planKind",kind);
        if(source==null)n.putNull("sourceId");else n.put("sourceId",source);
        var directories=new TreeSet<String>();directories.add("");var files=n.putObject("files");
        for(var operation:reservations.entrySet()){
            String relative=root.relativize(operation.getKey()).toString();Path path=Path.of(relative);
            while(path!=null){directories.add(path.toString());path=path.getParent();}
            for(var slot:operation.getValue().entrySet())for(String suffix:List.of("",".preparing"))files.put(relative+"/"+slot.getKey()+suffix,slot.getValue());
        }
        var ds=n.putArray("directories");directories.forEach(ds::add);
    }
    static void validate(Path root,JsonNode n){
        need(n.get("treeKind").stringValue().equals("ATTEMPT") && n.get("planKind").isString(),INPUT_INVALID);
        String kind=n.get("planKind").stringValue();need(Set.of("ORIGINAL","RECOVERY").contains(kind),INPUT_INVALID);
        UUID run=UUID.fromString(n.get("runId").stringValue()),attempt=UUID.fromString(n.get("attemptId").stringValue());
        need(run.toString().equals(n.get("runId").stringValue()) && attempt.toString().equals(n.get("attemptId").stringValue()),INPUT_INVALID);
        need(kind.equals("RECOVERY")?n.get("sourceId").isNull():n.get("sourceId").isString(),INPUT_INVALID);
        var expected=new ContentOperationPlan(root,run,attempt,kind.equals("RECOVERY")?null:n.get("sourceId").stringValue(),kind.equals("RECOVERY"));
        var copy=(ObjectNode)n.deepCopy();expected.apply(copy);
        need(Arrays.equals(SourceJson.canonical(n),SourceJson.canonical(copy)),INPUT_INVALID);
    }
}
