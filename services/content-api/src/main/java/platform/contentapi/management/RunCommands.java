package platform.contentapi.management;

import java.util.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.management.SourceFailure.require;

/** The two accepted submission DTOs. Selection order is an operational set order, never a native fingerprint. */
public final class RunCommands {
    private RunCommands() { }
    public record Source(String id,int revision) { }
    public record Command(String pipeline,String product,String profileId,List<Source> sources,List<UUID> artifacts,
                          UUID reference,String key,byte[] normalized) {
        public Command {sources=List.copyOf(sources);artifacts=List.copyOf(artifacts);normalized=normalized.clone();}
        @Override public byte[] normalized(){return normalized.clone();}
        @Override public String toString(){return "RunSubmission[redacted]";}
        public boolean content(){return pipeline.equals("CONTENT_RSS_V1");}
    }
    public static Command parse(byte[] bytes,String key) {
        require(key!=null && key.matches("[A-Za-z0-9_-]{1,128}"));
        JsonNode n=SourceJson.parse(bytes);String pipeline=SourceJson.text(n.get("pipeline"));
        require(Set.of("CONTENT_RSS_V1","RELIABILITY_V2").contains(pipeline));boolean content=pipeline.equals("CONTENT_RSS_V1");
        String sourceField=content?"fetchSources":"sources", inputField=content?"retainedSilverArtifactIds":"batchArtifactIds", refField=content?"publisherReferenceArtifactId":"serviceReferenceArtifactId";
        SourceJson.fields(n,"pipeline","executionProfileId",sourceField,inputField,refField);
        String profile=SourceCommands.uuid(SourceJson.text(n.get("executionProfileId")));
        var sources=n.get(sourceField);require(sources.isArray() && sources.size()>=1 && sources.size()<=(content?8:32));
        var selected=new ArrayList<Source>();Set<String> ids=new HashSet<>();
        for(var s:sources){SourceJson.fields(s,"sourceId","revision");String id=SourceCommands.source(SourceJson.text(s.get("sourceId")));int revision=SourceJson.integer(s.get("revision"));require(revision>=1 && revision<=1000 && ids.add(id));selected.add(new Source(id,revision));}
        selected.sort(Comparator.comparing(Source::id).thenComparingInt(Source::revision)); // IDs are ASCII.
        var inputs=n.get(inputField);require(inputs.isArray() && inputs.size()>=(content?0:1) && inputs.size()<=(content?128:32));
        var artifacts=new ArrayList<UUID>();Set<UUID> unique=new HashSet<>();
        for(var a:inputs){UUID id=UUID.fromString(SourceCommands.uuid(SourceJson.text(a)));require(unique.add(id));artifacts.add(id);}
        artifacts.sort(Comparator.comparing(UUID::toString));UUID reference=UUID.fromString(SourceCommands.uuid(SourceJson.text(n.get(refField))));require(!unique.contains(reference));
        var normalized=SourceJson.object().put("pipeline",pipeline).put("executionProfileId",profile);
        var ss=normalized.putArray(sourceField);for(var s:selected)ss.addObject().put("sourceId",s.id()).put("revision",s.revision());
        var aa=normalized.putArray(inputField);for(var id:artifacts)aa.add(id.toString());normalized.put(refField,reference.toString());
        return new Command(pipeline,content?"content-intelligence":"application-reliability",profile,selected,artifacts,reference,key,SourceJson.canonical(normalized));
    }
}
