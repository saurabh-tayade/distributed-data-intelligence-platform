package platform.contentapi.management;

import tools.jackson.databind.JsonNode;
import static platform.contentapi.management.SourceFailure.require;

/** Reviewed admission metadata only. A future worker must independently match this pinned deployment. */
final class RunProfile {
    private RunProfile() { }
    static JsonNode validate(JsonNode n,RunCommands.Command command) {
        return validate(n,command.pipeline());
    }
    static JsonNode validate(JsonNode n,String pipeline) {
        int version=SourceJson.integer(n.get("runProfileVersion"));
        if(version==2){SourceJson.fields(n,"runProfileVersion","pipeline","pipelineVersion","sourceProfile","runtime","contentRuntimeId");require(pipeline.equals("CONTENT_RSS_V1") && SourceJson.text(n.get("contentRuntimeId")).matches("[A-Za-z0-9_-]{1,128}"));}
        else SourceJson.fields(n,"runProfileVersion","pipeline","pipelineVersion","sourceProfile","runtime");
        require((version==1 || version==2) && SourceJson.integer(n.get("pipelineVersion"))==1);
        require(java.util.Set.of("CONTENT_RSS_V1","RELIABILITY_V2").contains(pipeline) && SourceJson.text(n.get("pipeline")).equals(pipeline));
        String source=SourceJson.text(n.get("sourceProfile"));
        require(pipeline.equals("CONTENT_RSS_V1")?source.equals("rss-atom-v1"):java.util.Set.of("request-jsonl-v2","request-parquet-v1").contains(source));
        var runtime=n.get("runtime");SourceJson.fields(runtime,"javaVersion","pythonVersion","pysparkVersion","deploymentSha256");
        require(SourceJson.text(runtime.get("javaVersion")).equals("21") && SourceJson.text(runtime.get("pythonVersion")).equals("3.12.15") && SourceJson.text(runtime.get("pysparkVersion")).equals("4.0.2"));
        require(SourceJson.text(runtime.get("deploymentSha256")).matches("[0-9a-f]{64}"));return n;
    }
}
