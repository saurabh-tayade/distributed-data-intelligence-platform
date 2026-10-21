package platform.contentapi.management;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

class RunCommandsTest {
    static final String PROFILE="10000000-0000-4000-8000-000000000001", REF="20000000-0000-4000-8000-000000000001", INPUT="30000000-0000-4000-8000-000000000001";
    static ObjectNode request(boolean content) {
        var n=SourceJson.object().put("pipeline",content?"CONTENT_RSS_V1":"RELIABILITY_V2").put("executionProfileId",PROFILE);
        n.putArray(content?"fetchSources":"sources").addObject().put("sourceId","demo").put("revision",1);
        var inputs=n.putArray(content?"retainedSilverArtifactIds":"batchArtifactIds");if(!content)inputs.add(INPUT);
        n.put(content?"publisherReferenceArtifactId":"serviceReferenceArtifactId",REF);return n;
    }
    static RunCommands.Command parse(ObjectNode n){return RunCommands.parse(SourceJson.canonical(n),"test");}
    @Test void contentVector(){var c=parse(request(true));assertThat(new String(c.normalized(),StandardCharsets.UTF_8)).isEqualTo("{\"executionProfileId\":\"10000000-0000-4000-8000-000000000001\",\"fetchSources\":[{\"revision\":1,\"sourceId\":\"demo\"}],\"pipeline\":\"CONTENT_RSS_V1\",\"publisherReferenceArtifactId\":\"20000000-0000-4000-8000-000000000001\",\"retainedSilverArtifactIds\":[]}");assertThat(SourceJson.sha(c.normalized())).isEqualTo("ccb5b21efeca0723add5093f11a1d88bfa7a7eb412cda9d03d4f87326b294de8");}
    @Test void reliabilityVector(){var c=parse(request(false));assertThat(SourceJson.sha(c.normalized())).isEqualTo("d7bcaa53a98d8a24872119379c64b60417982309e3fca8977207480b03e0a1b2");}
    @Test void setOrderCanonicalAndEvidenceSensitive(){for(boolean content:List.of(true,false)){var n=request(content);String ss=content?"fetchSources":"sources",aa=content?"retainedSilverArtifactIds":"batchArtifactIds";n.withArray(ss).addObject().put("sourceId","another").put("revision",2);n.withArray(aa).add("40000000-0000-4000-8000-000000000001");var before=parse(n).normalized();var ssArray=n.withArray(ss);var first=ssArray.remove(0);ssArray.add(first);var inputs=n.withArray(aa);if(inputs.size()>1){var input=inputs.remove(0);inputs.add(input);}assertThat(parse(n).normalized()).isEqualTo(before);((ObjectNode)ssArray.get(0)).put("revision",3);assertThat(parse(n).normalized()).isNotEqualTo(before);}}
    @ParameterizedTest @ValueSource(strings={"pipeline","executionProfileId","fetchSources","retainedSilverArtifactIds","publisherReferenceArtifactId"}) void missing(String f){var n=request(true);n.remove(f);assertThatThrownBy(()->parse(n)).hasMessage("INVALID_ARGUMENT");}
    @ParameterizedTest @ValueSource(strings={"null","1","true","[]","{}","1.5"}) void wronglyTyped(String v){var text=new String(SourceJson.canonical(request(true)),StandardCharsets.UTF_8).replace("\"CONTENT_RSS_V1\"",v);assertThatThrownBy(()->RunCommands.parse(text.getBytes(StandardCharsets.UTF_8),"key")).hasMessage("INVALID_ARGUMENT");}
    @Test void unknownDuplicateJsonAndFields(){var n=request(true).put("path","/private/data");assertThatThrownBy(()->parse(n)).hasMessage("INVALID_ARGUMENT");byte[] raw="{\"pipeline\":1,\"pipeline\":2}".getBytes(StandardCharsets.UTF_8);assertThatThrownBy(()->RunCommands.parse(raw,"key")).hasMessage("INVALID_ARGUMENT");}
    @Test void duplicateSelections(){var sources=request(false);sources.withArray("sources").add(sources.get("sources").get(0));assertThatThrownBy(()->parse(sources)).hasMessage("INVALID_ARGUMENT");var n=request(false);n.withArray("batchArtifactIds").add(INPUT);ObjectNode duplicate=n;assertThatThrownBy(()->parse(duplicate)).hasMessage("INVALID_ARGUMENT");}
    @ParameterizedTest @ValueSource(ints={0,1001,-1}) void invalidRevision(int v){var n=request(true);((ObjectNode)n.get("fetchSources").get(0)).put("revision",v);assertThatThrownBy(()->parse(n)).hasMessage("INVALID_ARGUMENT");}
    @Test void countBounds(){for(boolean content:List.of(true,false)){var n=request(content);String ss=content?"fetchSources":"sources";var a=n.withArray(ss);a.removeAll();assertThatThrownBy(()->parse(n)).hasMessage("INVALID_ARGUMENT");for(int i=0;i<(content?9:33);i++)a.addObject().put("sourceId","source"+i).put("revision",1);assertThatThrownBy(()->parse(n)).hasMessage("INVALID_ARGUMENT");}var n=request(true);for(int i=0;i<129;i++)n.withArray("retainedSilverArtifactIds").add(new UUID(0,i).toString());assertThatThrownBy(()->parse(n)).hasMessage("INVALID_ARGUMENT");}
    @ParameterizedTest @ValueSource(strings={"","a b","../x","ä"}) void invalidKey(String key){assertThatThrownBy(()->RunCommands.parse(SourceJson.canonical(request(true)),key)).hasMessage("INVALID_ARGUMENT");}
    @Test void boundedAndNoCoercion(){assertThatThrownBy(()->RunCommands.parse(new byte[65537],"k")).hasMessage("INVALID_ARGUMENT");var n=request(true);((ObjectNode)n.get("fetchSources").get(0)).put("revision","1");assertThatThrownBy(()->parse(n)).hasMessage("INVALID_ARGUMENT");}
    @Test void maximumSelectionsAndReliabilityBatchBounds(){for(boolean content:List.of(true,false)){var n=request(content);var sources=n.withArray(content?"fetchSources":"sources");sources.removeAll();for(int i=0;i<(content?8:32);i++)sources.addObject().put("sourceId","s"+i).put("revision",1000);var inputs=n.withArray(content?"retainedSilverArtifactIds":"batchArtifactIds");inputs.removeAll();for(int i=0;i<(content?128:32);i++)inputs.add(new UUID(0,i).toString());assertThat(parse(n).sources()).hasSize(content?8:32);}var n=request(false);n.withArray("batchArtifactIds").removeAll();assertThatThrownBy(()->parse(n)).hasMessage("INVALID_ARGUMENT");for(int i=0;i<33;i++)n.withArray("batchArtifactIds").add(new UUID(0,i).toString());assertThatThrownBy(()->parse(n)).hasMessage("INVALID_ARGUMENT");}
    @Test void unknownNestedAndDuplicateSourceAtDifferentRevision(){var n=request(true);((ObjectNode)n.get("fetchSources").get(0)).put("endpoint","https://private.example");assertThatThrownBy(()->parse(n)).hasMessage("INVALID_ARGUMENT");var duplicate=request(false);duplicate.withArray("sources").addObject().put("sourceId","demo").put("revision",2);assertThatThrownBy(()->parse(duplicate)).hasMessage("INVALID_ARGUMENT");}
    @Test void profileOnlyApprovedFormatAndPinnedVersions(){var c=parse(request(false));var n=RunFixtures.profile(false,"request-jsonl-v2");assertThat(RunProfile.validate(n,c)).isEqualTo(n);n.put("sourceProfile","rss-atom-v1");assertThatThrownBy(()->RunProfile.validate(n,c)).hasMessage("INVALID_ARGUMENT");}
}
