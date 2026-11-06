package platform.contentapi.execution;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

class NarrowContentFetchCompletionTest {
    static final byte[] COMMAND="{}".getBytes(StandardCharsets.UTF_8);
    ObjectNode marker(){
        var marker=SourceJson.object().put("journalCompletionVersion",1).put("directoryIdentity","1:2");var files=marker.putArray("files");
        for(String name:new TreeSet<>(RssJournalFiles.CANDIDATE)){
            var file=files.addObject().put("name",name).put("byteLength",2).put("sha256",SourceJson.sha(COMMAND));
            file.putObject("stamp").put("identity","1:3").put("modified","2026-11-06T00:00:00Z").put("size",2);
        }
        return marker;
    }
    @Test void markerDigestUsesTheOriginalFiveFileInventory(){
        var inventory=SourceJson.object();for(String name:new TreeSet<>(RssJournalFiles.CANDIDATE))inventory.putObject(name).put("bytes",2).put("sha256",SourceJson.sha(COMMAND));
        assertThat(NarrowContentFetchCompletion.markerCandidate(SourceJson.canonical(marker()),COMMAND)).isEqualTo(SourceJson.sha(SourceJson.canonical(inventory)));
    }
    @ParameterizedTest @ValueSource(strings={"version","identity","field","name","length","hash","stamp","command"})
    void malformedOrSubstitutedMarkerCannotBecomeIntent(String change){
        var marker=marker();var first=(ObjectNode)marker.get("files").get(0);byte[] command=COMMAND;
        switch(change){
            case "version"->marker.put("journalCompletionVersion",2);
            case "identity"->marker.put("directoryIdentity","../bad");
            case "field"->marker.put("unexpected",true);
            case "name"->first.put("name","foreign.json");
            case "length"->first.put("byteLength",65537);
            case "hash"->first.put("sha256","0".repeat(64));
            case "stamp"->((ObjectNode)first.get("stamp")).put("size",1);
            case "command"->command="{\"changed\":true}".getBytes(StandardCharsets.UTF_8);
        }
        byte[] supplied=command;assertThatThrownBy(()->NarrowContentFetchCompletion.markerCandidate(SourceJson.canonical(marker),supplied)).isInstanceOf(RuntimeException.class);
    }
    @ParameterizedTest @ValueSource(strings={"\"journalCompletionVersion\":1.0","\"journalCompletionVersion\":\"1\"","\"journalCompletionVersion\":1,\"journalCompletionVersion\":1"})
    void markerScalarAndDuplicateSubstitutionsAreRejected(String replacement){
        byte[] bytes=new String(SourceJson.canonical(marker()),StandardCharsets.UTF_8).replace("\"journalCompletionVersion\":1",replacement).getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(()->NarrowContentFetchCompletion.markerCandidate(bytes,COMMAND)).isInstanceOf(RuntimeException.class);
    }
}
