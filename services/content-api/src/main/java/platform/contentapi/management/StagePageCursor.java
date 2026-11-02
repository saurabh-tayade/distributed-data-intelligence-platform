package platform.contentapi.management;

import java.time.Clock;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.management.SourceFailure.require;

/** Signed stage-identity keysets with the same fixed lifetime as run history. */
final class StagePageCursor {
    record Key(String stage,int ordinal) implements Comparable<Key> {
        @Override public int compareTo(Key other){int c=stage.compareTo(other.stage);return c==0?Integer.compare(ordinal,other.ordinal):c;}
        String text(){return stage+":"+ordinal;}
    }
    record Position(Key high,Key after,long expires) { }
    private final SourceCursor signer;
    private final Clock clock;
    StagePageCursor(SourceCursor signer,Clock clock){this.signer=signer;this.clock=clock;}
    Position first(Key high){return new Position(high,null,clock.instant().getEpochSecond()+RunPageCursor.LIFETIME_SECONDS);}
    Position decode(String token,String scope) {
        JsonNode n=signer.decode(token);SourceJson.fields(n,"v","kind","scope","high","after","expires");
        require(SourceJson.integer(n.get("v"))==1 && SourceJson.text(n.get("kind")).equals("stages") && SourceJson.text(n.get("scope")).equals(scope));
        Key high=key(n.get("high")),after=key(n.get("after"));long expires=RunPageCursor.number(n.get("expires")),now=clock.instant().getEpochSecond();
        require(after.compareTo(high)<0 && now<expires && expires<=now+RunPageCursor.LIFETIME_SECONDS);
        return new Position(high,after,expires);
    }
    String next(String scope,Position p,Key after) {
        return signer.encode(SourceJson.object().put("v",1).put("kind","stages").put("scope",scope).put("high",p.high().text()).put("after",after.text()).put("expires",Long.toString(p.expires())));
    }
    private static Key key(JsonNode n) {
        String[] parts=SourceJson.text(n).split(":",-1);require(parts.length==2 && RunInspectionValues.STAGES.contains(parts[0]));
        require(parts[1].matches("0|[1-9][0-9]{0,2}"));int ordinal=Integer.parseInt(parts[1]);require(ordinal<=511);return new Key(parts[0],ordinal);
    }
}
