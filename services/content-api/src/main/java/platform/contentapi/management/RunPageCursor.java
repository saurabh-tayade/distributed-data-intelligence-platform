package platform.contentapi.management;

import java.time.Clock;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.management.SourceFailure.require;

/** Run history scopes use B2a's signer, with a fixed 24-hour traversal lifetime. */
final class RunPageCursor {
    static final long LIFETIME_SECONDS=86400;
    record Position(long high,long after,long expires) { }
    private final SourceCursor signer;
    private final Clock clock;
    RunPageCursor(SourceCursor signer,Clock clock){this.signer=signer;this.clock=clock;}
    Position first(long high,boolean descending){return new Position(high,descending?high:0,clock.instant().getEpochSecond()+LIFETIME_SECONDS);}
    Position decode(String token,String kind,String scope,boolean descending) {
        JsonNode n=signer.decode(token);SourceJson.fields(n,"v","kind","scope","high","after","expires");
        require(SourceJson.integer(n.get("v"))==1 && SourceJson.text(n.get("kind")).equals(kind) && SourceJson.text(n.get("scope")).equals(scope));
        long high=number(n.get("high")),after=number(n.get("after")),expires=number(n.get("expires"));
        require(high>0 && after>0 && (descending?after<=high:after<high) && clock.instant().getEpochSecond()<expires && expires<=clock.instant().getEpochSecond()+LIFETIME_SECONDS);
        return new Position(high,after,expires);
    }
    String next(String kind,String scope,Position p,long after) {
        return signer.encode(SourceJson.object().put("v",1).put("kind",kind).put("scope",scope).put("high",Long.toString(p.high())).put("after",Long.toString(after)).put("expires",Long.toString(p.expires())));
    }
    static long number(JsonNode n) {
        String s=SourceJson.text(n);require(s.matches("0|[1-9][0-9]{0,18}"));
        try{return Long.parseLong(s);}catch(NumberFormatException ex){throw new SourceFailure(SourceFailure.Code.INVALID_ARGUMENT);}
    }
}
