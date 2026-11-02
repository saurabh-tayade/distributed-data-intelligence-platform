package platform.contentapi.management;

import java.time.Clock;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.management.SourceFailure.require;

/** Opaque, endpoint/filter-scoped UUID keyset traversal. No snapshot or worker-readiness claim. */
final class DiscoveryCursor {
    static final long LIFETIME_SECONDS=86400;
    record Position(String after,long expires) { }
    private final SourceCursor signer;
    private final Clock clock;
    DiscoveryCursor(SourceCursor signer,Clock clock){this.signer=signer;this.clock=clock;}
    Position position(String token,String endpoint,String scope) {
        long now=clock.instant().getEpochSecond();
        if(token==null)return new Position(null,now+LIFETIME_SECONDS);
        JsonNode n=signer.decode(token);SourceJson.fields(n,"v","kind","scope","after","expires");
        require(SourceJson.integer(n.get("v"))==1 && SourceJson.text(n.get("kind")).equals(endpoint) && SourceJson.text(n.get("scope")).equals(scope));
        String after=SourceCommands.uuid(SourceJson.text(n.get("after")));long expires=RunPageCursor.number(n.get("expires"));
        require(now<expires && expires<=now+LIFETIME_SECONDS);return new Position(after,expires);
    }
    String next(String endpoint,String scope,Position p,String after) {
        SourceCommands.uuid(after);
        return signer.encode(SourceJson.object().put("v",1).put("kind",endpoint).put("scope",scope).put("after",after).put("expires",Long.toString(p.expires())));
    }
}
