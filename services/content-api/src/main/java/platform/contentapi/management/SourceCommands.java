package platform.contentapi.management;

import java.nio.charset.StandardCharsets;
import java.util.*;
import tools.jackson.databind.JsonNode;

/** Exact fixed HTTP source DTOs, validated independently of Jackson coercion/defaults. */
public final class SourceCommands {
    private SourceCommands() { }
    public enum Action { CREATE, REVISE, ENABLE }
    public record Command(Action action,String product,String source,String name,Boolean enabled,JsonNode config,
                          Long expectedVersion,String key,byte[] normalized,String scope) { }
    static String source(String value) { SourceFailure.require(value.matches("[a-z0-9][a-z0-9_-]{0,127}"));return value; }
    static String product(String value) { SourceFailure.require(Set.of("content-intelligence","application-reliability").contains(value));return value; }
    static String uuid(String value) {
        try { SourceFailure.require(UUID.fromString(value).toString().equals(value));return value; }
        catch(IllegalArgumentException ex) { throw new SourceFailure(SourceFailure.Code.INVALID_ARGUMENT); }
    }
    static String label(JsonNode node) {
        String s=SourceJson.text(node);SourceFailure.require(!s.isBlank() && s.getBytes(StandardCharsets.UTF_8).length<=128);
        SourceFailure.require(s.codePoints().noneMatch(c -> Character.isISOControl(c)));return s;
    }
    static JsonNode config(String product,JsonNode n) {
        SourceJson.fields(n,"configVersion","profile","dataOrigin","feedEndpointId","registeredProfileSha256","inputStorageProfileId");
        SourceFailure.require(SourceJson.integer(n.get("configVersion"))==1);
        String profile=SourceJson.text(n.get("profile"));uuid(SourceJson.text(n.get("inputStorageProfileId")));
        if(product.equals("content-intelligence")) {
            SourceFailure.require(profile.equals("rss-atom-v1") && SourceJson.text(n.get("dataOrigin")).equals("publisher") && n.get("registeredProfileSha256").isNull());
            uuid(SourceJson.text(n.get("feedEndpointId")));
        } else {
            SourceFailure.require(Set.of("request-jsonl-v2","request-parquet-v1").contains(profile) && SourceJson.text(n.get("dataOrigin")).equals("synthetic") && n.get("feedEndpointId").isNull());
            SourceFailure.require(SourceJson.text(n.get("registeredProfileSha256")).matches("[0-9a-f]{64}"));
        }
        SourceFailure.require(SourceJson.canonical(n).length<=65536);return n.deepCopy();
    }
    public static Command command(Action action,String product,String source,JsonNode body,String key,String ifMatch) {
        SourceFailure.require(key!=null && key.matches("[A-Za-z0-9_-]{1,128}"));
        Long expected=null;String name=null;Boolean enabled=null;JsonNode config=null;
        if(action==Action.CREATE) {
            SourceJson.fields(body,"productId","sourceId","displayName","enabled","config");
            product=product(SourceJson.text(body.get("productId")));source=source(SourceJson.text(body.get("sourceId")));
            SourceFailure.require(ifMatch==null);name=label(body.get("displayName"));enabled=SourceJson.bool(body.get("enabled"));config=config(product,body.get("config"));
        } else {
            product=product(product);source=source(source);
            if(ifMatch==null) throw new SourceFailure(SourceFailure.Code.PRECONDITION_REQUIRED);
            SourceFailure.require(ifMatch.matches("\"(0|[1-9][0-9]{0,18})\""));
            try { expected=Long.parseLong(ifMatch.substring(1,ifMatch.length()-1)); } catch(NumberFormatException ex) {throw new SourceFailure(SourceFailure.Code.INVALID_ARGUMENT);}
            if(action==Action.REVISE) { SourceJson.fields(body,"displayName","config");name=label(body.get("displayName"));config=config(product,body.get("config")); }
            else {SourceJson.fields(body,"enabled");enabled=SourceJson.bool(body.get("enabled"));}
        }
        var identity=SourceJson.object().put("action",action.name()).put("productId",product).put("sourceId",source);
        String scope=SourceJson.sha(SourceJson.canonical(identity));
        if(expected==null) identity.putNull("expectedVersion");else identity.put("expectedVersion",Long.toString(expected));
        identity.set("body",body); byte[] normalized=SourceJson.canonical(identity);
        SourceFailure.require(normalized.length<=65536);
        return new Command(action,product,source,name,enabled,config,expected,key,normalized,scope);
    }
}
