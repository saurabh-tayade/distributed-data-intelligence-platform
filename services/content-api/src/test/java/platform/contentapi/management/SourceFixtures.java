package platform.contentapi.management;

import java.nio.charset.StandardCharsets;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

final class SourceFixtures {
    static final String CONTENT="content-intelligence", RELIABILITY="application-reliability";
    static final String ENDPOINT="20000000-0000-4000-8000-000000000001", ENDPOINT2="20000000-0000-4000-8000-000000000002";
    static final String BRONZE="30000000-0000-4000-8000-000000000001", BATCH="30000000-0000-4000-8000-000000000002";
    static final String HASH="a".repeat(64), KEY="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    static ObjectNode rss() {
        return SourceJson.object().put("configVersion",1).put("profile","rss-atom-v1").put("dataOrigin","publisher")
                .put("feedEndpointId",ENDPOINT).putNull("registeredProfileSha256").put("inputStorageProfileId",BRONZE);
    }
    static ObjectNode requests(String profile) {
        return SourceJson.object().put("configVersion",1).put("profile",profile).put("dataOrigin","synthetic")
                .putNull("feedEndpointId").put("registeredProfileSha256",HASH).put("inputStorageProfileId",BATCH);
    }
    static ObjectNode create(String product,String source,JsonNode config) {
        var n=SourceJson.object().put("productId",product).put("sourceId",source).put("displayName","Reviewed source").put("enabled",true);n.set("config",config);return n;
    }
    static ObjectNode revise(String name,JsonNode config) {var n=SourceJson.object().put("displayName",name);n.set("config",config);return n;}
    static byte[] bytes(String s) {return s.getBytes(StandardCharsets.UTF_8);}
    static ObjectNode catalog() {
        var n=SourceJson.object().put("catalogVersion",1);
        var endpoints=n.putArray("endpoints");endpoints.addObject().put("endpointId",ENDPOINT).put("sourceId","demo-feed").put("uri","https://feeds.example.org/one");
        endpoints.addObject().put("endpointId",ENDPOINT2).put("sourceId","demo-feed").put("uri","https://feeds.example.org/two");
        var profiles=n.putArray("reliabilityProfiles");
        for(String source:new String[]{"demo-requests","second-requests"})for(String profile:new String[]{"request-jsonl-v2","request-parquet-v1"})
            profiles.addObject().put("sourceId",source).put("profile",profile).put("dataOrigin","synthetic").put("registeredProfileSha256",HASH);
        var storage=n.putArray("storageProfiles");storage.addObject().put("profileId",BRONZE).put("productId",CONTENT).put("role","BRONZE");
        storage.addObject().put("profileId",BATCH).put("productId",RELIABILITY).put("role","BATCH");return n;
    }
}
