package platform.contentapi.management;

import java.net.URI;
import java.nio.file.*;
import java.util.*;
import tools.jackson.databind.JsonNode;
import platform.contentapi.execution.ManagementFailure;

/** Operator-owned allowlist. No fetching, DNS lookup, raw input registration or artifact import. */
public final class ReviewedSourceCatalog {
    public record Endpoint(String id,String source,String uri) { }
    public record Profile(String source,String profile,String digest) { }
    public record Storage(String id,String product,String role) { }
    private final Map<String,Endpoint> endpoints;
    private final Set<Profile> profiles;
    private final Map<String,Storage> storage;
    private ReviewedSourceCatalog(Map<String,Endpoint> endpoints,Set<Profile> profiles,Map<String,Storage> storage) {
        this.endpoints=Map.copyOf(endpoints);this.profiles=Set.copyOf(profiles);this.storage=Map.copyOf(storage);
    }
    static ReviewedSourceCatalog load(String location) {
        try {
            if(location.isEmpty()) return new ReviewedSourceCatalog(Map.of(),Set.of(),Map.of());
            Path path=Path.of(location); SourceFailure.require(path.isAbsolute());
            byte[] bytes;try(var stream=Files.newInputStream(path)) { bytes=stream.readNBytes(65537); }
            return from(SourceJson.parse(bytes));
        } catch(Exception ex) { throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_CONFIGURATION_INVALID); }
    }
    static ReviewedSourceCatalog from(JsonNode root) {
        SourceJson.fields(root,"catalogVersion","endpoints","reliabilityProfiles","storageProfiles");
        SourceFailure.require(SourceJson.integer(root.get("catalogVersion"))==1);
        for(String key:List.of("endpoints","reliabilityProfiles","storageProfiles"))
            SourceFailure.require(root.get(key).isArray() && root.get(key).size()<=128);
        Map<String,Endpoint> endpoints=new HashMap<>(); Set<Profile> profiles=new HashSet<>(); Map<String,Storage> storage=new HashMap<>();
        for(JsonNode e:root.get("endpoints")) {
            SourceJson.fields(e,"endpointId","sourceId","uri");
            String id=SourceCommands.uuid(SourceJson.text(e.get("endpointId"))), source=SourceCommands.source(SourceJson.text(e.get("sourceId"))), uri=SourceJson.text(e.get("uri"));
            SourceFailure.require(uri.length()<=2048); URI u=URI.create(uri); String host=u.getHost()==null?null:u.getHost().toLowerCase(Locale.ROOT);
            SourceFailure.require(host!=null && host.matches("(?i)[a-z0-9][a-z0-9.-]*\\.[a-z][a-z0-9.-]*") &&
                    !host.endsWith(".localhost") && !host.endsWith(".local") && !host.endsWith(".internal") &&
                    Set.of("http","https").contains(u.getScheme().toLowerCase(Locale.ROOT)) && u.getUserInfo()==null && u.getQuery()==null && u.getFragment()==null &&
                    u.getPort()!=0 && u.getPort()<=65535);
            SourceFailure.require(endpoints.putIfAbsent(id,new Endpoint(id,source,uri))==null);
        }
        for(JsonNode e:root.get("reliabilityProfiles")) {
            SourceJson.fields(e,"sourceId","profile","dataOrigin","registeredProfileSha256");
            String source=SourceCommands.source(SourceJson.text(e.get("sourceId"))), profile=SourceJson.text(e.get("profile")), hash=SourceJson.text(e.get("registeredProfileSha256"));
            SourceFailure.require(Set.of("request-jsonl-v2","request-parquet-v1").contains(profile) && SourceJson.text(e.get("dataOrigin")).equals("synthetic") && hash.matches("[0-9a-f]{64}"));
            SourceFailure.require(profiles.add(new Profile(source,profile,hash)));
        }
        for(JsonNode e:root.get("storageProfiles")) {
            SourceJson.fields(e,"profileId","productId","role");
            String id=SourceCommands.uuid(SourceJson.text(e.get("profileId"))), product=SourceCommands.product(SourceJson.text(e.get("productId"))), role=SourceJson.text(e.get("role"));
            SourceFailure.require(role.equals(product.equals("content-intelligence")?"BRONZE":"BATCH"));
            SourceFailure.require(storage.putIfAbsent(id,new Storage(id,product,role))==null);
        }
        return new ReviewedSourceCatalog(endpoints,profiles,storage);
    }
    Storage validate(String product,String source,JsonNode config) {
        String profile=SourceJson.text(config.get("profile")), id=SourceJson.text(config.get("inputStorageProfileId"));
        Storage binding=storage.get(id); SourceFailure.require(binding!=null && binding.product().equals(product));
        if(profile.equals("rss-atom-v1")) {
            Endpoint endpoint=endpoints.get(SourceJson.text(config.get("feedEndpointId")));
            SourceFailure.require(endpoint!=null && endpoint.source().equals(source));
        } else SourceFailure.require(profiles.contains(new Profile(source,profile,SourceJson.text(config.get("registeredProfileSha256")))));
        return binding;
    }
    String endpointDigest(JsonNode config) {
        if(config.get("feedEndpointId").isNull())return null;
        return SourceJson.sha(SourceJson.canonical(endpointEvidence(config)));
    }
    JsonNode endpointEvidence(JsonNode config) {
        Endpoint e=endpoints.get(SourceJson.text(config.get("feedEndpointId")));SourceFailure.require(e!=null);
        return SourceJson.object().put("endpointId",e.id()).put("sourceId",e.source()).put("uri",e.uri());
    }
    JsonNode capabilities() {
        var n=SourceJson.object(); n.put("scope","SOURCE_MANAGEMENT_ONLY");
        var ps=n.putArray("profiles");for(String p:List.of("rss-atom-v1","request-jsonl-v2","request-parquet-v1")) ps.add(p);
        var es=n.putArray("endpoints");endpoints.values().stream().sorted(Comparator.comparing(Endpoint::id)).forEach(e -> es.addObject().put("endpointId",e.id()).put("sourceId",e.source()));
        var rs=n.putArray("reliabilityProfiles");profiles.stream().sorted(Comparator.comparing(Profile::source).thenComparing(Profile::profile).thenComparing(Profile::digest))
                .forEach(p -> rs.addObject().put("sourceId",p.source()).put("profile",p.profile()).put("dataOrigin","synthetic").put("registeredProfileSha256",p.digest()));
        var ss=n.putArray("storageProfiles");storage.values().stream().sorted(Comparator.comparing(Storage::id)).forEach(s -> ss.addObject().put("profileId",s.id()).put("productId",s.product()).put("role",s.role()));
        n.put("maxSources",128);n.put("maxRevisionsPerSource",1000);return n;
    }
}
