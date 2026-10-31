package platform.contentapi.management;

import tools.jackson.databind.JsonNode;

/** Test-only bridge for constructing a reviewed native-source fixture without widening production API. */
public final class PreflightCatalogFixture {
    private PreflightCatalogFixture() { }
    public static SourceCursor cursor(){return new SourceCursor("");}
    public static ReviewedSourceCatalog captured(JsonNode n,JsonNode frozen){
        try{var base=from(n);var type=ReviewedSourceCatalog.class;var endpoints=type.getDeclaredField("endpoints");var profiles=type.getDeclaredField("profiles");var storage=type.getDeclaredField("storage");for(var f:java.util.List.of(endpoints,profiles,storage))f.setAccessible(true);@SuppressWarnings("unchecked") var map=new java.util.HashMap<String,ReviewedSourceCatalog.Endpoint>((java.util.Map<String,ReviewedSourceCatalog.Endpoint>)endpoints.get(base));for(var source:frozen.get("sources")){var e=source.get("endpoint");map.put(e.get("endpointId").stringValue(),new ReviewedSourceCatalog.Endpoint(e.get("endpointId").stringValue(),e.get("sourceId").stringValue(),e.get("uri").stringValue()));}var ctor=type.getDeclaredConstructor(java.util.Map.class,java.util.Set.class,java.util.Map.class);ctor.setAccessible(true);return ctor.newInstance(map,profiles.get(base),storage.get(base));}catch(ReflectiveOperationException ex){throw new AssertionError(ex);}
    }
    public static ReviewedSourceCatalog from(JsonNode n){return ReviewedSourceCatalog.from(n);}
}
