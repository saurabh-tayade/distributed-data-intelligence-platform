package platform.contentapi.management;

import tools.jackson.databind.JsonNode;

/** Test-only bridge for constructing a reviewed native-source fixture without widening production API. */
public final class PreflightCatalogFixture {
    private PreflightCatalogFixture() { }
    public static ReviewedSourceCatalog from(JsonNode n){return ReviewedSourceCatalog.from(n);}
}
