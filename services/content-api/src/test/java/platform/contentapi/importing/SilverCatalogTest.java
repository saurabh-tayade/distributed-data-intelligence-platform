package platform.contentapi.importing;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
class SilverCatalogTest {
    ObjectNode selection(){String key="bronze/rss/source_id=ZGVtbw/ingestion_date=2026-10-12/ingestion_id=50000000-0000-4000-8000-000000000001/metadata.json";
        var n=SourceJson.object().put("kind","SILVER").put("productId","content-intelligence").put("storageBindingId","silver-test").put("contractVersion",1).put("processorRevision",1).put("relativeAddress","v1/25bf8e1a2393f1108d37029b3df5593236c755742ec93465bbafa9b290bddcf6/"+key.substring("bronze/rss/".length()).replace("metadata.json","complete.json"));
        n.set("identity",SourceJson.object().put("sourceId","demo").put("ingestionId","50000000-0000-4000-8000-000000000001").put("bronzeStorageBindingId","bronze-test").put("bronzeBucket","local").put("bronzeMetadataKey",key).put("bronzePayloadKey",key.replace("metadata.json","payload.xml")));return n;}
    ImportCatalog parse(ObjectNode e){var n=SourceJson.object().put("catalogVersion",1).put("operationId","40000000-0000-4000-8000-000000000001").put("executionProfileId","silver-runtime").put("controlBindingId","control");n.putArray("artifacts").add(e);return ImportCatalog.parse(SourceJson.canonical(n));}
    @Test void exactSilverHasNoNativeFingerprint(){var c=parse(selection());var e=c.entries().getFirst();assertThat(e.silver()).isTrue();assertThat(e.fingerprint()).isNull();assertThat(e.json().has("fingerprint")).isFalse();assertThat(e.nativeIdentity()).contains("metadataKeySha256").doesNotContain("inventory");assertThat(e.nativeIdentity().length()).isLessThan(512);}
    @ParameterizedTest @ValueSource(strings={"sourceId","ingestionId","bronzeBucket","bronzeMetadataKey","bronzePayloadKey"})void incompatibleNativeAddress(String field){var n=selection();((ObjectNode)n.get("identity")).put(field,"different");assertThatThrownBy(()->parse(n)).hasMessage("INVALID_CATALOG").hasNoCause();}
    @ParameterizedTest @ValueSource(strings={"/private/absolute","../escape","v1/*/complete.json"})void noDiscovery(String address){var n=selection().put("relativeAddress",address);assertThatThrownBy(()->parse(n)).hasMessage("INVALID_CATALOG");}
    @Test void nativeFingerprintCannotBeInvented(){assertThatThrownBy(()->parse(selection().put("fingerprint","a".repeat(64)))).hasMessage("INVALID_CATALOG");}
    @Test void versionsAndProductAreFixed(){for(String field:List.of("contractVersion","processorRevision"))assertThatThrownBy(()->parse(selection().put(field,2))).hasMessage("INVALID_CATALOG");assertThatThrownBy(()->parse(selection().put("productId","application-reliability"))).hasMessage("INVALID_CATALOG");}
}
