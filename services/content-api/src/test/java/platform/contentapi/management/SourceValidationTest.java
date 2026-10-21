package platform.contentapi.management;

import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.env.MockEnvironment;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

class SourceValidationTest {
    @Test void literalPythonCanonicalConfigVector() {
        String literal="{\"configVersion\":1,\"dataOrigin\":\"publisher\",\"feedEndpointId\":\"20000000-0000-4000-8000-000000000001\",\"inputStorageProfileId\":\"30000000-0000-4000-8000-000000000001\",\"profile\":\"rss-atom-v1\",\"registeredProfileSha256\":null}";
        assertThat(SourceJson.canonical(rss())).isEqualTo(bytes(literal));
        assertThat(SourceJson.sha(bytes(literal))).isEqualTo("621880a6619c9d7dc570a9fd3bfba86909d6628872503d7d7cba3d30cc489f86");
    }
    @Test void literalUnicodeControlAndArrayVector() {
        var input=SourceJson.parse(bytes("{\"z\":[\"\\b\",\"\\n\",\"雪\",\"😀\"],\"nullable\":null,\"enabled\":false,\"a\":{\"😀\":2,\"\ue000\":1}}"));
        String literal="{\"a\":{\"\ue000\":1,\"😀\":2},\"enabled\":false,\"nullable\":null,\"z\":[\"\\b\",\"\\n\",\"雪\",\"😀\"]}";
        assertThat(SourceJson.canonical(input)).isEqualTo(bytes(literal));assertThat(SourceJson.sha(bytes(literal))).isEqualTo("c53c663783ba3275e52b23a16cfaa7c4163a7f8323557966c876b72e7ca0f885");
    }
    @ParameterizedTest @ValueSource(strings={"{\"x\":1,\"x\":2}","{\"config\":{\"x\":1,\"x\":2}}","{} {}","{\"n\":1.0}","{\"n\":NaN}","{\"n\":true,}","{\"n\":\"\\ud800\"}","\ufeff{}",""})
    void malformedOrNoncanonicalTypesFailWithoutRawDiagnostics(String json) {
        assertThatThrownBy(() -> SourceJson.parse(bytes(json))).isInstanceOf(SourceFailure.class).hasMessage("INVALID_ARGUMENT").hasNoCause();
    }
    @Test void invalidUtf8AndBodyLimit() {
        assertThatThrownBy(() -> SourceJson.parse(new byte[]{(byte)0xc0,(byte)0x80})).hasMessage("INVALID_ARGUMENT");
        assertThatThrownBy(() -> SourceJson.parse(new byte[65537])).hasMessage("INVALID_ARGUMENT");
    }
    @ParameterizedTest @ValueSource(strings={"rss-atom-v1","request-jsonl-v2","request-parquet-v1"})
    void fixedProfilesAreAdmittedOnlyThroughReviewedReferences(String profile) {
        var config=profile.equals("rss-atom-v1")?rss():requests(profile);String product=profile.equals("rss-atom-v1")?CONTENT:RELIABILITY,source=profile.equals("rss-atom-v1")?"demo-feed":"demo-requests";
        var command=SourceCommands.command(SourceCommands.Action.CREATE,null,null,create(product,source,config),"key",null);
        assertThat(ReviewedSourceCatalog.from(catalog()).validate(product,source,command.config())).isNotNull();
    }
    @Test void canonicalOrderingNormalizesCommandIdentity() {
        var a=create(CONTENT,"demo-feed",rss());var b=SourceJson.parse(bytes("{\"config\":"+new String(SourceJson.canonical(rss()),java.nio.charset.StandardCharsets.UTF_8)+",\"enabled\":true,\"displayName\":\"Reviewed source\",\"sourceId\":\"demo-feed\",\"productId\":\"content-intelligence\"}"));
        assertThat(SourceCommands.command(SourceCommands.Action.CREATE,null,null,a,"key",null).normalized())
                .isEqualTo(SourceCommands.command(SourceCommands.Action.CREATE,null,null,b,"key",null).normalized());
    }
    @ParameterizedTest @ValueSource(strings={"../bad","CamelCase","","with space","a/b"})
    void invalidSourceIds(String id) {assertThatThrownBy(() -> SourceCommands.command(SourceCommands.Action.CREATE,null,null,create(CONTENT,id,rss()),"key",null)).hasMessage("INVALID_ARGUMENT");}
    @Test void exactFieldsTypesNullsAndProductCombinations() {
        var inputs=new ArrayList<tools.jackson.databind.node.ObjectNode>();
        var base=create(CONTENT,"demo-feed",rss());
        var extra=base.deepCopy().put("password","PRIVATE");inputs.add(extra);
        var missing=base.deepCopy();missing.remove("enabled");inputs.add(missing);
        inputs.add(base.deepCopy().put("enabled","true"));inputs.add(base.deepCopy().put("displayName",5));
        var config=rss();config.remove("registeredProfileSha256");inputs.add(create(CONTENT,"demo-feed",config));
        inputs.add(create(CONTENT,"demo-feed",rss().put("configVersion","1")));
        inputs.add(create(CONTENT,"demo-feed",requests("request-jsonl-v2")));
        inputs.add(create(RELIABILITY,"demo-requests",requests("request-jsonl-v2").put("dataOrigin","production")));
        inputs.add(create(CONTENT,"demo-feed",rss().put("feedEndpointId","https://unsafe.example/path")));
        inputs.add(create(CONTENT,"demo-feed",rss().put("inputStorageProfileId","/private/path")));
        inputs.add(create(CONTENT,"demo-feed",rss().put("credentials","PRIVATE")));
        for(var input:inputs)assertThatThrownBy(() -> SourceCommands.command(SourceCommands.Action.CREATE,null,null,input,"key",null)).hasMessage("INVALID_ARGUMENT").hasNoCause();
    }
    @Test void utf8DisplayByteLimitIsNotCharacterCount() {
        SourceCommands.command(SourceCommands.Action.CREATE,null,null,create(CONTENT,"demo-feed",rss()).put("displayName","é".repeat(64)),"key",null);
        assertThatThrownBy(() -> SourceCommands.command(SourceCommands.Action.CREATE,null,null,create(CONTENT,"demo-feed",rss()).put("displayName","é".repeat(65)),"key",null)).hasMessage("INVALID_ARGUMENT");
    }
    @ParameterizedTest @ValueSource(strings={"http://localhost/feed","http://127.0.0.1/feed","https://example.org/feed?q=secret","https://user:password@example.org/feed","file:///private/feed","https://example.org/feed#part","https://example.org:0/feed"})
    void serverCatalogUriPolicyHasNoLiveFetch(String uri) {
        var c=catalog();((tools.jackson.databind.node.ObjectNode)c.get("endpoints").get(0)).put("uri",uri);
        assertThatThrownBy(() -> ReviewedSourceCatalog.from(c)).isInstanceOf(RuntimeException.class);
    }
    @Test void unknownOrMismatchedReviewedReferencesFail() {
        var c=ReviewedSourceCatalog.from(catalog());
        assertThatThrownBy(() -> c.validate(CONTENT,"different-source",rss())).hasMessage("INVALID_ARGUMENT");
        assertThatThrownBy(() -> c.validate(RELIABILITY,"demo-requests",requests("request-jsonl-v2").put("registeredProfileSha256","b".repeat(64)))).hasMessage("INVALID_ARGUMENT");
        assertThatThrownBy(() -> c.validate(CONTENT,"demo-feed",rss().put("inputStorageProfileId",BATCH))).hasMessage("INVALID_ARGUMENT");
    }
    @ParameterizedTest @ValueSource(strings={"W/\"1\"","*","1","\"-1\"","\"01\"","\"9223372036854775808\""})
    void invalidConditionalHeaders(String header) {assertThatThrownBy(() -> SourceCommands.command(SourceCommands.Action.ENABLE,CONTENT,"demo-feed",SourceJson.object().put("enabled",false),"key",header)).hasMessage("INVALID_ARGUMENT");}
    @Test void missingPreconditionIsDistinct() {assertThatThrownBy(() -> SourceCommands.command(SourceCommands.Action.ENABLE,CONTENT,"demo-feed",SourceJson.object().put("enabled",false),"key",null)).hasMessage("PRECONDITION_REQUIRED");}
    @Test void cursorsAuthenticatePayloadAndSurviveConfiguredKeyRestart() {
        var one=new SourceCursor(KEY);String token=one.encode(SourceJson.object().put("v",1).put("kind","sources"));
        assertThat(new SourceCursor(KEY).decode(token).get("v").intValue()).isEqualTo(1);
        assertThatThrownBy(() -> one.decode("x"+token.substring(1))).hasMessage("INVALID_ARGUMENT");
        assertThatThrownBy(() -> new SourceCursor("").decode(token)).hasMessage("INVALID_ARGUMENT");
        assertThatThrownBy(() -> one.decode("x".repeat(513))).hasMessage("INVALID_ARGUMENT");
    }
    @Test void nonLoopbackManagementBindIsRejectedAndAnalyticsIgnoresManagementConfiguration() {
        assertThatThrownBy(() -> new ManagementConfiguration().localPolicy(new MockEnvironment().withProperty("server.address","0.0.0.0"))).hasMessage("MANAGEMENT_CONFIGURATION_INVALID");
        try(var context=new org.springframework.boot.builder.SpringApplicationBuilder(platform.contentapi.ContentApiApplication.class)
                .web(org.springframework.boot.WebApplicationType.NONE).run("--ddip.role=analytics","--ddip.management.catalog=/nonexistent/private-catalog","--ddip.management.cursor-key=invalid","--logging.level.root=OFF","--spring.main.banner-mode=off")) {
            assertThat(context.getBeansOfType(SourceRepository.class)).isEmpty();assertThat(context.getBeansOfType(SourceController.class)).isEmpty();
        }
    }
    @ParameterizedTest @ValueSource(strings={"http://example.org:8080","http://127.0.0.1:8080/path","http://127.0.0.1:0","http://127.0.0.1:8080?x=1"})
    void unsafeOriginConfigurationFailsClosed(String origin) {
        assertThatThrownBy(() -> new ManagementConfiguration().localPolicy(new MockEnvironment().withProperty("ddip.management.origins",origin))).hasMessage("MANAGEMENT_CONFIGURATION_INVALID").hasNoCause();
    }
}
