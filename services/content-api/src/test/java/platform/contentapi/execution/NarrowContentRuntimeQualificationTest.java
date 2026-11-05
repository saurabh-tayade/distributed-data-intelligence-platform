package platform.contentapi.execution;

import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

class NarrowContentRuntimeQualificationTest {
    record Invocation(String sql,List<Object> arguments) { }
    /** No connection exists: any unhandled JDBC operation, including mutation, fails the test. */
    static final class RecordingJdbcTemplate extends JdbcTemplate {
        private final Fixture fixture;
        final List<Invocation> calls=new ArrayList<>();
        RecordingJdbcTemplate(Fixture fixture){
            super(new org.springframework.jdbc.datasource.AbstractDataSource(){
                @Override public java.sql.Connection getConnection(){throw new AssertionError("Unexpected JDBC operation");}
                @Override public java.sql.Connection getConnection(String username,String password){throw new AssertionError("Unexpected JDBC operation");}
            });
            this.fixture=fixture;
        }
        private void record(String sql,Object[] args){
            var copy=new ArrayList<Object>();for(Object arg:args)copy.add(arg instanceof byte[] bytes?bytes.clone():arg);
            calls.add(new Invocation(sql,List.copyOf(copy)));
        }
        @Override public List<Map<String,Object>> queryForList(String sql,Object... args){
            record(sql,args);
            if(sql.equals("SELECT captured_selection,selection_sha256,execution_profile_id FROM ddip_management.runs WHERE run_id=?")){
                if(args.length!=1 || !fixture.run.equals(args[0]))throw new AssertionError("Unexpected run query arguments");
                return List.of(Map.of("captured_selection",SourceJson.canonical(fixture.frozen),"selection_sha256",sha(fixture.frozen),"execution_profile_id","profile"));
            }
            if(sql.equals("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?")){
                if(args.length!=1 || !(args[0] instanceof String))throw new AssertionError("Unexpected profile query arguments");
                var c=fixture.configs.get((String)args[0]);
                return c==null?List.of():List.of(Map.of("configuration_bytes",SourceJson.canonical(c),"configuration_sha256",sha(c)));
            }
            throw new AssertionError("Unexpected SQL: "+sql);
        }
        @Override public <T> T queryForObject(String sql,Class<T> type,Object... args){
            record(sql,args);
            String expected="SELECT EXISTS(SELECT 1 FROM ddip_management.execution_profile_storage p JOIN ddip_management.storage_bindings b USING(binding_id) WHERE p.profile_id=? AND b.binding_id=? AND b.installation_id=? AND b.role=? AND b.product=? AND b.configuration_sha256=? AND b.configuration_bytes=?)";
            if(!sql.equals(expected) || type!=Boolean.class || args.length!=7)throw new AssertionError("Unexpected SQL or membership query arguments: "+sql);
            return type.cast(fixture.membership);
        }
    }
    static final class Fixture {
        final RecordingJdbcTemplate jdbc=new RecordingJdbcTemplate(this);
        final ManagementTransactions tx=new ManagementTransactions(new org.springframework.transaction.support.TransactionTemplate(){
            @Override public <T> T execute(org.springframework.transaction.support.TransactionCallback<T> callback){return callback.doInTransaction(new org.springframework.transaction.support.SimpleTransactionStatus());}
        });
        final Map<String,ObjectNode> configs=new HashMap<>();
        final InstallationHostGuard.Identity identity=new InstallationHostGuard.Identity(1,2);
        final InstallationHostGuard.Binding host=new InstallationHostGuard.Binding("control",UUID.randomUUID(),Path.of("/nonexistent/control"),identity,identity,identity);
        final UUID run=UUID.randomUUID(),attempt=UUID.randomUUID();
        final ObjectNode frozen=SourceJson.object();
        final String deployment,profileHash;
        boolean membership=true;
        Fixture(){
            var manifest=SourceJson.object().put("adapter","platform.ingestion.managed.BoundedManagedRssFetch").put("javaRuntime","21");
            manifest.putArray("classpath").addObject().put("path","/nonexistent/code.jar").put("sha256","a".repeat(64));deployment=sha(manifest);
            var profile=SourceJson.object().put("runProfileVersion",2).put("pipeline","CONTENT_RSS_V1").put("pipelineVersion",1).put("sourceProfile","rss-atom-v1").put("contentRuntimeId","content");
            profile.putObject("runtime").put("javaVersion","21").put("pythonVersion","3.12.15").put("pysparkVersion","4.0.2").put("deploymentSha256",deployment);
            configs.put("profile",profile);profileHash=sha(profile);
            frozen.put("pipeline","CONTENT_RSS_V1");frozen.putObject("executionProfile").put("profileId","profile").put("configurationSha256",profileHash).set("configuration",profile);
            var source=frozen.putArray("sources").addObject().put("sourceId","source-one").put("revision",1);
            var sourceConfig=source.putObject("configuration").put("inputStorageProfileId","bronze-store");source.put("configurationSha256",sha(sourceConfig));
            var endpoint=source.putObject("endpoint").put("uri","http://127.0.0.1:1234/feed");source.put("endpointSha256",sha(endpoint));
            frozen.putArray("inputs").addObject().put("role","PUBLISHER_REFERENCE").put("ordinal",0).put("storageBindingId","reference-store");frozen.putArray("storageBindings");
            binding("control","CONTROL",SourceJson.parse(host.descriptor()));
            for(String role:List.of("BRONZE","SILVER","TEMP","GOLD","REFERENCE","CONTENT"))binding(role.toLowerCase()+"-store",role,role.equals("BRONZE")?SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:9000").put("region","local").put("bucket","bronze").put("credentialsFile","/never/read/credentials"):SourceJson.object().put("root","/nonexistent/"+role.toLowerCase()).put("rootDevice","1").put("rootInode","2"));
            var fetch=common().put("rssFetchRuntimeVersion",2).put("pipeline","CONTENT_RSS_V1").put("runProfileId","profile").put("javaHome","/nonexistent/java").put("controlBindingId","control").put("testEndpoint",true).put("httpTimeoutMillis",1000);fetch.set("manifest",manifest);configs.put("fetch",fetch);
            var bronze=common().put("bronzeRuntimeVersion",2).put("adapter","platform.ingestion.managed.BoundedManagedBronze").put("fetchRuntimeId","fetch").put("fetchRuntimeSha256",sha(fetch)).put("runProfileId","profile").put("controlBindingId","control").put("storageBindingId","bronze-store").put("storageBindingSha256",sha(frozen.get("storageBindings").get(1).get("configuration")));configs.put("bronze",bronze);
            var silver=common().put("silverRuntimeVersion",2).put("bronzeRuntimeId","bronze").put("bronzeRuntimeSha256",sha(bronze)).put("python","/nonexistent/python").put("javaHome","/nonexistent/java").put("processingRoot","/nonexistent/processing").put("outputBindingId","silver-store").put("tempBindingId","temp-store").put("seconds",30);modules(silver,RssSilverRuntime.MODULES);silver.set("versions",SourceJson.object().put("python","3.12.15").put("pyspark","4.0.2").put("boto3","1.43.114").put("java","21"));configs.put("silver",silver);
            var gold=SourceJson.object().put("goldRuntimeVersion",2).put("silverRuntimeId","silver").put("silverRuntimeSha256",sha(silver)).put("goldBindingId","gold-store").put("duckdb","1.4.5").put("seconds",30).put("python","/nonexistent/python");modules(gold,RssGoldRuntime.MODULES);configs.put("gold",gold);
            var content=SourceJson.object().put("contentRuntimeVersion",2).put("goldRuntimeId","gold").put("goldRuntimeSha256",sha(gold)).put("referenceBindingId","reference-store").put("contentBindingId","content-store").put("seconds",30);modules(content,RssContentRuntime.MODULES);configs.put("content",content);
        }
        void binding(String id,String role,JsonNode c){((tools.jackson.databind.node.ArrayNode)frozen.get("storageBindings")).addObject().put("bindingId",id).put("productId","product").put("role",role).put("configurationSha256",sha(c)).set("configuration",c);}
        ObjectNode common(){return SourceJson.object().put("runProfileSha256",profileHash).put("deploymentSha256",deployment);}
        NarrowContentEvidenceContext.Expected ready(){return new NarrowContentEvidenceContext.Expected(NarrowContentProfile.POLICY,run,attempt,NarrowContentEvidenceContext.PlanKind.ORIGINAL,"source-one",host,identity,identity,new NarrowContentEvidenceContext.FrozenIdentities(sha(frozen),profileHash,sha(configs.get("content"))),"d".repeat(64));}
        NarrowContentRuntimeQualification.Snapshot inspect(){return NarrowContentRuntimeQualification.inspect(jdbc,tx,ready());}
        void relinkAll(){configs.get("bronze").put("fetchRuntimeSha256",sha(configs.get("fetch")));relink();}
        void relink(){for(String[] pair:new String[][]{{"silver","bronze"},{"gold","silver"},{"content","gold"}})configs.get(pair[0]).put(pair[1]+"RuntimeSha256",sha(configs.get(pair[1])));}
    }
    static String sha(JsonNode n){return SourceJson.sha(SourceJson.canonical(n));}
    static void modules(ObjectNode c,List<String> legacy){var m=c.putObject("modules");for(String name:ContentEvidenceProfile.modules(legacy,2))m.put(name,"a".repeat(64));}
    @Test void readsWholeRegisteredChainWithoutTouchingNonexistentFilesAndReturnsNoAuthority(){
        var f=new Fixture();var snapshot=f.inspect();
        assertThat(snapshot.ledgerVersion()).isEqualTo(1);assertThat(snapshot.nativeLaunchCompatible()).isFalse();assertThat(snapshot.installedArtifactsVerified()).isFalse();
        for(var family:NarrowContentRuntimeQualification.Family.values())assertThat(snapshot.digest(family)).isEqualTo(sha(f.configs.get(family.name().toLowerCase())));
        assertThat(f.jdbc.calls).isNotEmpty().allSatisfy(i->assertThat(i.sql()).startsWith("SELECT "));
    }
    @Test void membershipChecksFrozenProduct(){var f=new Fixture();f.inspect();var calls=f.jdbc.calls.stream().filter(i->i.sql().startsWith("SELECT EXISTS")).toList();assertThat(calls).isNotEmpty();for(var call:calls){assertThat(call.sql()).contains("b.product=?");assertThat(call.arguments().get(4)).isEqualTo("product");}}
    @Test void recorderRejectsUnexpectedSqlAndMutation(){var f=new Fixture();assertThatThrownBy(()->f.jdbc.queryForList("SELECT unexpected")).isInstanceOf(AssertionError.class);assertThatThrownBy(()->f.jdbc.update("DELETE FROM unexpected")).isInstanceOf(AssertionError.class);}
    @Test void frozenSnapshotRetainsExisting256KiBReaderCeiling(){
        var f=new Fixture();f.frozen.put("syntheticSnapshotPadding","x".repeat(70000));
        var snapshot=f.inspect();assertThat(SourceJson.canonical(f.frozen).length).isGreaterThan(65536).isLessThan(256*1024);
        assertThat(snapshot.frozenSelection()).isEqualTo(f.frozen);
    }
    @Test void returnedConfigurationsAndFrozenSelectionAreDefensiveCopies(){var snapshot=new Fixture().inspect();((ObjectNode)snapshot.configuration("CONTENT")).put("contentRuntimeVersion",3);((ObjectNode)snapshot.frozenSelection()).put("pipeline","other");assertThat(snapshot.configuration("CONTENT").get("contentRuntimeVersion").intValue()).isEqualTo(2);assertThat(snapshot.frozenSelection().get("pipeline").stringValue()).isEqualTo("CONTENT_RSS_V1");}
    @Test void missingAnyRegisteredFamilyFails(){for(String id:List.of("content","gold","silver","bronze","fetch")){var f=new Fixture();var ready=f.ready();f.configs.remove(id);assertThatThrownBy(()->NarrowContentRuntimeQualification.inspect(f.jdbc,f.tx,ready)).isInstanceOf(PreflightFailure.class);}}
    @Test void selfConsistentReplacementCannotReplaceIndependentReadyHash(){var f=new Fixture();var ready=f.ready();f.configs.get("content").put("seconds",31);assertThatThrownBy(()->NarrowContentRuntimeQualification.inspect(f.jdbc,f.tx,ready)).hasMessage("RUNTIME_MISMATCH");}
    @Test void changedFrozenSelectionCannotReplaceReadyHash(){var f=new Fixture();var ready=f.ready();f.frozen.put("unexpected",true);assertThatThrownBy(()->NarrowContentRuntimeQualification.inspect(f.jdbc,f.tx,ready)).hasMessage("RUNTIME_MISMATCH");}
    @ParameterizedTest
    @CsvSource({"content,contentRuntimeVersion","gold,goldRuntimeVersion","silver,silverRuntimeVersion","bronze,bronzeRuntimeVersion","fetch,rssFetchRuntimeVersion"})
    void mixedUnknownAndOverflowVersionsFailEvenWithConsistentLinks(String family,String field){
        for(long version:new long[]{1,3,4294967298L}){
            var f=new Fixture();f.configs.get(family).put(field,version);f.relinkAll();
            assertThatThrownBy(f::inspect).as("%s version %s",family,version).hasMessage("RUNTIME_MISMATCH");
        }
    }
    @ParameterizedTest
    @CsvSource({"gold,seconds,31","silver,seconds,31","bronze,storageBindingId,alternate-bronze","fetch,httpTimeoutMillis,999"})
    void coherentChangedChildCannotSatisfyStaleParentHash(String family,String field,String value){
        var f=new Fixture();var child=f.configs.get(family);
        if(field.equals("storageBindingId"))child.put(field,value);else child.put(field,Integer.parseInt(value));
        assertThatThrownBy(f::inspect).as("stale edge to %s",family).hasMessage("RUNTIME_MISMATCH");
    }
    @ParameterizedTest
    @CsvSource({"content,goldRuntimeId,content","gold,silverRuntimeId,content","silver,bronzeRuntimeId,gold","bronze,fetchRuntimeId,silver"})
    void repeatedRuntimeIdsAndCyclesFail(String family,String field,String repeated){
        var f=new Fixture();f.configs.get(family).put(field,repeated);f.relinkAll();
        assertThatThrownBy(f::inspect).hasMessage("RUNTIME_MISMATCH");
    }
    @ParameterizedTest
    @ValueSource(strings={"silver","bronze","fetch"})
    void wrongDeploymentLinkFailsWithConsistentChain(String family){
        var f=new Fixture();f.configs.get(family).put("deploymentSha256","f".repeat(64));f.relinkAll();
        assertThatThrownBy(f::inspect).hasMessage("RUNTIME_MISMATCH");
    }
    @ParameterizedTest
    @ValueSource(strings={"CONTROL","BRONZE","SILVER","TEMP","GOLD","REFERENCE","CONTENT"})
    void missingRequiredRoleFails(String role){
        var f=new Fixture();var bindings=(tools.jackson.databind.node.ArrayNode)f.frozen.get("storageBindings");
        for(int i=0;i<bindings.size();i++)if(bindings.get(i).get("role").stringValue().equals(role)){bindings.remove(i);break;}
        assertThatThrownBy(f::inspect).as("missing %s",role).hasMessage("RUNTIME_MISMATCH");
    }
    @Test void duplicateRoleCannotSubstituteForRequiredRoleAndDuplicateIdsFail(){
        var f=new Fixture();((ObjectNode)f.frozen.get("storageBindings").get(2)).put("role","BRONZE");
        assertThatThrownBy(f::inspect).hasMessage("RUNTIME_MISMATCH");
        var g=new Fixture();((tools.jackson.databind.node.ArrayNode)g.frozen.get("storageBindings")).add(g.frozen.get("storageBindings").get(1).deepCopy());
        assertThatThrownBy(g::inspect).hasMessage("RUNTIME_MISMATCH");
    }
    @ParameterizedTest
    @ValueSource(strings={"content","gold","silver"})
    void illegalMissingAndUnknownModuleNamesFail(String family){
        for(String name:List.of("../bounded_evidence.py","unknown.py","missing")){
            var f=new Fixture();var modules=(ObjectNode)f.configs.get(family).get("modules");
            if(name.equals("missing"))modules.remove("bounded_evidence.py");else modules.put(name,"a".repeat(64));
            f.relinkAll();assertThatThrownBy(f::inspect).as("%s %s",family,name).hasMessage("RUNTIME_MISMATCH");
        }
    }
    @ParameterizedTest
    @CsvSource({"gold,python","silver,python","silver,javaHome","silver,processingRoot","fetch,javaHome"})
    void relativeAndUnnormalizedRuntimePathsFail(String family,String field){
        for(String path:List.of("relative/path","/nonexistent/../other")){
            var f=new Fixture();f.configs.get(family).put(field,path);f.relinkAll();
            assertThatThrownBy(f::inspect).as("%s %s %s",family,field,path).hasMessage("RUNTIME_MISMATCH");
        }
    }
    @Test void invalidFrozenRootAndCredentialPathsFailWithoutReadingThem(){
        for(String field:List.of("root","credentialsFile")){
            var f=new Fixture();var binding=(ObjectNode)f.frozen.get("storageBindings").get(field.equals("root")?2:1);
            ((ObjectNode)binding.get("configuration")).put(field,"relative/path");binding.put("configurationSha256",sha(binding.get("configuration")));
            if(field.equals("credentialsFile")){f.configs.get("bronze").put("storageBindingSha256",binding.get("configurationSha256").stringValue());f.relinkAll();}
            assertThatThrownBy(f::inspect).hasMessage("RUNTIME_MISMATCH");
        }
    }
    @Test void brokenHashLinkAndWrongProfileLinkFail(){var f=new Fixture();f.configs.get("silver").put("bronzeRuntimeSha256","f".repeat(64));f.configs.get("gold").put("silverRuntimeSha256",sha(f.configs.get("silver")));f.configs.get("content").put("goldRuntimeSha256",sha(f.configs.get("gold")));assertThatThrownBy(f::inspect).hasMessage("RUNTIME_MISMATCH");var g=new Fixture();g.configs.get("bronze").put("runProfileSha256","f".repeat(64));g.relink();assertThatThrownBy(g::inspect).hasMessage("RUNTIME_MISMATCH");}
    @Test void missingMembershipWrongStorageRoleAndUnknownFieldsFail(){var f=new Fixture();f.membership=false;assertThatThrownBy(f::inspect).hasMessage("RUNTIME_MISMATCH");var g=new Fixture();g.configs.get("content").put("contentBindingId","silver-store");assertThatThrownBy(g::inspect).hasMessage("RUNTIME_MISMATCH");var h=new Fixture();h.configs.get("content").put("launchOverride","evil");assertThatThrownBy(h::inspect).hasMessage("RUNTIME_MISMATCH");}
}
