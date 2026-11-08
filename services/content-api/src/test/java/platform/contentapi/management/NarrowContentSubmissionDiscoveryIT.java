package platform.contentapi.management;

import java.nio.file.Files;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import platform.contentapi.execution.*;
import platform.contentapi.importing.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Real PostgreSQL/API prerequisites with genuine strict registrations, never a native launch. */
class NarrowContentSubmissionDiscoveryIT extends NarrowContentPipelineFixtures {
    static final String PROFILES="/api/management/v1/execution-profiles";
    static final String CONTENT_PROFILES=PROFILES+"?pipeline=CONTENT_RSS_V1";
    static final String SOURCE_CHOICES="/api/management/v1/execution-profile-sources?executionProfileId="+CONTENT_PROFILE;
    static final String REFERENCES="/api/management/v1/artifacts?executionProfileId="+CONTENT_PROFILE+"&role=PUBLISHER_REFERENCE";
    DiscoveryRepository discovery(){return new DiscoveryRepository(jdbc,tx,INSTALL,catalog,new SourceCursor(KEY));}
    @Override void start(){
        // Preserve the real boundary and advice while using the fixture's reviewed loopback catalog.
        mvc=MockMvcBuilders.standaloneSetup(new DiscoveryController(discovery()),new RunController(runs,new RunHistoryRepository(jdbc,tx,new SourceCursor(KEY))))
                .setControllerAdvice(new SourceAdvice()).addFilters(new ManagementBoundary(new ManagementConfiguration.LocalPolicy(Set.of(origin),Set.of(host)))).build();
    }
    ObjectNode narrow(){var n=content();n.withArray("retainedSilverArtifactIds").removeAll();return n;}
    JsonNode choices(String path)throws Exception {
        var response=http(get(path));assertThat(response.getResponse().getStatus()).as(response.getResponse().getContentAsString()).isEqualTo(200);
        assertThat(response.getResponse().getHeader("Cache-Control")).isEqualTo("no-store");
        var n=body(response);assertThat(n.get("apiVersion").intValue()).isEqualTo(1);assertThat(n.get("meta").get("correlationId").stringValue()).matches("[a-f0-9-]{36}");return n;
    }
    void metadataOnly(JsonNode row){
        assertThat(row.get("dispatchDisabled").booleanValue()).isTrue();
        assertThat(row.get("qualificationScope").stringValue()).isEqualTo("REGISTRATION_METADATA_ONLY");
        assertThat(row.get("runtimeQualification").stringValue()).isEqualTo("NOT_CHECKED");
        assertThat(row.toString()).doesNotContain("python","javaHome","processingRoot","credentials","endpoint","relativeAddress","verification","modules","ready");
    }
    void noEffects(){for(String table:List.of("attempts","content_native_operations","content_pipeline_operations","stage_invocations","rss_deliveries"))assertThat(count(table)).as(table).isZero();assertThat(requests).hasValue(0);assertThat(objectRequests).hasValue(0);}
    void receipt(String... ids)throws Exception {
        var host=OwnershipFixtures.host(Files.createTempDirectory(temp,"content-discovery-"));OwnershipFixtures.provision(jdbc,host);
        var repository=new ImportOwnershipRepository(jdbc,tx,INSTALL);
        try(var guard=InstallationHostGuard.acquire(host)) {
            UUID operation=UUID.randomUUID();var request=SourceJson.object().put("catalogVersion",1).put("operationId",operation.toString()).put("executionProfileId","import-test").put("controlBindingId",host.id());
            var entries=request.putArray("artifacts");var proofs=new HashMap<String,byte[]>();
            for(String id:ids){byte[] bytes=jdbc.queryForObject("SELECT evidence_bytes FROM ddip_management.artifacts WHERE artifact_id=?",byte[].class,UUID.fromString(id));var q=RegisteredImportEvidence.validate(bytes,SourceJson.sha(bytes));entries.add(q.selection().json());proofs.put(q.selection().digest(),bytes);}
            var parsed=ImportCatalog.parse(SourceJson.canonical(request));var token=repository.reserve(guard,new ImportOwnershipRepository.Reservation(operation,"import-test",OwnershipFixtures.HASH,parsed.requestBytes()));
            repository.finishVerified(guard,token,parsed.entries().stream().map(e->RegisteredImportFixture.qualified(operation,proofs.get(e.digest()))).toList());
        }
    }
    @Test void narrowShapeRejectsBeforeCreatingAnyRunAndAcceptsExactlyOneFreshSource()throws Exception {
        code(http(postRun(content(),"retained")),400,"INVALID_ARGUMENT");
        var multiple=narrow();multiple.withArray("fetchSources").addObject().put("sourceId","second-feed").put("revision",1);
        code(http(postRun(multiple,"multiple")),400,"INVALID_ARGUMENT");assertThat(count("runs")).isZero();assertThat(count("run_inputs")).isZero();assertThat(count("run_sources")).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE action='RUN_SUBMITTED'",Long.class)).isZero();
        code(http(postRun(narrow().put("publisherReferenceArtifactId",SERVICE),"wrong-reference")),400,"INVALID_ARGUMENT");
        var accepted=http(postRun(narrow(),"accepted"));assertThat(accepted.getResponse().getStatus()).isEqualTo(202);assertThat(body(accepted).get("data").get("state").stringValue()).isEqualTo("QUEUED");
        assertThat(count("run_sources")).isEqualTo(1);assertThat(count("run_inputs")).isEqualTo(1);noEffects();
    }
    @Test void oldV2ProfileStillAcceptsRetainedSilverAndKeepsOriginalBytes() {
        String legacy=UUID.randomUUID().toString();jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) SELECT ?,configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",legacy,CONTENT_PROFILE);
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.execution_profile_storage WHERE profile_id=?",legacy,CONTENT_PROFILE);
        assertThat(submit(content().put("executionProfileId",legacy),"legacy").get("state").stringValue()).isEqualTo("QUEUED");
        var frozen=SourceJson.parse(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class));assertThat(frozen.get("inputs").size()).isEqualTo(2);
        assertThat(frozen.get("executionProfile").get("configuration").get("runProfileVersion").intValue()).isEqualTo(2);noEffects();
    }
    void duplicateStrict(){jdbc.update("INSERT INTO ddip_management.content_pipeline_profiles(profile_id,configuration_bytes,configuration_sha256) SELECT 'duplicate-strict',configuration_bytes,configuration_sha256 FROM ddip_management.content_pipeline_profiles WHERE profile_id=?",PIPELINE_PROFILE);}
    @Test void replayRemainsFrozenBeforeNewProfileAmbiguitySourceAndCoordinatorChecks()throws Exception {
        var request=narrow();var original=submit(request,"same");byte[] frozen=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class);
        duplicateStrict();sources.execute(SourceCommands.command(SourceCommands.Action.ENABLE,CONTENT,"demo-feed",SourceJson.object().put("enabled",false),"disable","\"1\""),"test");
        jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");
        assertThat(submit(request,"same")).isEqualTo(original);assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class)).isEqualTo(frozen);
        code(http(postRun(content(),"same")),409,"IDEMPOTENCY_CONFLICT");noEffects();
    }
    @Test void ambiguousStrictRegistrationIsNeitherSelectableNorNewlySubmittable()throws Exception {
        duplicateStrict();assertThat(choices(CONTENT_PROFILES).get("data").isEmpty()).isTrue();assertThat(choices(SOURCE_CHOICES).get("data").isEmpty()).isTrue();
        code(http(postRun(narrow(),"ambiguous")),409,"STATE_CONFLICT");assertThat(count("runs")).isZero();noEffects();
    }
    @Test void contentDiscoveryIsOptInBoundedAndNotAnInstalledRuntimeClaim()throws Exception {
        var before=metadata();Files.delete(installedFetchJar);
        var defaults=choices(PROFILES).get("data");assertThat(defaults.size()).isEqualTo(2);assertThat(defaults.toString()).doesNotContain("CONTENT_RSS_V1");assertThat(choices(PROFILES+"?pipeline=RELIABILITY_V2").get("data")).isEqualTo(defaults);
        var rows=choices(CONTENT_PROFILES).get("data");assertThat(rows.size()).isEqualTo(1);var row=rows.get(0);metadataOnly(row);
        SourceJson.fields(row,"executionProfileId","productId","pipeline","runProfileVersion","pipelineVersion","sourceProfile","inputFormat","configurationSha256","createdAt","dispatchDisabled","qualificationScope","runtimeQualification","supportedSelection");
        assertThat(row.get("executionProfileId").stringValue()).isEqualTo(CONTENT_PROFILE);assertThat(row.get("inputFormat").stringValue()).isEqualTo("RSS_ATOM");
        assertThat(row.get("supportedSelection")).isEqualTo(SourceJson.object().put("freshSourceCount",1).put("retainedSilverCount",0).put("publisherReferenceCount",1));
        assertThat(metadata()).isEqualTo(before);noEffects();
    }
    @Test void sourceCandidatesUseCurrentEnabledReviewedRevisionAndNoPrivateEndpoint()throws Exception {
        var rows=choices(SOURCE_CHOICES).get("data");assertThat(rows.size()).isEqualTo(1);var row=rows.get(0);metadataOnly(row);
        SourceJson.fields(row,"executionProfileId","productId","pipeline","sourceId","revision","sourceProfile","configurationSha256","dispatchDisabled","qualificationScope","runtimeQualification");
        assertThat(row.get("sourceId").stringValue()).isEqualTo("demo-feed");assertThat(row.get("revision").intValue()).isEqualTo(2);
        sources.execute(SourceCommands.command(SourceCommands.Action.REVISE,CONTENT,"demo-feed",revise("updated",rss().put("feedEndpointId",TEST_ENDPOINT)),"revise","\"1\""),"test");
        assertThat(choices(SOURCE_CHOICES).get("data").get(0).get("revision").intValue()).isEqualTo(3);code(http(postRun(narrow(),"stale")),409,"STATE_CONFLICT");
        sources.execute(SourceCommands.command(SourceCommands.Action.ENABLE,CONTENT,"demo-feed",SourceJson.object().put("enabled",false),"disable","\"2\""),"test");
        var current=narrow();((ObjectNode)current.get("fetchSources").get(0)).put("revision",3);
        assertThat(choices(SOURCE_CHOICES).get("data").isEmpty()).isTrue();code(http(postRun(current,"disabled")),409,"SOURCE_DISABLED");noEffects();
    }
    @Test void changedCatalogOrForeignProfileCannotAdvertiseSourceCandidates()throws Exception {
        var changed=catalog();changed.withArray("endpoints").addObject().put("endpointId",TEST_ENDPOINT).put("sourceId","demo-feed").put("uri","https://feeds.example.org/changed-uri");
        var changedUri=new DiscoveryRepository(jdbc,tx,INSTALL,ReviewedSourceCatalog.from(changed),new SourceCursor(KEY));assertThat(changedUri.sources(CONTENT_PROFILE,20,null).data().isEmpty()).isTrue();
        var empty=SourceJson.object().put("catalogVersion",1);for(String field:List.of("endpoints","reliabilityProfiles","storageProfiles"))empty.putArray(field);
        var removed=new DiscoveryRepository(jdbc,tx,INSTALL,ReviewedSourceCatalog.from(empty),new SourceCursor(KEY));assertThat(removed.sources(CONTENT_PROFILE,20,null).data().isEmpty()).isTrue();
        assertThat(choices(SOURCE_CHOICES.replace(CONTENT_PROFILE,JSON_PROFILE)).get("data").isEmpty()).isTrue();
        assertThat(choices(SOURCE_CHOICES.replace(CONTENT_PROFILE,UUID.randomUUID().toString())).get("data").isEmpty()).isTrue();noEffects();
    }
    @Test void referenceCandidatesRequireImportReceiptAndRemainHistoricalMetadata()throws Exception {
        assertThat(choices(REFERENCES).get("data").isEmpty()).isTrue();receipt(PUB,SERVICE);var before=metadata();
        var rows=choices(REFERENCES).get("data");assertThat(rows.size()).isEqualTo(1);var row=rows.get(0);metadataOnly(row);
        assertThat(row.get("artifactId").stringValue()).isEqualTo(PUB);assertThat(row.get("productId").stringValue()).isEqualTo(CONTENT);assertThat(row.get("role").stringValue()).isEqualTo("PUBLISHER_REFERENCE");
        assertThat(row.get("contractVersion").intValue()).isEqualTo(1);assertThat(row.get("processorRevision").intValue()).isEqualTo(1);assertThat(row.get("nativeFormat").isNull()).isTrue();SourceJson.fields(row.get("identity"),"referenceId","revision");
        assertThat(choices(REFERENCES.replace(CONTENT_PROFILE,JSON_PROFILE)).get("data").isEmpty()).isTrue();
        assertThat(choices(REFERENCES.replace("PUBLISHER_REFERENCE","SERVICE_REFERENCE")).get("data").isEmpty()).isTrue();
        assertThat(metadata()).isEqualTo(before);noEffects();
    }
    @Test void queryAllowlistsBoundaryAndCrossPipelineCursorsFailClosed()throws Exception {
        for(String path:List.of(PROFILES+"?pipeline=unknown",PROFILES+"?pipeline=CONTENT_RSS_V1&pipeline=RELIABILITY_V2",SOURCE_CHOICES+"&sourceId=demo-feed",SOURCE_CHOICES+"&limit=101",SOURCE_CHOICES+"&executionProfileId="+CONTENT_PROFILE,REFERENCES+"&sourceId=demo-feed",REFERENCES.replace("PUBLISHER_REFERENCE","RETAINED_SILVER"),"/api/management/v1/execution-profile-sources"))code(http(get(path)),400,"INVALID_ARGUMENT");
        String cursor=choices(PROFILES+"?limit=1").get("meta").get("nextCursor").stringValue();
        assertThat(choices(PROFILES+"?pipeline=RELIABILITY_V2&cursor="+cursor).get("data").get(0).get("executionProfileId").stringValue()).isEqualTo(PARQUET_PROFILE);
        code(http(get(CONTENT_PROFILES+"&cursor="+cursor)),400,"INVALID_ARGUMENT");code(http(get(SOURCE_CHOICES+"&cursor="+cursor)),400,"INVALID_ARGUMENT");
        code(http(get(SOURCE_CHOICES).header("Origin","https://not-local.example")),403,"LOCAL_BOUNDARY_REJECTED");noEffects();
    }
    @Test void reviewedSourceWithUnassociatedBronzeBindingIsNotACandidate()throws Exception {
        String reviewedEndpoint="60000000-0000-4000-8000-000000000099",extra="50000000-0000-4000-8000-000000000099";storage(extra,CONTENT,"BRONZE");
        var approved=catalog();approved.withArray("endpoints").addObject().put("endpointId",reviewedEndpoint).put("sourceId","demo-feed").put("uri","https://feeds.example.org/reviewed");
        approved.withArray("storageProfiles").addObject().put("profileId",extra).put("productId",CONTENT).put("role","BRONZE");
        catalog=ReviewedSourceCatalog.from(approved);sources=new SourceRepository(jdbc,tx,INSTALL,catalog,new SourceCursor(KEY));
        sources.execute(SourceCommands.command(SourceCommands.Action.REVISE,CONTENT,"demo-feed",revise("unassociated",rss().put("feedEndpointId",reviewedEndpoint).put("inputStorageProfileId",extra)),"unassociated","\"1\""),"test");
        assertThat(choices(CONTENT_PROFILES).get("data").size()).isEqualTo(1);assertThat(choices(SOURCE_CHOICES).get("data").isEmpty()).isTrue();noEffects();
    }
    @Test void publisherReferencePagesPreserveUuidOrderAndProfileScope()throws Exception {
        var ids=new ArrayList<String>();
        for(int i=0;i<3;i++) {
            String id=String.format("70000000-0000-4000-8000-%012d",i),reference="page-publisher-"+i;
            var evidence=RunFixtures.evidence("PUBLISHER_REFERENCE",REFS,"demo-feed",null);var selection=(ObjectNode)evidence.get("selection");
            ((ObjectNode)selection.get("identity")).put("referenceId",reference);String address="reference_id="+reference+"/revision=r1";selection.put("relativeAddress",address);
            RunFixtures.guards((ObjectNode)evidence.get("verification").get("capture"),address);artifact(id,evidence);ids.add(id);
        }
        receipt(ids.toArray(String[]::new));var first=choices(REFERENCES+"&limit=2");assertThat(first.get("data").size()).isEqualTo(2);
        assertThat(first.get("data").get(0).get("artifactId").stringValue()).isEqualTo(ids.get(0));assertThat(first.get("data").get(1).get("artifactId").stringValue()).isEqualTo(ids.get(1));
        String cursor=first.get("meta").get("nextCursor").stringValue();var last=choices(REFERENCES+"&limit=1&cursor="+cursor);
        assertThat(last.get("data").size()).isEqualTo(1);assertThat(last.get("data").get(0).get("artifactId").stringValue()).isEqualTo(ids.get(2));assertThat(last.get("meta").get("hasMore").booleanValue()).isFalse();
        code(http(get(REFERENCES.replace(CONTENT_PROFILE,JSON_PROFILE)+"&cursor="+cursor)),400,"INVALID_ARGUMENT");noEffects();
    }
    @Test void sourcePaginationUsesAsciiKeysBoundedLookaheadAndProfileScope()throws Exception {
        var approved=catalog();
        for(int i=0;i<105;i++)approved.withArray("endpoints").addObject().put("endpointId",new UUID(0,i+1).toString())
                .put("sourceId",String.format("page-%03d",i)).put("uri","https://feeds.example.org/page-"+i);
        catalog=ReviewedSourceCatalog.from(approved);sources=new SourceRepository(jdbc,tx,INSTALL,catalog,new SourceCursor(KEY));
        for(int i=0;i<105;i++)createSource(CONTENT,String.format("page-%03d",i),rss().put("feedEndpointId",new UUID(0,i+1).toString()));
        var first=choices(SOURCE_CHOICES+"&limit=100");assertThat(first.get("data").size()).isEqualTo(100);
        for(int i=0;i<100;i++)assertThat(first.get("data").get(i).get("sourceId").stringValue()).isEqualTo(String.format("page-%03d",i));
        assertThat(SourceJson.canonical(first).length).isLessThan(128*1024);String cursor=first.get("meta").get("nextCursor").stringValue();
        var last=choices(SOURCE_CHOICES+"&limit=100&cursor="+cursor);assertThat(last.get("data").size()).isEqualTo(5);assertThat(last.get("data").get(0).get("sourceId").stringValue()).isEqualTo("page-100");
        assertThat(last.get("meta").get("hasMore").booleanValue()).isFalse();
        code(http(get(SOURCE_CHOICES.replace(CONTENT_PROFILE,JSON_PROFILE)+"&cursor="+cursor)),400,"INVALID_ARGUMENT");
        code(http(get(REFERENCES+"&cursor="+cursor)),400,"INVALID_ARGUMENT");noEffects();
    }
    @Test void narrowQueueLimitDoesNotConsumeGlobalOwnershipOrBlockReplay()throws Exception {
        var before=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");JsonNode first=null;
        for(int i=0;i<20;i++){var accepted=submit(narrow(),"queued-"+i);if(i==0)first=accepted;}
        code(http(postRun(narrow(),"overflow")),429,"MANAGEMENT_CAPACITY_EXCEEDED");assertThat(submit(narrow(),"queued-0")).isEqualTo(first);
        assertThat(count("runs")).isEqualTo(20);assertThat(count("run_inputs")).isEqualTo(20);
        assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(before);noEffects();
    }
    @Test void metadataQueriesNeverReadArtifactProofOrResolvedStorage() {
        var queries=new ArrayList<String>();var traced=new JdbcTemplate(jdbc.getDataSource()){
            @Override public <T> List<T> query(String sql,RowMapper<T> mapper,Object... args){queries.add(sql);return super.query(sql,mapper,args);}
        };
        var repository=new DiscoveryRepository(traced,transactions(traced),INSTALL,catalog,new SourceCursor(KEY));
        assertThat(repository.profiles("CONTENT_RSS_V1",20,null).data().size()).isEqualTo(1);assertThat(repository.sources(CONTENT_PROFILE,20,null).data().size()).isEqualTo(1);repository.artifacts(CONTENT_PROFILE,"PUBLISHER_REFERENCE",null,20,null);
        assertThat(String.join("\n",queries)).doesNotContain("a.evidence_bytes","b.configuration_bytes","FOR UPDATE","INSERT ","UPDATE ");noEffects();
    }
}
