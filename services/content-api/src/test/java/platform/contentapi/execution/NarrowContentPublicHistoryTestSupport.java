package platform.contentapi.execution;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;

/** Metadata-only versioned history fixtures; no native child or network test capability. */
public final class NarrowContentPublicHistoryTestSupport {
    public static void registerProfiles(JdbcTemplate jdbc,String runProfile,String control){
        var fetch=configuration(jdbc,"fetch");var bronze=configuration(jdbc,"bronze");var silver=configuration(jdbc,"silver");
        var publicFetch=PublicContentProfileTestSupport.fetchLaunch(fetch,"fetch",hash(fetch),SourceJson.object().put("path","/nonexistent/public-provider.jar").put("sha256","b".repeat(64)));
        var publicBronze=PublicContentProfileTestSupport.bronze(bronze,"bronze",hash(bronze),fetch,"public-fetch",hash(publicFetch),false);
        var publicVerifier=PublicContentProfileTestSupport.bronze(bronze,"bronze",hash(bronze),fetch,"public-fetch",hash(publicFetch),true);
        insert(jdbc,"content_fetch_launch_profiles","launch_profile_id","public-fetch",publicFetch);
        insert(jdbc,"content_bronze_launch_profiles","launch_profile_id","public-bronze",publicBronze);
        insert(jdbc,"content_bronze_verification_profiles","verification_profile_id","public-verifier",publicVerifier);
        var pipeline=SourceJson.object().put("strictContentPipelineVersion",2).put("runProfileId",runProfile).put("runProfileSha256",hash(configuration(jdbc,runProfile)))
                .put("contentRuntimeId","content").put("contentRuntimeSha256",hash(configuration(jdbc,"content"))).put("controlBindingId",control)
                .put("strictRuntimeSha256",StrictContentPipelineRuntime.protocolDigest())
                .put("fetchLaunchProfileId","public-fetch").put("fetchLaunchProfileSha256",hash(publicFetch))
                .put("bronzeLaunchProfileId","public-bronze").put("bronzeLaunchProfileSha256",hash(publicBronze))
                .put("bronzeVerificationProfileId","public-verifier").put("bronzeVerificationProfileSha256",hash(publicVerifier));
        for(String key:List.of("python","processingRoot","javaHome"))pipeline.set(key,silver.get(key));
        pipeline.set("versions",StrictContentPipelineRuntime.versions());var modules=pipeline.putObject("modules");
        for(String name:List.of("strict_native_content.py","strict_native_runtime.py","strict_native_sink.py"))modules.put(name,"a".repeat(64));
        insert(jdbc,"content_pipeline_profiles","profile_id","public-pipeline",pipeline);
    }
    private static JsonNode configuration(JdbcTemplate jdbc,String id){return SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id=?",byte[].class,id));}
    private static String hash(JsonNode n){return SourceJson.sha(SourceJson.canonical(n));}
    private static void insert(JdbcTemplate jdbc,String table,String column,String id,JsonNode c){byte[] raw=SourceJson.canonical(c);jdbc.update("INSERT INTO ddip_management."+table+"("+column+",configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,raw,SourceJson.sha(raw));}
    public static void reserve(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation){
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        var registered=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);
        var profile=ContentFetchLaunchRuntime.forRunProfile(jdbc,tx,registered.registration(),ready.host());
        new NarrowContentFetchReservation(jdbc,tx,workers).reserve(guard,owner,operation,profile,b->{});
    }
    public static String digest(JdbcTemplate jdbc,ManagementTransactions tx,UUID run,UUID attempt){return NarrowContentPipelineRecoveryAssessment.historicalSqlDigest(jdbc,tx,run,attempt);}
    private NarrowContentPublicHistoryTestSupport(){}
}
