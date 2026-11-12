package platform.contentapi.execution;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.RUNTIME_MISMATCH;

/** Exact version dispatch. Metadata reconstruction never attests or touches installed executables. */
interface ContentFetchLaunchRuntime {
    String id();
    String digest();
    List<String> command();
    Map<String,String> environment();
    void check();

    record Profile(String id,String digest,JsonNode configuration) {
        public Profile {
            need(id!=null && id.matches("[A-Za-z0-9_-]{1,128}") && digest!=null && digest.matches("[0-9a-f]{64}") && configuration!=null,RUNTIME_MISMATCH);
            byte[] raw=SourceJson.canonical(configuration);need(raw.length<=65536 && SourceJson.sha(raw).equals(digest),RUNTIME_MISMATCH);
            configuration=configuration.deepCopy();
        }
        @Override public JsonNode configuration(){return configuration.deepCopy();}
        int version(){return ContentFetchLaunchRuntime.version(configuration,"strictFetchLaunchVersion");}
    }
    static Profile registered(JdbcTemplate jdbc,ManagementTransactions tx,
            NarrowContentRuntimeQualification.Registration registered,InstallationHostGuard.Binding binding,String id){
        var profile=read(jdbc,tx,"content_fetch_launch_profiles","launch_profile_id",id);
        var c=profile.configuration();
        int version=version(c,"strictFetchLaunchVersion");
        if(version==1)StrictFetchLaunchRuntime.validateRegistration(c,registered,binding);
        else StrictPublicFetchLaunchRuntime.validateRegistration(c,registered,binding);
        return profile;
    }
    /** Unique registered full composition, usable even before any LAUNCH_BOUND phase exists. */
    static Profile forRunProfile(JdbcTemplate jdbc,ManagementTransactions tx,
            NarrowContentRuntimeQualification.Registration registered,InstallationHostGuard.Binding binding){
        var rows=tx.required(()->jdbc.queryForList("SELECT profile_id FROM ddip_management.content_pipeline_profiles WHERE convert_from(configuration_bytes,'UTF8')::jsonb->>'runProfileId'=? LIMIT 2",registered.profileId()));
        need(rows.size()==1,RUNTIME_MISMATCH);
        var pipeline=read(jdbc,tx,"content_pipeline_profiles","profile_id",(String)rows.getFirst().get("profile_id"));
        StrictContentPipelineRuntime.validateRegistration(pipeline.configuration(),registered,binding);
        return StrictContentPipelineRuntime.validateComposition(jdbc,tx,pipeline.configuration(),registered,binding);
    }
    static void validateSelection(Profile profile,NarrowContentRuntimeQualification.Snapshot registered,InstallationHostGuard.Binding binding){
        if(profile.version()==1)StrictFetchLaunchRuntime.validate(profile.configuration(),registered,binding);
        else {need(profile.version()==2,RUNTIME_MISMATCH);StrictPublicFetchLaunchRuntime.validate(profile.configuration(),registered,binding);}
    }
    static ContentFetchLaunchRuntime load(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,
            NarrowContentRuntimeQualification.Snapshot registered,String id){
        var profile=registered(jdbc,tx,registered.registration(),guard.binding(),id);
        return profile.version()==1?StrictFetchLaunchRuntime.load(jdbc,tx,guard,registered,id)
                :StrictPublicFetchLaunchRuntime.load(jdbc,tx,guard,registered,id);
    }
    static int version(JsonNode c,String key){
        need(c.has(key) && c.get(key).isInt() && (c.get(key).intValue()==1 || c.get(key).intValue()==2),RUNTIME_MISMATCH);
        return c.get(key).intValue();
    }
    /** Internal table names are closed constants at call sites, never input SQL. */
    static Profile read(JdbcTemplate jdbc,ManagementTransactions tx,String table,String column,String id){
        need(Set.of("content_fetch_launch_profiles:launch_profile_id","content_bronze_launch_profiles:launch_profile_id",
                "content_bronze_verification_profiles:verification_profile_id","content_pipeline_profiles:profile_id").contains(table+":"+column)
                && id!=null && id.matches("[A-Za-z0-9_-]{1,128}"),RUNTIME_MISMATCH);
        var rows=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management."+table+" WHERE "+column+"=?",id));
        need(rows.size()==1,RUNTIME_MISMATCH);var row=rows.getFirst();byte[] raw=(byte[])row.get("configuration_bytes");String digest=(String)row.get("configuration_sha256");
        need(SourceJson.sha(raw).equals(digest),RUNTIME_MISMATCH);
        return new Profile(id,digest,PreflightFiles.canonical(raw,65536));
    }
}
