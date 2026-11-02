package platform.contentapi.management;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;
import java.time.format.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.ManagementTransactions;
import platform.contentapi.importing.ImportCatalog;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.management.SourceFailure.require;

/** Bounded database-only projections. Never selects artifact proof bytes or resolved storage configuration. */
public final class DiscoveryRepository {
    private static final String PRODUCT="application-reliability",PIPELINE="RELIABILITY_V2";
    private static final String UUID_PATTERN="^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";
    private static final DateTimeFormatter UTC=new DateTimeFormatterBuilder().appendInstant(6).toFormatter();
    // Only UUID RunProfile v1 registrations can be selected by RELIABILITY_V2 submission.
    // Import/execution runtime descriptors share this table; they are not selectable RunProfiles.
    private static final String PROFILES="""
        FROM ddip_management.execution_profiles p
        CROSS JOIN LATERAL (SELECT convert_from(p.configuration_bytes,'UTF8')::jsonb AS config) c
        WHERE p.profile_id ~ ? AND c.config->>'pipeline'='RELIABILITY_V2' AND c.config->>'runProfileVersion'='1'
          AND c.config->>'pipelineVersion'='1' AND c.config->>'sourceProfile' IN ('request-jsonl-v2','request-parquet-v1')
          AND c.config#>>'{runtime,javaVersion}'='21' AND c.config#>>'{runtime,pythonVersion}'='3.12.15'
          AND c.config#>>'{runtime,pysparkVersion}'='4.0.2'
          AND (SELECT count(*) FROM ddip_management.execution_profile_storage ps WHERE ps.profile_id=p.profile_id) BETWEEN 1 AND 128
          AND NOT EXISTS (SELECT 1 FROM ddip_management.execution_profile_storage ps JOIN ddip_management.storage_bindings b USING(binding_id)
              WHERE ps.profile_id=p.profile_id AND (b.installation_id<>? OR (b.product<>'application-reliability' AND b.role<>'CONTROL')))
          AND NOT EXISTS (SELECT 1 FROM (VALUES ('BATCH'),('REFERENCE'),('RELIABILITY'),('CONTROL'),('STAGING'),('TEMP')) needed(role)
              WHERE NOT EXISTS (SELECT 1 FROM ddip_management.execution_profile_storage ps JOIN ddip_management.storage_bindings b USING(binding_id)
                  WHERE ps.profile_id=p.profile_id AND b.role=needed.role))
        """;
    private final JdbcTemplate jdbc;
    private final ManagementTransactions tx;
    private final UUID installation;
    private final ReviewedSourceCatalog catalog;
    private final DiscoveryCursor cursors;
    public DiscoveryRepository(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,ReviewedSourceCatalog catalog,SourceCursor signer) {
        this(jdbc,tx,installation,catalog,signer,Clock.systemUTC());
    }
    DiscoveryRepository(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,ReviewedSourceCatalog catalog,SourceCursor signer,Clock clock) {
        this.jdbc=jdbc;this.tx=tx;this.installation=installation;this.catalog=catalog;this.cursors=new DiscoveryCursor(signer,clock);
    }
    private record Profile(String id,String sourceProfile,String format,ObjectNode projection) { }
    private static String time(ResultSet r,String key)throws SQLException{return UTC.format(r.getObject(key,OffsetDateTime.class).toInstant());}
    private Profile mapProfile(ResultSet r,int ignored)throws SQLException {
        try {
            byte[] bytes=r.getBytes("configuration_bytes");String digest=r.getString("configuration_sha256");var config=SourceJson.parse(bytes);
            check(Arrays.equals(bytes,SourceJson.canonical(config)) && SourceJson.sha(bytes).equals(digest));RunProfile.validate(config,PIPELINE);
            String id=SourceCommands.uuid(r.getString("profile_id")),source=SourceJson.text(config.get("sourceProfile")),format=source.equals("request-jsonl-v2")?"JSONL":"PARQUET";
            var n=SourceJson.object().put("executionProfileId",id).put("productId",PRODUCT).put("pipeline",PIPELINE).put("runProfileVersion",1)
                    .put("pipelineVersion",1).put("sourceProfile",source).put("inputFormat",format).put("configurationSha256",digest).put("createdAt",time(r,"created_at"));
            return new Profile(id,source,format,n);
        } catch(RuntimeException ex){throw invalid();}
    }
    public SourceRepository.Page profiles(int limit,String cursor) {
        limit(limit);String endpoint="execution-profiles",scope=scope(endpoint,null,null,null);var position=cursors.position(cursor,endpoint,scope);
        return tx.required(()->{
            var args=new ArrayList<Object>(List.of(UUID_PATTERN,installation));String where="";
            if(position.after()!=null){where=" AND p.profile_id COLLATE \"C\">?";args.add(position.after());}args.add(limit+1);
            var rows=jdbc.query("SELECT p.profile_id,p.configuration_bytes,p.configuration_sha256,p.created_at "+PROFILES+where+" ORDER BY p.profile_id COLLATE \"C\" LIMIT ?",this::mapProfile,args.toArray());
            return page(rows.stream().map(Profile::projection).toList(),limit,endpoint,scope,position,"executionProfileId");
        });
    }
    private Profile profile(String id) {
        var rows=jdbc.query("SELECT p.profile_id,p.configuration_bytes,p.configuration_sha256,p.created_at "+PROFILES+" AND p.profile_id=?",this::mapProfile,UUID_PATTERN,installation,id);
        return rows.isEmpty()?null:rows.getFirst();
    }
    public SourceRepository.Page artifacts(String profileId,String role,String sourceId,int limit,String cursor) {
        limit(limit);require(profileId!=null);SourceCommands.uuid(profileId);require(role!=null && Set.of("RELIABILITY_BATCH","SERVICE_REFERENCE").contains(role));
        boolean batch=role.equals("RELIABILITY_BATCH");if(sourceId!=null){SourceCommands.source(sourceId);require(batch);}
        String endpoint="artifacts",scope=scope(endpoint,profileId,role,sourceId);var position=cursors.position(cursor,endpoint,scope);
        return tx.required(()->{
            Profile profile=profile(profileId);if(profile==null)return empty(limit);
            var args=new ArrayList<Object>();args.add(installation);args.add(profileId);args.add(installation);
            String where=" WHERE a.product_id='application-reliability' AND a.kind=? AND a.contract_version=? AND a.processor_revision=? AND b.role=?";
            args.add(batch?"RELIABILITY_BATCH_V2":"SERVICE_REFERENCE");args.add(batch?2:1);args.add(batch?2:1);args.add(batch?"BATCH":"REFERENCE");
            where+=" AND NOT EXISTS(SELECT 1 FROM ddip_management.artifact_dependencies d WHERE d.artifact_id=a.artifact_id)";
            if(batch) {
                where+=" AND a.native_format=?";args.add(profile.format());
                where+=" AND EXISTS (SELECT 1 FROM ddip_management.sources s JOIN ddip_management.source_revisions sr ON (s.product_id,s.source_id,s.current_revision)=(sr.product_id,sr.source_id,sr.revision) "
                    +"CROSS JOIN LATERAL (SELECT convert_from(sr.configuration_bytes,'UTF8')::jsonb AS config) sc "
                    +"WHERE s.product_id=a.product_id AND s.enabled AND s.profile_family=? AND sc.config->>'profile'=? "
                    +"AND s.source_id=m.selection#>>'{identity,sourceId}' AND sr.revision::text=m.selection#>>'{identity,sourceRevision}' "
                    +"AND sc.config->>'registeredProfileSha256'=m.selection#>>'{identity,registeredProfileSha256}' AND sc.config->>'inputStorageProfileId'=a.storage_binding_id ";
                args.add(profile.sourceProfile());args.add(profile.sourceProfile());
                var approved=catalog.capabilities();var profiles=new ArrayList<String>();var bindings=new ArrayList<String>();
                for(var p:approved.get("reliabilityProfiles"))profiles.add(p.get("sourceId").stringValue()+":"+p.get("profile").stringValue()+":"+p.get("registeredProfileSha256").stringValue());
                for(var b:approved.get("storageProfiles"))if(b.get("productId").stringValue().equals(PRODUCT))bindings.add(b.get("profileId").stringValue());
                if(profiles.isEmpty() || bindings.isEmpty())return empty(limit);
                where+=" AND s.source_id||':'||s.profile_family||':'||(sc.config->>'registeredProfileSha256') IN ("+placeholders(profiles.size())+")";args.addAll(profiles);
                where+=" AND a.storage_binding_id IN ("+placeholders(bindings.size())+"))";args.addAll(bindings);
                if(sourceId!=null){where+=" AND m.selection#>>'{identity,sourceId}'=?";args.add(sourceId);}
            } else where+=" AND a.native_format IS NULL";
            if(position.after()!=null){where+=" AND a.artifact_id>?::uuid";args.add(position.after());}args.add(limit+1);
            var rows=jdbc.query("SELECT a.artifact_id,a.product_id,a.kind,a.contract_version,a.processor_revision,a.native_identity,a.native_format,a.storage_binding_id,a.relative_address,a.verified_at,m.catalog::text AS catalog "+ARTIFACTS+where+" ORDER BY a.artifact_id LIMIT ?",
                    (r,i)->artifact(r,role),args.toArray());
            return page(rows,limit,endpoint,scope,position,"artifactId");
        });
    }
    // Registration catalogs and completion receipts are each <=64 KiB and <=16 artifacts.
    // They link the safe selection to the immutable UUID/digest, without reading a.evidence_bytes.
    private static final String ARTIFACTS="""
        FROM ddip_management.artifacts a
        JOIN ddip_management.storage_bindings b ON b.binding_id=a.storage_binding_id AND b.product=a.product_id AND b.installation_id=?
        JOIN ddip_management.execution_profile_storage ps ON ps.binding_id=b.binding_id AND ps.profile_id=?
        JOIN LATERAL (
            SELECT request->'artifacts'->(registered.ordinality-1)::int AS selection,
                   jsonb_set(request,'{artifacts}',jsonb_build_array(request->'artifacts'->(registered.ordinality-1)::int)) AS catalog
            FROM ddip_management.artifact_import_operations o
            CROSS JOIN LATERAL (SELECT convert_from(o.request_bytes,'UTF8')::jsonb AS request,convert_from(o.result_bytes,'UTF8')::jsonb AS result) documents
            CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(result->'artifacts')='array' THEN result->'artifacts' ELSE '[]'::jsonb END) WITH ORDINALITY registered(value,ordinality)
            WHERE o.installation_id=? AND o.kind='IMPORT' AND o.state='SUCCEEDED' AND o.outcome_code='SAFE_COMPLETION'
              AND result->>'protocolVersion'='1' AND result->>'status'='VERIFIED' AND result->>'operationId'=o.import_id::text
              AND request->>'catalogVersion'='1' AND request->>'operationId'=o.import_id::text
              AND registered.ordinality<=16 AND registered.value->>'artifactId'=a.artifact_id::text
              AND result->'artifactIds'->>(registered.ordinality-1)::int=a.artifact_id::text
              AND registered.value->>'evidenceSha256'=a.evidence_sha256
            ORDER BY o.started_at,o.import_id LIMIT 1
        ) m ON true
        """;
    private ObjectNode artifact(ResultSet r,String role)throws SQLException {
        try {
            var entries=ImportCatalog.parse(r.getString("catalog").getBytes(StandardCharsets.UTF_8)).entries();check(entries.size()==1);var e=entries.getFirst();
            check(e.product().equals(r.getString("product_id")) && e.kind().equals(r.getString("kind")) && e.contract()==r.getInt("contract_version") && e.processor()==r.getInt("processor_revision")
                    && e.nativeIdentity().equals(r.getString("native_identity")) && Objects.equals(e.format(),r.getString("native_format")) && e.bindingId().equals(r.getString("storage_binding_id")) && e.address().equals(r.getString("relative_address")));
            var n=SourceJson.object().put("artifactId",r.getObject("artifact_id",UUID.class).toString()).put("productId",PRODUCT).put("kind",e.kind()).put("role",role)
                .put("contractVersion",e.contract()).put("processorRevision",e.processor()).put("nativeFormat",e.format()).put("storageBindingId",e.bindingId()).put("nativeFingerprint",e.fingerprint()).put("verifiedAt",time(r,"verified_at"));
            n.set("identity",e.identity());return n;
        } catch(RuntimeException ex){throw invalid();}
    }
    private SourceRepository.Page page(List<ObjectNode> rows,int limit,String endpoint,String scope,DiscoveryCursor.Position p,String id) {
        boolean more=rows.size()>limit;String next=more?cursors.next(endpoint,scope,p,rows.get(limit-1).get(id).stringValue()):null;
        var data=SourceJson.object().putArray("unused");rows.subList(0,Math.min(limit,rows.size())).forEach(data::add);return new SourceRepository.Page(data,limit,next);
    }
    private static SourceRepository.Page empty(int limit){return new SourceRepository.Page(SourceJson.object().putArray("unused"),limit,null);}
    private static String placeholders(int count){return String.join(",",Collections.nCopies(count,"?"));}
    private static String scope(String endpoint,String profile,String role,String source){return SourceJson.sha(SourceJson.canonical(SourceJson.object().put("endpoint",endpoint).put("order","uuid_asc").put("executionProfileId",profile).put("role",role).put("sourceId",source)));}
    private static void limit(int limit){require(limit>=1 && limit<=100);}
    private static void check(boolean valid){if(!valid)throw invalid();}
    private static SourceFailure invalid(){return new SourceFailure(SourceFailure.Code.INTERNAL_FAILURE);}
}
