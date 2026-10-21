package platform.contentapi.management;

import java.sql.*;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.management.SourceFailure.Code.*;

/** Fixed source SQL only. Coordinator -> source lock ordering is reusable by B2b input capture. */
public final class SourceRepository {
    public record Result(JsonNode data,int status) { public String etag() {return "\""+data.get("rowVersion").stringValue()+"\"";} }
    public record Page(JsonNode data,int limit,String nextCursor) { }
    private static final DateTimeFormatter UTC=new DateTimeFormatterBuilder().appendInstant(6).toFormatter();
    private static final String SELECT="SELECT s.*,r.display_name,r.configuration_bytes,r.configuration_sha256 FROM ddip_management.sources s JOIN ddip_management.source_revisions r ON (s.product_id,s.source_id,s.current_revision)=(r.product_id,r.source_id,r.revision) ";
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;
    private final ReviewedSourceCatalog catalog;private final SourceCursor cursors;
    public SourceRepository(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,ReviewedSourceCatalog catalog,SourceCursor cursors) {
        this.jdbc=jdbc;this.tx=tx;this.installation=installation;this.catalog=catalog;this.cursors=cursors;
    }
    private ObjectNode map(ResultSet r,int row) throws SQLException {
        ObjectNode n=SourceJson.object().put("productId",r.getString("product_id")).put("sourceId",r.getString("source_id"))
                .put("displayName",r.getString("display_name")).put("enabled",r.getBoolean("enabled"))
                .put("currentRevision",r.getInt("current_revision")).put("rowVersion",Long.toString(r.getLong("row_version")))
                .put("configSha256",r.getString("configuration_sha256")).put("createdAt",time(r,"created_at")).put("updatedAt",time(r,"updated_at"));
        JsonNode config=storedConfig(r,n.get("productId").stringValue());
        if(!r.getString("profile_family").equals(config.get("profile").stringValue())) throw new SourceFailure(INTERNAL_FAILURE);
        n.set("config",config);return n;
    }
    private JsonNode storedConfig(ResultSet r,String product) throws SQLException {
        try {
            byte[] bytes=r.getBytes("configuration_bytes");JsonNode n=SourceJson.parse(bytes);
            if(!Arrays.equals(bytes,SourceJson.canonical(n)) || !SourceJson.sha(bytes).equals(r.getString("configuration_sha256"))) throw new SourceFailure(INTERNAL_FAILURE);
            return SourceCommands.config(product,n);
        } catch(SourceFailure ex) {throw new SourceFailure(INTERNAL_FAILURE);}
    }
    private static String time(ResultSet r,String name) throws SQLException {return UTC.format(r.getObject(name,OffsetDateTime.class).toInstant());}
    private ObjectNode current(String product,String source,boolean lock) {
        var rows=jdbc.query(SELECT+"WHERE s.product_id=? AND s.source_id=?"+(lock?" FOR UPDATE OF s":""),this::map,product,source);
        if(rows.isEmpty()) throw new SourceFailure(NOT_FOUND);return rows.getFirst();
    }
    public JsonNode get(String product,String source) {
        SourceCommands.product(product);SourceCommands.source(source);return tx.required(() -> current(product,source,false));
    }
    public Result execute(SourceCommands.Command c,String correlation) {
        return tx.required(() -> {
            var owners=jdbc.queryForList("SELECT installation_id FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE");
            if(owners.size()!=1 || !installation.equals(owners.getFirst().get("installation_id"))) throw new SourceFailure(MANAGEMENT_UNAVAILABLE);
            var bindings=jdbc.queryForList("SELECT normalized_request,response_status,response_data FROM ddip_management.command_bindings WHERE scope=? AND idempotency_key=?",c.scope(),c.key());
            if(!bindings.isEmpty()) {
                var b=bindings.getFirst();if(!Arrays.equals(c.normalized(),(byte[])b.get("normalized_request"))) throw new SourceFailure(IDEMPOTENCY_CONFLICT);
                if(b.get("response_data")==null) throw new SourceFailure(MANAGEMENT_UNAVAILABLE);
                return storedResult(c,(byte[])b.get("response_data"),((Number)b.get("response_status")).intValue());
            }
            ObjectNode previous=null;
            if(c.action()!=SourceCommands.Action.CREATE) {
                previous=current(c.product(),c.source(),true);
                if(!previous.get("rowVersion").stringValue().equals(Long.toString(c.expectedVersion()))) throw new SourceFailure(PRECONDITION_FAILED);
            }
            if(c.config()!=null) {
                ReviewedSourceCatalog.Storage storage=catalog.validate(c.product(),c.source(),c.config());
                Integer matches=jdbc.queryForObject("SELECT count(*) FROM ddip_management.storage_bindings WHERE binding_id=? AND product=? AND role=? AND installation_id=?",Integer.class,storage.id(),storage.product(),storage.role(),installation);
                SourceFailure.require(matches!=null && matches==1);
                String endpointHash=catalog.endpointDigest(c.config());
                if(endpointHash!=null && Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.source_revisions WHERE endpoint_id=? AND endpoint_sha256<>?)",Boolean.class,UUID.fromString(SourceJson.text(c.config().get("feedEndpointId"))),endpointHash)))
                    throw new SourceFailure(STATE_CONFLICT);
            }
            capacity("command_bindings",200000);capacity("audit_events",1000000);
            int status=200;boolean changed=false;String action;
            if(c.action()==SourceCommands.Action.CREATE) {
                if(Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.sources WHERE product_id=? AND source_id=?)",Boolean.class,c.product(),c.source()))) throw new SourceFailure(STATE_CONFLICT);
                capacity("sources",128);status=201;changed=true;action="SOURCE_CREATED";
                jdbc.update("INSERT INTO ddip_management.sources(product_id,source_id,profile_family,current_revision,enabled) VALUES (?,?,?,1,?)",c.product(),c.source(),SourceJson.text(c.config().get("profile")),c.enabled());
                insertRevision(c,1);
            } else if(c.action()==SourceCommands.Action.REVISE) {
                if(!previous.get("config").get("profile").equals(c.config().get("profile"))) throw new SourceFailure(STATE_CONFLICT);
                changed=!previous.get("displayName").stringValue().equals(c.name()) || !previous.get("config").equals(c.config());
                action=changed?"SOURCE_REVISED":"SOURCE_UNCHANGED";
                if(changed) {
                    int revision=previous.get("currentRevision").intValue()+1;if(revision>1000) throw new SourceFailure(MANAGEMENT_CAPACITY_EXCEEDED);
                    insertRevision(c,revision);
                    jdbc.update("UPDATE ddip_management.sources SET current_revision=?,row_version=row_version+1,updated_at=clock_timestamp() WHERE product_id=? AND source_id=?",revision,c.product(),c.source());
                }
            } else {
                changed=previous.get("enabled").booleanValue()!=c.enabled();action=changed?(c.enabled()?"SOURCE_ENABLED":"SOURCE_DISABLED"):"SOURCE_GATE_UNCHANGED";
                if(changed) jdbc.update("UPDATE ddip_management.sources SET enabled=?,row_version=row_version+1,updated_at=clock_timestamp() WHERE product_id=? AND source_id=?",c.enabled(),c.product(),c.source());
            }
            ObjectNode result=current(c.product(),c.source(),false);byte[] response=SourceJson.canonical(result);SourceFailure.require(response.length<=65536);
            String command=UUID.randomUUID().toString();
            jdbc.update("INSERT INTO ddip_management.command_bindings(scope,idempotency_key,normalized_request,request_sha256,response_object_id,response_status,response_data) VALUES (?,?,?,?,?,?,?)",
                    c.scope(),c.key(),c.normalized(),SourceJson.sha(c.normalized()),command,status,response);
            var details=SourceJson.object().put("commandId",command).put("correlationId",correlation).put("changed",changed).put("resultingRevision",result.get("currentRevision").intValue());
            if(previous==null)details.putNull("previousRevision");else details.put("previousRevision",previous.get("currentRevision").intValue());
            jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,source_product,source_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?,?)",
                    c.product(),c.source(),action,previous==null?null:(previous.get("enabled").booleanValue()?"ENABLED":"DISABLED"),result.get("enabled").booleanValue()?"ENABLED":"DISABLED",
                    previous==null?null:Long.valueOf(previous.get("rowVersion").stringValue()),Long.valueOf(result.get("rowVersion").stringValue()),c.action()==SourceCommands.Action.CREATE?"CREATED":changed?"UPDATED":"UNCHANGED",SourceJson.canonical(details));
            return new Result(result,status);
        });
    }
    private Result storedResult(SourceCommands.Command c,byte[] bytes,int status) {
        try {
            JsonNode n=SourceJson.parse(bytes);
            SourceJson.fields(n,"productId","sourceId","displayName","enabled","currentRevision","rowVersion","configSha256","createdAt","updatedAt","config");
            SourceFailure.require(Arrays.equals(bytes,SourceJson.canonical(n)) && SourceJson.text(n.get("productId")).equals(c.product()) && SourceJson.text(n.get("sourceId")).equals(c.source()));
            SourceCommands.label(n.get("displayName"));SourceJson.bool(n.get("enabled"));number(n.get("rowVersion"));
            int revision=SourceJson.integer(n.get("currentRevision"));SourceFailure.require(revision>=1 && revision<=1000);
            JsonNode config=SourceCommands.config(c.product(),n.get("config"));SourceFailure.require(SourceJson.sha(SourceJson.canonical(config)).equals(SourceJson.text(n.get("configSha256"))));
            for(String field:List.of("createdAt","updatedAt")) {
                String time=SourceJson.text(n.get(field));SourceFailure.require(time.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z"));java.time.Instant.parse(time);
            }
            SourceFailure.require(status==(c.action()==SourceCommands.Action.CREATE?201:200));return new Result(n,status);
        } catch(RuntimeException ex) {throw new SourceFailure(INTERNAL_FAILURE);}
    }
    private void capacity(String table,long maximum) {
        // Names are fixed call-site constants, never client input.
        Long count=jdbc.queryForObject("SELECT count(*) FROM ddip_management."+table,Long.class);
        if(count==null || count>=maximum) throw new SourceFailure(MANAGEMENT_CAPACITY_EXCEEDED);
    }
    private void insertRevision(SourceCommands.Command c,int revision) {
        byte[] bytes=SourceJson.canonical(c.config());
        jdbc.update("INSERT INTO ddip_management.source_revisions(product_id,source_id,revision,display_name,config_version,configuration_bytes,configuration_sha256,actor,endpoint_id,endpoint_sha256) VALUES (?,?,?,?,1,?,?,'local-operator',?,?)",c.product(),c.source(),revision,c.name(),bytes,SourceJson.sha(bytes),c.config().get("feedEndpointId").isNull()?null:UUID.fromString(SourceJson.text(c.config().get("feedEndpointId"))),catalog.endpointDigest(c.config()));
    }
    public Page sources(String product,Boolean enabled,int limit,String cursor) {
        SourceFailure.require(limit>=1 && limit<=100);if(product!=null)SourceCommands.product(product);
        return tx.required(() -> {
            long high;String afterProduct="",afterSource="";
            String filter=(product==null?"*":product)+":"+(enabled==null?"*":enabled);
            if(cursor==null) high=jdbc.queryForObject("SELECT coalesce(max(source_seq),0) FROM ddip_management.sources",Long.class);
            else {
                JsonNode state=cursors.decode(cursor);SourceJson.fields(state,"v","kind","filter","high","afterProduct","afterSource");
                SourceFailure.require(SourceJson.integer(state.get("v"))==1 && SourceJson.text(state.get("kind")).equals("sources") && SourceJson.text(state.get("filter")).equals(filter));
                high=number(state.get("high"));afterProduct=SourceCommands.product(SourceJson.text(state.get("afterProduct")));afterSource=SourceCommands.source(SourceJson.text(state.get("afterSource")));
            }
            var rows=jdbc.query(SELECT+"WHERE s.source_seq<=? AND (?::text IS NULL OR s.product_id=?) AND (?::boolean IS NULL OR s.enabled=?) " +
                    "AND (s.product_id COLLATE \"C\",s.source_id COLLATE \"C\")>(?,?) ORDER BY s.product_id COLLATE \"C\",s.source_id COLLATE \"C\" LIMIT ?",this::map,high,product,product,enabled,enabled,afterProduct,afterSource,limit+1);
            boolean more=rows.size()>limit;var out=SourceJson.JSON.createArrayNode();for(var row:rows.subList(0,Math.min(limit,rows.size()))) {var summary=row.deepCopy();summary.remove("config");out.add(summary);}
            String next=null;if(more) {var last=rows.get(limit-1);next=cursors.encode(SourceJson.object().put("v",1).put("kind","sources").put("filter",filter).put("high",Long.toString(high)).put("afterProduct",last.get("productId").stringValue()).put("afterSource",last.get("sourceId").stringValue()));}
            return new Page(out,limit,next);
        });
    }
    public Page revisions(String product,String source,int limit,String cursor) {
        SourceCommands.product(product);SourceCommands.source(source);SourceFailure.require(limit>=1 && limit<=100);
        return tx.required(() -> {
            int high=current(product,source,false).get("currentRevision").intValue(),after=0;
            if(cursor!=null) {
                JsonNode state=cursors.decode(cursor);SourceJson.fields(state,"v","kind","product","source","high","after");
                SourceFailure.require(SourceJson.integer(state.get("v"))==1 && SourceJson.text(state.get("kind")).equals("revisions") && SourceJson.text(state.get("product")).equals(product) && SourceJson.text(state.get("source")).equals(source));
                high=SourceJson.integer(state.get("high"));after=SourceJson.integer(state.get("after"));SourceFailure.require(high>=1 && high<=1000 && after>=1 && after<high);
            }
            var rows=jdbc.query("SELECT * FROM ddip_management.source_revisions WHERE product_id=? AND source_id=? AND revision>? AND revision<=? ORDER BY revision LIMIT ?",(r,i) -> {
                var n=SourceJson.object().put("productId",product).put("sourceId",source).put("revision",r.getInt("revision")).put("displayName",r.getString("display_name")).put("configSha256",r.getString("configuration_sha256")).put("createdAt",time(r,"created_at"));n.set("config",storedConfig(r,product));return n;
            },product,source,after,high,limit+1);
            boolean more=rows.size()>limit;var out=SourceJson.JSON.createArrayNode();out.addAll(rows.subList(0,Math.min(limit,rows.size())));
            String next=more?cursors.encode(SourceJson.object().put("v",1).put("kind","revisions").put("product",product).put("source",source).put("high",high).put("after",rows.get(limit-1).get("revision").intValue())):null;
            return new Page(out,limit,next);
        });
    }
    private static long number(JsonNode value) {
        String text=SourceJson.text(value);SourceFailure.require(text.matches("0|[1-9][0-9]{0,18}"));
        try{return Long.parseLong(text);}catch(NumberFormatException ex){throw new SourceFailure(INVALID_ARGUMENT);}
    }
}
