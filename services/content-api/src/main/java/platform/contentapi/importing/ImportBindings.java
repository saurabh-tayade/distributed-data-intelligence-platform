package platform.contentapi.importing;

import java.nio.file.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.importing.ImportFailure.Code.*;

/** Immutable server-provisioned DB profiles. No catalog executable or absolute artifact path. */
public final class ImportBindings {
    public static final List<String> MODULES=List.of("artifact_import_verifier.py","publication_metadata.py","reliability_format_inputs.py",
            "reliability_format_metadata.py","reliability_format_parquet.py","reliability_inputs.py","reliability_metadata.py","publisher_reference.py","service_reference.py");
    public record RuntimeProfile(String id,String digest,Path python,Path processing,int timeoutSeconds,JsonNode modules) {
        public RuntimeProfile {modules=modules.deepCopy();}
        @Override public JsonNode modules(){return modules.deepCopy();}
        public void check() {
            try {
                ImportFailure.require(python.isAbsolute() && Files.isRegularFile(python) && Files.isExecutable(python) && processing.equals(processing.toRealPath()),RUNTIME_INVALID);
                for(String name:MODULES)ImportFailure.require(ImportJson.hash(modules.get(name)).equals(SourceJson.sha(ImportJson.read(processing.resolve(name),1024*1024))),RUNTIME_CHANGED);
            } catch(ImportFailure f){throw f;}catch(Exception ex){throw new ImportFailure(RUNTIME_INVALID);}
        }
        @Override public String toString(){return "ImportRuntime[redacted]";}
    }
    public record Storage(Path root,String device,String inode) {
        public JsonNode json(){return SourceJson.object().put("root",root.toString()).put("rootDevice",device).put("rootInode",inode);}
        public void check() {
            try {
                var observed=InstallationHostGuard.identity(root);
                ImportFailure.require(root.equals(root.toRealPath()) && Files.isDirectory(root,LinkOption.NOFOLLOW_LINKS) &&
                        Long.toString(observed.device()).equals(device) && Long.toString(observed.inode()).equals(inode),BINDING_INVALID);
            } catch(Exception ex){throw new ImportFailure(BINDING_INVALID);}
        }
        @Override public String toString(){return "ImportStorage[redacted]";}
    }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;
    public ImportBindings(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation){this.jdbc=jdbc;this.tx=tx;this.installation=installation;}
    public RuntimeProfile runtime(String id) {
        var n=tx.required(() -> {
            var rows=jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",id);
            ImportFailure.require(rows.size()==1,BINDING_INVALID);return rows.getFirst();
        });
        byte[] raw=(byte[])n.get("configuration_bytes");JsonNode c=ImportJson.parse(raw,65536);ImportJson.canonical(raw,c);
        ImportFailure.require(SourceJson.sha(raw).equals(n.get("configuration_sha256")),BINDING_INVALID);
        ImportJson.fields(c,"importProtocolVersion","python","processingRoot","pythonVersion","duckdbVersion","timeoutSeconds","modules");ImportJson.integer(c.get("importProtocolVersion"),1,1);
        ImportFailure.require(ImportJson.text(c.get("pythonVersion")).equals("3.12.15") && ImportJson.text(c.get("duckdbVersion")).equals("1.4.5"),RUNTIME_INVALID);
        Path python=Path.of(ImportJson.text(c.get("python"))),processing=Path.of(ImportJson.text(c.get("processingRoot")));
        ImportFailure.require(python.isAbsolute() && processing.isAbsolute() && python.equals(python.normalize()) && processing.equals(processing.normalize()),RUNTIME_INVALID);
        ImportJson.fields(c.get("modules"),MODULES.toArray(String[]::new));
        return new RuntimeProfile(id,SourceJson.sha(raw),python,processing,ImportJson.integer(c.get("timeoutSeconds"),1,120),c.get("modules"));
    }
    private JsonNode storageRow(String id,String profile,String product,String role) {
        byte[] raw=tx.required(() -> {
            var rows=jdbc.queryForList("SELECT b.configuration_bytes,b.configuration_sha256,b.product,b.role FROM ddip_management.storage_bindings b JOIN ddip_management.execution_profile_storage s USING(binding_id) WHERE b.binding_id=? AND b.installation_id=? AND s.profile_id=?",id,installation,profile);
            ImportFailure.require(rows.size()==1,BINDING_INVALID);var row=rows.getFirst();
            ImportFailure.require(row.get("role").equals(role) && (product==null || row.get("product").equals(product)),BINDING_INVALID);
            byte[] bytes=(byte[])row.get("configuration_bytes");ImportFailure.require(SourceJson.sha(bytes).equals(row.get("configuration_sha256")),BINDING_INVALID);return bytes;
        });
        var c=ImportJson.parse(raw,65536);ImportJson.canonical(raw,c);return c;
    }
    public InstallationHostGuard.Binding control(String id,String profile) {
        var c=storageRow(id,profile,null,"CONTROL");
        ImportJson.fields(c,"bindingVersion","bindingId","installationId","root","rootDevice","rootInode","guardDevice","guardInode","installationDevice","installationInode");
        ImportJson.integer(c.get("bindingVersion"),1,1);
        ImportFailure.require(ImportJson.token(c.get("bindingId")).equals(id) && ImportJson.uuid(c.get("installationId")).equals(installation),BINDING_INVALID);
        var binding=new InstallationHostGuard.Binding(id,installation,Path.of(ImportJson.text(c.get("root"))),identity(c,"root"),identity(c,"guard"),identity(c,"installation"));
        ImportFailure.require(Arrays.equals(binding.descriptor(),SourceJson.canonical(c)),BINDING_INVALID);return binding;
    }
    private InstallationHostGuard.Identity identity(JsonNode c,String prefix) {
        try {return new InstallationHostGuard.Identity(Long.parseLong(ImportJson.text(c.get(prefix+"Device"))),Long.parseLong(ImportJson.text(c.get(prefix+"Inode"))));}
        catch(RuntimeException e){throw new ImportFailure(BINDING_INVALID);}
    }
    public Storage storage(ImportCatalog.Entry e,String profile) {
        var c=storageRow(e.bindingId(),profile,e.product(),e.format()==null?"REFERENCE":"BATCH");
        ImportJson.fields(c,"importStorageVersion","root","rootDevice","rootInode");ImportJson.integer(c.get("importStorageVersion"),1,1);
        Path root=Path.of(ImportJson.text(c.get("root")));ImportFailure.require(root.isAbsolute() && root.equals(root.normalize()),BINDING_INVALID);
        String device=ImportJson.match(c.get("rootDevice"),"-?[0-9]{1,20}"),inode=ImportJson.match(c.get("rootInode"),"-?[0-9]{1,20}");
        if(e.format()!=null)tx.required(() -> {
            var i=e.identity();var rows=jdbc.queryForList("SELECT r.configuration_bytes,r.configuration_sha256,s.profile_family FROM ddip_management.source_revisions r JOIN ddip_management.sources s USING(product_id,source_id) WHERE r.product_id=? AND r.source_id=? AND r.revision=?",e.product(),i.get("sourceId").stringValue(),i.get("sourceRevision").intValue());
            ImportFailure.require(rows.size()==1,BINDING_INVALID);var row=rows.getFirst();byte[] raw=(byte[])row.get("configuration_bytes");var config=ImportJson.parse(raw,65536);ImportJson.canonical(raw,config);
            ImportJson.fields(config,"configVersion","profile","dataOrigin","feedEndpointId","registeredProfileSha256","inputStorageProfileId");ImportJson.integer(config.get("configVersion"),1,1);
            ImportFailure.require(config.get("feedEndpointId").isNull() && ImportJson.text(config.get("profile")).equals(row.get("profile_family")) && SourceJson.sha(raw).equals(row.get("configuration_sha256")) && ImportJson.text(config.get("profile")).equals(e.format().equals("JSONL")?"request-jsonl-v2":"request-parquet-v1") &&
                    ImportJson.text(config.get("dataOrigin")).equals("synthetic") && ImportJson.text(config.get("registeredProfileSha256")).equals(i.get("registeredProfileSha256").stringValue()) &&
                    ImportJson.text(config.get("inputStorageProfileId")).equals(e.bindingId()),BINDING_INVALID);return null;
        });
        return new Storage(root,device,inode);
    }
}
