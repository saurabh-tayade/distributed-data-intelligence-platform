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
    public static final List<String> SILVER_MODULES=java.util.stream.Stream.concat(MODULES.stream(),java.util.stream.Stream.of("silver_import_qualification.py","silver.py","silver_inputs.py","bronze_to_silver.py")).toList();
    public record SparkRuntime(Path javaHome,Storage scratch) {
        public JsonNode json(){return SourceJson.object().put("javaHome",javaHome.toString()).put("scratchRoot",scratch.root().toString()).put("scratchDevice",scratch.device()).put("scratchInode",scratch.inode());}
        public void check(){try{scratch.check();ImportFailure.require(javaHome.equals(javaHome.toRealPath()) && Files.isExecutable(javaHome.resolve("bin/java")),RUNTIME_INVALID);privateFile(scratch.root(),true);}catch(ImportFailure f){throw f;}catch(Exception e){throw new ImportFailure(RUNTIME_INVALID);}}
        public void disjoint(Path nativeRoot){ImportFailure.require(!scratch.root().startsWith(nativeRoot) && !nativeRoot.startsWith(scratch.root()),BINDING_INVALID);}
        @Override public String toString(){return "SparkImportRuntime[redacted]";}
    }
    public record RuntimeProfile(String id,String digest,Path python,Path processing,int timeoutSeconds,JsonNode modules,SparkRuntime spark) {
        public RuntimeProfile {modules=modules.deepCopy();}
        @Override public JsonNode modules(){return modules.deepCopy();}
        public void check() {
            try {
                ImportFailure.require(python.isAbsolute() && Files.isRegularFile(python) && Files.isExecutable(python) && processing.equals(processing.toRealPath()),RUNTIME_INVALID);
                for(String name:spark==null?MODULES:SILVER_MODULES)ImportFailure.require(ImportJson.hash(modules.get(name)).equals(SourceJson.sha(ImportJson.read(processing.resolve(name),1024*1024))),RUNTIME_CHANGED);
                if(spark!=null)spark.check();
            } catch(ImportFailure f){throw f;}catch(Exception ex){throw new ImportFailure(RUNTIME_INVALID);}
        }
        @Override public String toString(){return "ImportRuntime[redacted]";}
    }
    public record Bronze(Path credentials,byte[] credentialsBytes,JsonNode configuration) {
        public Bronze{credentialsBytes=credentialsBytes.clone();configuration=configuration.deepCopy();}
        @Override public byte[] credentialsBytes(){return credentialsBytes.clone();}
        @Override public JsonNode configuration(){return configuration.deepCopy();}
        public void check(){privateFile(credentials,false);ImportFailure.require(Arrays.equals(credentialsBytes,ImportJson.read(credentials,8192)),BINDING_INVALID);}
        @Override public String toString(){return "ImportBronze[redacted]";}
    }
    public record Storage(Path root,String device,String inode,Bronze bronze) {
        public Storage(Path root,String device,String inode){this(root,device,inode,null);}
        public JsonNode json(){return SourceJson.object().put("root",root.toString()).put("rootDevice",device).put("rootInode",inode);}
        public void check() {
            try {
                if(bronze!=null)bronze.check();
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
        int version=ImportJson.integer(c.get("importProtocolVersion"),1,2);
        if(version==1)ImportJson.fields(c,"importProtocolVersion","python","processingRoot","pythonVersion","duckdbVersion","timeoutSeconds","modules");
        else ImportJson.fields(c,"importProtocolVersion","python","processingRoot","pythonVersion","duckdbVersion","timeoutSeconds","modules","spark");
        ImportFailure.require(ImportJson.text(c.get("pythonVersion")).equals("3.12.15") && ImportJson.text(c.get("duckdbVersion")).equals("1.4.5"),RUNTIME_INVALID);
        Path python=Path.of(ImportJson.text(c.get("python"))),processing=Path.of(ImportJson.text(c.get("processingRoot")));
        ImportFailure.require(python.isAbsolute() && processing.isAbsolute() && python.equals(python.normalize()) && processing.equals(processing.normalize()),RUNTIME_INVALID);
        ImportJson.fields(c.get("modules"),(version==1?MODULES:SILVER_MODULES).toArray(String[]::new));
        SparkRuntime spark=null;
        if(version==2){
            var nSpark=c.get("spark");ImportJson.fields(nSpark,"javaHome","pysparkVersion","javaVersion","scratchBindingId");
            ImportFailure.require(ImportJson.text(nSpark.get("pysparkVersion")).equals("4.0.2") && ImportJson.text(nSpark.get("javaVersion")).equals("21"),RUNTIME_INVALID);
            Path java=Path.of(ImportJson.text(nSpark.get("javaHome")));ImportFailure.require(java.isAbsolute(),RUNTIME_INVALID);
            spark=new SparkRuntime(java,localStorage(storageRow(ImportJson.token(nSpark.get("scratchBindingId")),id,"content-intelligence","TEMP"),null));
        }
        return new RuntimeProfile(id,SourceJson.sha(raw),python,processing,ImportJson.integer(c.get("timeoutSeconds"),1,version==1?120:900),c.get("modules"),spark);
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
        var c=storageRow(e.bindingId(),profile,e.product(),e.silver()?"SILVER":e.format()==null?"REFERENCE":"BATCH");
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
        return new Storage(root,device,inode,e.silver()?bronze(e,profile):null);
    }
    private Storage localStorage(JsonNode c,Bronze bronze) {
        ImportJson.fields(c,"importStorageVersion","root","rootDevice","rootInode");ImportJson.integer(c.get("importStorageVersion"),1,1);
        Path root=Path.of(ImportJson.text(c.get("root")));ImportFailure.require(root.isAbsolute() && root.equals(root.normalize()),BINDING_INVALID);
        return new Storage(root,ImportJson.match(c.get("rootDevice"),"-?[0-9]{1,20}"),ImportJson.match(c.get("rootInode"),"-?[0-9]{1,20}"),bronze);
    }
    private Bronze bronze(ImportCatalog.Entry e,String profile) {
        var i=e.identity();var c=storageRow(ImportJson.token(i.get("bronzeStorageBindingId")),profile,"content-intelligence","BRONZE");
        return bronzeConfiguration(c,ImportJson.text(i.get("bronzeBucket")));
    }
    private Bronze bronzeConfiguration(JsonNode c,String bucket) {
        ImportJson.fields(c,"importBronzeVersion","endpoint","region","bucket","credentialsFile");ImportJson.integer(c.get("importBronzeVersion"),1,1);
        ImportFailure.require(ImportJson.text(c.get("bucket")).equals(bucket),BINDING_INVALID);
        try{var uri=java.net.URI.create(ImportJson.text(c.get("endpoint")));ImportFailure.require(uri.getScheme().equals("http") && Set.of("localhost","127.0.0.1","[::1]","::1").contains(uri.getHost()) && uri.getPort()>0 && uri.getPort()<=65535 && uri.getUserInfo()==null && uri.getQuery()==null && uri.getFragment()==null && Set.of("","/").contains(uri.getPath()),BINDING_INVALID);}catch(RuntimeException f){throw new ImportFailure(BINDING_INVALID);}
        Path file=Path.of(ImportJson.text(c.get("credentialsFile")));privateFile(file,false);byte[] raw=ImportJson.read(file,8192);var secrets=ImportJson.parse(raw,8192);ImportJson.fields(secrets,"accessKey","secretKey");
        for(String key:List.of("accessKey","secretKey")){String text=ImportJson.text(secrets.get(key));ImportFailure.require(!text.isBlank() && text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=4096 && text.codePoints().noneMatch(Character::isISOControl),BINDING_INVALID);}
        var configuration=SourceJson.object().put("endpoint",ImportJson.text(c.get("endpoint"))).put("region",ImportJson.match(c.get("region"),"[A-Za-z0-9_-]{1,128}")).put("bucket",ImportJson.text(c.get("bucket"))).put("accessKey",secrets.get("accessKey").stringValue()).put("secretKey",secrets.get("secretKey").stringValue());
        return new Bronze(file,raw,configuration);
    }
    /** Worker startup checks only roots/credentials/configuration; no object GET or native verification.
     * Reuses import storage descriptors. All filesystem checks occur after the metadata transaction ends.
     */
    public void checkWorkerStorage(String profile,String product) {
        var rows=tx.required(()->jdbc.queryForList("SELECT b.binding_id,b.role,b.product FROM ddip_management.storage_bindings b JOIN ddip_management.execution_profile_storage p USING(binding_id) WHERE p.profile_id=? ORDER BY b.binding_id",profile));
        ImportFailure.require(!rows.isEmpty() && rows.size()<=128,BINDING_INVALID);
        Set<String> roles=new HashSet<>();List<Path> nativeRoots=new ArrayList<>(),privateRoots=new ArrayList<>();
        for(var row:rows) {
            String role=(String)row.get("role"),id=(String)row.get("binding_id");
            ImportFailure.require(product.equals(row.get("product")) || role.equals("CONTROL"),BINDING_INVALID);roles.add(role);
            var c=storageRow(id,profile,role.equals("CONTROL")?null:product,role);
            if(role.equals("CONTROL")){privateRoots.add(control(id,profile).root());continue;}
            if(role.equals("BRONZE")){bronzeConfiguration(c,ImportJson.text(c.get("bucket"))).check();continue;}
            Storage s=localStorage(c,null);s.check();ImportFailure.require(Files.isReadable(s.root()),BINDING_INVALID);
            if(Set.of("STAGING","TEMP").contains(role)){privateFile(s.root(),true);ImportFailure.require(Files.isWritable(s.root()),BINDING_INVALID);privateRoots.add(s.root());}else nativeRoots.add(s.root());
        }
        ImportFailure.require(roles.containsAll(product.equals("content-intelligence")?Set.of("BRONZE","SILVER","REFERENCE","GOLD","CONTENT","CONTROL","STAGING","TEMP"):Set.of("BATCH","REFERENCE","RELIABILITY","CONTROL","STAGING","TEMP")),BINDING_INVALID);
        for(Path p:privateRoots)for(Path n:nativeRoots)ImportFailure.require(!p.startsWith(n) && !n.startsWith(p),BINDING_INVALID);
        for(int i=0;i<privateRoots.size();i++)for(int j=0;j<i;j++)ImportFailure.require(!privateRoots.get(i).startsWith(privateRoots.get(j)) && !privateRoots.get(j).startsWith(privateRoots.get(i)),BINDING_INVALID);
    }
    static void privateFile(Path file,boolean directory) {
        try{ImportFailure.require(file.isAbsolute() && file.equals(file.toRealPath()) && (directory?Files.isDirectory(file,LinkOption.NOFOLLOW_LINKS):Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)) && Files.getOwner(file).equals(Files.getOwner(Path.of(System.getProperty("user.home")))) &&
                Files.getPosixFilePermissions(file).stream().noneMatch(p->p.name().startsWith("GROUP_") || p.name().startsWith("OTHERS_")),BINDING_INVALID);
        }catch(ImportFailure f){throw f;}catch(Exception f){throw new ImportFailure(BINDING_INVALID);}
    }

}
