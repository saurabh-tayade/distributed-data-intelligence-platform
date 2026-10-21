package platform.contentapi.execution;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;

public final class OwnershipFixtures {
    public static final UUID INSTALL=UUID.fromString("10000000-0000-4000-8000-000000000001");
    public static final byte[] OBJECT="{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    public static final String HASH=SourceJson.sha(OBJECT);
    public static final byte[] REQUEST=SourceJson.canonical(SourceJson.object().put("protocolVersion",1).put("selectionSha256","a".repeat(64)));
    public static final byte[] RESULT=SourceJson.canonical(SourceJson.object().put("protocolVersion",1).put("coordinationEvidenceSha256","b".repeat(64)));
    private OwnershipFixtures() { }
    public static InstallationHostGuard.Binding host(Path base) throws IOException {
        Path root=Files.createDirectory(base.resolve("control")).toRealPath();
        Files.setPosixFilePermissions(root,PosixFilePermissions.fromString("rwx------"));
        Path guard=Files.write(root.resolve("host.guard"),new byte[0]);
        Path installation=Files.writeString(root.resolve("installation.id"),INSTALL+"\n");
        for(Path p:new Path[]{guard,installation})Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rw-------"));
        return new InstallationHostGuard.Binding("test-control",INSTALL,root,InstallationHostGuard.identity(root),InstallationHostGuard.identity(guard),InstallationHostGuard.identity(installation));
    }
    public static void provision(JdbcTemplate jdbc,InstallationHostGuard.Binding binding) {
        jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES ('import-test',?,?)",OBJECT,HASH);
        jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,'content-intelligence','CONTROL',?,?,?)",binding.id(),binding.descriptor(),binding.digest(),INSTALL);
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage VALUES ('import-test',?)",binding.id());
    }
    public static ImportOwnershipRepository.Reservation reservation(UUID id) {return new ImportOwnershipRepository.Reservation(id,"import-test",HASH,REQUEST);}
    public static InstallationHostGuard.Binding readBinding(Path file) throws IOException {
        var n=SourceJson.parse(Files.readAllBytes(file));
        var root=Path.of(n.get("root").stringValue());
        var r=new InstallationHostGuard.Identity(Long.parseLong(n.get("rootDevice").stringValue()),Long.parseLong(n.get("rootInode").stringValue()));
        var g=new InstallationHostGuard.Identity(Long.parseLong(n.get("guardDevice").stringValue()),Long.parseLong(n.get("guardInode").stringValue()));
        var i=new InstallationHostGuard.Identity(Long.parseLong(n.get("installationDevice").stringValue()),Long.parseLong(n.get("installationInode").stringValue()));
        return new InstallationHostGuard.Binding(n.get("bindingId").stringValue(),UUID.fromString(n.get("installationId").stringValue()),root,r,g,i);
    }
    public static ProcessBuilder child(Path binding,String mode,UUID id) {
        String cp=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));
        return new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",cp,OwnershipTestChild.class.getName(),binding.toString(),mode,id.toString())
                .redirectError(ProcessBuilder.Redirect.DISCARD);
    }
}
