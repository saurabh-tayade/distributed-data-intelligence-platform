package platform.contentapi.execution;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;

/**
 * Test-only, synthetic journal construction for read-only and mutation-rejection component gates.
 * This does not run a child, resolve DNS, qualify public transport, or reconstruct an Owner.
 */
public final class NarrowContentPublicCheckpointTestSupport {
    public static byte[] packet(byte[] reservation,long pid){return NarrowContentFetchLaunch.packet(reservation,pid);}
    public static String candidate(Path directory,byte[] command){return NarrowContentFetchLaunch.candidate(directory,command);}

    public static Path syntheticCommand(JdbcTemplate jdbc,InstallationHostGuard guard,UUID operation)throws Exception{
        byte[] command=jdbc.queryForObject("SELECT command_bytes FROM ddip_management.content_native_operations WHERE operation_id=?",byte[].class,operation);
        var c=SourceJson.parse(command);
        Path directory=guard.binding().root().resolve("runs/"+c.get("runId").stringValue()+"/attempts/"+c.get("attemptId").stringValue()
                +"/fetch-source/"+c.get("sourceId").stringValue()+"/"+c.get("checkpointId").stringValue());
        for(Path p:List.of(directory.getParent().getParent(),directory.getParent(),directory))
            Files.createDirectory(p,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        Path path=directory.resolve("command.json");
        Files.createFile(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        Files.write(path,command);return directory;
    }
    public static Path syntheticCandidate(JdbcTemplate jdbc,InstallationHostGuard guard,UUID operation)throws Exception{
        Path directory=syntheticCommand(jdbc,guard,operation);
        byte[] command=jdbc.queryForObject("SELECT command_bytes FROM ddip_management.content_native_operations WHERE operation_id=?",byte[].class,operation);
        var c=SourceJson.parse(command);
        byte[] payload="<feed xmlns=\"http://www.w3.org/2005/Atom\"><title>Synthetic component fixture</title></feed>".getBytes(StandardCharsets.UTF_8);
        String ingestion="94000000-0000-4000-8000-000000000001",at="2026-11-08T00:00:00Z";
        String prefix="bronze/rss/source_id="+Base64.getUrlEncoder().withoutPadding().encodeToString(c.get("sourceId").stringValue().getBytes(StandardCharsets.UTF_8))
                +"/ingestion_date=2026-11-08/ingestion_id="+ingestion+"/";
        var descriptor=SourceJson.object().put("journalVersion",1).put("commandSha256",SourceJson.sha(command));
        for(String key:List.of("runId","attemptId","ownerEpoch","stage","itemOrdinal","checkpointId","sourceId","revision","capturedConfigSha256","endpointSha256","runtimeSha256"))descriptor.set(key,c.get(key));
        descriptor.put("ingestionId",ingestion).put("ingestedAt",at).put("httpStatus",200).put("contentType","application/atom+xml")
                .put("declaredContentLength",payload.length).put("payloadByteLength",payload.length).put("payloadSha256",SourceJson.sha(payload));
        var metadata=SourceJson.object().put("schemaVersion",1);
        for(String key:List.of("ingestionId","sourceId","ingestedAt","httpStatus","contentType","declaredContentLength","payloadByteLength","payloadSha256"))metadata.set(key,descriptor.get(key));
        metadata.set("sourceUri",c.get("feedUri"));metadata.put("payloadKey",prefix+"payload.xml");
        byte[] metadataBytes=SourceJson.canonical(metadata);
        descriptor.putObject("expected").put("payloadKey",prefix+"payload.xml").put("metadataKey",prefix+"metadata.json")
                .put("metadataByteLength",metadataBytes.length).put("metadataSha256",SourceJson.sha(metadataBytes));
        var result=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))result.set(key,c.get(key));
        result.put("status","OK").putNull("code");result.set("evidenceId",c.get("checkpointId"));result.putObject("counts");
        var files=Map.of("payload.xml",payload,"metadata.json",metadataBytes,
                "descriptor.json",SourceJson.canonical(descriptor),"result.json",SourceJson.canonical(result));
        for(var entry:files.entrySet()){
            Path path=directory.resolve(entry.getKey());
            Files.createFile(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            Files.write(path,entry.getValue());
        }
        return directory;
    }
    private NarrowContentPublicCheckpointTestSupport(){}
}
