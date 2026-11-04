package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;

/** Test-only transport and snapshots, not a durable admission receipt format. */
final class NarrowContentPreparationFixtures {
    static final NarrowContentEvidenceContext.FrozenIdentities FROZEN=
            new NarrowContentEvidenceContext.FrozenIdentities("a".repeat(64),"b".repeat(64),"c".repeat(64));
    static NarrowContentPreparation.Specification specification(InstallationHostGuard.Binding host){
        return new NarrowContentPreparation.Specification(NarrowContentProfile.POLICY,UUID.randomUUID(),UUID.randomUUID(),"source-one",host,FROZEN);
    }
    static Path root(NarrowContentPreparation.Specification s){return s.host().root().resolve("runs/"+s.run()+"/attempts/"+s.attempt());}
    record Entry(boolean directory,boolean link,FileTime modified,InstallationHostGuard.Identity identity,
                 Set<PosixFilePermission> permissions,String content){}
    static Map<String,Entry> snapshot(Path root)throws Exception{
        var result=new TreeMap<String,Entry>();
        try(var tree=Files.walk(root)){
            for(Path path:tree.toList()){
                var a=Files.readAttributes(path,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
                String content=a.isDirectory()?null:a.isSymbolicLink()?Files.readSymbolicLink(path).toString():Base64.getEncoder().encodeToString(Files.readAllBytes(path));
                result.put(root.relativize(path).toString(),new Entry(a.isDirectory(),a.isSymbolicLink(),a.lastModifiedTime(),
                        InstallationHostGuard.identity(path),Files.getPosixFilePermissions(path,LinkOption.NOFOLLOW_LINKS),content));
            }
        }
        return result;
    }
    static byte[] specificationBytes(NarrowContentPreparation.Specification s){
        return SourceJson.canonical(SourceJson.object().put("run",s.run().toString()).put("attempt",s.attempt().toString()).put("source",s.source()));
    }
    static NarrowContentPreparation.Specification specification(InstallationHostGuard.Binding host,JsonNode n){
        return new NarrowContentPreparation.Specification(NarrowContentProfile.POLICY,UUID.fromString(n.get("run").stringValue()),
                UUID.fromString(n.get("attempt").stringValue()),n.get("source").stringValue(),host,FROZEN);
    }
    static byte[] expectedBytes(NarrowContentEvidenceContext.Expected e){
        var n=SourceJson.object().put("run",e.run().toString()).put("attempt",e.attempt().toString()).put("source",e.source())
                .put("rootDevice",Long.toString(e.rootIdentity().device())).put("rootInode",Long.toString(e.rootIdentity().inode()))
                .put("lockDevice",Long.toString(e.lockIdentity().device())).put("lockInode",Long.toString(e.lockIdentity().inode()))
                .put("ledgerSha256",e.ledgerSha256());
        return SourceJson.canonical(n);
    }
    static NarrowContentEvidenceContext.Expected expected(InstallationHostGuard.Binding host,JsonNode n){
        var spec=specification(host,n);
        return new NarrowContentEvidenceContext.Expected(spec.policy(),spec.run(),spec.attempt(),NarrowContentEvidenceContext.PlanKind.ORIGINAL,
                spec.source(),host,new InstallationHostGuard.Identity(Long.parseLong(n.get("rootDevice").stringValue()),Long.parseLong(n.get("rootInode").stringValue())),
                new InstallationHostGuard.Identity(Long.parseLong(n.get("lockDevice").stringValue()),Long.parseLong(n.get("lockInode").stringValue())),
                FROZEN,n.get("ledgerSha256").stringValue());
    }
    private NarrowContentPreparationFixtures(){}
}
