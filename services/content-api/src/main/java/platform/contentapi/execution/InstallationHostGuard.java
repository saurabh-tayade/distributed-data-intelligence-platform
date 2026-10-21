package platform.contentapi.execution;

import java.io.IOException;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import platform.contentapi.management.SourceJson;
import static platform.contentapi.execution.ImportOwnershipFailure.Code.*;

/** Cooperative local POSIX guard. Provision the private root/files separately; never create/unlink here. */
public final class InstallationHostGuard implements AutoCloseable {
    public record Identity(long device,long inode) { }
    public record Binding(String id,UUID installationId,Path root,Identity rootIdentity,Identity guardIdentity,Identity installationIdentity) {
        public Binding {
            ImportOwnershipFailure.require(id!=null && id.matches("[A-Za-z0-9_-]{1,128}") && installationId!=null && root!=null && root.isAbsolute()
                    && root.equals(root.normalize()) && rootIdentity!=null && guardIdentity!=null && installationIdentity!=null,HOST_BINDING_INVALID);
        }
        public byte[] descriptor() {
            var n=SourceJson.object().put("bindingVersion",1).put("bindingId",id).put("installationId",installationId.toString()).put("root",root.toString());
            for(var e:Map.of("root",rootIdentity,"guard",guardIdentity,"installation",installationIdentity).entrySet())
                n.put(e.getKey()+"Device",Long.toString(e.getValue().device())).put(e.getKey()+"Inode",Long.toString(e.getValue().inode()));
            return SourceJson.canonical(n);
        }
        public String digest() { return SourceJson.sha(descriptor()); }
        @Override public String toString() { return "HostBinding[redacted]"; }
    }
    public record ProcessIdentity(UUID instanceId,long pid,Instant startedAt) { }
    private final Binding binding;
    private final FileChannel channel;
    private final FileLock lock;
    private final ProcessIdentity process;

    private InstallationHostGuard(Binding binding,FileChannel channel,FileLock lock) {
        this.binding=binding;this.channel=channel;this.lock=lock;
        process=new ProcessIdentity(UUID.randomUUID(),ProcessHandle.current().pid(),ProcessHandle.current().info().startInstant()
                .orElseThrow(() -> new ImportOwnershipFailure(HOST_BINDING_INVALID)).truncatedTo(ChronoUnit.MICROS));
    }
    public static Identity identity(Path path) {
        try {
            var a=Files.readAttributes(path,"unix:dev,ino",LinkOption.NOFOLLOW_LINKS);
            return new Identity(((Number)a.get("dev")).longValue(),((Number)a.get("ino")).longValue());
        } catch(IOException | RuntimeException ex) { throw new ImportOwnershipFailure(HOST_BINDING_INVALID); }
    }
    public static InstallationHostGuard acquire(Binding binding) {
        FileChannel channel=null;FileLock lock=null;
        try {
            validate(binding);
            channel=FileChannel.open(binding.root().resolve("host.guard"),StandardOpenOption.READ,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
            try { lock=channel.tryLock(); } catch(OverlappingFileLockException ex) { throw new ImportOwnershipFailure(HOST_GUARD_OCCUPIED); }
            ImportOwnershipFailure.require(lock!=null,HOST_GUARD_OCCUPIED);
            validate(binding);
            return new InstallationHostGuard(binding,channel,lock);
        } catch(IOException | RuntimeException ex) {
            try { if(lock!=null) lock.release(); } catch(IOException ignored) { }
            try { if(channel!=null) channel.close(); } catch(IOException ignored) { }
            if(ex instanceof ImportOwnershipFailure f)throw f;
            throw new ImportOwnershipFailure(HOST_BINDING_INVALID);
        }
    }
    private static void privateEntry(Path path,boolean directory,Identity expected) throws IOException {
        var a=Files.readAttributes(path,PosixFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
        var owner=path.getFileSystem().getUserPrincipalLookupService().lookupPrincipalByName(System.getProperty("user.name"));
        ImportOwnershipFailure.require((directory?a.isDirectory():a.isRegularFile()) && a.owner().equals(owner) && identity(path).equals(expected),HOST_BINDING_INVALID);
        Set<PosixFilePermission> permissions=directory?PosixFilePermissions.fromString("rwx------"):PosixFilePermissions.fromString("rw-------");
        ImportOwnershipFailure.require(a.permissions().equals(permissions),HOST_BINDING_INVALID);
        if(!directory)ImportOwnershipFailure.require(((Number)Files.getAttribute(path,"unix:nlink",LinkOption.NOFOLLOW_LINKS)).longValue()==1,HOST_BINDING_INVALID);
    }
    private static void validate(Binding b) throws IOException {
        ImportOwnershipFailure.require(b!=null && b.root().equals(b.root().toRealPath()),HOST_BINDING_INVALID);
        privateEntry(b.root(),true,b.rootIdentity());
        privateEntry(b.root().resolve("host.guard"),false,b.guardIdentity());
        privateEntry(b.root().resolve("installation.id"),false,b.installationIdentity());
        byte[] raw;
        try(var in=Files.newInputStream(b.root().resolve("installation.id"),LinkOption.NOFOLLOW_LINKS)) {raw=in.readNBytes(38);}
        ImportOwnershipFailure.require(Arrays.equals(raw,(b.installationId()+"\n").getBytes(StandardCharsets.US_ASCII)),HOST_BINDING_INVALID);
        privateEntry(b.root().resolve("installation.id"),false,b.installationIdentity());
    }
    public void checkHeld(UUID installation) {
        ImportOwnershipFailure.require(binding.installationId().equals(installation) && lock.isValid() && channel.isOpen(),HOST_BINDING_INVALID);
        try { validate(binding); } catch(IOException ex) { throw new ImportOwnershipFailure(HOST_BINDING_INVALID); }
    }
    public Binding binding() {return binding;}
    public ProcessIdentity process() {return process;}
    @Override public void close() {
        try { channel.close(); } catch(IOException ex) { throw new ImportOwnershipFailure(HOST_BINDING_INVALID); }
        // Closing the guard never clears durable ownership, even on normal JVM shutdown.
    }
    @Override public String toString() {return "InstallationHostGuard[redacted]";}
}
