package platform.contentapi.execution;

import java.util.UUID;

/** Database ownership only: no method asserts that a filesystem writer is stopped. */
public interface CoordinatorRepository {
    enum Mode { READY, STOPPING, RECOVERY_ONLY }
    record Snapshot(UUID installationId, Mode mode, long epoch, UUID activeAttemptId, UUID ownerInstanceId, UUID activeImportId, long rowVersion) { }
    Snapshot read();
    boolean compareAndSetIdleMode(long expectedVersion, Mode mode);
    boolean isCurrentOwner(UUID attemptId, UUID instanceId, long epoch);
}
