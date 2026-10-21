package platform.contentapi.execution;

/** Internal fixed diagnostics. Never attach filesystem, process or SQL exception text. */
public final class ImportOwnershipFailure extends RuntimeException {
    public enum Code { INVALID_REQUEST, HOST_BINDING_INVALID, HOST_GUARD_OCCUPIED, OWNERSHIP_STALE,
        QUIESCENCE_REQUIRED, OWNER_PROCESS_UNRESOLVED, IMPORT_NOT_FOUND, MANAGEMENT_CAPACITY_EXCEEDED }
    public ImportOwnershipFailure(Code code) { super(code.name(),null,false,false); }
    static void require(boolean condition,Code code) { if(!condition) throw new ImportOwnershipFailure(code); }
}
