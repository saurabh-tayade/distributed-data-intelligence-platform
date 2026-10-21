package platform.contentapi.execution;

/** Fixed local diagnostics. Never retain an underlying database/native exception. */
public final class WorkerFailure extends RuntimeException {
    public enum Code { WORKER_CONFIGURATION_INVALID, WORKER_NOT_READY, OWNERSHIP_LOST, RUNTIME_MISMATCH, CLAIM_UNCERTAIN }
    private final Code code;
    public WorkerFailure(Code code) { super(code.name(),null,false,false);this.code=code; }
    public Code code(){return code;}
    static void require(boolean valid,Code code){if(!valid)throw new WorkerFailure(code);}
}
