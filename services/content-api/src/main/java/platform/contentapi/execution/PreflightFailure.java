package platform.contentapi.execution;

/** Bounded private classifications; never retain a native message or underlying exception. */
public final class PreflightFailure extends RuntimeException {
    public enum Code { RUNTIME_MISMATCH, INPUT_UNAVAILABLE, INPUT_INVALID, INPUT_CHANGED, RESOURCE_LIMIT,
        PROCESS_START_FAILED, STAGE_TIMEOUT, STAGE_OUTPUT_LIMIT, RESULT_INVALID, TERMINATION_UNCONFIRMED, INTERRUPTED }
    private final Code code;private final boolean quiescent;
    public PreflightFailure(Code code){this(code,true);}
    public PreflightFailure(Code code,boolean quiescent){super(code.name(),null,false,false);this.code=code;this.quiescent=quiescent;}
    public Code code(){return code;}
    public boolean quiescent(){return quiescent;}
    static void need(boolean value,Code code){if(!value)throw new PreflightFailure(code);}
}
