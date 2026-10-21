package platform.contentapi.execution;

/** Fixed diagnostics deliberately omit database URLs, credentials and driver exception chains. */
public final class ManagementFailure extends RuntimeException {
    public enum Code { MANAGEMENT_CONFIGURATION_INVALID, MANAGEMENT_UNAVAILABLE, MANAGEMENT_INCOMPATIBLE, STATE_CONFLICT }
    private final Code code;
    public ManagementFailure(Code code) { super(code.name(), null, false, false); this.code = code; }
    public Code code() { return code; }
}
