package platform.contentapi.management;

public final class SourceFailure extends RuntimeException {
    public enum Code {
        INVALID_ARGUMENT(400,"Invalid management request.",false),
        LOCAL_BOUNDARY_REJECTED(403,"Local request boundary rejected.",false),
        NOT_FOUND(404,"Management object not found.",false),
        IDEMPOTENCY_CONFLICT(409,"Idempotency key has different content.",false),
        SOURCE_DISABLED(409,"Selected source is disabled.",false),
        COORDINATOR_RECOVERY_REQUIRED(503,"Coordinator is not ready for admission.",true),
        STATE_CONFLICT(409,"Source state conflicts with this command.",false),
        PRECONDITION_FAILED(412,"Source version has changed.",false),
        PRECONDITION_REQUIRED(428,"If-Match is required.",false),
        MANAGEMENT_CAPACITY_EXCEEDED(429,"Management capacity is exhausted.",false),
        MANAGEMENT_UNAVAILABLE(503,"Management database is unavailable.",true),
        INTERNAL_FAILURE(500,"Management request failed.",false);
        public final int status; public final String message; public final boolean retryable;
        Code(int status,String message,boolean retryable) { this.status=status;this.message=message;this.retryable=retryable; }
    }
    public final Code code;
    public SourceFailure(Code code) { super(code.name(),null,false,false);this.code=code; }
    static void require(boolean condition) { if(!condition) throw new SourceFailure(Code.INVALID_ARGUMENT); }
}
