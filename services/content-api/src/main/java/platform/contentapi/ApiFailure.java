package platform.contentapi;

public final class ApiFailure extends RuntimeException {
    public enum Code {
        INVALID_ARGUMENT(400), PUBLICATION_UNAVAILABLE(503), ANALYTICS_RUNTIME_UNAVAILABLE(503),
        ANALYTICS_CAPACITY_EXCEEDED(503), ANALYTICS_TIMEOUT(504),
        ANALYTICS_EXECUTION_FAILED(500), ANALYTICS_RESPONSE_INVALID(500);

        private final int status;
        Code(int status) { this.status = status; }
        public int status() { return status; }
    }

    private final Code code;
    public ApiFailure(Code code) {
        // Never retain raw subprocess diagnostics, paths or parser exception messages.
        super(code.name());
        this.code = code;
    }
    public Code code() { return code; }
}
