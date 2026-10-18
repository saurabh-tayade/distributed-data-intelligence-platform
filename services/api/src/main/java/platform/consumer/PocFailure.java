package platform.consumer;

/** Fixed diagnostics only. Never forward filesystem, JDBC or publisher details. */
public final class PocFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public PocFailure(String code) {
        super(code);
    }
}
