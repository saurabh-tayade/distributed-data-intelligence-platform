package platform.ingestion.bronze;

/** A failed request can have an uncertain remote outcome; retry the same immutable input. */
public final class BronzeStorageException extends Exception {
    private static final long serialVersionUID = 1L;
    public enum Kind { VALIDATION, CONFLICT, PERSISTENCE }
    public enum Stage { VALIDATE, SERIALIZE, CHECK_EXISTING, PAYLOAD, METADATA }
    private final Kind kind;
    private final Stage stage;

    BronzeStorageException(Kind kind, Stage stage, Throwable cause) {
        super("Bronze write failed: " + kind + " at " + stage, cause);
        this.kind = kind;
        this.stage = stage;
    }

    public Kind kind() { return kind; }
    public Stage stage() { return stage; }
}
