package platform.contentapi.importing;

/** Only the fixed native verifier in this package can construct qualification evidence. */
public final class QualifiedArtifact {
    private final java.util.UUID operation;
    private final ImportCatalog.Entry selection;
    private final byte[] evidence;
    QualifiedArtifact(java.util.UUID operation,ImportCatalog.Entry selection,byte[] evidence) {this.operation=operation;this.selection=selection;this.evidence=evidence.clone();}
    public java.util.UUID operation(){return operation;}
    public ImportCatalog.Entry selection(){return selection;}
    public byte[] evidence(){return evidence.clone();}
    @Override public String toString(){return "QualifiedArtifact[redacted]";}
}
