package platform.contentapi.importing;

/** Fixed operator diagnostics; never attach native/OS/database exception text. */
public final class ImportFailure extends RuntimeException {
    public enum Code { INVALID_CATALOG, BINDING_INVALID, RUNTIME_INVALID, RUNTIME_CHANGED, NATIVE_INVALID,
        NATIVE_UNAVAILABLE, NATIVE_CHANGED, NATIVE_LIMIT, NATIVE_UNSUPPORTED, START_FAILED, TIMEOUT,
        RESULT_INVALID, INTERRUPTED, CLEANUP_UNCERTAIN, IDENTITY_CONFLICT }
    private final Code code;
    private final boolean quiescent;
    private final String nativeCode;
    public ImportFailure(Code code) {this(code,true);}
    public ImportFailure(Code code,boolean quiescent) {this(code,quiescent,null);}
    ImportFailure(Code code,boolean quiescent,String nativeCode) {super(code.name(),null,false,false);this.code=code;this.quiescent=quiescent;this.nativeCode=nativeCode;}
    public String nativeCode(){return nativeCode;}
    public Code code() {return code;}
    public boolean quiescent() {return quiescent;}
    static void require(boolean b,Code code) {if(!b)throw new ImportFailure(code);}
}
