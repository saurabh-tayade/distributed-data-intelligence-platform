package platform.contentapi.execution;

/** Internal fixed diagnostics. No cause, process details or native exception text. */
final class RecoveryFailure extends RuntimeException {
    enum Code { INVALID_REQUEST, OWNERSHIP_LOST, QUIESCENCE_REQUIRED, PROCESS_UNRESOLVED,
        EVIDENCE_CHANGED, EVIDENCE_UNAVAILABLE, RUNTIME_MISMATCH, CAPACITY_EXCEEDED,
        FRESH_QUALIFICATION_REQUIRED, HANDOFF_UNRESOLVED }
    RecoveryFailure(Code code){super(code.name(),null,false,false);}
    static void need(boolean valid,Code code){if(!valid)throw new RecoveryFailure(code);}
}
