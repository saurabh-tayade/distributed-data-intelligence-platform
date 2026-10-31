package platform.contentapi.management;

import java.util.Set;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.management.SourceFailure.Code.INTERNAL_FAILURE;

/** Public classifications only. Never copy a free-form private diagnostic or audit detail into a DTO. */
final class RunInspectionValues {
    private RunInspectionValues() { }
    static final Set<String> STATES=Set.of("PAUSED","QUEUED","RUNNING","CANCEL_REQUESTED","SUCCEEDED","FAILED","CANCELLED","RECOVERY_REQUIRED");
    private static final Set<String> CODES=Set.of("ACCEPTED","CLAIMED","STARTED","VERIFIED","SUCCEEDED","FAILED","CANCELLED","INTERRUPTED","UNKNOWN","UPDATED","UNCHANGED",
            "INPUT_UNAVAILABLE","INPUT_INVALID","INPUT_CHANGED","PUBLICATION_UNAVAILABLE","PUBLICATION_INVALID","PUBLICATION_CHANGED","INCOMPLETE_OUTPUT","FETCH_FAILED","FETCH_EVIDENCE_LOST","BRONZE_CONFLICT","BRONZE_ABSENT","BRONZE_PAYLOAD_ONLY",
            "STAGE_TIMEOUT","STAGE_OUTPUT_LIMIT","PROCESS_START_FAILED","TERMINATION_UNCONFIRMED","RUNTIME_MISMATCH","RESOURCE_LIMIT","SOURCE_DISABLED","RETRY_NOT_SAFE","TOO_LATE","RECOVERY_REQUIRED","RECOVERY_HANDOFF","RECOVERY_CLOSED","STATE_CONFLICT",
            "RECOVERED_PUBLICATION","PUBLICATION_ABSENT","DEPENDENCY_CHANGED","DEPENDENCY_INVALID","RECONCILED_SUCCEEDED","RECONCILED_FAILED","RECONCILED_PAUSED","RECONCILED_CANCELLED","RECOVERED_CHECKPOINT",
            "VERIFIED_PUBLICATION","OUTPUT_ABSENT","OUTPUT_INCOMPLETE","OUTPUT_CHANGED","OUTPUT_INVALID","OUTPUT_UNAVAILABLE","DEPENDENCY_UNAVAILABLE","RUNTIME_INCOMPATIBLE",
            "EVIDENCE_INVALID","EVIDENCE_UNAVAILABLE");
    private static final Set<String> ACTIONS=Set.of("RUN_SUBMITTED","RUN_CLAIMED","RUN_STARTED","RUN_CANCEL_REQUESTED","RUN_CANCELLED","RUN_FAILED","RUN_SUCCEEDED","RUN_RECOVERY_REQUIRED","RUN_REQUEUED",
            "ATTEMPT_CLAIMED","ATTEMPT_STARTED","ATTEMPT_FINISHED","ATTEMPT_INTERRUPTED","STAGE_INTENT","STAGE_LAUNCH_AUTHORIZED","STAGE_STARTED","STAGE_VERIFIED","STAGE_FAILED","STAGE_UNKNOWN",
            "CONTROL_REQUESTED","CONTROL_APPLIED","CONTROL_TOO_LATE","CONTROL_REJECTED","RECOVERY_HANDOFF","RECOVERY_CLOSED","SOURCE_DISABLED_AT_DISPATCH",
            "RECOVERY_ATTEMPT_RUNNING","RECOVERY_STAGE_INTENT","RECOVERY_LAUNCH_AUTHORIZED","RECOVERY_CHILD_STARTED","RECOVERY_STAGE_OBSERVED",
            "RECOVERY_ATTEMPT_FINISHED","RECOVERY_RUN_FINISHED","RECOVERY_CONTROL_APPLIED","RECOVERY_SLOT_RELEASED","RECOVERY_FINALIZED");
    private static final Set<String> AUDIT_STATES=Set.of("PAUSED","QUEUED","RUNNING","CANCEL_REQUESTED","SUCCEEDED","FAILED","CANCELLED","RECOVERY_REQUIRED","CLAIMED","INTERRUPTED","INTENT","VERIFIED","UNKNOWN","REQUESTED","APPLIED","TOO_LATE","REJECTED","RECOVERY_ONLY","READY");
    private static final Set<String> COUNTS=Set.of("accepted","rejected","articles","occurrences","events","windows","rows","files","bytes");
    static void check(boolean valid){if(!valid)throw new SourceFailure(INTERNAL_FAILURE);}
    static String code(String s){check(s==null || CODES.contains(s));return s;}
    static String action(String s){check(ACTIONS.contains(s));return s;}
    static String state(String s){check(s==null || AUDIT_STATES.contains(s));return s;}
    static JsonNode counts(byte[] bytes) {
        if(bytes==null)return null;
        try {
            JsonNode n=SourceJson.parse(bytes);check(n.isObject() && n.size()<=COUNTS.size());
            for(String k:n.propertyNames()){check(COUNTS.contains(k));RunPageCursor.number(n.get(k));}
            return n;
        } catch(RuntimeException ex){throw new SourceFailure(INTERNAL_FAILURE);}
    }
}
