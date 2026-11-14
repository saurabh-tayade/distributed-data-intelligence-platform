package platform.contentapi.execution;

/** Fixed packaged adapter eligibility, not a configurable bean or external dispatch API.
 * Lifecycle startup and run-bound qualification remain independent prerequisites.
 */
interface WorkerExecutionCapability {
    boolean available();
    boolean supports(String pipeline);
    /** Packaged adapter: eligibility is additionally pinned to the registered session profile. */
    WorkerExecutionCapability RELIABILITY_V2=new WorkerExecutionCapability(){
        public boolean available(){return true;}
        public boolean supports(String pipeline){return "RELIABILITY_V2".equals(pipeline);}
    };
    /** Original-only strict Content adapter; lifecycle requires complete public composition-v2. */
    WorkerExecutionCapability CONTENT_NARROW_ORIGINAL=new WorkerExecutionCapability(){
        public boolean available(){return true;}
        public boolean supports(String pipeline){return "CONTENT_RSS_V1".equals(pipeline);}
    };
    WorkerExecutionCapability NONE=new WorkerExecutionCapability(){
        public boolean available(){return false;}
        public boolean supports(String pipeline){return false;}
    };
}
