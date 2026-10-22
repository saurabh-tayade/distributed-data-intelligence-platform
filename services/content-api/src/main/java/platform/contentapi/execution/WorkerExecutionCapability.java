package platform.contentapi.execution;

/** Internal C2 integration seam, not a configurable bean, executable or dispatch API.
 * A real adapter must supply preflight and stage supervision before enabling this boundary.
 */
interface WorkerExecutionCapability {
    boolean available();
    boolean supports(String pipeline);
    /** Packaged adapter: eligibility is additionally pinned to the registered session profile. */
    WorkerExecutionCapability RELIABILITY_V2=new WorkerExecutionCapability(){
        public boolean available(){return true;}
        public boolean supports(String pipeline){return "RELIABILITY_V2".equals(pipeline);}
    };
    WorkerExecutionCapability NONE=new WorkerExecutionCapability(){
        public boolean available(){return false;}
        public boolean supports(String pipeline){return false;}
    };
}
