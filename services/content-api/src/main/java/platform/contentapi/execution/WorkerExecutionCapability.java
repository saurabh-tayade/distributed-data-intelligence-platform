package platform.contentapi.execution;

/** Internal C2 integration seam, not a configurable bean, executable or dispatch API.
 * A real adapter must supply preflight and stage supervision before enabling this boundary.
 */
interface WorkerExecutionCapability {
    boolean available();
    boolean supports(String pipeline);
    WorkerExecutionCapability NONE=new WorkerExecutionCapability(){
        public boolean available(){return false;}
        public boolean supports(String pipeline){return false;}
    };
}
