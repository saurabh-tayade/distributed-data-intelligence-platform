package platform.contentapi.importing;

import java.util.*;
import platform.contentapi.execution.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;

/** One explicit catalog. Short SQL transactions surround read-only external qualification. */
public final class OperatorImportService {
    private final ImportBindings bindings;
    private final ImportOwnershipRepository ownership;
    private final CoordinatorRepository coordinator;
    private final NativeImportVerifier verifier;
    private final UUID installation;
    public OperatorImportService(ImportBindings bindings,ImportOwnershipRepository ownership,CoordinatorRepository coordinator,NativeImportVerifier verifier,UUID installation) {
        this.bindings=bindings;this.ownership=ownership;this.coordinator=coordinator;this.verifier=verifier;this.installation=installation;
    }
    public JsonNode inspect(ImportCatalog catalog,UUID id) {
        var found=ownership.lookup(id,catalog.requestBytes());
        if(found.isEmpty())return summary(id,"NOT_FOUND");
        var op=found.get();var n=summary(id,op.state()==ImportOwnershipRepository.State.ACTIVE?"RECOVERY_REQUIRED":"HISTORICAL_RESULT");
        n.put("state",op.state().name()).put("revalidated",false);
        if(op.resultBytes()!=null)n.set("result",ImportJson.parse(op.resultBytes(),65536));return n;
    }
    public JsonNode execute(ImportCatalog catalog) {
        // Reusing a command ID only reconciles its result. A fresh verification requires a fresh ID.
        if(ownership.lookup(catalog.operationId(),catalog.requestBytes()).isPresent())return inspect(catalog,catalog.operationId());
        var runtime=bindings.runtime(catalog.profileId());runtime.check();
        var control=bindings.control(catalog.controlBindingId(),catalog.profileId());
        if(runtime.spark()!=null)runtime.spark().disjoint(control.root());
        try(var guard=InstallationHostGuard.acquire(control)) {
            var token=ownership.reserve(guard,new ImportOwnershipRepository.Reservation(catalog.operationId(),runtime.id(),runtime.digest(),catalog.requestBytes()));
            try {
                var qualified=new ArrayList<QualifiedArtifact>();var storage=new ArrayList<ImportBindings.Storage>();
                for(var entry:catalog.entries()) {
                    var s=bindings.storage(entry,runtime.id());storage.add(s);
                    qualified.add(verifier.verify(catalog.operationId(),entry,s,runtime));
                }
                for(int i=0;i<qualified.size();i++)verifier.recheck(catalog.operationId(),qualified.get(i),storage.get(i),runtime);
                runtime.check();for(var s:storage)s.check();guard.checkHeld(installation);
                return ImportJson.parse(ownership.finishVerified(guard,token,qualified),65536);
            } catch(ImportFailure failure) {
                if(!failure.quiescent())return summary(catalog.operationId(),"RECOVERY_REQUIRED").put("code",failure.code().name());
                guard.checkHeld(installation);
                var failed=summary(catalog.operationId(),"FAILED").put("code",failure.code().name());
                if(failure.nativeCode()!=null)failed.put("nativeCode",failure.nativeCode());
                byte[] result=SourceJson.canonical(failed);
                ownership.finish(guard,token,ImportOwnershipRepository.Outcome.SAFE_FAILURE,result,true);
                return ImportJson.parse(result,65536);
            }
            // SQL/ownership/unknown failure: no compensating release. Re-query or explicitly reconcile.
        }
    }
    public JsonNode recover(ImportCatalog catalog,UUID target,UUID recovery,boolean confirmed) {
        if(ownership.lookup(recovery,catalog.requestBytes()).isPresent())return inspect(catalog,recovery);
        var control=bindings.control(catalog.controlBindingId(),catalog.profileId());
        try(var guard=InstallationHostGuard.acquire(control)) {
            var old=ownership.lookup(target,catalog.requestBytes()).orElseThrow(() -> new ImportOwnershipFailure(ImportOwnershipFailure.Code.IMPORT_NOT_FOUND));
            var slot=coordinator.read();
            var expected=new ImportOwnershipRepository.Token(old.importId(),old.processInstanceId(),old.epoch(),old.rowVersion(),slot.rowVersion());
            var token=ownership.handoff(guard,expected,recovery,confirmed);
            var result=summary(recovery,"RECOVERY_CLOSED").put("predecessorOperationId",target.toString());
            ownership.finish(guard,token,ImportOwnershipRepository.Outcome.RECOVERY_CLOSED,SourceJson.canonical(result),true);
            return result;
        }
    }
    static tools.jackson.databind.node.ObjectNode summary(UUID id,String status) {
        var n=SourceJson.object().put("protocolVersion",1).put("status",status);if(id!=null)n.put("operationId",id.toString());return n;
    }
}
