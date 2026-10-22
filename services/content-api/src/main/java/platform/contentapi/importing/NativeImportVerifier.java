package platform.contentapi.importing;

import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.importing.ImportFailure.Code.*;

/** Fixed, hash-bound read-only module invocation; never accepts a command from a catalog. */
public final class NativeImportVerifier {
    private static final Set<String> CODES=Set.of("ARGUMENT_INVALID","PATH_UNSAFE","UNSUPPORTED_VERSION","INTAKE_NOT_APPROVED",
        "INPUT_UNAVAILABLE","INPUT_CHANGED","BATCH_UNAVAILABLE","BATCH_INCOMPLETE","BATCH_INVALID","BATCH_CONFLICT","CSV_INVALID","VOCABULARY_INVALID",
        "SNAPSHOT_UNAVAILABLE","SNAPSHOT_INVALID","SNAPSHOT_CHANGED","SNAPSHOT_INCOMPLETE","IDENTITY_MISMATCH","DEPENDENCY_UNAVAILABLE","DEPENDENCY_INVALID",
        "DEPENDENCY_CHANGED","PARQUET_INVALID","PROVENANCE_UNSUPPORTED","RESOURCE_LIMIT","EVIDENCE_UNAVAILABLE","EVIDENCE_INVALID","EVIDENCE_CHANGED","RUNTIME_INCOMPATIBLE","ADAPTER_INVALID","SILVER_INVALID","SILVER_UNAVAILABLE","BRONZE_INVALID","BRONZE_UNAVAILABLE","BRONZE_CHANGED","SPARK_CLEANUP_UNCERTAIN","SCHEMA_MISMATCH","COUNT_MISMATCH","PROVENANCE_MISMATCH","INVALID_ROW","PUBLICATION_CONFLICT","INGESTION_CONFLICT","REPLAY_CONFLICT","ARTICLE_IDENTITY_CONFLICT","ACCEPTED_REJECTED_CONFLICT","PARQUET_VALIDATION_FAILED","INVALID_MARKER","MARKER_PATH_MISMATCH","INVALID_PUBLICATION_PATH","MISSING_DATASET","INVALID_DATASET","INPUT_READ_FAILED");
    private final NativeImportProcess process;
    public NativeImportVerifier(NativeImportProcess process){this.process=process;}
    public QualifiedArtifact verify(UUID operation,ImportCatalog.Entry entry,ImportBindings.Storage storage,ImportBindings.RuntimeProfile runtime) {
        JsonNode evidence=invoke(operation,entry,storage,runtime,null);
        var persisted=SourceJson.object().put("qualificationVersion",1);
        persisted.set("selection",entry.json());persisted.set("verification",evidence);
        byte[] bytes=SourceJson.canonical(persisted);ImportFailure.require(bytes.length<=ImportJson.PROTOCOL_LIMIT,NATIVE_LIMIT);
        return new QualifiedArtifact(operation,entry,bytes);
    }
    public void recheck(UUID operation,QualifiedArtifact artifact,ImportBindings.Storage storage,ImportBindings.RuntimeProfile runtime) {
        var n=ImportJson.parse(artifact.evidence(),ImportJson.PROTOCOL_LIMIT);
        invoke(operation,artifact.selection(),storage,runtime,n.get("verification").get("capture"));
    }
    private JsonNode invoke(UUID operation,ImportCatalog.Entry entry,ImportBindings.Storage storage,ImportBindings.RuntimeProfile runtime,JsonNode capture) {
        runtime.check();storage.check();
        if(entry.silver()){ImportFailure.require(runtime.spark()!=null && storage.bronze()!=null,RUNTIME_INVALID);runtime.spark().disjoint(storage.root());}
        int version=entry.silver()?2:1;String mode=capture==null?"VERIFY":"RECHECK";
        var request=SourceJson.object().put("protocolVersion",version).put("operationId",operation.toString()).put("selectionSha256",entry.digest()).put("mode",mode);
        request.set("binding",storage.json());request.set("artifact",entry.json());request.set("expectedCapture",capture);
        if(entry.silver()){request.set("bronze",storage.bronze().configuration());request.set("runtime",runtime.spark().json());}
        var command=List.of(runtime.python().toString(),"-I","-B","-X","utf8",runtime.processing().resolve("artifact_import_verifier.py").toString());
        var result=entry.silver() && capture==null?process.runSpark(command,runtime.spark().scratch().root(),runtime.timeoutSeconds(),SourceJson.canonical(request),runtime.spark().javaHome()):process.run(command,runtime.processing(),Math.min(runtime.timeoutSeconds(),120),SourceJson.canonical(request));
        try {
            var n=ImportJson.parse(result.stdout(),ImportJson.PROTOCOL_LIMIT);
            ImportJson.fields(n,"protocolVersion","operationId","selectionSha256","mode","status","code","classification","evidence");
            ImportJson.integer(n.get("protocolVersion"),version,version);
            ImportFailure.require(ImportJson.uuid(n.get("operationId")).equals(operation) && ImportJson.hash(n.get("selectionSha256")).equals(entry.digest()) && ImportJson.text(n.get("mode")).equals(mode),RESULT_INVALID);
            String status=ImportJson.text(n.get("status"));
            if(status.equals("FAILED")) {
                String code=ImportJson.text(n.get("code"));ImportFailure.require(result.exitCode()==1 && CODES.contains(code) && n.get("evidence").isNull(),RESULT_INVALID);
                String classification=code.equals("SPARK_CLEANUP_UNCERTAIN")?"UNRESOLVED":code.endsWith("_CHANGED")?"CHANGED":(code.endsWith("_UNAVAILABLE") || code.equals("INPUT_READ_FAILED"))?"UNAVAILABLE":code.equals("RESOURCE_LIMIT")?"LIMIT":Set.of("UNSUPPORTED_VERSION","RUNTIME_INCOMPATIBLE").contains(code)?"UNSUPPORTED":"INVALID";
                ImportFailure.require(ImportJson.text(n.get("classification")).equals(classification),RESULT_INVALID);
                throw new ImportFailure(switch(classification){case "UNRESOLVED"->CLEANUP_UNCERTAIN;case "CHANGED"->NATIVE_CHANGED;case "UNAVAILABLE"->NATIVE_UNAVAILABLE;case "LIMIT"->NATIVE_LIMIT;case "UNSUPPORTED"->NATIVE_UNSUPPORTED;default->NATIVE_INVALID;},!classification.equals("UNRESOLVED"),code);
            }
            ImportFailure.require(result.exitCode()==0 && status.equals(capture==null?"VERIFIED":"UNCHANGED") && n.get("code").isNull() && n.get("classification").isNull(),RESULT_INVALID);
            var evidence=n.get("evidence");
            if(capture!=null)ImportFailure.require(evidence.isNull(),RESULT_INVALID);
            else {
                ImportEvidence.validate(entry,evidence);
            }
            runtime.check();storage.check();return evidence;
        } catch(ImportFailure f) {if(f.code()==INVALID_CATALOG || f.code()==RESULT_INVALID)throw new ImportFailure(RESULT_INVALID,!(entry.silver() && capture==null));throw f;}
        catch(RuntimeException ex){throw new ImportFailure(RESULT_INVALID,!(entry.silver() && capture==null));}
    }
}
