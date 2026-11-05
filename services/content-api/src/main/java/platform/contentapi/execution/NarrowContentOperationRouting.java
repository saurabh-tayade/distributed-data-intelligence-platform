package platform.contentapi.execution;

import java.nio.file.Path;
import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/**
 * Inactive correspondence catalog for the original fixed plan, not an operation allocator.
 * Ledger coordinates, historical SQL attribution and native checkpoint provenance are distinct.
 * No reservation, command creation, child launch or native input verification occurs here.
 */
final class NarrowContentOperationRouting {
    enum Family { FETCH, BRONZE, SILVER, GOLD, CONTENT }
    enum History { STAGE, AUXILIARY, BEFORE_STAGE, AFTER_SUCCESS }
    enum Operation {
        FETCH_SOURCE("FETCH_SOURCE",0,Family.FETCH,null,History.STAGE,"FETCH_SOURCE"),
        STORE_BRONZE("STORE_BRONZE",0,Family.BRONZE,"PUBLISH",History.STAGE,"STORE_BRONZE"),
        BRONZE_BEFORE_SILVER("INSPECT_BRONZE",0,Family.BRONZE,"INSPECT",History.BEFORE_STAGE,"SILVER"),
        PUBLISH_SILVER("SILVER",0,Family.SILVER,"PUBLISH",History.STAGE,"SILVER"),
        VERIFY_SILVER("VERIFY_SILVER",0,Family.SILVER,"VERIFY",History.AUXILIARY,"SILVER"),
        RECHECK_SILVER("RECHECK_SILVER",0,Family.SILVER,"RECHECK",History.AUXILIARY,"SILVER"),
        BRONZE_BEFORE_GOLD_PREPARATION("INSPECT_BRONZE",1,Family.BRONZE,"INSPECT",History.BEFORE_STAGE,"PREPARE_GOLD"),
        PREPARE_GOLD("PREPARE_GOLD",0,Family.GOLD,"PREPARE",History.STAGE,"PREPARE_GOLD"),
        BRONZE_BEFORE_GOLD_PUBLICATION("INSPECT_BRONZE",2,Family.BRONZE,"INSPECT",History.BEFORE_STAGE,"PUBLISH_GOLD"),
        PUBLISH_GOLD("PUBLISH_GOLD",0,Family.GOLD,"PUBLISH",History.STAGE,"PUBLISH_GOLD"),
        VERIFY_GOLD("VERIFY_RESULT",0,Family.GOLD,"VERIFY",History.AUXILIARY,"PUBLISH_GOLD"),
        RECHECK_GOLD("CHECK_INPUTS",0,Family.GOLD,"RECHECK",History.AUXILIARY,"PUBLISH_GOLD"),
        BRONZE_BEFORE_CONTENT_PREPARATION("INSPECT_BRONZE",3,Family.BRONZE,"INSPECT",History.BEFORE_STAGE,"CHECK_INPUTS"),
        PREPARE_CONTENT("CHECK_INPUTS",1,Family.CONTENT,"PREPARE",History.STAGE,"CHECK_INPUTS"),
        PUBLISH_CONTENT("PUBLISH_CONTENT",0,Family.CONTENT,"PUBLISH",History.STAGE,"PUBLISH_CONTENT"),
        VERIFY_CONTENT("VERIFY_RESULT",1,Family.CONTENT,"VERIFY",History.STAGE,"VERIFY_RESULT"),
        RECHECK_CONTENT("CHECK_INPUTS",2,Family.CONTENT,"RECHECK",History.AUXILIARY,"VERIFY_RESULT"),
        BRONZE_BEFORE_CONTENT_INSPECTION("INSPECT_BRONZE",4,Family.BRONZE,"INSPECT",History.AFTER_SUCCESS,"VERIFY_RESULT"),
        INSPECT_CONTENT("CHECK_INPUTS",3,Family.CONTENT,"VERIFY",History.AFTER_SUCCESS,"VERIFY_RESULT");

        final String ledgerStage,mode,historyStage;final int ledgerOrdinal;
        final Family family;final History history;
        Operation(String stage,int ordinal,Family family,String mode,History history,String historyStage){
            ledgerStage=stage;ledgerOrdinal=ordinal;this.family=family;this.mode=mode;this.history=history;this.historyStage=historyStage;
        }
        /** Historical attribution only; auxiliary/inspection entries do not create a SQL row. */
        int historyOrdinal(){return 0;}
        int sourceOrdinal(){return 0;}
        Operation nativeCheckpointOrigin(){return switch(this){
            case VERIFY_SILVER,RECHECK_SILVER -> PUBLISH_SILVER;
            case VERIFY_GOLD,RECHECK_GOLD -> PUBLISH_GOLD;
            case RECHECK_CONTENT -> VERIFY_CONTENT;
            default -> this;
        };}
        Path directory(ContentOperationPlan plan){return plan.directory(ledgerStage,ledgerOrdinal);}
    }

    /** Read-only output: even a matching header is neither a verified command nor launch authority. */
    record Correspondence(Operation operation,String commandSha256,UUID nativeCheckpoint,
                          UUID directoryCheckpoint,int commandProtocolVersion,int runtimeLedgerVersion) {
        boolean nativeLaunchCompatible(){return false;}
        boolean nativeInputsVerified(){return false;}
        @Override public String toString(){return "NarrowContentCommandCorrespondence[not-authority]";}
    }

    static Operation operation(String stage,int ordinal){
        for(var operation:Operation.values())if(operation.ledgerStage.equals(stage) && operation.ledgerOrdinal==ordinal)return operation;
        throw new PreflightFailure(INPUT_INVALID);
    }

    /**
     * Inspect historical native command identity only. Exact checkpoint observations must come
     * independently from the original invocation facts, never the command being inspected.
     * In particular, an independent Content inspection command UUID is not its directory UUID.
     * No checkpoint is allocated here; no caller-supplied observation becomes durable authority.
     */
    static Correspondence inspectCommand(Operation operation,ContentOperationPlan plan,UUID run,UUID attempt,
            String source,long epoch,Map<Operation,UUID> nativeCheckpoints,
            NarrowContentRuntimeQualification.Snapshot runtime,byte[] command){
        try{
            need(operation!=null && plan!=null && run!=null && attempt!=null && source!=null && epoch>0
                    && nativeCheckpoints!=null && runtime!=null,INPUT_INVALID);
            need(plan.root().endsWith(Path.of("runs",run.toString(),"attempts",attempt.toString())),INPUT_INVALID);
            // Reconstruct the exact original grammar, not a recovery pool with overlapping coordinates.
            var expectedPlan=ContentOperationPlan.original(plan.root(),run,attempt,source);
            need(expectedPlan.reservations().equals(plan.reservations()),INPUT_INVALID);
            need(runtime.run().equals(run) && runtime.attempt().equals(attempt),RUNTIME_MISMATCH);
            var frozen=runtime.frozenSelection();
            need(frozen.get("sources").get(0).get("sourceId").stringValue().equals(source),RUNTIME_MISMATCH);
            int maximum=plan.reservations().get(operation.directory(plan)).get("command.json");
            need(command!=null && command.length>0,INPUT_INVALID);
            need(command.length<=maximum,RESOURCE_LIMIT);byte[] snapshot=command.clone();
            var c=PreflightFiles.canonical(snapshot,maximum);
            SourceJson.fields(c,fields(operation.family).split(" "));
            need(integer(c.get("protocolVersion"),1) && integer(c.get("ownerEpoch"),epoch)
                    && run.toString().equals(c.get("runId").stringValue()) && attempt.toString().equals(c.get("attemptId").stringValue()),INPUT_INVALID);
            UUID checkpoint=nativeCheckpoints.get(operation.nativeCheckpointOrigin());need(checkpoint!=null,INPUT_INVALID);
            need(checkpoint.toString().equals(c.get("checkpointId").stringValue()),INPUT_CHANGED);
            need(runtime.digest(operation.family.name()).equals(c.get("runtimeSha256").stringValue()),RUNTIME_MISMATCH);
            if(operation.mode!=null)need(operation.mode.equals(c.get("mode").stringValue()),INPUT_INVALID);
            if(operation.family==Family.FETCH || operation.family==Family.BRONZE){
                need(operation.ledgerStage.equals(c.get("stage").stringValue()) && integer(c.get("itemOrdinal"),0),INPUT_INVALID);
            }
            if(operation.family==Family.FETCH)fetch(c,frozen,runtime.configuration("FETCH"));
            if(operation.family==Family.SILVER || operation.family==Family.GOLD || operation.family==Family.CONTENT)
                python(c,runtime,operation.family);
            if(operation.family==Family.GOLD || operation.family==Family.CONTENT){
                boolean prepare=operation.mode.equals("PREPARE"),inspection=operation==Operation.INSPECT_CONTENT;
                need(prepare?c.get("prepared").isNull():c.get("prepared").isObject(),INPUT_INVALID);
                need(operation.mode.equals("RECHECK") || inspection?c.get("verified").isObject():c.get("verified").isNull(),INPUT_INVALID);
            }
            UUID directory=UUID.fromString(operation.directory(plan).getFileName().toString());
            return new Correspondence(operation,SourceJson.sha(snapshot),checkpoint,directory,1,runtime.ledgerVersion());
        }catch(PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new PreflightFailure(INPUT_INVALID);}
    }

    private static boolean integer(JsonNode value,long expected){return value!=null && value.isIntegralNumber() && value.canConvertToLong() && value.longValue()==expected;}
    private static void fetch(JsonNode c,JsonNode frozen,JsonNode runtime){
        var selected=frozen.get("sources").get(0);
        for(String key:List.of("sourceId","revision","endpointSha256"))need(c.get(key).equals(selected.get(key)),INPUT_CHANGED);
        need(c.get("capturedConfigSha256").equals(selected.get("configurationSha256")) && c.get("feedUri").equals(selected.get("endpoint").get("uri")),INPUT_CHANGED);
        need(c.get("javaRuntime").equals(runtime.get("manifest").get("javaRuntime")) && c.get("testEndpoint").equals(runtime.get("testEndpoint"))
                && c.get("httpTimeoutMillis").equals(runtime.get("httpTimeoutMillis")),RUNTIME_MISMATCH);
    }
    private static void python(JsonNode c,NarrowContentRuntimeQualification.Snapshot runtime,Family family){
        var silver=runtime.configuration("SILVER");var frozen=runtime.frozenSelection();
        var scratch=binding(frozen,silver.get("tempBindingId").stringValue());
        var expected=SourceJson.object().put("javaHome",silver.get("javaHome").stringValue()).put("scratchRoot",scratch.get("root").stringValue())
                .put("scratchDevice",scratch.get("rootDevice").stringValue()).put("scratchInode",scratch.get("rootInode").stringValue());
        need(c.get("runtime").equals(expected),RUNTIME_MISMATCH);
        switch(family){
            case SILVER -> need(c.get("binding").equals(rootBinding(frozen,silver.get("outputBindingId").stringValue())),RUNTIME_MISMATCH);
            case GOLD -> {
                need(c.get("silverBindingId").equals(silver.get("outputBindingId")) && c.get("silver").equals(rootBinding(frozen,silver.get("outputBindingId").stringValue()))
                        && c.get("gold").equals(rootBinding(frozen,runtime.configuration("GOLD").get("goldBindingId").stringValue())),RUNTIME_MISMATCH);
            }
            case CONTENT -> {
                var content=runtime.configuration("CONTENT");
                need(c.get("referenceBindingId").equals(content.get("referenceBindingId"))
                        && c.get("referenceBinding").equals(rootBinding(frozen,content.get("referenceBindingId").stringValue()))
                        && c.get("content").equals(rootBinding(frozen,content.get("contentBindingId").stringValue()))
                        && c.get("reference").equals(frozen.get("inputs").get(0)),RUNTIME_MISMATCH);
            }
            default -> throw new PreflightFailure(INPUT_INVALID);
        }
    }
    private static JsonNode binding(JsonNode frozen,String id){
        for(var binding:frozen.get("storageBindings"))if(id.equals(binding.get("bindingId").stringValue()))return binding.get("configuration");
        throw new PreflightFailure(RUNTIME_MISMATCH);
    }
    private static JsonNode rootBinding(JsonNode frozen,String id){
        var source=binding(frozen,id);var n=SourceJson.object();
        for(String key:List.of("root","rootDevice","rootInode"))n.set(key,source.get(key));return n;
    }
    static String fields(Family family){
        String common="protocolVersion runId attemptId ownerEpoch checkpointId runtimeSha256";
        return common+switch(family){
            case FETCH -> " stage itemOrdinal sourceId revision capturedConfigSha256 endpointSha256 feedUri javaRuntime testEndpoint httpTimeoutMillis";
            case BRONZE -> " mode stage itemOrdinal journal storage";
            case SILVER -> " mode receipt artifact binding bronze runtime";
            case GOLD -> " mode inputSha256 silverBindingId inputs silver gold runtime prepared verified";
            case CONTENT -> " mode inputSha256 goldProof reference referenceBindingId referenceBinding content runtime prepared verified";
        };
    }
    private NarrowContentOperationRouting(){}
}
