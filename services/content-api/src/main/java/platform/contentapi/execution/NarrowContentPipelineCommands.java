package platform.contentapi.execution;

import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.NarrowContentOperationRouting.Operation;

/** Pure strict protocol-v2 builders. All inputs originate in immutable SQL or this live chain's proofs. */
final class NarrowContentPipelineCommands {
    private static final JsonNode NIL=SourceJson.object().putNull("value").get("value");
    static ObjectNode command(Operation op,NarrowContentEvidenceContext.Expected ready,NarrowContentRuntimeQualification.Snapshot runtime,
            long epoch,JsonNode receipt,JsonNode silverInput,JsonNode goldProof,JsonNode prepared,JsonNode verified,JsonNode silverCapture){
        var context=new NarrowContentEvidenceContext(ready);
        var n=SourceJson.object().put("protocolVersion",2).put("mode",op.mode).put("runId",ready.run().toString())
                .put("attemptId",ready.attempt().toString()).put("ownerEpoch",epoch)
                .put("checkpointId",context.originalDirectory(op.nativeCheckpointOrigin().ledgerStage,op.nativeCheckpointOrigin().ledgerOrdinal).getFileName().toString())
                .put("runtimeSha256",runtime.digest(op.family.name()));
        var silver=runtime.configuration("SILVER");var content=runtime.configuration("CONTENT");var gold=runtime.configuration("GOLD");
        if(op.family==NarrowContentOperationRouting.Family.BRONZE){n.set("receipt",receipt);n.set("bronze",binding(runtime,"BRONZE").get("configuration"));}
        else if(op.family==NarrowContentOperationRouting.Family.SILVER){
            n.set("receipt",receipt);n.set("artifact",silverSelection(receipt,silver.get("outputBindingId").stringValue()));
            n.set("binding",root(binding(runtime,"SILVER")));n.set("bronze",binding(runtime,"BRONZE").get("configuration"));
            n.set("runtime",nativeRuntime(runtime));n.set("expectedCapture",copy(silverCapture));
        }else{
            n.set("runtime",nativeRuntime(runtime));n.set("prepared",copy(prepared));n.set("verified",copy(verified));
            n.putObject("projectionContext").put("projectionContextVersion",1).put("policy",ready.policy()).put("kind",op.family.name())
                    .put("outputBindingId",(op.family==NarrowContentOperationRouting.Family.GOLD?gold:content).get(op.family.name().toLowerCase(Locale.ROOT)+"BindingId").stringValue());
            if(op.family==NarrowContentOperationRouting.Family.GOLD){
                need(silverInput!=null,INPUT_INVALID);var inputs=n.putArray("inputs");inputs.add(silverInput);
                n.put("inputSha256",hash(inputs)).put("silverBindingId",silver.get("outputBindingId").stringValue());
                n.set("silver",root(binding(runtime,"SILVER")));n.set("gold",root(binding(runtime,"GOLD")));
            }else{
                need(goldProof!=null,INPUT_INVALID);var reference=RssContentInputs.reference(runtime.frozenSelection());
                n.set("goldProof",goldProof);n.set("reference",reference);n.put("referenceBindingId",content.get("referenceBindingId").stringValue());
                n.set("referenceBinding",root(binding(runtime,"REFERENCE")));n.set("content",root(binding(runtime,"CONTENT")));
                n.put("inputSha256",hash(RssContentInputs.dependencies(goldProof,reference)));
            }
        }
        byte[] raw=SourceJson.canonical(n);need(raw.length<=ContentEvidenceBudget.slots(op.ledgerStage).get("command.json"),RESOURCE_LIMIT);
        PreflightFiles.canonical(raw,ContentEvidenceBudget.slots(op.ledgerStage).get("command.json"));return n;
    }
    static ObjectNode expectations(Operation op,NarrowContentRuntimeQualification.Snapshot runtime,JsonNode command,String credentialsSha){
        var expected=SourceJson.object();
        if(op.family==NarrowContentOperationRouting.Family.BRONZE){expected.put("receiptSha256",hash(command.get("receipt"))).put("bronzeSha256",hash(command.get("bronze"))).put("credentialsSha256",credentialsSha);}
        else if(op.family==NarrowContentOperationRouting.Family.SILVER){
            for(String key:List.of("receipt","artifact","binding","bronze","runtime","expectedCapture"))expected.put(key+"Sha256",hash(command.get(key)));
            expected.put("credentialsSha256",credentialsSha);
        }else{
            for(String family:List.of("CONTENT","GOLD","SILVER")){
                var record=expected.putObject(family.toLowerCase(Locale.ROOT)+"Runtime").put("id",runtime.id(NarrowContentRuntimeQualification.Family.valueOf(family))).put("sha256",runtime.digest(family));
                record.set("configuration",runtime.configuration(family));
            }
            var bindings=expected.putObject("bindings");
            for(String role:List.of("GOLD","CONTENT","REFERENCE","SILVER","TEMP")){
                var original=binding(runtime,role);var b=bindings.putObject(role).put("bindingId",original.get("bindingId").stringValue()).put("role",role);b.set("configuration",root(original));
            }
            var inputs=op.family==NarrowContentOperationRouting.Family.GOLD?command.get("inputs"):command.get("goldProof").get("command").get("inputs");
            expected.put("silverInputSha256",hash(inputs.get(0)));
            if(op.family==NarrowContentOperationRouting.Family.GOLD)expected.putNull("referenceSha256").putNull("goldProofSha256");
            else expected.put("referenceSha256",hash(command.get("reference"))).put("goldProofSha256",hash(command.get("goldProof")));
        }
        return expected;
    }
    static ObjectNode bootstrap(Operation op,NarrowContentEvidenceContext.Expected ready,WorkerRepository.Owner owner,UUID operation,
            JsonNode command,NarrowContentRuntimeQualification.Snapshot runtime,String credentialsSha){
        UUID slot=UUID.fromString(new NarrowContentEvidenceContext(ready).originalDirectory(op.ledgerStage,op.ledgerOrdinal).getFileName().toString());
        var n=NarrowContentOperationJournal.bootstrap(ready,owner,operation,slot,hash(command));
        n.put("bootstrapVersion",2).put("mode","STRICT_CONTENT").put("stage",op.ledgerStage).put("ordinal",op.ledgerOrdinal)
                .put("family",op.family.name()).put("nativeMode",op.mode).put("nativeRuntimeSha256",runtime.digest(op.family.name()))
                .put("strictRuntimeSha256",StrictContentPipelineRuntime.protocolDigest());
        n.set("expectations",expectations(op,runtime,command,credentialsSha));
        need(SourceJson.canonical(n).length<65500,RESOURCE_LIMIT);return n;
    }
    /** Size-only models, never retained commands or launch authority. Dynamic native proofs
     * enter bootstrap packets only as fixed-width hashes; all remaining bytes are known now. */
    static void qualifyBootstrapCapacity(NarrowContentEvidenceContext.Expected ready,WorkerRepository.Owner owner,
            NarrowContentRuntimeQualification.Snapshot runtime){
        var shape=SourceJson.object();
        for(String key:List.of("receipt","artifact","binding","bronze","runtime","expectedCapture","reference"))shape.putNull(key);
        shape.putArray("inputs").addNull();shape.putObject("goldProof").putObject("command").putArray("inputs").addNull();
        for(var operation:NarrowContentPipeline.core().subList(2,17)){
            var packet=bootstrap(operation,ready,owner,new UUID(0,0),shape,runtime,"0".repeat(64));
            packet.put("childPid",Long.toString(Long.MAX_VALUE));
            need(SourceJson.canonical(packet).length<65536,RESOURCE_LIMIT);
        }
    }
    static JsonNode silverSelection(JsonNode r,String binding){
        var e=r.get("expected");String key=e.get("metadataKey").stringValue();need(key.startsWith("bronze/rss/") && key.endsWith("metadata.json"),INPUT_INVALID);
        String address="v1/"+SourceJson.sha(r.get("bucket").stringValue().getBytes(java.nio.charset.StandardCharsets.UTF_8))+"/"+key.substring("bronze/rss/".length(),key.length()-"metadata.json".length())+"complete.json";
        var n=SourceJson.object().put("kind","SILVER").put("productId","content-intelligence").put("storageBindingId",binding).put("relativeAddress",address).put("contractVersion",1).put("processorRevision",1);
        var identity=n.putObject("identity");for(String keyName:List.of("sourceId","ingestionId"))identity.set(keyName,r.get(keyName));
        identity.set("bronzeStorageBindingId",r.get("storageBindingId"));identity.set("bronzeBucket",r.get("bucket"));identity.set("bronzeMetadataKey",e.get("metadataKey"));identity.set("bronzePayloadKey",e.get("payloadKey"));return n;
    }
    static JsonNode nativeRuntime(NarrowContentRuntimeQualification.Snapshot r){var scratch=root(binding(r,"TEMP"));return SourceJson.object()
            .put("javaHome",r.configuration("SILVER").get("javaHome").stringValue()).put("scratchRoot",scratch.get("root").stringValue())
            .put("scratchDevice",scratch.get("rootDevice").stringValue()).put("scratchInode",scratch.get("rootInode").stringValue());}
    static JsonNode binding(NarrowContentRuntimeQualification.Snapshot r,String role){
        JsonNode result=null;for(var b:r.frozenSelection().get("storageBindings"))if(b.get("role").stringValue().equals(role)){need(result==null,RUNTIME_MISMATCH);result=b;}
        need(result!=null,RUNTIME_MISMATCH);return result;
    }
    static JsonNode root(JsonNode binding){var n=SourceJson.object();for(String key:List.of("root","rootDevice","rootInode"))n.set(key,binding.get("configuration").get(key));return n;}
    static String hash(JsonNode n){return SourceJson.sha(SourceJson.canonical(n));}
    static JsonNode copy(JsonNode n){return n==null?NIL:n.deepCopy();}
    private NarrowContentPipelineCommands(){}
}
