package platform.contentapi.execution;

import java.lang.reflect.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Measurement only. Never loads a runtime, contacts a database, or dispatches a child.
 * Run with -Denvelope.measurement.directory=/absolute/generated/case.
 * fixture.json supplies inputs, reference, bindings (silver/gold/reference/content/scratch), javaHome.
 * envelopes supplies gold-prepared, gold-verification, content-prepared, content-verification JSON.
 * Runtime/identity scaffolding is deliberately synthetic. Native validity belongs to the Python fixture.
 */
class ContentEnvelopeMeasurementTest {
    private static final int SLOT=1024*1024;
    private static final UUID RUN=UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final UUID ATTEMPT=UUID.fromString("00000000-0000-0000-0000-000000000002");
    private static final UUID CHECKPOINT=UUID.fromString("00000000-0000-0000-0000-000000000003");
    private static final String DIGEST="0".repeat(64);
    // Only fixture loading uses the ordinary mapper. Acceptance always uses production SourceJson.
    private static final JsonMapper FIXTURE=JsonMapper.builder().build();

    @Test void byteBoundariesAreIndependentOfStringAndSlotLimits() {
        for(int maximum:new int[]{65536,SLOT,PreflightFiles.LARGE}) {
            byte[] exact=asciiDocument(maximum);
            assertThat(exact).hasSize(maximum);
            assertThatCode(()->SourceJson.parse(exact,maximum)).doesNotThrowAnyException();
            byte[] over=asciiDocument(maximum+1);
            assertThatThrownBy(()->SourceJson.parse(over,maximum)).isInstanceOf(RuntimeException.class);
        }
        // Crossing the fixed reservation is not crossing the legacy parser byte maximum.
        assertThatCode(()->SourceJson.parse(asciiDocument(SLOT+1),PreflightFiles.LARGE)).doesNotThrowAnyException();
        assertThat(ContentEvidenceBudget.slots("VERIFY_RESULT").get("evidence.json")).isEqualTo(SLOT);
    }

    @Test void nestingBoundaryUsesActualReaderAndEncoder() {
        byte[] exact=("[".repeat(16)+"0"+"]".repeat(16)).getBytes(StandardCharsets.UTF_8);
        assertThatCode(()->SourceJson.parse(exact,PreflightFiles.LARGE)).doesNotThrowAnyException();
        byte[] over=("[".repeat(17)+"0"+"]".repeat(17)).getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(()->SourceJson.parse(over,PreflightFiles.LARGE)).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(()->SourceJson.canonical(FIXTURE.readTree(over))).isInstanceOf(RuntimeException.class);
    }

    @Test void evidenceStringBoundaryUsesActualReader() {
        byte[] exact=SourceJson.canonical(SourceJson.object().put("s","x".repeat(131072)));
        assertThatCode(()->SourceJson.parse(exact,PreflightFiles.LARGE)).doesNotThrowAnyException();
        byte[] over=SourceJson.canonical(SourceJson.object().put("s","x".repeat(131073)));
        assertThatThrownBy(()->SourceJson.parse(over,PreflightFiles.LARGE)).isInstanceOf(RuntimeException.class);
        // The small entry point cannot fit a 65,536-character value plus JSON framing; bytes reject first.
        assertThatThrownBy(()->SourceJson.parse(exact)).isInstanceOf(RuntimeException.class);
    }

    @Test void measureGeneratedEnvelopesWithProductionCommandConstructors() throws Exception {
        String property=System.getProperty("envelope.measurement.directory");
        assumeTrue(property!=null && !property.isBlank(),"Optional generated native fixture not supplied");
        Path directory=Path.of(property).toAbsolutePath();
        JsonNode f=FIXTURE.readTree(Files.readAllBytes(directory.resolve("fixture.json")));
        Path output=Files.createDirectory(directory.resolve("java-envelopes"));
        var report=SourceJson.object().put("measurementOnly",true)
                .put("runtimeQualification","NOT_EXECUTED").put("databaseConstructors","NOT_EXECUTED").put("semanticCommandQualification","NOT_EXECUTED")
                .put("inputProvenance","Python-generated native publications and reference; retained Silver input and runtime/IDs are shape-only synthetic scaffolding")
                .put("parserMaximumBytes",PreflightFiles.LARGE).put("fixedSlotBytes",SLOT)
                .put("parserDepthLimit",16).put("evidenceStringLimit",131072);
        if(f.has("provenance"))report.set("fixtureProvenance",f.get("provenance"));
        var rows=report.putArray("measurements");
        Map<String,JsonNode> nativeEnvelopes=new HashMap<>();
        try(var paths=Files.list(directory.resolve("envelopes"))) {
            for(Path path:paths.filter(p->p.toString().endsWith(".json")).sorted().toList()) {
                byte[] bytes=Files.readAllBytes(path);String name=path.getFileName().toString().replaceFirst("\\.json$","");
                JsonNode value=FIXTURE.readTree(bytes);nativeEnvelopes.put(name,value);
                rows.add(measure("python/"+name,bytes,"Python canonical_bytes fixture",value));
            }
        }
        JsonNode gp=required(nativeEnvelopes,"gold-prepared"),gv=required(nativeEnvelopes,"gold-verification"),
                cp=required(nativeEnvelopes,"content-prepared"),cv=required(nativeEnvelopes,"content-verification");
        JsonNode b=f.get("bindings");
        var silverConfig=SourceJson.object().put("outputBindingId","silver-measurement").put("javaHome",f.get("javaHome").stringValue());
        RssSilverRuntime silver=construct(RssSilverRuntime.class,silverConfig,DIGEST,b.get("silver"),b.get("scratch"),null);
        RssGoldRuntime gold=new RssGoldRuntime(SourceJson.object().put("goldBindingId","gold-measurement"),b.get("gold"),silver,DIGEST);
        RssContentRuntime content=construct(RssContentRuntime.class,SourceJson.object().put("referenceBindingId","reference-measurement").put("contentBindingId","content-measurement"),b.get("reference"),b.get("content"),gold,DIGEST);
        Object goldContext=context(RssGoldExecution.class,RUN,ATTEMPT,SourceJson.object(),f.get("inputs"),gold,SourceJson.sha(SourceJson.canonical(f.get("inputs"))),null);
        JsonNode original=command(RssGoldExecution.class,goldContext,"PUBLISH",gp,null);
        emit(output,rows,"gold-publish",original,"Actual RssGoldExecution.command via reflection");
        emit(output,rows,"gold-recheck",command(RssGoldExecution.class,goldContext,"RECHECK",gp,gv),"Actual RssGoldExecution.command via reflection");
        // Mirrors the inline artifact construction, not the database-backed publish()/committed() flows.
        var artifact=artifact("Gold","GOLD",gp.get("inputSetFingerprint").stringValue(),"gold-measurement","input_set",gv);
        var links=artifact.putArray("inputs");
        for(JsonNode input:f.get("inputs"))links.addObject().put("artifactId",input.get("artifactId").stringValue()).put("evidenceSha256",input.get("evidenceSha256").stringValue());
        emit(output,rows,"gold-artifact",artifact,"Shape mirror: RssGoldExecution.publish artifact");
        var proof=SourceJson.object().put("artifactId","00000000-0000-0000-0000-000000000004").put("evidenceSha256",SourceJson.sha(SourceJson.canonical(artifact)));
        proof.set("artifact",artifact);proof.set("command",original);
        emit(output,rows,"gold-committed-proof",proof,"Shape mirror: RssGoldExecution.committed; original PUBLISH command retained");
        Object contentContext=context(RssContentExecution.class,RUN,ATTEMPT,SourceJson.object(),proof,f.get("reference"),content,SourceJson.sha(SourceJson.canonical(RssContentInputs.dependencies(proof,f.get("reference")))));
        JsonNode publish=command(RssContentExecution.class,contentContext,"PUBLISH",cp,null);
        JsonNode recheck=command(RssContentExecution.class,contentContext,"RECHECK",cp,cv);
        emit(output,rows,"content-publish",publish,"Actual RssContentExecution.command via reflection");
        emit(output,rows,"content-recheck",recheck,"Actual RssContentExecution.command via reflection");
        var contentArtifact=artifact("Content","CONTENT",cp.get("fingerprint").stringValue(),"content-measurement","publication",cv);
        contentArtifact.set("inputs",RssContentInputs.dependencies(proof,f.get("reference")));
        emit(output,rows,"content-artifact",contentArtifact,"Shape mirror: RssContentExecution.execute artifact; actual dependency constructor");
        // ContentRecovery changes the retained original PUBLISH to VERIFY and clears verified.
        var recoveryOriginal=(ObjectNode)publish.deepCopy();recoveryOriginal.put("mode","VERIFY");recoveryOriginal.putNull("verified");
        var recovery=SourceJson.object().put("kind","CONTENT");recovery.set("original",recoveryOriginal);
        emit(output,rows,"content-recovery",recovery,"Shape mirror: ContentRecovery publication branch, original PUBLISH changed to VERIFY with verified=null");
        var recoveryStress=SourceJson.object().put("kind","CONTENT");recoveryStress.set("original",recheck);
        emit(output,rows,"content-recovery-recheck-shape-stress",recoveryStress,"Artificial nesting stress only: wrapped RECHECK is not the ContentRecovery publication path");
        var goldRecovery=SourceJson.object().put("kind","GOLD");goldRecovery.set("original",command(RssGoldExecution.class,goldContext,"VERIFY",gp,null));
        emit(output,rows,"gold-recovery",goldRecovery,"Shape mirror: ContentRecovery.nativeRead {kind,original}");
        var resume=SourceJson.object().put("boundary","CONTENT_INPUTS");
        resume.set("reference",f.get("reference"));resume.set("referenceBinding",content.binding(content.reference));resume.set("content",content.binding(content.content));
        // Actual private command constructor, deterministic checkpoint instead of resumeCommand's random UUID.
        resume.set("contentCommand",command(RssContentExecution.class,contentContext,"PREPARE",cp,null));
        emit(output,rows,"content-resume",resume,"Shape mirror: ContentResumeEvidence.verify repeated reference/bindings; actual Content PREPARE command");
        Files.write(directory.resolve("java-measurements.json"),SourceJson.canonical(report),StandardOpenOption.CREATE_NEW);
    }

    private static ObjectNode artifact(String managed,String kind,String fingerprint,String binding,String namespace,JsonNode verification) {
        var n=SourceJson.object().put("managed"+managed+"Version",1).put("kind",kind).put("productId","content-intelligence")
                .put("contractVersion",1).put("processorRevision",1).put("nativeFingerprint",fingerprint)
                .put("storageBindingId",binding).put("relativeAddress","v1/processor_revision=1/"+namespace+"="+fingerprint);
        n.set("verification",verification);return n;
    }
    private static JsonNode required(Map<String,JsonNode> values,String key){assertThat(values).containsKey(key);return values.get(key);}
    @SuppressWarnings("unchecked") private static <T> T construct(Class<T> type,Object... arguments) throws Exception {
        Constructor<?> c=type.getDeclaredConstructors()[0];c.setAccessible(true);return (T)c.newInstance(arguments);
    }
    private static Object context(Class<?> owner,Object... args) throws Exception {
        Class<?> type=Arrays.stream(owner.getDeclaredClasses()).filter(c->c.getSimpleName().equals("Context")).findFirst().orElseThrow();
        return construct(type,args);
    }
    private static JsonNode command(Class<?> owner,Object context,String mode,JsonNode prepared,JsonNode verified) throws Exception {
        Method m=owner.getDeclaredMethod("command",context.getClass(),UUID.class,long.class,String.class,JsonNode.class,JsonNode.class);m.setAccessible(true);
        byte[] bytes=(byte[])m.invoke(null,context,CHECKPOINT,1L,mode,prepared,verified);
        // Command construction itself uses SourceJson.canonical. Record downstream acceptance separately.
        return FIXTURE.readTree(bytes);
    }
    private static void emit(Path out,tools.jackson.databind.node.ArrayNode rows,String name,JsonNode value,String provenance) throws Exception {
        byte[] bytes=SourceJson.canonical(value);Files.write(out.resolve(name+".json"),bytes,StandardOpenOption.CREATE_NEW);rows.add(measure(name,bytes,provenance,value));
    }
    private static ObjectNode measure(String name,byte[] bytes,String provenance,JsonNode value) {
        var row=SourceJson.object().put("name",name).put("provenance",provenance).put("bytes",bytes.length)
                .put("sha256",SourceJson.sha(bytes)).put("fitsFixedSlot",bytes.length<=SLOT)
                .put("fitsLegacyByteMaximum",bytes.length<=PreflightFiles.LARGE)
                .put("maximumValueDepth",depth(value)).put("maximumStringValueUtf16Units",stringLength(value))
                .put("maximumKeyUtf16Units",keyLength(value)).put("maximumContainerNesting",containers(value));
        try{byte[] canonical=SourceJson.canonical(SourceJson.parse(bytes,PreflightFiles.LARGE));row.put("sourceJsonAccepted",true).put("canonicalRoundtrip",Arrays.equals(bytes,canonical));}
        catch(RuntimeException e){row.put("sourceJsonAccepted",false).put("rejectionClass",e.getClass().getSimpleName());}
        return row;
    }
    private static int depth(JsonNode n){int max=0;for(JsonNode child:n)max=Math.max(max,1+depth(child));return max;}
    private static int stringLength(JsonNode n){int max=n.isString()?n.stringValue().length():0;for(JsonNode child:n)max=Math.max(max,stringLength(child));return max;}
    private static int keyLength(JsonNode n){int max=0;if(n.isObject())for(String key:n.propertyNames())max=Math.max(max,key.length());for(JsonNode child:n)max=Math.max(max,keyLength(child));return max;}
    private static int containers(JsonNode n){if(!n.isContainer())return 0;int max=0;for(JsonNode child:n)max=Math.max(max,containers(child));return 1+max;}
    private static byte[] asciiDocument(int size) {
        // Many individually short strings avoid confusing aggregate bytes with maxStringLength.
        StringBuilder s=new StringBuilder("[");int remaining=size-2;
        while(remaining>4098){s.append('"').append("x".repeat(4095)).append("\",");remaining-=4098;}
        s.append('"').append("x".repeat(remaining-2)).append("\"]");return s.toString().getBytes(StandardCharsets.UTF_8);
    }
}
