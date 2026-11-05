package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Optional fresh Python projection corpus; no runtime load, SQL, child launch or file writes. */
class PublicationProjectionParityTest {
    private static final JsonMapper FIXTURE = JsonMapper.builder().build();

    @Test void projectedAndActualNativeShapesUseTheSameCompleteJavaQualification() throws Exception {
        String root = System.getProperty("projection.parity.directory");
        assumeTrue(root != null && !root.isBlank(), "Optional fresh native projection corpus not supplied");
        for (String name : List.of("small_nonempty", "gold_902_empty_escaped", "content_902_empty_escaped", "both_902_empty_escaped")) {
            for (String kind : List.of("GOLD", "CONTENT")) {
                JsonNode fixture = FIXTURE.readTree(Files.readAllBytes(Path.of(root, name, kind.toLowerCase(Locale.ROOT) + ".sizing-fixture.json")));
                assertThat(fixture.get("purpose").stringValue()).isEqualTo("SYNTHETIC_SIZE_PARITY_ONLY_NOT_NATIVE_EVIDENCE_OR_EXECUTABLE_COMMAND");
                assertThat(fixture.get("kind").stringValue()).isEqualTo(kind);
                JsonNode command = fixture.get("command"), projected = fixture.get("sizingValue"), actual = fixture.get("actualValue");
                assertThat(SourceJson.canonical(projected).length).isEqualTo(SourceJson.canonical(actual).length);
                var normalized = (tools.jackson.databind.node.ObjectNode) actual.deepCopy();
                if (kind.equals("CONTENT")) {
                    assertThat(SourceJson.canonical(actual.get("smoke"))).hasSize(339);
                    assertThat(SourceJson.canonical(projected.get("smoke"))).hasSize(339);
                    normalized.set("smoke", projected.get("smoke"));
                }
                assertThat(SourceJson.canonical(projected)).isEqualTo(SourceJson.canonical(normalized));
                JsonNode gold = kind.equals("GOLD") ? command : command.get("goldProof").get("command");
                var receipts = SourceJson.object().putArray("items");
                for (JsonNode input : gold.get("inputs")) receipts.add(input.get("receipt"));
                String binding = fixture.get("outputBindingId").stringValue();
                for (JsonNode value : List.of(projected, actual)) {
                    org.assertj.core.api.ThrowableAssert.ThrowingCallable qualify = () -> {
                        if (kind.equals("GOLD")) NarrowPublicationEnvelopes.qualifyGoldV1(command, value, binding, receipts);
                        else NarrowPublicationEnvelopes.qualifyContentV1(command, value, binding, receipts);
                    };
                    if (fixture.get("fitsWholeStage").booleanValue()) assertThatCode(qualify).doesNotThrowAnyException();
                    else assertThatThrownBy(qualify).isInstanceOfSatisfying(PreflightFailure.class,
                        error -> assertThat(error.code()).isEqualTo(PreflightFailure.Code.RESOURCE_LIMIT));
                }
                if (kind.equals("CONTENT")) {
                    JsonNode proof = command.get("goldProof");
                    JsonNode constructed = NarrowPublicationEnvelopes.goldProofV1(gold, proof.get("artifact").get("verification"),
                        proof.get("artifact").get("storageBindingId").stringValue(), UUID.fromString(proof.get("artifactId").stringValue()));
                    assertThat(SourceJson.canonical(constructed)).isEqualTo(SourceJson.canonical(proof));
                }
            }
        }
    }
}
