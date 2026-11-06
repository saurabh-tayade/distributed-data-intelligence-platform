package platform.contentapi.execution;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;

/** Metadata schema/encoding boundary only; complete semantic validation is exercised by the ITs. */
class NarrowContentFetchCandidateVerificationTest {
    @Test void acceptsNativeInsertionOrderWithoutRequiringCanonicalBytes(){
        byte[] bytes="{\"schemaVersion\":1,\"ingestionId\":\"native-order\"}".getBytes(StandardCharsets.UTF_8);
        assertThat(bytes).isNotEqualTo(SourceJson.canonical(SourceJson.parse(bytes)));
        assertThatCode(()->NarrowContentFetchCandidateVerification.metadataSchema(bytes)).doesNotThrowAnyException();
    }
    @ParameterizedTest @ValueSource(strings={"{}","[]","{\"schemaVersion\":\"1\"}","{\"schemaVersion\":1.5}","{\"schemaVersion\":true}","{\"schemaVersion\":null}","{\"schemaVersion\":-1}","{\"schemaVersion\":4294967297}","{\"schemaVersion\":1,\"schemaVersion\":1}","{\"schemaVersion\":1} {}","{\"schemaVersion\":1", "\ufeff{\"schemaVersion\":1}"})
    void rejectsMalformedDuplicateTrailingAndWrongTypeMetadata(String json){
        assertThatThrownBy(()->NarrowContentFetchCandidateVerification.metadataSchema(json.getBytes(StandardCharsets.UTF_8))).isInstanceOf(RuntimeException.class);
    }
    @Test void rejectsOversizedAndInvalidUtf8Metadata(){
        assertThatThrownBy(()->NarrowContentFetchCandidateVerification.metadataSchema(new byte[65537])).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(()->NarrowContentFetchCandidateVerification.metadataSchema(new byte[]{(byte)0xc3,0x28})).isInstanceOf(RuntimeException.class);
    }
}
