package platform.contentapi.management;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

class ControlCommandsTest {
    static final String RUN="60000000-0000-4000-8000-000000000001",ATTEMPT="70000000-0000-4000-8000-000000000001";
    static final String BODY="{\"action\":\"RECONCILE\",\"attemptId\":\""+ATTEMPT+"\",\"quiescenceConfirmed\":true}";
    static byte[] bytes(String s){return s.getBytes(StandardCharsets.UTF_8);}
    static ControlCommands.Command parse(String body,String etag){return ControlCommands.parse(RUN,bytes(body),"key",etag);}
    @Test void literalVectorAndEquivalentObjectOrder(){
        var c=parse(BODY,"\"5\"");
        assertThat(new String(c.normalized(),StandardCharsets.UTF_8)).isEqualTo("{\"action\":\"RECONCILE\",\"attemptId\":\"70000000-0000-4000-8000-000000000001\",\"expectedVersion\":\"5\",\"quiescenceConfirmed\":true,\"runId\":\"60000000-0000-4000-8000-000000000001\"}");
        // Independently calculated using Python hashlib over literal sorted compact JSON UTF-8 bytes.
        assertThat(SourceJson.sha(c.normalized())).isEqualTo("46b24ff4d50645dc0961d129b3e61dbaa0b8a7db911e6e648664713c3bf0f528");
        assertThat(parse("{ \"quiescenceConfirmed\": true, \"attemptId\":\""+ATTEMPT+"\",\"action\":\"RECONCILE\"}","\"5\"").normalized()).isEqualTo(c.normalized());
        assertThat(parse(BODY,"\"6\"").normalized()).isNotEqualTo(c.normalized());
        var b=c.normalized();b[0]=0;assertThat(c.normalized()[0]).isEqualTo((byte)'{');assertThat(c.toString()).doesNotContain(RUN,ATTEMPT,"key");
    }
    @Test void resumeUsesSameStrictConditionalEnvelope(){var c=parse("{\"action\":\"RESUME\",\"attemptId\":null,\"quiescenceConfirmed\":null}","\"5\"");assertThat(c.action()).isEqualTo("RESUME");assertThat(c.attemptId()).isNull();assertThat(c.expectedVersion()).isEqualTo(5);assertThat(new String(c.normalized(),StandardCharsets.UTF_8)).contains("\"expectedVersion\":\"5\"");}
    @ParameterizedTest @ValueSource(strings={"CANCEL","RETRY","PAUSE","RESUME","reconcile",""}) void unsupported(String action){assertThatThrownBy(()->parse(BODY.replace("RECONCILE",action),"\"0\"")).hasMessage("INVALID_ARGUMENT");}
    @ParameterizedTest @ValueSource(strings={"false","null","1","\"true\"","{}","[]"}) void confirmationIsExactTrue(String value){assertThatThrownBy(()->parse(BODY.replace("true",value),"\"0\"")).hasMessage("INVALID_ARGUMENT");}
    @ParameterizedTest @ValueSource(strings={"","1","null","\"\"","W/\"1\"","\"01\"","\"-1\"","\"9223372036854775808\"","\"1\",\"2\""}) void badEtag(String value){assertThatThrownBy(()->parse(BODY,value)).hasMessage("INVALID_ARGUMENT");}
    @Test void missingEtagAndKeys(){assertThatThrownBy(()->parse(BODY,null)).hasMessage("PRECONDITION_REQUIRED");for(String key:new String[]{null,"","a b","../x","x".repeat(129)})assertThatThrownBy(()->ControlCommands.parse(RUN,bytes(BODY),key,"\"0\"")).hasMessage("INVALID_ARGUMENT");}
    @Test void largeVersionIsNeverRounded(){var c=parse(BODY,"\"9223372036854775807\"");assertThat(c.expectedVersion()).isEqualTo(Long.MAX_VALUE);assertThat(new String(c.normalized(),StandardCharsets.UTF_8)).contains("\"expectedVersion\":\"9223372036854775807\"");}
    @ParameterizedTest @ValueSource(strings={"bad","7-0-4-8-1","70000000-0000-4000-8000-00000000000A","../x",""}) void canonicalUuid(String value){assertThatThrownBy(()->parse(BODY.replace(ATTEMPT,value),"\"0\"")).hasMessage("INVALID_ARGUMENT");assertThatThrownBy(()->ControlCommands.parse(value,bytes(BODY),"key","\"0\"")).hasMessage("INVALID_ARGUMENT");}
    @Test void strictFieldsTypesEncodingAndSize(){
        for(String field:new String[]{"action","attemptId","quiescenceConfirmed"}){var n=SourceJson.parse(bytes(BODY)).deepCopy();((tools.jackson.databind.node.ObjectNode)n).remove(field);assertThatThrownBy(()->ControlCommands.parse(RUN,SourceJson.canonical(n),"key","\"0\"")).hasMessage("INVALID_ARGUMENT");}
        for(String raw:new String[]{BODY.replace("}",",\"reason\":\"private\"}"),BODY.replace("}",",\"action\":\"RECONCILE\"}"),BODY.replace("\""+ATTEMPT+"\"","null"),BODY.replace("\"RECONCILE\"","1"),"[]","null"})assertThatThrownBy(()->parse(raw,"\"0\"")).hasMessage("INVALID_ARGUMENT");
        assertThatThrownBy(()->ControlCommands.parse(RUN,new byte[65537],"key","\"0\"")).hasMessage("INVALID_ARGUMENT");assertThatThrownBy(()->ControlCommands.parse(RUN,new byte[]{(byte)0xff},"key","\"0\"")).hasMessage("INVALID_ARGUMENT");
    }
}
