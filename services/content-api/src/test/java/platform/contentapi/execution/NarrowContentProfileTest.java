package platform.contentapi.execution;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

class NarrowContentProfileTest {
    private static ObjectNode qualification() {
        var root=SourceJson.object().put("qualificationVersion",1);
        root.putObject("selection");var verification=root.putObject("verification").put("evidenceVersion",2);
        verification.putObject("native");var capture=verification.putObject("capture");capture.putObject("local");capture.putObject("bronze");
        verification.putObject("inventoryIdentity").put("kind","E5_SILVER_INVENTORY_V1").put("sha256","a".repeat(64)).putObject("value");
        return root;
    }
    private static void fill(ObjectNode root,ObjectNode target,int size) {
        target.put("padding","");target.put("padding","x".repeat(size-SourceJson.canonical(root).length));
        assertThat(SourceJson.canonical(root)).hasSize(size);
    }
    @Test void wholeQualificationAndSelectionFitActualUnchangedReadersAtExactCaps() {
        var qualification=qualification();fill(qualification,(ObjectNode)qualification.get("selection"),NarrowContentProfile.SILVER_QUALIFICATION);
        byte[] qualified=NarrowContentProfile.silverQualification(qualification);
        assertThat(SourceJson.parse(qualified)).isEqualTo(qualification);
        ((ObjectNode)qualification.get("selection")).put("padding",qualification.get("selection").get("padding").stringValue()+"x");
        assertThatThrownBy(()->NarrowContentProfile.silverQualification(qualification)).hasMessage("RESOURCE_LIMIT");
        var selection=SourceJson.object().put("inputSetFingerprint","a".repeat(64));var manifest=selection.putObject("manifest");
        fill(selection,manifest,NarrowContentProfile.SILVER_SELECTION);
        assertThat(SourceJson.parse(NarrowContentProfile.silverSelection(selection))).isEqualTo(selection);
        manifest.put("padding",manifest.get("padding").stringValue()+"x");
        assertThatThrownBy(()->NarrowContentProfile.silverSelection(selection)).hasMessage("RESOURCE_LIMIT");
    }
    @Test void eachNestedSilverComponentHasItsOwnBudget() {
        for(String component:new String[]{"local","identity","verification"}){
            var whole=qualification();var verification=(ObjectNode)whole.get("verification");
            var target=(ObjectNode)(component.equals("local")?verification.get("capture").get("local"):
                    component.equals("identity")?verification.get("inventoryIdentity").get("value"):verification.get("native"));
            var measured=(ObjectNode)(component.equals("verification")?verification:target);
            int cap=component.equals("local")?NarrowContentProfile.SILVER_CAPTURE:component.equals("identity")?NarrowContentProfile.SILVER_IDENTITY:NarrowContentProfile.SILVER_VERIFICATION;
            fill(measured,target,cap);NarrowContentProfile.silverQualification(whole);
            target.put("padding",target.get("padding").stringValue()+"x");
            assertThatThrownBy(()->NarrowContentProfile.silverQualification(whole)).hasMessage("RESOURCE_LIMIT");
        }
    }
    @Test void actualReaderRejectsDepthNumberStringAndNonScalarUnicodeBeforeAcceptance() {
        JsonNode nested=SourceJson.object().put("v",0);
        for(int i=0;i<15;i++){var parent=SourceJson.object();parent.set("x",nested);nested=parent;}
        NarrowContentProfile.envelope(nested,65536,65536);
        var tooDeep=SourceJson.object();tooDeep.set("x",nested);
        assertThatThrownBy(()->NarrowContentProfile.envelope(tooDeep,65536,65536)).hasMessage("INPUT_INVALID");
        var number=SourceJson.object().put("n",new BigInteger("9".repeat(20)));
        NarrowContentProfile.envelope(number,65536,65536);number.put("n",new BigInteger("9".repeat(21)));
        assertThatThrownBy(()->NarrowContentProfile.envelope(number,65536,65536)).hasMessage("INPUT_INVALID");
        number.put("n",new BigInteger("-"+"9".repeat(19)));NarrowContentProfile.envelope(number,65536,65536);
        number.put("n",new BigInteger("-"+"9".repeat(20)));
        assertThatThrownBy(()->NarrowContentProfile.envelope(number,65536,65536)).hasMessage("INPUT_INVALID");
        var text=SourceJson.object().put("v","x".repeat(131072));NarrowContentProfile.envelope(text,1024*1024,1024*1024);
        text.put("v","x".repeat(131073));assertThatThrownBy(()->NarrowContentProfile.envelope(text,1024*1024,1024*1024)).hasMessage("INPUT_INVALID");
        for(String invalid:new String[]{"\uD800","\uDFFF"})assertThatThrownBy(()->NarrowContentProfile.envelope(SourceJson.object().put("private",invalid),65536,65536)).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->NarrowContentProfile.envelope(SourceJson.object().put("private",1.5),65536,65536)).hasMessage("INPUT_INVALID");
    }
    @Test void actualReaderHasAnIndependentFieldNameLimit() {
        for(String name:new String[]{"x".repeat(50000),"é".repeat(50000),"\uD83D\uDE42".repeat(25000)}){
            NarrowContentProfile.envelope(SourceJson.object().put(name,0),1024*1024,1024*1024);
            assertThatThrownBy(()->NarrowContentProfile.envelope(SourceJson.object().put(name+"x",0),1024*1024,1024*1024)).hasMessage("INPUT_INVALID");
        }
    }
    @Test void productionPythonSerializerVectorsRoundTripThroughActualJavaReaders()throws Exception {
        byte[] bytes;
        try(var stream=getClass().getResourceAsStream("/narrow-content-profile-native-vectors.json")){bytes=stream.readAllBytes();}
        var all=SourceJson.parse(bytes);assertThat(SourceJson.canonical(all)).isEqualTo(bytes);
        var qualification=all.get("qualification");
        assertThat(SourceJson.parse(NarrowContentProfile.silverQualification(qualification))).isEqualTo(qualification);
        var selection=all.get("nativeSelection");
        assertThat(SourceJson.parse(NarrowContentProfile.silverSelection(selection))).isEqualTo(selection);
        for(var report:all.get("reports"))assertThat(SourceJson.parse(NarrowContentProfile.envelope(report,NarrowContentProfile.REPORT_ROW,65536))).isEqualTo(report);
    }
    @Test void canonicalEscapesAndSupplementaryScalarsAreCountedInBytes() {
        var value=SourceJson.object().put("\uD83D\uDE42", "quote\" slash\\ newline\n control\u0001 é \uD83D\uDE42");
        byte[] expected=SourceJson.canonical(value);
        assertThat(NarrowContentProfile.envelope(value,expected.length,65536)).isEqualTo(expected);
        assertThatThrownBy(()->NarrowContentProfile.envelope(value,expected.length-1,65536)).hasMessage("RESOURCE_LIMIT");
    }
    @Test void malformedBudgetShapesFailWithoutCopyingPrivateValues() {
        var invalid=qualification();invalid.put("private", "secret");
        assertThatThrownBy(()->NarrowContentProfile.silverQualification(invalid)).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->NarrowContentProfile.silverSelection(SourceJson.object())).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->NarrowContentProfile.envelope(SourceJson.object(),65537,65536)).hasMessage("INPUT_INVALID");
    }
    @Test void effectfulContinuationLeavesOneRecoveryAttempt() {
        NarrowContentProfile.continuationCapacity(0);NarrowContentProfile.continuationCapacity(18);
        for(long count:new long[]{-1,19,20,Long.MAX_VALUE})assertThatThrownBy(()->NarrowContentProfile.continuationCapacity(count)).hasMessage("RESOURCE_LIMIT");
    }
}
