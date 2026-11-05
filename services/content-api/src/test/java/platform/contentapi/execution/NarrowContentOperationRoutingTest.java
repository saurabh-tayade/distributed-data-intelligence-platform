package platform.contentapi.execution;

import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.execution.NarrowContentOperationRouting.*;

class NarrowContentOperationRoutingTest {
    final UUID run=UUID.fromString("10000000-0000-4000-8000-000000000001"),attempt=UUID.fromString("20000000-0000-4000-8000-000000000001");
    final Path root=Path.of("/not-opened/runs/"+run+"/attempts/"+attempt);
    final ContentOperationPlan plan=ContentOperationPlan.original(root,run,attempt,"source");

    @Test void exhaustiveBijectionPreservesAllNineteenOriginalReservations(){
        Set<Path> mapped=new HashSet<>();Set<String> coordinates=new HashSet<>();
        for(var operation:Operation.values()){
            assertThat(mapped.add(operation.directory(plan))).isTrue();
            assertThat(coordinates.add(operation.ledgerStage+":"+operation.ledgerOrdinal)).isTrue();
            assertThat(NarrowContentOperationRouting.operation(operation.ledgerStage,operation.ledgerOrdinal)).isSameAs(operation);
        }
        assertThat(mapped).hasSize(19).containsExactlyInAnyOrderElementsOf(plan.reservations().keySet());
        assertThat(Operation.values()).hasSize(19);
    }
    @Test void checksAndVerifiersHaveDistinctNativeMeaningAndSqlAttribution(){
        assertThat(operation("CHECK_INPUTS",0)).isSameAs(Operation.RECHECK_GOLD);
        assertThat(operation("CHECK_INPUTS",1)).isSameAs(Operation.PREPARE_CONTENT);
        assertThat(operation("CHECK_INPUTS",2)).isSameAs(Operation.RECHECK_CONTENT);
        assertThat(operation("CHECK_INPUTS",3)).isSameAs(Operation.INSPECT_CONTENT);
        assertThat(operation("VERIFY_RESULT",0)).isSameAs(Operation.VERIFY_GOLD);
        assertThat(operation("VERIFY_RESULT",1)).isSameAs(Operation.VERIFY_CONTENT);
        assertThat(Operation.PREPARE_CONTENT.historyStage).isEqualTo("CHECK_INPUTS");
        assertThat(Operation.PREPARE_CONTENT.historyOrdinal()).isZero();
        assertThat(Operation.VERIFY_CONTENT.historyOrdinal()).isZero();
        assertThat(Operation.VERIFY_GOLD.history).isEqualTo(History.AUXILIARY);
        assertThat(Operation.VERIFY_GOLD.historyStage).isEqualTo("PUBLISH_GOLD");
        assertThat(Operation.RECHECK_GOLD.history).isEqualTo(History.AUXILIARY);
        assertThat(Operation.INSPECT_CONTENT.mode).isEqualTo("VERIFY");
    }
    @Test void onlyEightOperationsAreHistoricalSqlStages(){
        assertThat(Arrays.stream(Operation.values()).filter(o->o.history==History.STAGE).map(o->o.historyStage).toList())
                .containsExactly("FETCH_SOURCE","STORE_BRONZE","SILVER","PREPARE_GOLD","PUBLISH_GOLD","CHECK_INPUTS","PUBLISH_CONTENT","VERIFY_RESULT");
    }
    @Test void bronzeOrdinalsNameCallRolesAndNeverNativeSourceIndices(){
        List<Operation> inspections=List.of(Operation.BRONZE_BEFORE_SILVER,Operation.BRONZE_BEFORE_GOLD_PREPARATION,
                Operation.BRONZE_BEFORE_GOLD_PUBLICATION,Operation.BRONZE_BEFORE_CONTENT_PREPARATION,Operation.BRONZE_BEFORE_CONTENT_INSPECTION);
        for(int i=0;i<inspections.size();i++){
            var operation=inspections.get(i);assertThat(operation.ledgerOrdinal).isEqualTo(i);
            assertThat(operation.sourceOrdinal()).isZero();assertThat(operation.mode).isEqualTo("INSPECT");
            assertThat(operation.family).isEqualTo(Family.BRONZE);
            assertThat(operation.history).isEqualTo(i==4?History.AFTER_SUCCESS:History.BEFORE_STAGE);
        }
        assertThat(Arrays.stream(Operation.values()).filter(o->o.history==History.AFTER_SUCCESS).toList())
                .containsExactly(Operation.BRONZE_BEFORE_CONTENT_INSPECTION,Operation.INSPECT_CONTENT);
    }
    @Test void nativeCheckpointReferencesDoNotRewriteFixedDirectoryIdentities(){
        assertThat(Operation.VERIFY_SILVER.nativeCheckpointOrigin()).isSameAs(Operation.PUBLISH_SILVER);
        assertThat(Operation.RECHECK_SILVER.nativeCheckpointOrigin()).isSameAs(Operation.PUBLISH_SILVER);
        assertThat(Operation.VERIFY_GOLD.nativeCheckpointOrigin()).isSameAs(Operation.PUBLISH_GOLD);
        assertThat(Operation.RECHECK_GOLD.nativeCheckpointOrigin()).isSameAs(Operation.PUBLISH_GOLD);
        assertThat(Operation.RECHECK_CONTENT.nativeCheckpointOrigin()).isSameAs(Operation.VERIFY_CONTENT);
        assertThat(Operation.VERIFY_SILVER.directory(plan).getFileName()).isEqualTo(Operation.PUBLISH_SILVER.directory(plan).getFileName());
        for(var operation:List.of(Operation.RECHECK_SILVER,Operation.VERIFY_GOLD,Operation.RECHECK_GOLD,Operation.RECHECK_CONTENT))
            assertThat(operation.directory(plan).getFileName()).isNotEqualTo(operation.nativeCheckpointOrigin().directory(plan).getFileName());
        assertThat(Operation.INSPECT_CONTENT.nativeCheckpointOrigin()).isSameAs(Operation.INSPECT_CONTENT);
    }
    @ParameterizedTest @EnumSource(Operation.class)
    void catalogUsesExistingCommandAllowanceAndDoesNotTouchDisk(Operation operation){
        int size=plan.reservations().get(operation.directory(plan)).get("command.json");
        assertThat(size).isEqualTo(Set.of(Family.FETCH,Family.BRONZE,Family.SILVER).contains(operation.family)?65536:1024*1024);
        assertThat(operation.directory(plan)).doesNotExist();
    }
    @Test void unknownRecoveryAndExtraInspectionCoordinatesHaveNoRoute(){
        for(String stage:List.of("CHECK_INPUTS","INSPECT_BRONZE","VERIFY_RESULT","VERIFY_SILVER","RECOVERY","READ_ONLY_PROBE"))
            assertThatThrownBy(()->operation(stage,99)).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->operation("CHECK_INPUTS",-1)).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->operation("VERIFY_RESULT",2)).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->operation("VERIFY_SILVER",1)).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->operation("CHECK_INPUTS",4)).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->operation(null,0)).hasMessage("INPUT_INVALID");
    }
}
