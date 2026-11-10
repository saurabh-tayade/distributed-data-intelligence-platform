package platform.contentapi.execution;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;

class RetryCancellationOutputTest {
    @ParameterizedTest @ValueSource(strings={"CANCELLED","RECOVERY_REQUIRED"})
    void cancelledInvocationDoesNotClaimArtifactSuccess(String status){
        UUID run=UUID.randomUUID(),retry=UUID.randomUUID();var output=new ByteArrayOutputStream();var previous=System.out;
        var command=new OperatorRecoveryConfiguration.Command(null,null,UUID.randomUUID(),null);
        try{System.setOut(new PrintStream(output,true,StandardCharsets.UTF_8));command.cancelledRetry(retry,new ReliabilityRetry.CancellationRecorded(run,status));}finally{System.setOut(previous);}
        var record=SourceJson.parse(output.toString(StandardCharsets.UTF_8).strip().getBytes(StandardCharsets.UTF_8));
        assertThat(record.get("status").stringValue()).isEqualTo(status);assertThat(record.get("runId").stringValue()).isEqualTo(run.toString());assertThat(record.get("retryControlId").stringValue()).isEqualTo(retry.toString());
        assertThat(record.has("artifactId")).isFalse();assertThat(record.has("controlId")).isFalse();assertThat(command.getExitCode()).isEqualTo(status.equals("CANCELLED")?0:2);
    }
}
