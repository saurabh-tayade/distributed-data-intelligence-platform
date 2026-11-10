package platform.contentapi.execution;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;

/** Shared offline recovery-to-retry setup; no inherited tests. */
abstract class ReliabilityRetryFixtures extends RecoveryVerificationFixtures {
    ReliabilityRetry retryService(){return new ReliabilityRetry(jdbc,tx,INSTALL);}
    RecoveryQuiescence.Review freshReview(){clock.now=Instant.now();return new RecoveryQuiescence.Review(review.targetSha256(),clock.instant(),true,true,true,review.reviewedCheckpoints(),review.descendants());}
    UUID recoverAbsent(boolean cancelled)throws Exception {
        deleteTree(publication());
        if(cancelled){var body=SourceJson.canonical(SourceJson.object().put("action","CANCEL").putNull("attemptId").putNull("quiescenceConfirmed"));new ControlRepository(jdbc,tx,INSTALL).request(ControlCommands.parse(run.toString(),body,"cancel-before-retry","\"1\""));}
        handoff();var verifier=recoveryService();var observed=verifier.verify(guard,bootstrap,review);var finalizer=new RecoveryFinalization(jdbc,tx,handoffs);var q=finalizer.qualify(guard,bootstrap,observed,freshReview());var receipt=finalizer.finish(guard,q,freshReview());
        assertThat(receipt.status()).isEqualTo(RecoveryFinalization.Status.CONFIRMED);assertThat(receipt.retryEligible()).isTrue();assertThat(receipt.runState()).isEqualTo(cancelled?"CANCELLED":"FAILED");return control;
    }
    List<RecoveryQuiescence.Identity> reviewedDescendants(){return List.of();}
    @Override void handoff(){
        clock.now=Instant.now();var body=SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true);long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);
        control=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL).request(ControlCommands.parse(run.toString(),SourceJson.canonical(body),"recovery-"+owner.attemptId(),"\""+version+"\"")).get("controlId").stringValue());var registered=new WorkerRepository(jdbc,tx,INSTALL).register(guard,PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());var t=handoffs.inspect(guard,registered,control);review=new RecoveryQuiescence.Review(t.digest(),clock.instant(),true,true,true,t.unresolvedCheckpoints(),reviewedDescendants());qualification=handoffs.qualify(guard,t,review,RUNTIME);recoveryOwner=handoffs.transfer(guard,qualification).owner();bootstrap=handoffs.bootstrap(guard,qualification.address(),qualification.sha256(),RUNTIME);
    }
    ControlCommands.Command retryCommand(String key){long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);return ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","RETRY").putNull("attemptId").putNull("quiescenceConfirmed")),key,"\""+version+"\"");}
    UUID admit(){return UUID.fromString(new ControlRepository(jdbc,tx,INSTALL).request(retryCommand("retry-one")).get("controlId").stringValue());}
}
