package platform.contentapi.execution;

import java.nio.file.Path;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Real launcher JVM for crash tests. Inspection acquires no guard or execution authority. */
public final class NarrowContentFetchLaunchTestChild {
    public static void main(String[] args)throws Exception{
        var binding=OwnershipFixtures.readBinding(Path.of(args[0]));
        var jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("DDIP_TEST_DB_URL"),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));
        var tx=new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource())));
        var workers=WorkerTestSupport.enabled(jdbc,tx,binding.installationId());
        UUID operation=UUID.fromString(args[5]);
        if(args[1].equals("inspect")){
            var facts=NarrowContentFetchLaunchTestSupport.inspect(jdbc,tx,workers,operation);
            var assessment=NarrowContentFetchRecoveryAssessmentTestSupport.inspect(jdbc,tx,null,UUID.fromString(args[3]),UUID.fromString(args[4]));
            if(assessment.authorization() || assessment.quiescenceEstablished() || assessment.nativeLaunchCompatible())throw new AssertionError("ASSESSMENT_GRANTED_AUTHORITY");
            if(!assessment.durable().prefix().equals(facts.phases().getLast()) || !assessment.integrity().equals("NOT_INSPECTED_NO_CHECKPOINT"))throw new AssertionError("INVALID_ASSESSMENT");
            System.out.println("NARROW_LAUNCH_HISTORY "+ProcessHandle.current().pid()+" "+String.join(",",facts.phases())+" "+facts.candidateSha256());
            if(facts.completed() || facts.nativeLaunchCompatible())throw new AssertionError("HISTORY_MUST_NOT_AUTHORIZE_EXECUTION");
            return;
        }
        try(var guard=InstallationHostGuard.acquire(binding)){
            String digest=jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,args[2]);
            var session=workers.register(guard,args[2],digest,NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,args[2]));
            var claim=NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.fromString(args[4]),UUID.fromString(args[3]));
            NarrowContentAdmissionTestSupport.prepare(jdbc,tx,workers,guard,claim.owner());
            NarrowContentFetchLaunchTestSupport.launch(jdbc,tx,workers,guard,claim.owner(),operation,args[6],boundary->{
                if(boundary.equals(args[1]) || args[1].equals("HTTP") && boundary.equals("RELEASE_AUTHORIZED")){
                    System.out.println("NARROW_LAUNCH_BOUNDARY "+ProcessHandle.current().pid()+" "+boundary);System.out.flush();
                    if(!args[1].equals("HTTP"))try{System.in.read();}catch(java.io.IOException ex){throw new IllegalStateException("TEST_INPUT_CLOSED");}
                }
            });
            throw new AssertionError("LAUNCHER_WAS_NOT_KILLED");
        }
    }
    private NarrowContentFetchLaunchTestChild(){}
}
