package platform.contentapi.execution;

import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import platform.contentapi.management.SourceJson;

/** Separate disposable owner JVM. Never packaged or exposed as an operator command. */
final class RecoveryBootstrapTestProcess {
    public static void main(String[] args)throws Exception {
        var jdbc=new JdbcTemplate(new DriverManagerDataSource(args[1],args[2],args[3]));jdbc.setQueryTimeout(3);
        var tx=ReliabilityExecutionFixtures.transactions(jdbc);var binding=OwnershipFixtures.readBinding(Path.of(args[0]));
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var profile=jdbc.queryForMap("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",ReliabilityExecutionFixtures.PROFILE);
            var config=SourceJson.parse((byte[])profile.get("configuration_bytes"));
            var worker=new WorkerRepository(jdbc,tx,binding.installationId()).register(guard,ReliabilityExecutionFixtures.PROFILE,(String)profile.get("configuration_sha256"),config.get("runtime").get("deploymentSha256").stringValue());
            var handoff=new RecoveryHandoff(jdbc,tx,binding.installationId());var target=handoff.inspect(guard,worker,UUID.fromString(args[4]));
            var review=new RecoveryQuiescence.Review(target.digest(),Instant.now(),true,true,true,target.unresolvedCheckpoints(),List.of());
            var q=handoff.qualify(guard,target,review,ReliabilityExecutionFixtures.RUNTIME);handoff.transfer(guard,q);
            System.out.println("BOOTSTRAP "+q.address()+" "+q.sha256());
        }
    }
}
