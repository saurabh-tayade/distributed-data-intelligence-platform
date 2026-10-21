package platform.contentapi.execution;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods=false)
@Conditional(DatabaseConfiguration.Enabled.class)
public class DatabaseConfiguration {
    static final class Enabled implements Condition {
        @Override public boolean matches(ConditionContext c, AnnotatedTypeMetadata m) { return RuntimeRole.from(c.getEnvironment()).databaseRequired(); }
    }
    @Bean DatabaseSettings databaseSettings(Environment environment) { return DatabaseSettings.from(environment); }
    @Bean(destroyMethod="close") HikariDataSource managementDataSource(DatabaseSettings settings, Environment environment) {
        HikariConfig c=new HikariConfig();
        c.setJdbcUrl(settings.url()); c.setUsername(settings.username()); c.setPassword(settings.password());
        c.setPoolName("management"); c.setMaximumPoolSize(switch(RuntimeRole.from(environment)) { case API -> 4; default -> 2; });
        c.setMinimumIdle(0); c.setConnectionTimeout(3000); c.setValidationTimeout(1000); c.setInitializationFailTimeout(-1);
        c.addDataSourceProperty("connectTimeout","3"); c.addDataSourceProperty("socketTimeout","3");
        c.addDataSourceProperty("tcpKeepAlive","true");
        c.setConnectionInitSql("SET TIME ZONE 'UTC'; SET statement_timeout='3s'; SET lock_timeout='3s'; SET idle_in_transaction_session_timeout='5s'");
        return new HikariDataSource(c);
    }
    @Bean JdbcTemplate managementJdbc(HikariDataSource dataSource) {
        JdbcTemplate jdbc=new JdbcTemplate(dataSource); jdbc.setQueryTimeout(3); return jdbc;
    }
    @Bean ManagementTransactions managementTransactions(HikariDataSource dataSource) {
        TransactionTemplate tx=new TransactionTemplate(new JdbcTransactionManager(dataSource)); tx.setTimeout(3);
        return new ManagementTransactions(tx);
    }
    @Bean JdbcCoordinatorRepository coordinatorRepository(JdbcTemplate jdbc,ManagementTransactions tx,DatabaseSettings settings) {
        return new JdbcCoordinatorRepository(jdbc,tx,settings.installationId());
    }
    @Bean ImportOwnershipRepository importOwnershipRepository(JdbcTemplate jdbc,ManagementTransactions tx,DatabaseSettings settings) {
        return new ImportOwnershipRepository(jdbc,tx,settings.installationId());
    }
    @Bean Flyway managementFlyway(HikariDataSource dataSource) {
        return Flyway.configure().dataSource(dataSource).defaultSchema("ddip_management").schemas("ddip_management")
                .locations("classpath:db/migration").cleanDisabled(true).baselineOnMigrate(false).outOfOrder(false)
                .ignoreMigrationPatterns(new String[0]).validateMigrationNaming(true).lockRetryCount(0).connectRetries(0).load();
    }
    @Bean SchemaLifecycle schemaLifecycle(Flyway flyway,JdbcTemplate jdbc,JdbcCoordinatorRepository coordinator) {
        return new SchemaLifecycle(flyway,jdbc,coordinator);
    }
    @Bean SmartInitializingSingleton databaseReadiness(SchemaLifecycle schema,Environment environment) {
        return () -> {
            RuntimeRole role=RuntimeRole.from(environment);
            if(role==RuntimeRole.WORKER) validateWorkerPaths(environment);
            schema.verify(role==RuntimeRole.MIGRATE);
        };
    }
    private static void validateWorkerPaths(Environment e) {
        try {
            Path python=Path.of(e.getProperty("ddip.worker.python",""));
            Path javaHome=Path.of(e.getProperty("ddip.worker.java-home",""));
            Path processing=Path.of(e.getProperty("ddip.worker.processing-root",""));
            if(!python.isAbsolute() || !Files.isRegularFile(python) || !Files.isExecutable(python) ||
                    !javaHome.isAbsolute() || !Files.isExecutable(javaHome.resolve("bin/java")) || !processing.isAbsolute() ||
                    !List.of("content_publication.py","reliability_format_publication.py").stream().allMatch(p -> Files.isRegularFile(processing.resolve(p))))
                throw new IllegalArgumentException();
        } catch(RuntimeException ex) { throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_CONFIGURATION_INVALID); }
    }
}
