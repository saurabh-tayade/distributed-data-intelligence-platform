package platform.contentapi.importing;

import java.nio.file.*;
import java.util.UUID;
import org.springframework.boot.*;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.*;
import platform.contentapi.management.SourceJson;

@Configuration(proxyBeanMethods=false)
@Conditional(OperatorImportConfiguration.Enabled.class)
public class OperatorImportConfiguration {
    static final class Enabled implements Condition {
        public boolean matches(ConditionContext c,AnnotatedTypeMetadata m){return RuntimeRole.from(c.getEnvironment())==RuntimeRole.OPERATOR_IMPORT;}
    }
    @Bean OperatorImportService operatorImportService(JdbcTemplate jdbc,ManagementTransactions tx,DatabaseSettings settings,ImportOwnershipRepository ownership,CoordinatorRepository coordinator) {
        return new OperatorImportService(new ImportBindings(jdbc,tx,settings.installationId()),ownership,coordinator,new NativeImportVerifier(new NativeImportProcess()),settings.installationId());
    }
    @Bean OperatorCommand operatorImportCommand(OperatorImportService service,Environment environment){return new OperatorCommand(service,environment);}
    public static final class OperatorCommand implements ApplicationRunner,ExitCodeGenerator {
        private final OperatorImportService service;private final Environment environment;private int exitCode=1;
        OperatorCommand(OperatorImportService service,Environment environment){this.service=service;this.environment=environment;}
        public void run(ApplicationArguments args) {
            tools.jackson.databind.JsonNode result;
            try {
                Path path=Path.of(environment.getProperty("ddip.import.catalog",""));
                ImportFailure.require(path.isAbsolute() && path.equals(path.normalize()) && path.equals(path.toRealPath()) && Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS),ImportFailure.Code.INVALID_CATALOG);
                // The catalog contains private approved addresses. It must belong to this operator and be private.
                ImportFailure.require(Files.getOwner(path).equals(Files.getOwner(Path.of(System.getProperty("user.home")))) &&
                        Files.getPosixFilePermissions(path).stream().noneMatch(p -> p.name().startsWith("GROUP_") || p.name().startsWith("OTHERS_")),ImportFailure.Code.INVALID_CATALOG);
                var catalog=ImportCatalog.load(path);
                result=switch(environment.getProperty("ddip.import.action","import")) {
                    case "import" -> service.execute(catalog);
                    case "inspect" -> service.inspect(catalog,target(catalog));
                    case "recover" -> service.recover(catalog,target(catalog),uuid(environment.getProperty("ddip.import.recovery-id","")),environment.getProperty("ddip.import.quiescence-confirmed",Boolean.class,false));
                    default -> throw new ImportFailure(ImportFailure.Code.INVALID_CATALOG);
                };
                String status=result.get("status").stringValue();
                exitCode=switch(status){case "VERIFIED","RECOVERY_CLOSED","HISTORICAL_RESULT"->0;case "RECOVERY_REQUIRED"->2;default->1;};
            } catch(ImportFailure f){exitCode=f.quiescent()?1:2;result=OperatorImportService.summary(null,f.quiescent()?"FAILED":"RECOVERY_REQUIRED").put("code",f.code().name());}
            catch(ImportOwnershipFailure | ManagementFailure f){exitCode=2;result=OperatorImportService.summary(null,"OWNERSHIP_UNRESOLVED").put("code",f.getMessage());}
            catch(Exception f){exitCode=2;result=OperatorImportService.summary(null,"OWNERSHIP_UNRESOLVED").put("code","OPERATOR_FAILURE");}
            System.out.println(new String(SourceJson.canonical(result),java.nio.charset.StandardCharsets.UTF_8));
        }
        private UUID target(ImportCatalog c){return uuid(environment.getProperty("ddip.import.target-id",environment.getProperty("DDIP_IMPORT_TARGET_ID",c.operationId().toString())));}
        private UUID uuid(String s){try{UUID id=UUID.fromString(s);ImportFailure.require(id.toString().equals(s),ImportFailure.Code.INVALID_CATALOG);return id;}catch(RuntimeException f){throw new ImportFailure(ImportFailure.Code.INVALID_CATALOG);}}
        public int getExitCode(){return exitCode;}
    }
}
