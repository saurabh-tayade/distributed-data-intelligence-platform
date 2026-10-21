package platform.contentapi.execution;

import java.util.UUID;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.mock.env.MockEnvironment;
import platform.contentapi.*;
import static org.assertj.core.api.Assertions.*;

class RuntimeRoleTest {
    private MockEnvironment configuration() {
        return new MockEnvironment().withProperty("ddip.database.url","jdbc:postgresql://127.0.0.1:15432/ddip")
                .withProperty("ddip.database.username","local_test").withProperty("ddip.database.password","PRIVATE-test-secret")
                .withProperty("ddip.installation-id",UUID.randomUUID().toString());
    }
    @Test void analyticsStartsWithoutDatabaseEvenWithInvalidDatabaseConfiguration() {
        try(var context=new SpringApplicationBuilder(ContentApiApplication.class).web(WebApplicationType.NONE).run(
                "--spring.main.banner-mode=off","--logging.level.root=OFF","--ddip.role=analytics",
                "--ddip.database.url=invalid-private-url","--ddip.database.password=PRIVATE-test-secret")) {
            assertThat(context.getBeansOfType(DataSource.class)).isEmpty();
            assertThat(context.getBeansOfType(Flyway.class)).isEmpty();
            assertThat(context.getBeansOfType(CoordinatorRepository.class)).isEmpty();
            assertThat(context.getBeansOfType(WorkerIdleLifecycle.class)).isEmpty();
            assertThat(context.getBeansOfType(AnalyticsController.class)).hasSize(1);
            assertThat(context.getBeansOfType(PublisherController.class)).hasSize(1);
        }
    }
    @ParameterizedTest @ValueSource(strings={"", "postgres://localhost/db", "jdbc:postgresql://example.com:5432/ddip", "jdbc:postgresql://127.0.0.1:5432/ddip?password=secret", "jdbc:postgresql://127.0.0.1:0/ddip", "jdbc:postgresql://127.0.0.1:65536/ddip"})
    void invalidConnectionConfigurationHasOnlyFixedDiagnostic(String value) {
        var env=configuration().withProperty("ddip.database.url",value);
        assertThatThrownBy(() -> DatabaseSettings.from(env)).isInstanceOf(ManagementFailure.class)
                .hasMessage("MANAGEMENT_CONFIGURATION_INVALID").hasNoCause();
    }
    @Test void settingsNeverRenderSecrets() {
        assertThat(DatabaseSettings.from(configuration()).toString()).isEqualTo("DatabaseSettings[redacted]");
    }
    @ParameterizedTest @ValueSource(strings={"", "not-a-uuid", "10000000-0000-4000-8000-00000000000A"})
    void installationIdentityIsExplicitCanonicalUuid(String id) {
        assertThatThrownBy(() -> DatabaseSettings.from(configuration().withProperty("ddip.installation-id",id)))
                .hasMessage("MANAGEMENT_CONFIGURATION_INVALID").hasNoCause();
    }
    @ParameterizedTest @ValueSource(strings={"operator-import","management","unknown","WORKER",""})
    void unsupportedRolesAreNotAccidentallyAnalytics(String role) {
        assertThatThrownBy(() -> RuntimeRole.from(new MockEnvironment().withProperty("ddip.role",role)))
                .hasMessage("MANAGEMENT_CONFIGURATION_INVALID").hasNoCause();
    }
}
