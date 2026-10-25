package platform.contentapi.execution;

import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/** Applied before context selection, including applications launched outside main(). */
public final class RuntimeEnvironment implements EnvironmentPostProcessor, Ordered {
    @Override public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        RuntimeRole role = RuntimeRole.from(environment);
        if (role == RuntimeRole.WORKER || role == RuntimeRole.MIGRATE || role == RuntimeRole.OPERATOR_IMPORT || role == RuntimeRole.OPERATOR_RECOVERY) {
            environment.getPropertySources().addFirst(new MapPropertySource("ddipNonWebRole",
                    Map.of("spring.main.web-application-type", "none")));
        }
        if (role == RuntimeRole.OPERATOR_RECOVERY) {
            environment.getPropertySources().addFirst(new MapPropertySource("ddipRecoveryDiagnostics",
                    Map.of("logging.level.root", "OFF", "spring.main.banner-mode", "off")));
        }
    }
    @Override public int getOrder() { return Ordered.LOWEST_PRECEDENCE; }
}
