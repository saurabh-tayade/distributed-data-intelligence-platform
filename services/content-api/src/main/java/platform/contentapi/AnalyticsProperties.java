package platform.contentapi;

import java.nio.file.Path;
import java.time.Duration;
import jakarta.validation.constraints.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("analytics")
@Validated
public record AnalyticsProperties(
        @DefaultValue("python3") @NotBlank String pythonExecutable,
        @DefaultValue("processing/gold_analytics.py") @NotNull Path script,
        @DefaultValue("outputs/gold") @NotNull Path goldRoot,
        @DefaultValue("") @NotNull @Pattern(regexp = "(?:[0-9a-f]{64})?") String publication,
        @DefaultValue("30s") @NotNull Duration timeout,
        @DefaultValue("2") @Min(1) @Max(32) int maxProcesses,
        @DefaultValue("1048576") @Min(128) @Max(67108864) int maxStdoutBytes,
        @DefaultValue("65536") @Min(128) @Max(1048576) int maxStderrBytes) {
    @AssertTrue(message = "analytics.timeout must be between 1ms and 5m")
    public boolean isTimeoutBounded() {
        return timeout != null && timeout.compareTo(Duration.ofMillis(1)) >= 0
                && timeout.compareTo(Duration.ofMinutes(5)) <= 0;
    }
}
