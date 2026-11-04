package platform.contentapi;

import java.nio.file.Path;
import jakarta.validation.constraints.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/** Operator-owned selection. HTTP callers cannot choose paths, scripts or publications. */
@ConfigurationProperties("reliability.analytics")
@Validated
public record ReliabilityAnalyticsProperties(
        @DefaultValue("python3") @NotBlank String pythonExecutable,
        @DefaultValue("processing") @NotNull Path processingRoot,
        @DefaultValue("outputs/reliability") @NotNull Path root,
        @DefaultValue("outputs/reliability-batches") @NotNull Path batchRoot,
        @DefaultValue("outputs/service-reference") @NotNull Path referenceRoot,
        @DefaultValue("") @NotNull @Pattern(regexp = "(?:[0-9a-f]{64})?") String publication) { }
