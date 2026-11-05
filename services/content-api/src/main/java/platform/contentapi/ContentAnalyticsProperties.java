package platform.contentapi;

import java.nio.file.Path;
import jakarta.validation.constraints.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/** Content and its retained dependencies are selected only by the trusted operator. */
@ConfigurationProperties("content.analytics")
@Validated
public record ContentAnalyticsProperties(
        @DefaultValue("python3") @NotBlank String pythonExecutable,
        @DefaultValue("processing") @NotNull Path processingRoot,
        @DefaultValue("outputs/content") @NotNull Path root,
        @DefaultValue("outputs/gold") @NotNull Path goldRoot,
        @DefaultValue("outputs/publisher-reference") @NotNull Path referenceRoot,
        @DefaultValue("") @NotNull @Pattern(regexp = "(?:[0-9a-f]{64})?") String publication) { }
