package platform.contentapi.execution;

import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test-classpath bridge exposing observations only, never runtime launch authority. */
public final class NarrowContentInstalledRuntimeTestSupport {
    public record Facts(UUID run, UUID attempt, boolean installedArtifactsVerified,
                        boolean nativeLaunchCompatible, boolean runtimeVersionsVerified,
                        boolean strictSinkCompatible, int artifacts, int roots, int executables) { }

    public static Facts inspect(JdbcTemplate jdbc, ManagementTransactions tx, InstallationHostGuard guard,
                                UUID run, UUID attempt) {
        var observation=NarrowContentInstalledRuntimeQualification.inspect(jdbc,tx,guard,run,attempt);
        return new Facts(observation.ready().run(),observation.ready().attempt(),
                observation.installedArtifactsVerified(),observation.nativeLaunchCompatible(),
                observation.runtimeVersionsVerified(),observation.strictSinkCompatible(),
                observation.artifacts().size(),observation.roots().size(),observation.executables().size());
    }

    public static void inspectWithBoundary(JdbcTemplate jdbc, ManagementTransactions tx, InstallationHostGuard guard,
                                           UUID run, UUID attempt, java.util.function.Consumer<String> afterInstalled) {
        NarrowContentInstalledRuntimeQualification.inspect(jdbc,tx,guard,run,attempt,boundary->afterInstalled.accept(boundary.name()));
    }

    public static List<String> modules(String family) {
        return ContentEvidenceProfile.modules(switch(family) {
            case "silver" -> RssSilverRuntime.MODULES;
            case "gold" -> RssGoldRuntime.MODULES;
            case "content" -> RssContentRuntime.MODULES;
            default -> throw new IllegalArgumentException(family);
        },2);
    }
    private NarrowContentInstalledRuntimeTestSupport() { }
}
