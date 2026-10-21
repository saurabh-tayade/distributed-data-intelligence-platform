package platform.contentapi.execution;

import org.springframework.core.env.Environment;

public enum RuntimeRole {
    ANALYTICS, API, WORKER, MIGRATE;

    public static RuntimeRole from(Environment environment) {
        return switch (environment.getProperty("ddip.role", "analytics")) {
            case "analytics" -> ANALYTICS;
            case "api" -> API;
            case "worker" -> WORKER;
            case "migrate" -> MIGRATE;
            default -> throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_CONFIGURATION_INVALID);
        };
    }
    public boolean databaseRequired() { return this != ANALYTICS; }
    public boolean analyticsEnabled() { return this == ANALYTICS || this == API; }
}
