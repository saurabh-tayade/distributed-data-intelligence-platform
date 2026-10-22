package platform.contentapi.execution;

import java.util.UUID;
import org.springframework.core.env.Environment;

public record DatabaseSettings(String url, String username, String password, UUID installationId) {
    public static DatabaseSettings from(Environment env) {
        String url = env.getProperty("ddip.database.url", "");
        String user = env.getProperty("ddip.database.username", "");
        String password = env.getProperty("ddip.database.password", "");
        try {
            String identity = env.getProperty("ddip.installation-id", "");
            UUID installation = UUID.fromString(identity);
            if (!installation.toString().equals(identity) ||
                    !url.matches("jdbc:postgresql://(127\\.0\\.0\\.1|localhost|\\[::1\\]):[0-9]{1,5}/[A-Za-z0-9_-]{1,63}") ||
                    !user.matches("[A-Za-z_][A-Za-z0-9_]{0,62}") || password.isBlank() || password.length() > 1024) {
                throw new IllegalArgumentException();
            }
            int port = Integer.parseInt(url.substring(url.lastIndexOf(':') + 1, url.lastIndexOf('/')));
            if (port < 1 || port > 65535) throw new IllegalArgumentException();
            return new DatabaseSettings(url,user,password,installation);
        } catch (IllegalArgumentException ex) {
            throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_CONFIGURATION_INVALID);
        }
    }
    @Override public String toString() { return "DatabaseSettings[redacted]"; }
}
