package platform.contentapi.management;

import org.springframework.jdbc.core.JdbcTemplate;

/** Called with coordinator locked. Reserve future bounded lifecycle events before accepting more metadata work. */
public final class AuditCapacity {
    private AuditCapacity() { }
    public static final long EVENTS_PER_UNFINISHED_RUN=4096;
    public static void requireAvailable(JdbcTemplate jdbc,long events,long unfinished) {
        if(!available(jdbc,events,unfinished))throw new SourceFailure(SourceFailure.Code.MANAGEMENT_CAPACITY_EXCEEDED);
    }
    public static boolean available(JdbcTemplate jdbc,long events,long unfinished) {
        long count=jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class);
        long imports=jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifact_import_operations WHERE state='ACTIVE'",Long.class);
        return allows(count,events,unfinished,imports);
    }
    static boolean allows(long count,long events,long unfinished,long imports) {
        return count>=0 && events>=0 && unfinished>=0 && imports>=0 && count+events+unfinished*EVENTS_PER_UNFINISHED_RUN+imports*3<=1000000;
    }
}
