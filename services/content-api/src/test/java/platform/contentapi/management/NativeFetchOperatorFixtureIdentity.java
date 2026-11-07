package platform.contentapi.management;

import java.sql.Timestamp;

/** Isolated fixture INSERT shaping; never changes already-recorded worker identity. */
final class NativeFetchOperatorFixtureIdentity {
    static final String INSERT="INSERT INTO ddip_management.worker_instances(instance_id,installation_id,execution_profile_id,profile_sha256,host_binding_sha256,pid,process_started_at,heartbeat_at,status) VALUES (?,?,?,?,?,?,?,clock_timestamp(),?)";
    private NativeFetchOperatorFixtureIdentity() { }
    static Object[] atInsert(String sql,Object[] arguments,long pid,Timestamp started) {
        if(!INSERT.equals(sql) || arguments.length!=8 || !(arguments[5] instanceof Long)
                || !(arguments[6] instanceof Timestamp) || pid<=0 || started==null)
            throw new IllegalArgumentException("Fixture worker registration INSERT shape changed");
        var result=arguments.clone();result[5]=pid;result[6]=started;return result;
    }
}
