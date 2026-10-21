package platform.contentapi.management;

import java.net.URI;
import java.util.*;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.*;

@Configuration(proxyBeanMethods=false)
@Conditional(ManagementConfiguration.ApiOnly.class)
public class ManagementConfiguration {
    public static final class ApiOnly implements Condition {
        @Override public boolean matches(ConditionContext c,AnnotatedTypeMetadata m) {return RuntimeRole.from(c.getEnvironment())==RuntimeRole.API;}
    }
    @Bean ReviewedSourceCatalog reviewedSourceCatalog(Environment e) {return ReviewedSourceCatalog.load(e.getProperty("ddip.management.catalog",""));}
    @Bean SourceCursor sourceCursor(Environment e) {return new SourceCursor(e.getProperty("ddip.management.cursor-key",""));}
    @Bean SourceRepository sourceRepository(JdbcTemplate jdbc,ManagementTransactions tx,DatabaseSettings settings,ReviewedSourceCatalog catalog,SourceCursor cursor) {
        return new SourceRepository(jdbc,tx,settings.installationId(),catalog,cursor);
    }
    @Bean RunRepository runRepository(JdbcTemplate jdbc,ManagementTransactions tx,DatabaseSettings settings,ReviewedSourceCatalog catalog) {
        return new RunRepository(jdbc,tx,settings.installationId(),catalog);
    }
    @Bean LocalPolicy localPolicy(Environment e) {
        try {
            String address=e.getProperty("server.address","127.0.0.1");SourceFailure.require(Set.of("127.0.0.1","::1","localhost").contains(address));
            String origins=e.getProperty("ddip.management.origins","http://127.0.0.1:"+e.getProperty("server.port","8080"));
            Set<String> allowed=new HashSet<>(), hosts=new HashSet<>();
            SourceFailure.require(origins.length()<=2048);
            for(String o:origins.split(",",-1)) {
                URI u=URI.create(o);String host=u.getHost();
                SourceFailure.require(host!=null && Set.of("127.0.0.1","localhost","[::1]").contains(host) &&
                        Set.of("http","https").contains(u.getScheme()) && u.getPort()>0 && u.getPort()<=65535 &&
                        (u.getRawPath()==null || u.getRawPath().isEmpty()) && u.getQuery()==null && u.getFragment()==null && u.getUserInfo()==null);
                SourceFailure.require(allowed.add(o));hosts.add(u.getRawAuthority());
            }
            SourceFailure.require(allowed.size()<=8);return new LocalPolicy(Set.copyOf(allowed),Set.copyOf(hosts));
        } catch(RuntimeException ex) {throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_CONFIGURATION_INVALID);}
    }
    record LocalPolicy(Set<String> origins,Set<String> hosts) { }
}
