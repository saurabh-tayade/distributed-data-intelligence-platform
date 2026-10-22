package platform.contentapi.management;

import java.io.IOException;
import java.util.*;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE+20)
@Conditional(ManagementConfiguration.ApiOnly.class)
public final class ManagementBoundary extends OncePerRequestFilter {
    public static final String CORRELATION=ManagementBoundary.class.getName()+".correlation";
    private final ManagementConfiguration.LocalPolicy policy;
    public ManagementBoundary(ManagementConfiguration.LocalPolicy policy) {this.policy=policy;}
    @Override protected boolean shouldNotFilter(HttpServletRequest r) {return !r.getRequestURI().startsWith("/api/management/");}
    static String header(HttpServletRequest r,String name) {
        var values=Collections.list(r.getHeaders(name));SourceFailure.require(values.size()<=1);
        return values.isEmpty()?null:values.getFirst();
    }
    @Override protected void doFilterInternal(HttpServletRequest r,HttpServletResponse response,FilterChain chain) throws ServletException,IOException {
        r.setAttribute(CORRELATION,UUID.randomUUID().toString());
        try {
            String host=header(r,"Host"),origin=header(r,"Origin");
            boolean mutation=Set.of("POST","PUT","PATCH","DELETE").contains(r.getMethod());
            if(host==null || !policy.hosts().contains(host) || origin!=null && !policy.origins().contains(origin) || r.getMethod().equals("OPTIONS"))
                throw new SourceFailure(SourceFailure.Code.LOCAL_BOUNDARY_REJECTED);
            if(!r.getRequestURI().startsWith("/api/management/v1/")) throw new SourceFailure(SourceFailure.Code.NOT_FOUND);
            if(mutation) {
                if(origin==null || !"1".equals(header(r,"X-DDIP-Local-Request"))) throw new SourceFailure(SourceFailure.Code.LOCAL_BOUNDARY_REJECTED);
                String type=header(r,"Content-Type");
                try {
                    MediaType m=MediaType.parseMediaType(type==null?"":type);
                    if(!m.getType().equals("application") || !m.getSubtype().equals("json") ||
                            m.getParameters().keySet().stream().anyMatch(p -> !p.equalsIgnoreCase("charset")) ||
                            m.getCharset()!=null && !m.getCharset().equals(java.nio.charset.StandardCharsets.UTF_8))
                        throw new IllegalArgumentException();
                } catch(RuntimeException ex) {throw new SourceFailure(SourceFailure.Code.LOCAL_BOUNDARY_REJECTED);}
                if(header(r,"Content-Encoding")!=null) throw new SourceFailure(SourceFailure.Code.INVALID_ARGUMENT);
                if(r.getContentLengthLong()>65536) throw new SourceFailure(SourceFailure.Code.INVALID_ARGUMENT);
            }
            chain.doFilter(r,response);
        } catch(SourceFailure ex) {
            response.setStatus(ex.code.status);response.setContentType("application/json");response.setCharacterEncoding("UTF-8");
            response.setHeader("Cache-Control","no-store");response.getOutputStream().write(SourceResponses.error(ex.code,r));
        }
    }
}
