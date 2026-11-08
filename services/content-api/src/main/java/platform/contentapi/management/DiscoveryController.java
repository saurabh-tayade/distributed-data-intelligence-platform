package platform.contentapi.management;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.context.annotation.Conditional;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/** Read-only selection metadata; POST /runs remains authoritative. */
@RestController
@RequestMapping("/api/management/v1")
@Conditional(ManagementConfiguration.ApiOnly.class)
public final class DiscoveryController {
    private final DiscoveryRepository repository;
    public DiscoveryController(DiscoveryRepository repository){this.repository=repository;}
    private static int query(HttpServletRequest r,String... filters) {
        var allowed=new java.util.HashSet<>(java.util.List.of(filters));allowed.addAll(java.util.Set.of("limit","cursor"));
        r.getParameterMap().forEach((k,v)->SourceFailure.require(allowed.contains(k) && v.length==1));
        String text=r.getParameter("limit");if(text==null)return 20;
        SourceFailure.require(text.matches("[1-9][0-9]{0,2}"));int limit=Integer.parseInt(text);SourceFailure.require(limit<=100);return limit;
    }
    @GetMapping("/execution-profiles") ResponseEntity<byte[]> profiles(HttpServletRequest r) {
        int limit=query(r,"pipeline");String pipeline=r.getParameter("pipeline");
        var page=repository.profiles(pipeline==null?"RELIABILITY_V2":pipeline,limit,r.getParameter("cursor"));return SourceResponses.response(page.data(),200,null,page,r);
    }
    @GetMapping("/execution-profile-sources") ResponseEntity<byte[]> sources(HttpServletRequest r) {
        int limit=query(r,"executionProfileId");
        var page=repository.sources(r.getParameter("executionProfileId"),limit,r.getParameter("cursor"));
        return SourceResponses.response(page.data(),200,null,page,r);
    }
    @GetMapping("/artifacts") ResponseEntity<byte[]> artifacts(HttpServletRequest r) {
        int limit=query(r,"executionProfileId","role","sourceId");
        var page=repository.artifacts(r.getParameter("executionProfileId"),r.getParameter("role"),r.getParameter("sourceId"),limit,r.getParameter("cursor"));
        return SourceResponses.response(page.data(),200,null,page,r);
    }
}
