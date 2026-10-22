package platform.contentapi.management;

import java.io.IOException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.context.annotation.Conditional;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/management/v1/runs")
@Conditional(ManagementConfiguration.ApiOnly.class)
public final class RunController {
    private final RunRepository repository;
    private final RunHistoryRepository history;
    public RunController(RunRepository repository,RunHistoryRepository history){this.repository=repository;this.history=history;}
    private static void query(HttpServletRequest r){SourceFailure.require(r.getParameterMap().isEmpty());}
    @PostMapping ResponseEntity<byte[]> submit(HttpServletRequest r) throws IOException {
        query(r);SourceFailure.require(ManagementBoundary.header(r,"If-Match")==null);
        var c=RunCommands.parse(r.getInputStream().readNBytes(65537),ManagementBoundary.header(r,"Idempotency-Key"));
        var result=repository.submit(c,SourceResponses.correlation(r));
        var response=SourceResponses.response(result,202,"\""+result.get("rowVersion").stringValue()+"\"",null,r);
        return ResponseEntity.status(202).headers(response.getHeaders()).header("Location","/api/management/v1/runs/"+result.get("runId").stringValue()).body(response.getBody());
    }
    private static int listQuery(HttpServletRequest r,String... filters) {
        var allowed=new java.util.HashSet<>(java.util.List.of(filters));allowed.addAll(java.util.Set.of("limit","cursor"));
        r.getParameterMap().forEach((k,v)->SourceFailure.require(allowed.contains(k) && v.length==1));
        String text=r.getParameter("limit");if(text==null)return 20;SourceFailure.require(text.matches("[1-9][0-9]{0,2}"));int limit=Integer.parseInt(text);SourceFailure.require(limit<=100);return limit;
    }
    @GetMapping ResponseEntity<byte[]> list(HttpServletRequest r) {
        int limit=listQuery(r,"productId","sourceId","state");var page=history.runs(r.getParameter("productId"),r.getParameter("sourceId"),r.getParameter("state"),limit,r.getParameter("cursor"));return SourceResponses.response(page.data(),200,null,page,r);
    }
    @GetMapping("/{id}/attempts") ResponseEntity<byte[]> attempts(@PathVariable String id,HttpServletRequest r) {
        var page=history.attempts(id,listQuery(r),r.getParameter("cursor"));return SourceResponses.response(page.data(),200,null,page,r);
    }
    @GetMapping("/{id}/events") ResponseEntity<byte[]> events(@PathVariable String id,HttpServletRequest r) {
        var page=history.events(id,listQuery(r),r.getParameter("cursor"));return SourceResponses.response(page.data(),200,null,page,r);
    }
    @GetMapping("/{id}") ResponseEntity<byte[]> get(@PathVariable String id,HttpServletRequest r) {
        query(r);var result=history.get(id);return SourceResponses.response(result,200,"\""+result.get("rowVersion").stringValue()+"\"",null,r);
    }
}
