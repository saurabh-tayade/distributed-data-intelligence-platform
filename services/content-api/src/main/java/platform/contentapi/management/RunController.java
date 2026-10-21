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
    public RunController(RunRepository repository){this.repository=repository;}
    private static void query(HttpServletRequest r){SourceFailure.require(r.getParameterMap().isEmpty());}
    @PostMapping ResponseEntity<byte[]> submit(HttpServletRequest r) throws IOException {
        query(r);SourceFailure.require(ManagementBoundary.header(r,"If-Match")==null);
        var c=RunCommands.parse(r.getInputStream().readNBytes(65537),ManagementBoundary.header(r,"Idempotency-Key"));
        var result=repository.submit(c,SourceResponses.correlation(r));
        var response=SourceResponses.response(result,202,"\""+result.get("rowVersion").stringValue()+"\"",null,r);
        return ResponseEntity.status(202).headers(response.getHeaders()).header("Location","/api/management/v1/runs/"+result.get("runId").stringValue()).body(response.getBody());
    }
    @GetMapping("/{id}") ResponseEntity<byte[]> get(@PathVariable String id,HttpServletRequest r) {
        query(r);var result=repository.get(id);return SourceResponses.response(result,200,"\""+result.get("rowVersion").stringValue()+"\"",null,r);
    }
}
