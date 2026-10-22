package platform.contentapi.management;

import java.io.IOException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.context.annotation.Conditional;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/management/v1/runs/{run}/controls")
@Conditional(ManagementConfiguration.ApiOnly.class)
public final class ControlController {
    private final ControlRepository repository;
    public ControlController(ControlRepository repository){this.repository=repository;}
    private static void query(HttpServletRequest request){SourceFailure.require(request.getParameterMap().isEmpty());}
    @PostMapping ResponseEntity<byte[]> request(@PathVariable String run,HttpServletRequest request)throws IOException {
        query(request);var command=ControlCommands.parse(run,request.getInputStream().readNBytes(65537),ManagementBoundary.header(request,"Idempotency-Key"),ManagementBoundary.header(request,"If-Match"));
        var data=repository.request(command);var response=SourceResponses.response(data,202,"\""+data.get("rowVersion").stringValue()+"\"",null,request);
        return ResponseEntity.status(202).headers(response.getHeaders()).header("Location","/api/management/v1/runs/"+data.get("runId").stringValue()+"/controls/"+data.get("controlId").stringValue()).body(response.getBody());
    }
    @GetMapping("/{control}") ResponseEntity<byte[]> get(@PathVariable String run,@PathVariable String control,HttpServletRequest request){query(request);var data=repository.get(run,control);return SourceResponses.response(data,200,"\""+data.get("rowVersion").stringValue()+"\"",null,request);}
}
