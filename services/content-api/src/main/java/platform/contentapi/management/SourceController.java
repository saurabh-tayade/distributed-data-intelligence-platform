package platform.contentapi.management;

import java.io.IOException;
import java.util.*;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.context.annotation.Conditional;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.JsonNode;

@RestController
@RequestMapping("/api/management/v1")
@Conditional(ManagementConfiguration.ApiOnly.class)
public final class SourceController {
    private final SourceRepository repository;private final ReviewedSourceCatalog catalog;
    public SourceController(SourceRepository repository,ReviewedSourceCatalog catalog) {this.repository=repository;this.catalog=catalog;}
    private static void query(HttpServletRequest r,String... allowed) {
        Set<String> keys=Set.of(allowed);r.getParameterMap().forEach((k,v) -> SourceFailure.require(keys.contains(k) && v.length==1));
    }
    private static int limit(HttpServletRequest r) {
        String value=r.getParameter("limit");if(value==null)return 20;
        SourceFailure.require(value.matches("[1-9][0-9]{0,2}"));int limit=Integer.parseInt(value);SourceFailure.require(limit<=100);return limit;
    }
    @GetMapping("/capabilities") ResponseEntity<byte[]> capabilities(HttpServletRequest r) {
        query(r);return SourceResponses.response(catalog.capabilities(),200,null,null,r);
    }
    @GetMapping("/sources") ResponseEntity<byte[]> list(HttpServletRequest r) {
        query(r,"productId","enabled","limit","cursor");String enabled=r.getParameter("enabled");
        SourceFailure.require(enabled==null || enabled.equals("true") || enabled.equals("false"));
        var page=repository.sources(r.getParameter("productId"),enabled==null?null:Boolean.valueOf(enabled),limit(r),r.getParameter("cursor"));
        return SourceResponses.response(page.data(),200,null,page,r);
    }
    @GetMapping("/sources/{product}/{source}") ResponseEntity<byte[]> get(@PathVariable String product,@PathVariable String source,HttpServletRequest r) {
        query(r);JsonNode data=repository.get(product,source);return SourceResponses.response(data,200,"\""+data.get("rowVersion").stringValue()+"\"",null,r);
    }
    @GetMapping("/sources/{product}/{source}/revisions") ResponseEntity<byte[]> revisions(@PathVariable String product,@PathVariable String source,HttpServletRequest r) {
        query(r,"limit","cursor");var page=repository.revisions(product,source,limit(r),r.getParameter("cursor"));return SourceResponses.response(page.data(),200,null,page,r);
    }
    @PostMapping("/sources") ResponseEntity<byte[]> create(HttpServletRequest r) throws IOException {return command(SourceCommands.Action.CREATE,null,null,r);}
    @PostMapping("/sources/{product}/{source}/revisions") ResponseEntity<byte[]> revise(@PathVariable String product,@PathVariable String source,HttpServletRequest r) throws IOException {return command(SourceCommands.Action.REVISE,product,source,r);}
    @PutMapping("/sources/{product}/{source}/enabled") ResponseEntity<byte[]> enabled(@PathVariable String product,@PathVariable String source,HttpServletRequest r) throws IOException {return command(SourceCommands.Action.ENABLE,product,source,r);}
    private ResponseEntity<byte[]> command(SourceCommands.Action action,String product,String source,HttpServletRequest r) throws IOException {
        query(r);JsonNode body=SourceJson.parse(r.getInputStream().readNBytes(65537));
        var command=SourceCommands.command(action,product,source,body,ManagementBoundary.header(r,"Idempotency-Key"),ManagementBoundary.header(r,"If-Match"));
        var result=repository.execute(command,SourceResponses.correlation(r));
        return SourceResponses.response(result.data(),result.status(),result.etag(),null,r);
    }
    @RequestMapping("/**") ResponseEntity<byte[]> absent() {throw new SourceFailure(SourceFailure.Code.NOT_FOUND);}
}
