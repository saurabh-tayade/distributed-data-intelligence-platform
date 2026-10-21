package platform.contentapi.management;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.*;
import tools.jackson.databind.JsonNode;

final class SourceResponses {
    private SourceResponses() { }
    static String correlation(HttpServletRequest r) {return (String)r.getAttribute(ManagementBoundary.CORRELATION);}
    static byte[] error(SourceFailure.Code code,HttpServletRequest r) {
        return SourceJson.canonical(SourceJson.object().put("apiVersion",1).put("code",code.name()).put("message",code.message)
                .put("correlationId",correlation(r)).put("retryable",code.retryable));
    }
    static ResponseEntity<byte[]> response(JsonNode data,int status,String etag,SourceRepository.Page page,HttpServletRequest r) {
        var root=SourceJson.object().put("apiVersion",1);root.set("data",data);
        var meta=root.putObject("meta").put("correlationId",correlation(r));
        if(page!=null) {meta.put("limit",page.limit()).put("hasMore",page.nextCursor()!=null);if(page.nextCursor()==null)meta.putNull("nextCursor");else meta.put("nextCursor",page.nextCursor());}
        byte[] bytes=SourceJson.canonical(root);
        if(bytes.length>1048576) throw new SourceFailure(SourceFailure.Code.INTERNAL_FAILURE);
        var response=ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).cacheControl(CacheControl.noStore());
        if(etag!=null) response.header("ETag",etag);
        return response.body(bytes);
    }
}
