package platform.contentapi.management;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.context.annotation.Conditional;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import platform.contentapi.execution.ManagementFailure;

@RestControllerAdvice(assignableTypes={SourceController.class,RunController.class,ControlController.class})
@Conditional(ManagementConfiguration.ApiOnly.class)
public final class SourceAdvice {
    private ResponseEntity<byte[]> error(SourceFailure.Code code,HttpServletRequest request) {
        return ResponseEntity.status(code.status).contentType(MediaType.APPLICATION_JSON).cacheControl(CacheControl.noStore()).body(SourceResponses.error(code,request));
    }
    @ExceptionHandler(SourceFailure.class) ResponseEntity<byte[]> source(SourceFailure failure,HttpServletRequest request) {return error(failure.code,request);}
    @ExceptionHandler(ManagementFailure.class) ResponseEntity<byte[]> database(ManagementFailure failure,HttpServletRequest request) {
        return error(failure.code()==ManagementFailure.Code.STATE_CONFLICT?SourceFailure.Code.STATE_CONFLICT:SourceFailure.Code.MANAGEMENT_UNAVAILABLE,request);
    }
    @ExceptionHandler(Exception.class) ResponseEntity<byte[]> unexpected(Exception ignored,HttpServletRequest request) {return error(SourceFailure.Code.INTERNAL_FAILURE,request);}
}
