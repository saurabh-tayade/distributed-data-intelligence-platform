package platform.contentapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice(assignableTypes = {PublisherController.class, AnalyticsController.class})
public class ApiExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ApiExceptionHandler.class);
    public record ErrorResponse(String code) { }

    @ExceptionHandler(ApiFailure.class)
    public ResponseEntity<ErrorResponse> failure(ApiFailure failure) {
        return ResponseEntity.status(failure.code().status()).body(new ErrorResponse(failure.code().name()));
    }

    @ExceptionHandler({MethodArgumentTypeMismatchException.class, HandlerMethodValidationException.class})
    public ResponseEntity<ErrorResponse> invalid(Exception ignored) {
        return failure(new ApiFailure(ApiFailure.Code.INVALID_ARGUMENT));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> unexpected(Exception ignored) {
        LOG.error("report=analytics outcome=ANALYTICS_EXECUTION_FAILED");
        return failure(new ApiFailure(ApiFailure.Code.ANALYTICS_EXECUTION_FAILED));
    }
}
