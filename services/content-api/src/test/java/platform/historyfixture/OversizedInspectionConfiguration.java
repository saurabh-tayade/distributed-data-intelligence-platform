package platform.historyfixture;

import org.springframework.context.annotation.*;
import org.springframework.web.servlet.config.annotation.*;
import org.springframework.web.servlet.HandlerInterceptor;
import jakarta.servlet.http.*;

/** Explicit test-only interceptor outside the application's scan. Exercises the real HTTP error boundary. */
@Configuration(proxyBeanMethods=false)
public class OversizedInspectionConfiguration implements WebMvcConfigurer {
    @Override public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new HandlerInterceptor(){
            @Override public boolean preHandle(HttpServletRequest request,HttpServletResponse response,Object handler)throws Exception {
                platform.contentapi.management.RunInspectionTest.oversized(request);
                throw new AssertionError("Expected response limit failure");
            }
        }).addPathPatterns("/api/management/v1/runs");
    }
}
