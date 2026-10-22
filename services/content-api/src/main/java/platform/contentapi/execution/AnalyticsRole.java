package platform.contentapi.execution;

import java.lang.annotation.*;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Conditional(AnalyticsRole.Enabled.class)
public @interface AnalyticsRole {
    final class Enabled implements Condition {
        @Override public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return RuntimeRole.from(context.getEnvironment()).analyticsEnabled();
        }
    }
}
