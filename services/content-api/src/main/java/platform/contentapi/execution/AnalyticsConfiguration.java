package platform.contentapi.execution;

import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import platform.contentapi.AnalyticsProperties;
import platform.contentapi.ReliabilityAnalyticsProperties;
import platform.contentapi.ContentAnalyticsProperties;

@Configuration(proxyBeanMethods = false)
@AnalyticsRole
@EnableConfigurationProperties({AnalyticsProperties.class, ReliabilityAnalyticsProperties.class, ContentAnalyticsProperties.class})
public class AnalyticsConfiguration { }
