package platform.contentapi.execution;

import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import platform.contentapi.AnalyticsProperties;

@Configuration(proxyBeanMethods = false)
@AnalyticsRole
@EnableConfigurationProperties(AnalyticsProperties.class)
public class AnalyticsConfiguration { }
