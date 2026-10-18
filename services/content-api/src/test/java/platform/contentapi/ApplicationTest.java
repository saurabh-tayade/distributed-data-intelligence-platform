package platform.contentapi;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(classes = ContentApiApplication.class, properties = "analytics.publication=")
@AutoConfigureMockMvc
class ApplicationTest {
    @Autowired MockMvc mvc;

    @Test
    void livenessDoesNotRequireRuntimeOrPublication() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk()).andExpect(content().json("{\"status\":\"UP\"}"))
                .andExpect(jsonPath("$.groups").doesNotExist()).andExpect(jsonPath("$.components").doesNotExist());
        mvc.perform(get("/api/v1/publishers")).andExpect(status().isServiceUnavailable())
                .andExpect(content().json("{\"code\":\"PUBLICATION_UNAVAILABLE\"}"));
        mvc.perform(get("/actuator/env")).andExpect(status().isNotFound());
        mvc.perform(get("/actuator/health/readiness")).andExpect(status().isNotFound());
        for (String report : new String[]{"articles", "daily", "quality"}) {
            mvc.perform(get("/api/v1/" + report)).andExpect(status().isServiceUnavailable())
                    .andExpect(content().json("{\"code\":\"PUBLICATION_UNAVAILABLE\"}"));
        }
        mvc.perform(get("/api/v1/publication")).andExpect(status().isNotFound());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(AnalyticsProperties.class)
    static class PropertiesConfiguration { }

    @ParameterizedTest
    @ValueSource(strings = {"analytics.publication=latest", "analytics.publication=../private", "analytics.timeout=0s",
            "analytics.timeout=6m", "analytics.max-processes=0", "analytics.max-processes=33",
            "analytics.max-stdout-bytes=0", "analytics.max-stderr-bytes=0", "analytics.python-executable="})
    void invalidConfigurationFailsInitialization(String property) {
        new ApplicationContextRunner().withUserConfiguration(PropertiesConfiguration.class).withPropertyValues(property)
                .run(context -> assertThat(context).hasFailed());
    }
}
