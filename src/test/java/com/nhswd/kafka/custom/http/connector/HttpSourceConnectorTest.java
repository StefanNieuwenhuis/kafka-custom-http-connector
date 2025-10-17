package com.nhswd.kafka.custom.http.connector;

import com.nhswd.kafka.custom.http.connector.config.HttpSourceConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class HttpSourceConnectorTest {
    private final HttpSourceConnector httpSourceConnector = new HttpSourceConnector();

    @Test
    void version_withConnector_returnsExpectedVersion() {
        String version = httpSourceConnector.version();

        assertThat(version).isEqualTo(HttpSourceConfig.VERSION);
    }

    @Test
    void config_withConnector_returnsNonNullConfigDef(){
        ConfigDef configDef = httpSourceConnector.config();

        assertThat(configDef).isNotNull();
    }

    @Test
    void taskClass_with_connector_returnsHttpSourceTaskClass() {
        Class<? extends Task> taskClass = httpSourceConnector.taskClass();

        assertThat(taskClass).isEqualTo(HttpSourceTask.class);
    }

    @Test
    void startAndTaskConfigs_withValidProps_returnsConfigForMaxTasks() {
        Map<String, String> configProps = new HashMap<>();
        configProps.put("http.url", "http://example.com/api");
        configProps.put("topic", "test-topic");

        httpSourceConnector.start(configProps);

        int maxTasks = 3;
        List<Map<String, String>> taskConfigs = httpSourceConnector.taskConfigs(maxTasks);

        assertThat(taskConfigs).hasSize(maxTasks);

        for (Map<String, String> taskConfig: taskConfigs) {
            assertThat(taskConfig).isEqualTo(configProps);
        }
    }
}
