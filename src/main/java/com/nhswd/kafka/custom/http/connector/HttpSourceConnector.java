package com.nhswd.kafka.custom.http.connector;

import com.nhswd.kafka.custom.http.connector.config.HttpSourceConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.nhswd.kafka.custom.http.connector.config.HttpSourceConfig.VERSION;

public class HttpSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(HttpSourceConnector.class);

    private Map<String, String> configProps = null;

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting HTTPSourceConnector {}", props);
        configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(configProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("Stopping HttpSourceConnector");
    }

    @Override
    public ConfigDef config() {
        return HttpSourceConfig.getConfig();
    }

    @Override
    public String version() {
        return VERSION;
    }
}
