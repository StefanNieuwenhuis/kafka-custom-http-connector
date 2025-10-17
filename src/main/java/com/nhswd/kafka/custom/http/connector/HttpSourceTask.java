package com.nhswd.kafka.custom.http.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nhswd.kafka.custom.http.connector.config.HttpSourceConfig;
import com.nhswd.kafka.custom.http.connector.model.GitHubEvent;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

public class HttpSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(HttpSourceTask.class);

    private String url;
    private String method;
    private long pollIntervalMs;
    private String topic;
    private HttpApiClient apiClient;
    private Map<String, String> sourcePartition;
    private Map<String, Object> sourceOffset;
    private long lastPollTime = 0L;
    private Set<String> allowedEventTypes;

    private SourceRecord getSourceRecord(String payload, long currentTime) {
        log.info("Successfully fetched data. Payload size: {}", payload.length());
        return new SourceRecord(
                this.sourcePartition,
                Collections.singletonMap("last_polled_timestamp", currentTime),
                this.topic,
                Schema.STRING_SCHEMA,
                Instant.now().toString(),
                Schema.STRING_SCHEMA,
                payload
        );
    }

    @Override
    public String version() {
        return HttpSourceConfig.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting HttpSourceTask with properties: {}", props);

        try{
            HttpSourceConfig config = new HttpSourceConfig(props);
            this.url = config.getString(HttpSourceConfig.HTTP_URL);
            this.method = config.getString(HttpSourceConfig.HTTP_METHOD);
            this.pollIntervalMs = config.getInt(HttpSourceConfig.HTTP_POLL_INTERVAL_MS);
            this.topic = config.getString(HttpSourceConfig.TOPIC);

            this.apiClient = new HttpApiClient(config);

            this.allowedEventTypes = new HashSet<>(config.getList(HttpSourceConfig.HTTP_EVENT_TYPES));
            if (allowedEventTypes.isEmpty()) {
                log.info("No event types specified, all events will be accepted.");
            } else {
                log.info("Filtering for event types: {}", allowedEventTypes);
            }


            if (allowedEventTypes.isEmpty()) {
                log.info("No event types specified, all events will be accepted.");
            } else {
                log.info("Filtering for event types: {}", allowedEventTypes);
            }

            this.sourcePartition = Collections.singletonMap("url", this.url);
            this.sourceOffset = context.offsetStorageReader().offset(this.sourcePartition);

            if (this.sourceOffset != null) {
                log.info("Found persisted offset: {}", this.sourceOffset);
                Long lastPolledTimestamp = (Long) this.sourceOffset.get("last_polled_timestamp");
                if (lastPolledTimestamp != null) {
                    this.lastPollTime = lastPolledTimestamp;
                }
            } else {
                log.info("No previous offset found. Starting from scratch.");
            }
        } catch (ConfigException e) {
            throw new ConnectException("Invalid connector configuration.", e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        long currentTime = System.currentTimeMillis();

        if (currentTime - lastPollTime < pollIntervalMs) {
            long waitTime = pollIntervalMs - (currentTime - lastPollTime);
            log.info("Waiting for {} ms before next poll.", waitTime);
            Thread.sleep(waitTime);
            return Collections.emptyList();
        }

        try {
            String payload = this.apiClient.executeRequest(url, method);

            // Parse payload as JSON array
            ObjectMapper objectMapper = new ObjectMapper();
            GitHubEvent[] gitHubEvents = objectMapper.readValue(payload, GitHubEvent[].class);

            List<SourceRecord> recordList = new ArrayList<>();

            for (GitHubEvent event: gitHubEvents) {
                if (!allowedEventTypes.isEmpty() && !allowedEventTypes.contains(event.getType())) {
                    log.debug("Skipping event type {}", event.getType());
                    continue;
                }

                recordList.add(getSourceRecord(objectMapper.writeValueAsString(event), currentTime));
            }


            log.debug("Publishing {} events", recordList.size());
            this.lastPollTime = currentTime;

            return recordList;
        } catch (ConnectException e) {
            log.error("API client reported an unrecoverable error.", e);
            throw e;
        } catch (IOException e) {
            log.warn("An I/O error occurred during the HTTP request. This is likely temporary.", e);
            throw new RetriableException("I/O error during HTTP request.", e);
        } catch (Exception e) {
            log.error("An unexpected error occurred during the HTTP request.", e);
            throw new ConnectException("Unexpected error.", e);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping HttpSourceTask");
        if (this.apiClient != null) {
            try {
                this.apiClient.close();
            } catch (IOException e) {
                log.error("Failed to close HTTP client.", e);
            }
        }
    }

    public long getLastPollTime() {
        return lastPollTime;
    }

    void setLastPollTime(long lastPollTime) {
        this.lastPollTime = lastPollTime;
    }

    /**
     * Set the HTTP API client. This method is primarily for testing.
     *
     * @param apiClient The new HTTP API client instance.
     */
    void setApiClient(HttpApiClient apiClient) {
        this.apiClient = apiClient;
    }
}
