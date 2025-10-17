package com.nhswd.kafka.custom.http.connector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nhswd.kafka.custom.http.connector.model.GitHubEvent;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import com.nhswd.kafka.custom.http.connector.config.HttpSourceConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class HttpSourceTaskTest {

    @Mock
    private HttpSourceTask httpSourceTask;
    private Map<String, String> baseProps;

    private final SourceTaskContext mockSourceTaskContext = mock(SourceTaskContext.class);
    private final OffsetStorageReader mockOffsetStorageReader = mock(OffsetStorageReader.class);
    private final HttpApiClient mockHttpApiClient = mock(HttpApiClient.class);

    @BeforeEach
    public void setUp() {
        httpSourceTask = new HttpSourceTask();
        httpSourceTask.initialize(mockSourceTaskContext);

        doReturn(mockOffsetStorageReader).when(mockSourceTaskContext).offsetStorageReader();

        baseProps = Map.of(
                "http.url", "http://example.com",
                "http.method", "GET",
                "http.poll.interval.ms", "5000",
                "topic", "test-topic"
        );

        httpSourceTask.start(baseProps);
        httpSourceTask.setApiClient(mockHttpApiClient);
    }

    @Test
    void version_withHttpSourceTask_returnsExpectedVersion() {
        assertThat(httpSourceTask.version()).isEqualTo(HttpSourceConfig.VERSION);
    }

    @Test
    void start_withNoPreviousOffset_setsLastPollTimeToZero() {
        when(mockOffsetStorageReader.offset(any())).thenReturn(null);
        assertThat(httpSourceTask.getLastPollTime()).isEqualTo(0L);
    }

    @Test
    void start_withPersistedOffset_setsLastPollTimeFromOffset() {
        Map<String, Object> persistedOffset = Collections.singletonMap("last_polled_timestamp", 12345L);
        when(mockOffsetStorageReader.offset(any())).thenReturn(persistedOffset);

        httpSourceTask.start(baseProps);
        assertThat(httpSourceTask.getLastPollTime()).isEqualTo(12345L);
    }

    @Test
    void poll_withElapsedInterval_returnsSourceRecord() throws InterruptedException, IOException, URISyntaxException {
        String apiResponse = "["
                + "{\"type\":\"WatchEvent\",\"created_at\":\"2025-10-10T12:00:00Z\",\"repo\":{\"name\":\"apache/spark\"}},"
                + "{\"type\":\"PullRequestEvent\",\"created_at\":\"2025-10-10T12:01:00Z\",\"repo\":{\"name\":\"apache/kafka\"}}"
                + "]";
        when(mockHttpApiClient.executeRequest(any(), any())).thenReturn(apiResponse);
        httpSourceTask.setLastPollTime(System.currentTimeMillis() - 6000L);

        List<SourceRecord> records = httpSourceTask.poll();

        assertThat(records).hasSize(2);

        ObjectMapper mapper = new ObjectMapper();
        GitHubEvent firstEvent = mapper.readValue((String) records.get(0).value(), GitHubEvent.class);
        GitHubEvent secondEvent = mapper.readValue((String) records.get(1).value(), GitHubEvent.class);

        assertThat(firstEvent.getType()).isEqualTo("WatchEvent");
        assertThat(firstEvent.getCreatedAt()).isEqualTo("2025-10-10T12:00:00Z");
        assertThat(firstEvent.getRepo().getName()).isEqualTo("apache/spark");

        assertThat(secondEvent.getType()).isEqualTo("PullRequestEvent");
        assertThat(secondEvent.getCreatedAt()).isEqualTo("2025-10-10T12:01:00Z");
        assertThat(secondEvent.getRepo().getName()).isEqualTo("apache/kafka");
    }

    @Test
    void poll_withinInterval_returnsEmptyList() throws InterruptedException, IOException, URISyntaxException {
        String apiResponse = "{\"data\": \"some_data\"}";
        httpSourceTask.setLastPollTime(System.currentTimeMillis());
        when(mockHttpApiClient.executeRequest(any(), any())).thenReturn(apiResponse);

        List<SourceRecord> records = httpSourceTask.poll();

        assertThat(records).isEmpty();
        verify(mockHttpApiClient, never()).executeRequest(any(), any());
    }

    @Test
    void poll_withIOException_throwsRetriableException() throws IOException, URISyntaxException {
        when(mockHttpApiClient.executeRequest(any(), any())).thenThrow(new IOException("Test IO Exception"));

        assertThatThrownBy(() -> httpSourceTask.poll())
                .isInstanceOf(RetriableException.class)
                .hasMessageContaining("I/O error during HTTP request.");
    }

    @Test
    void poll_withRuntimeException_throwsConnectException() throws IOException, URISyntaxException {
        when(mockHttpApiClient.executeRequest(any(), any())).thenThrow(new RuntimeException("Unexpected test error"));

        assertThatThrownBy(() -> httpSourceTask.poll())
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Unexpected error.");
    }

    @Test
    void stop_withTask_closesApiClient() throws IOException {
        httpSourceTask.stop();

        verify(mockHttpApiClient).close();
    }
}
