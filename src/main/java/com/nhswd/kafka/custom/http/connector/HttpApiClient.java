package com.nhswd.kafka.custom.http.connector;

import com.nhswd.kafka.custom.http.connector.config.HttpSourceConfig;
import org.apache.hc.client5.http.classic.methods.*;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.*;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;


/**
 * <h1>Http Api Client</h1>
 * <p>HttpApiClient is a simple wrapper around Apache HTTP5 Client that consumes the configuration values from HttpSourceConfig</p>
 */
public class HttpApiClient implements AutoCloseable {

    private final static Logger log = LoggerFactory.getLogger(HttpApiClient.class);

    private final String requestParams;
    private final String requestBody;
    private final String headers;
    private final String authUsername;
    private final String authPassword;
    private final String authBearer;
    private final String proxyHost;
    private final int proxyPort;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    private CloseableHttpClient httpClient;

    public HttpApiClient(AbstractConfig config) {
        this.requestParams = config.getString(HttpSourceConfig.HTTP_QUERY_PARAMS);
        this.requestBody = config.getString(HttpSourceConfig.HTTP_REQUEST_BODY);
        this.headers = config.getString(HttpSourceConfig.HTTP_HEADERS);
        this.authUsername = config.getString(HttpSourceConfig.HTTP_AUTH_USERNAME);
        this.authPassword = config.getPassword(HttpSourceConfig.HTTP_AUTH_PASSWORD).value();
        this.authBearer = config.getPassword(HttpSourceConfig.HTTP_AUTH_BEARER).value();
        this.proxyHost = config.getString(HttpSourceConfig.HTTP_PROXY_HOST);
        this.proxyPort = config.getInt(HttpSourceConfig.HTTP_PROXY_PORT);
        this.connectTimeoutMs = config.getInt(HttpSourceConfig.HTTP_CONNECT_TIMEOUT_MS);
        this.readTimeoutMs = config.getInt(HttpSourceConfig.HTTP_READ_TIMEOUT_MS);

        ConnectionConfig connectionConfig = ConnectionConfig.custom()
                .setSocketTimeout(Timeout.ofMilliseconds(connectTimeoutMs))
                .build();

        PoolingHttpClientConnectionManager connectionManager =
                PoolingHttpClientConnectionManagerBuilder.create()
                        .setDefaultConnectionConfig(connectionConfig)
                        .build();

        RequestConfig requestConfig = RequestConfig.custom()
                .setResponseTimeout(Timeout.ofMilliseconds(readTimeoutMs))
                .build();

        HttpClientBuilder clientBuilder = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(requestConfig);

        if (proxyHost != null && !proxyHost.isEmpty() && proxyPort > 0) {
            HttpHost proxy = new HttpHost(proxyHost, proxyPort);
            clientBuilder.setProxy(proxy);
        }

        this.httpClient = clientBuilder.build();
    }

    private URI buildUriWithParameters(String baseUri) throws URISyntaxException {
        if (requestParams == null || requestParams.isEmpty()) {
            return new URI(baseUri);
        }

        StringBuilder stringBuilder = new StringBuilder(baseUri);
        if (baseUri.contains("?")) {
            stringBuilder.append("&");
        } else {
            stringBuilder.append("?");
        }

        stringBuilder.append(requestParams);

        return new URI(stringBuilder.toString());
    }

    private void addHeaders(HttpUriRequestBase request) {
        if (headers == null || headers.isEmpty()) {
            return;
        }
        String[] headersArray = headers.split(",");
        for (String header : headersArray) {
            String[] parts = header.split("=", 2);
            if (parts.length == 2) {
                request.addHeader(parts[0].trim(), parts[1].trim());
            } else {
                log.warn("Skipping invalid header format: {}", header);
            }
        }
    }

    private void addAuth(HttpUriRequestBase request) {
        if (authBearer != null && !authBearer.isEmpty()) {
            request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + authBearer);
        } else if (authUsername != null && !authUsername.isEmpty() && authPassword != null && !authPassword.isEmpty()) {
            String auth = authUsername + ":" + authPassword;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
        }
    }

    private void addBody(HttpUriRequestBase requestBase) {
        if (requestBody == null || requestBody.isEmpty()) {
            return;
        }

        if (requestBase instanceof HttpPost || requestBase instanceof HttpPut || requestBase instanceof HttpPatch) {
            StringEntity entity = new StringEntity(requestBody, StandardCharsets.UTF_8);
            requestBase.setEntity(entity);
        }
    }

    private HttpUriRequestBase createHttpRequest(String baseUri, Method method) throws URISyntaxException {
        URI uri = buildUriWithParameters(baseUri);
        HttpUriRequestBase requestBase;

        switch (method) {
            case GET:
                requestBase = new HttpGet(uri);
                break;
            case POST:
                requestBase = new HttpPost(uri);
                addBody(requestBase);
                break;
            case PUT:
                requestBase = new HttpPut(uri);
                addBody(requestBase);
                break;
            case PATCH:
                requestBase = new HttpPatch(uri);
                addBody(requestBase);
                break;
            case DELETE:
                requestBase = new HttpDelete(uri);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported HTTP Method " + method);
        }

        addHeaders(requestBase);
        addAuth(requestBase);
        log.debug("Computed HTTP request={}", requestBase);
        return requestBase;
    }

    public String executeRequest(String baseUri, String method) throws IOException, URISyntaxException {
        log.info("Polling API at {}", baseUri);

        return httpClient.execute(createHttpRequest(baseUri, Method.normalizedValueOf(method.toUpperCase())), new StringResponseHandler());
    }

    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }


    static class StringResponseHandler implements HttpClientResponseHandler<String> {
        @Override
        public String handleResponse(ClassicHttpResponse classicHttpResponse) throws HttpException, IOException {
            int statusCode = classicHttpResponse.getCode();

            if(statusCode >= HttpStatus.SC_OK && statusCode < HttpStatus.SC_REDIRECTION) {
                HttpEntity httpEntity = classicHttpResponse.getEntity();

                try {
                    return EntityUtils.toString(httpEntity);
                } catch (ParseException e) {
                    throw new ConnectException("Failed to parse HTTP response.");
                }
            } else {
                throw new ConnectException("HTTP request failed with status code: " + statusCode);
            }
        }
    }

    /**
     * A setter for the HTTP client. Used primarily for unit testing to inject a mock client.
     *
     * @param httpClient The mock or real HTTP client instance.
     */
    void setHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }
}
