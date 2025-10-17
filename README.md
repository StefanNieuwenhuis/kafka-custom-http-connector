# Custom HTTP Source Connector for Apache Kafka Connect

This is a custom HTTP Source Connector that poll a REST API (e.g. [GitHub events API](https://api.github.com/events)) at configurable intervals and publishes the response body to a Kafka Topic.

## Features

- **Configurable HTTP method**: Supports `GET`, `POST`, `PUT`, `PATCH`, `DELETE`
- **Customizable headers and query parameters**: Pass additional information with requests
- **Proxy support**: Past proxy host & port
- **Authentication**: Supports basic auth and Auth Bearer tokens
- **Timeout management**: Configure connection and read timeouts
- **Polling interval**: Frequency for fetching (i.e. polling) an endpoint for new data.
- **Idempotency**: The connector uses the URL as part of the source partition to ensure each unique URL is processed independently. It also tracks the last successful poll time to avoid re-polling too frequently.

## Building the HTTP Source Connector

To build a FAT Jar of the connector, run the following Maven command from the project's root directory:

```bash
mvn clean package
```

This create a FAT Jar file in the `target/` directory, which can be deployed to your Kafka Connect Environment.

## Configuration options

| Name                      | Type        | Importance | Default                           | Description                                                                                        |
|:--------------------------|:------------|:-----------|:----------------------------------|:---------------------------------------------------------------------------------------------------|
| `topic`                   | `STRING`    | `HIGH`     | N/A                               | The Kafka topic to write the fetched data to.                                                      |
| `http.url`                | `STRING`    | `HIGH`     | `"https://api.github.com/events"` | The base HTTP URL to fetch data from.                                                              |
| `http.proxy.host`         | `STRING`    | `MEDIUM`   | `""`                              | Optional HTTP proxy host to route requests through.                                                |
| `http.proxy.port`         | `INT`       | `MEDIUM`   | `-1`                              | Optional HTTP proxy port. Must be set if `http.proxy.host` is provided.                            |
| `http.poll.interval.ms`   | `INT`       | `HIGH`     | `60000`                           | Polling interval in milliseconds between consecutive HTTP requests. Minimum allowed is 5000 ms.    |
| `http.method`             | `STRING`    | `HIGH`     | `GET`                             | The HTTP method to use for requests. Valid values are `GET`, `POST`, `PATCH`, `PUT`, `DELETE`.     |
| `http.connect.timeout.ms` | `INT`       | `MEDIUM`   | `5000`                            | Timeout in milliseconds for establishing the HTTP connection.                                      |
| `http.read.timeout.ms`    | `INT`       | `MEDIUM`   | `10000`                           | Timeout in milliseconds for reading the HTTP response.                                             |
| `http.headers`            | `STRING`    | `LOW`      | `""`                              | Optional HTTP request headers in 'key=value' pairs separated by commas.                            |
| `http.query.params`       | `STRING`    | `LOW`      | `""`                              | Optional query parameters appended to the HTTP request URL in 'key=value' pairs separated by '&'.  |
| `http.request.body`       | `STRING`    | `MEDIUM`   | `""`                              | The HTTP request body to be sent with the request. Only applicable for methods like POST and PUT.  |
| `http.auth.username`      | `STRING`    | `MEDIUM`   | `""`                              | Username for HTTP Basic Authentication. Used with `http.auth.password`.                            |
| `http.auth.password`      | `PASSWORD`  | `MEDIUM`   | `""`                              | Password for HTTP Basic Authentication. Used with `http.auth.username`.                            |
| `http.auth.bearer`        | `PASSWORD`  | `MEDIUM`   | `""`                              | Bearer token for the Authorization header.                                                         |