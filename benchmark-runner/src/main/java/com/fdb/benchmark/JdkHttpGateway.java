package com.fdb.benchmark;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

public final class JdkHttpGateway implements HttpGateway {
  private static final long DEFAULT_CONNECT_TIMEOUT_SEC = 5;
  private static final long DEFAULT_REQUEST_TIMEOUT_SEC = 30;

  private final HttpClient client;
  private final Duration requestTimeout;

  public JdkHttpGateway() {
    this(System.getenv());
  }

  JdkHttpGateway(HttpClient client) {
    this(client, System.getenv());
  }

  JdkHttpGateway(Map<String, String> env) {
    this(HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(positiveLong(env, "FDB_BENCHMARK_HTTP_CONNECT_TIMEOUT_SEC",
            DEFAULT_CONNECT_TIMEOUT_SEC)))
        .build(), env);
  }

  JdkHttpGateway(HttpClient client, Map<String, String> env) {
    this.client = client;
    this.requestTimeout = Duration.ofSeconds(positiveLong(env, "FDB_BENCHMARK_HTTP_TIMEOUT_SEC",
        DEFAULT_REQUEST_TIMEOUT_SEC));
  }

  @Override
  public String get(URI uri) throws IOException, InterruptedException {
    HttpRequest request = HttpRequest.newBuilder(uri).timeout(requestTimeout).GET().build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new IOException("GET " + uri + " returned HTTP " + response.statusCode());
    }
    return response.body();
  }

  private static long positiveLong(Map<String, String> env, String key, long defaultValue) {
    String raw = env.get(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      long value = Long.parseLong(raw.trim());
      return value > 0 ? value : defaultValue;
    } catch (NumberFormatException ignored) {
      return defaultValue;
    }
  }
}
