package com.fdb.benchmark;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public final class JdkHttpGateway implements HttpGateway {
  private final HttpClient client;

  public JdkHttpGateway() {
    this(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build());
  }

  JdkHttpGateway(HttpClient client) {
    this.client = client;
  }

  @Override
  public String get(URI uri) throws IOException, InterruptedException {
    HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET().build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new IOException("GET " + uri + " returned HTTP " + response.statusCode());
    }
    return response.body();
  }
}
