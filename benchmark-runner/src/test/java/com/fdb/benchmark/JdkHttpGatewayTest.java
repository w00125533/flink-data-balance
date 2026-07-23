package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.Authenticator;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.junit.jupiter.api.Test;

class JdkHttpGatewayTest {

  @Test
  void uses_configured_request_timeout() throws Exception {
    CapturingHttpClient client = new CapturingHttpClient();
    JdkHttpGateway gateway = new JdkHttpGateway(client, Map.of("FDB_BENCHMARK_HTTP_TIMEOUT_SEC", "45"));

    gateway.get(URI.create("http://localhost/metrics"));

    assertThat(client.request.timeout()).contains(Duration.ofSeconds(45));
  }

  private static final class CapturingHttpClient extends HttpClient {
    private HttpRequest request;

    @Override
    public Optional<CookieHandler> cookieHandler() {
      return Optional.empty();
    }

    @Override
    public Optional<Duration> connectTimeout() {
      return Optional.empty();
    }

    @Override
    public Redirect followRedirects() {
      return Redirect.NEVER;
    }

    @Override
    public Optional<ProxySelector> proxy() {
      return Optional.empty();
    }

    @Override
    public SSLContext sslContext() {
      return null;
    }

    @Override
    public SSLParameters sslParameters() {
      return null;
    }

    @Override
    public Optional<Authenticator> authenticator() {
      return Optional.empty();
    }

    @Override
    public Version version() {
      return Version.HTTP_1_1;
    }

    @Override
    public Optional<Executor> executor() {
      return Optional.empty();
    }

    @Override
    public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
        throws IOException, InterruptedException {
      this.request = request;
      @SuppressWarnings("unchecked")
      T body = (T) "";
      return new StaticHttpResponse<>(request, body);
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
        HttpResponse.BodyHandler<T> responseBodyHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
        HttpResponse.BodyHandler<T> responseBodyHandler, HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public WebSocket.Builder newWebSocketBuilder() {
      throw new UnsupportedOperationException();
    }

    private static HttpResponse.ResponseInfo responseInfo() {
      return new HttpResponse.ResponseInfo() {
        @Override
        public int statusCode() {
          return 200;
        }

        @Override
        public HttpHeaders headers() {
          return HttpHeaders.of(Map.of(), (left, right) -> true);
        }

        @Override
        public HttpClient.Version version() {
          return HttpClient.Version.HTTP_1_1;
        }
      };
    }
  }

  private record StaticHttpResponse<T>(HttpRequest request, T body) implements HttpResponse<T> {
    @Override
    public int statusCode() {
      return 200;
    }

    @Override
    public HttpHeaders headers() {
      return HttpHeaders.of(Map.of(), (left, right) -> true);
    }

    @Override
    public Optional<HttpResponse<T>> previousResponse() {
      return Optional.empty();
    }

    @Override
    public Optional<javax.net.ssl.SSLSession> sslSession() {
      return Optional.empty();
    }

    @Override
    public URI uri() {
      return request.uri();
    }

    @Override
    public HttpClient.Version version() {
      return HttpClient.Version.HTTP_1_1;
    }
  }
}
