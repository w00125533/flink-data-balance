package com.fdb.benchmark;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

final class FakeHttpGateway implements HttpGateway {
  private final Map<String, String> responses;

  FakeHttpGateway(Map<String, String> responses) {
    this.responses = responses;
  }

  @Override
  public String get(URI uri) throws IOException {
    String key = uri.getQuery() == null ? uri.getPath() : uri.getPath() + "?" + uri.getQuery();
    String body = responses.get(key);
    if (body == null) {
      body = responses.get(uri.getPath());
    }
    if (body == null) {
      throw new IOException("missing fake response for " + key);
    }
    return body;
  }
}
