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
    String body = responses.get(uri.getPath());
    if (body == null) {
      throw new IOException("missing fake response for " + uri.getPath());
    }
    return body;
  }
}
