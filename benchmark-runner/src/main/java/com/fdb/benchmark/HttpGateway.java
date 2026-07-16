package com.fdb.benchmark;

import java.io.IOException;
import java.net.URI;

public interface HttpGateway {
  String get(URI uri) throws IOException, InterruptedException;
}
