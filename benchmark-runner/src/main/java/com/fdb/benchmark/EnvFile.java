package com.fdb.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

final class EnvFile {
  private EnvFile() {
  }

  static Map<String, String> load(Path path, Map<String, String> inherited) throws IOException {
    Map<String, String> values = new HashMap<>(inherited);
    if (!Files.exists(path)) {
      throw new IOException("env file not found: " + path);
    }
    for (String rawLine : Files.readAllLines(path)) {
      String line = rawLine.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      if (line.startsWith("export ")) {
        line = line.substring("export ".length()).trim();
      }
      int equals = line.indexOf('=');
      if (equals <= 0) {
        continue;
      }
      String key = line.substring(0, equals).trim();
      String value = stripQuotes(line.substring(equals + 1).trim());
      values.put(key, value);
    }
    return values;
  }

  private static String stripQuotes(String value) {
    if (value.length() >= 2) {
      char first = value.charAt(0);
      char last = value.charAt(value.length() - 1);
      if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
        return value.substring(1, value.length() - 1);
      }
    }
    return value;
  }
}
