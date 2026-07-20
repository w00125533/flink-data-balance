package com.fdb.benchmark;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

record ParsedArgs(String target, Path envFile, Map<String, String> overrides, boolean dryRun, boolean help) {
  static ParsedArgs parse(String[] args) {
    if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
      return new ParsedArgs("", Path.of(".env"), Map.of(), false, true);
    }
    String target = args[0];
    if (!"local".equals(target) && !"external-yarn".equals(target)) {
      throw new IllegalArgumentException("unsupported target: " + target);
    }
    Path envFile = Path.of(".env");
    Map<String, String> overrides = new HashMap<>();
    boolean dryRun = false;
    for (int i = 1; i < args.length; i++) {
      String arg = args[i];
      if ("--env".equals(arg)) {
        if (i + 1 >= args.length) {
          throw new IllegalArgumentException("--env requires a file path");
        }
        envFile = Path.of(args[++i]);
      } else if ("--set".equals(arg)) {
        if (i + 1 >= args.length) {
          throw new IllegalArgumentException("--set requires KEY=VALUE");
        }
        addOverride(overrides, args[++i]);
      } else if ("--dry-run".equals(arg)) {
        dryRun = true;
      } else {
        throw new IllegalArgumentException("unsupported argument: " + arg);
      }
    }
    return new ParsedArgs(target, envFile, Map.copyOf(overrides), dryRun, false);
  }

  private static void addOverride(Map<String, String> overrides, String raw) {
    int equals = raw.indexOf('=');
    if (equals <= 0) {
      throw new IllegalArgumentException("--set requires KEY=VALUE");
    }
    String key = raw.substring(0, equals).trim();
    if (!key.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw new IllegalArgumentException("--set key must be an environment variable name: " + key);
    }
    overrides.put(key, raw.substring(equals + 1));
  }
}
