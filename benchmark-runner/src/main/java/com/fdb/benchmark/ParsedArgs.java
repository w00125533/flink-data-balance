package com.fdb.benchmark;

import java.nio.file.Path;

record ParsedArgs(String target, Path envFile, boolean dryRun, boolean help) {
  static ParsedArgs parse(String[] args) {
    if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
      return new ParsedArgs("", Path.of(".env"), false, true);
    }
    String target = args[0];
    if (!"local".equals(target) && !"external-yarn".equals(target)) {
      throw new IllegalArgumentException("unsupported target: " + target);
    }
    Path envFile = Path.of(".env");
    boolean dryRun = false;
    for (int i = 1; i < args.length; i++) {
      String arg = args[i];
      if ("--env".equals(arg)) {
        if (i + 1 >= args.length) {
          throw new IllegalArgumentException("--env requires a file path");
        }
        envFile = Path.of(args[++i]);
      } else if ("--dry-run".equals(arg)) {
        dryRun = true;
      } else {
        throw new IllegalArgumentException("unsupported argument: " + arg);
      }
    }
    return new ParsedArgs(target, envFile, dryRun, false);
  }
}
