package com.fdb.simulator;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

final class SourceMetricsWriter {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path path;

    SourceMetricsWriter(String envKey) {
        this(envPath(envKey));
    }

    SourceMetricsWriter(Path path) {
        this.path = path;
    }

    void write(String source, long targetEps, long published, double observedEps) {
        if (path == null) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("source", source);
        payload.put("targetEps", targetEps);
        payload.put("published", published);
        payload.put("observedEps", observedEps);
        payload.put("updatedAtEpochMs", System.currentTimeMillis());
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            JSON.writeValue(path.toFile(), payload);
        } catch (IOException e) {
            throw new IllegalStateException("failed to write source metrics " + path, e);
        }
    }

    private static Path envPath(String envKey) {
        String value = System.getenv(envKey);
        return value == null || value.isBlank() ? null : Path.of(value);
    }
}
