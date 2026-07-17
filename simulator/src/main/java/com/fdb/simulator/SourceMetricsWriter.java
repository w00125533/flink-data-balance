package com.fdb.simulator;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;

final class SourceMetricsWriter {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MOVE_RETRY_ATTEMPTS = 20;
    private static final long MOVE_RETRY_DELAY_MS = 50L;
    private final Path path;

    SourceMetricsWriter(String envKey) {
        this(envPath(envKey));
    }

    SourceMetricsWriter(Path path) {
        this.path = path;
    }

    void write(String source, long targetEps, long published, double observedEps) {
        write(source, targetEps, published, observedEps, 0L, 0L, 0L);
    }

    void write(String source, long targetEps, long published, double observedEps,
               long durationMs, long backlogRecords, long undeliveredRecords) {
        if (path == null) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("source", source);
        payload.put("targetEps", targetEps);
        payload.put("published", published);
        payload.put("observedEps", observedEps);
        payload.put("durationMs", durationMs);
        payload.put("backlogRecords", backlogRecords);
        payload.put("undeliveredRecords", undeliveredRecords);
        payload.put("updatedAtEpochMs", System.currentTimeMillis());
        Path tmp = null;
        try {
            Path target = path.toAbsolutePath();
            Path parent = target.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            tmp = Files.createTempFile(parent, path.getFileName().toString(), ".tmp");
            JSON.writeValue(tmp.toFile(), payload);
            moveIntoPlace(tmp, target);
        } catch (IOException e) {
            throw new IllegalStateException("failed to write source metrics " + path, e);
        } finally {
            if (tmp != null) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignored) {
                    // Best effort cleanup for abandoned temp files.
                }
            }
        }
    }

    private static void moveIntoPlace(Path tmp, Path target) throws IOException {
        IOException lastFailure = null;
        for (int attempt = 1; attempt <= MOVE_RETRY_ATTEMPTS; attempt++) {
            try {
                moveOnce(tmp, target);
                return;
            } catch (IOException e) {
                lastFailure = e;
                if (attempt == MOVE_RETRY_ATTEMPTS) {
                    throw e;
                }
                sleepBeforeRetry(e);
            }
        }
        throw lastFailure;
    }

    private static void moveOnce(Path tmp, Path target) throws IOException {
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void sleepBeforeRetry(IOException failure) throws IOException {
        try {
            Thread.sleep(MOVE_RETRY_DELAY_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            IOException interrupted = new IOException("interrupted while retrying source metrics write", e);
            interrupted.addSuppressed(failure);
            throw interrupted;
        }
    }

    private static Path envPath(String envKey) {
        String value = System.getenv(envKey);
        return value == null || value.isBlank() ? null : Path.of(value);
    }
}
