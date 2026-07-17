package com.fdb.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SourceMetricsWriterTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir Path tempDir;

    @Test
    void writes_source_metrics_payload() throws Exception {
        Path path = tempDir.resolve("chr-source-metrics.json");
        SourceMetricsWriter writer = new SourceMetricsWriter(path);

        writer.write("chr", 300L, 600L, 294.0d, 2_000L, 12L, 12L);

        JsonNode payload = JSON.readTree(path.toFile());
        assertThat(payload.get("source").asText()).isEqualTo("chr");
        assertThat(payload.get("targetEps").asLong()).isEqualTo(300L);
        assertThat(payload.get("published").asLong()).isEqualTo(600L);
        assertThat(payload.get("observedEps").asDouble()).isEqualTo(294.0d);
        assertThat(payload.get("durationMs").asLong()).isEqualTo(2_000L);
        assertThat(payload.get("backlogRecords").asLong()).isEqualTo(12L);
        assertThat(payload.get("undeliveredRecords").asLong()).isEqualTo(12L);
        assertThat(payload.has("updatedAtEpochMs")).isTrue();
    }

    @Test
    void replaces_metrics_file_without_leaving_temp_siblings() throws Exception {
        Path path = tempDir.resolve("pm-source-metrics.json");
        SourceMetricsWriter writer = new SourceMetricsWriter(path);

        writer.write("pm", 0L, 10L, 1.0d, 1_000L, 0L, 0L);
        writer.write("pm", 0L, 20L, 2.0d, 2_000L, 0L, 0L);

        JsonNode payload = JSON.readTree(path.toFile());
        assertThat(payload.get("published").asLong()).isEqualTo(20L);
        assertThat(Files.list(tempDir).map(p -> p.getFileName().toString()).toList())
            .containsExactly("pm-source-metrics.json");
    }
}
