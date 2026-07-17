package com.fdb.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

        writer.write("chr", 300L, 600L, 294.0d);

        JsonNode payload = JSON.readTree(path.toFile());
        assertThat(payload.get("source").asText()).isEqualTo("chr");
        assertThat(payload.get("targetEps").asLong()).isEqualTo(300L);
        assertThat(payload.get("published").asLong()).isEqualTo(600L);
        assertThat(payload.get("observedEps").asDouble()).isEqualTo(294.0d);
        assertThat(payload.has("updatedAtEpochMs")).isTrue();
    }
}
