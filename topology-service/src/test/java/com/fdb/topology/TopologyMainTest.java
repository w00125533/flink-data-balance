package com.fdb.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.avro.TopologyRecord;
import com.fdb.common.config.ConfigLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopologyMainTest {
  @TempDir Path tempDir;

  @Test
  void parses_target_cells_from_topology_env_style_key() throws Exception {
    ConfigLoader.Config raw = ConfigLoader.builder()
        .defaultResource("topology-default.yaml")
        .envPrefix("FDB_")
        .envSource(Map.of("FDB_TOPOLOGY_TARGET_CELLS", "1234"))
        .build()
        .load();

    TopologyConfig config = TopologyMain.parseConfig(raw);

    assertThat(config.getSites().getTargetCells()).isEqualTo(1234);
  }

  @Test
  void writes_topology_metrics_json_when_metrics_file_is_configured() throws Exception {
    TopologyConfig config = new TopologyConfig();
    config.getSites().setCount(3);
    config.getSites().getCellsPerSite().setMin(2);
    config.getSites().getCellsPerSite().setMax(2);
    List<TopologyRecord> records = new TopologyGenerator(config).generate();
    TopologyMetrics metrics = TopologyMetrics.from(
        records,
        11,
        new KafkaTopologyPublisher.PublishResult(records.size(), records.size() - 1L, 1),
        22,
        33);

    Path metricsFile = tempDir.resolve("runs/bench-a/topology-metrics.json");
    TopologyMain.writeMetrics(metricsFile, metrics);

    String json = Files.readString(metricsFile);
    assertThat(json)
        .contains("\"generatedRecords\" : 6")
        .contains("\"siteCount\" : 3")
        .contains("\"frequencyBandCount\"")
        .contains("\"generationDurationMs\" : 11")
        .contains("\"publishDurationMs\" : 22")
        .contains("\"totalDurationMs\" : 33")
        .contains("\"publishFailures\" : 1")
        .contains("\"minLatitude\"")
        .contains("\"maxLongitude\"");
  }
}
