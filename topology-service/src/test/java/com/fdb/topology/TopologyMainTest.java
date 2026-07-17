package com.fdb.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.config.ConfigLoader;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TopologyMainTest {
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
}
