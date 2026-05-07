package com.fdb.topology;

import com.fdb.common.avro.TopoCellType;
import com.fdb.common.avro.TopologyRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TopologyGeneratorTest {

    @Test
    void generates_expected_number_of_sites() {
        TopologyConfig config = new TopologyConfig();
        config.setSeed(42);
        config.getSites().setCount(100);

        List<TopologyRecord> records = new TopologyGenerator(config).generate();

        assertThat(records).isNotEmpty();
        long siteCount = records.stream().map(TopologyRecord::getSiteId).distinct().count();
        assertThat(siteCount).isEqualTo(100);
    }

    @Test
    void records_have_correct_cell_count_range() {
        TopologyConfig config = new TopologyConfig();
        config.setSeed(42);
        config.getSites().setCount(50);

        List<TopologyRecord> records = new TopologyGenerator(config).generate();

        for (String siteId : records.stream().map(TopologyRecord::getSiteId).distinct().toList()) {
            long cellsForSite = records.stream().filter(r -> r.getSiteId().equals(siteId)).count();
            assertThat(cellsForSite).isBetween(3L, 9L);
        }
    }

    @Test
    void generates_deterministic_output_with_same_seed() {
        TopologyConfig config = new TopologyConfig();
        config.setSeed(123);

        List<TopologyRecord> a = new TopologyGenerator(config).generate();
        List<TopologyRecord> b = new TopologyGenerator(config).generate();

        assertThat(a).hasSameSizeAs(b);
        for (int i = 0; i < a.size(); i++) {
            assertThat(a.get(i).getCellId()).isEqualTo(b.get(i).getCellId());
            assertThat(a.get(i).getPci()).isEqualTo(b.get(i).getPci());
            assertThat(a.get(i).getSiteLat()).isEqualTo(b.get(i).getSiteLat());
        }
    }

    @Test
    void all_cells_have_unique_pci_across_proximal_sites() {
        TopologyConfig config = new TopologyConfig();
        config.setSeed(42);
        config.getSites().setCount(200);

        List<TopologyRecord> records = new TopologyGenerator(config).generate();

        assertThat(records.stream().map(TopologyRecord::getPci).distinct().count())
            .isGreaterThan(100);
    }

    @Test
    void defaults_to_nr_sa_cell_type() {
        TopologyConfig config = new TopologyConfig();
        config.setSeed(42);
        config.getSites().setCount(10);

        List<TopologyRecord> records = new TopologyGenerator(config).generate();
        assertThat(records).allMatch(r -> r.getCellType() == TopoCellType.NR_SA);
    }
}
