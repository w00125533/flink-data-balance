package com.fdb.job.sink;

import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.JoinQuality;
import com.fdb.common.avro.WindowKind;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CellKpiIcebergMapperTest {

    @Test
    void maps_cell_kpi_to_row_data_with_utc_partitions() throws Exception {
        CellKpi kpi = CellKpi.newBuilder()
            .setWindowStartTs(1780383600000L)
            .setWindowEndTs(1780383660000L)
            .setSourceEventTsAvg(1780383630000L)
            .setSourceEventTsMin(1780383605000L)
            .setSourceEventTsMax(1780383655000L)
            .setSourceEventCount(11L)
            .setWindowKind(WindowKind.MIN_1)
            .setJoinQuality(JoinQuality.JOINED)
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setGridId("grid-a")
            .setNumChrEvents(11L)
            .setNumUsers(7L)
            .setRsrpSampleCount(9L)
            .setSinrSampleCount(8L)
            .setAttachAttempts(6L)
            .setAvgRsrp(-95.5f)
            .setAvgSinr(12.25f)
            .setAvgPrbUsageDl(45.5f)
            .setThroughputDlMbpsAvg(88.75f)
            .setDropRate(0.5f)
            .setHoSuccessRate(99.25f)
            .setAttachSuccessRate(98.5f)
            .build();

        RowData row = new CellKpiIcebergMapper().map(kpi);

        assertThat(row.getLong(0)).isEqualTo(1780383600000L);
        assertThat(row.getLong(1)).isEqualTo(1780383660000L);
        assertThat(row.getLong(2)).isEqualTo(1780383630000L);
        assertThat(row.getLong(3)).isEqualTo(1780383605000L);
        assertThat(row.getLong(4)).isEqualTo(1780383655000L);
        assertThat(row.getLong(5)).isEqualTo(11L);
        assertThat(row.getString(6).toString()).isEqualTo("site-a");
        assertThat(row.getString(7).toString()).isEqualTo("cell-a");
        assertThat(row.getString(8).toString()).isEqualTo("grid-a");
        assertThat(row.getLong(9)).isEqualTo(11L);
        assertThat(row.getLong(10)).isEqualTo(7L);
        assertThat(row.getLong(11)).isEqualTo(9L);
        assertThat(row.getLong(12)).isEqualTo(8L);
        assertThat(row.getLong(13)).isEqualTo(6L);
        assertThat(row.getFloat(14)).isEqualTo(-95.5f);
        assertThat(row.getFloat(15)).isEqualTo(12.25f);
        assertThat(row.getFloat(16)).isEqualTo(45.5f);
        assertThat(row.getFloat(17)).isEqualTo(88.75f);
        assertThat(row.getFloat(18)).isEqualTo(0.5f);
        assertThat(row.getFloat(19)).isEqualTo(99.25f);
        assertThat(row.getFloat(20)).isEqualTo(98.5f);
        assertThat(row.getString(21).toString()).isEqualTo("JOINED");
        assertThat(row.getString(22).toString()).isEqualTo("MIN_1");
        assertThat(row.getString(23).toString()).isEqualTo("2026-06-02");
        assertThat(row.getString(24).toString()).isEqualTo("07");
    }
}
