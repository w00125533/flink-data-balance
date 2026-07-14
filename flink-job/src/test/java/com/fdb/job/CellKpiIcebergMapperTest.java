package com.fdb.job;

import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.JoinQuality;
import com.fdb.common.avro.WindowKind;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CellKpiIcebergMapperTest {

    @Test
    void maps_cell_kpi_to_row_data_with_utc_partitions() throws Exception {
        CellKpi kpi = new CellKpi(
            1780383600000L,
            1780383660000L,
            WindowKind.MIN_1,
            JoinQuality.JOINED,
            "site-a",
            "cell-a",
            "grid-a",
            11L,
            7L,
            9L,
            8L,
            6L,
            -95.5f,
            12.25f,
            45.5f,
            88.75f,
            0.5f,
            99.25f,
            98.5f);

        RowData row = new CellKpiIcebergMapper().map(kpi);

        assertThat(row.getLong(0)).isEqualTo(1780383600000L);
        assertThat(row.getLong(1)).isEqualTo(1780383660000L);
        assertThat(row.getString(2).toString()).isEqualTo("site-a");
        assertThat(row.getString(3).toString()).isEqualTo("cell-a");
        assertThat(row.getString(4).toString()).isEqualTo("grid-a");
        assertThat(row.getLong(5)).isEqualTo(11L);
        assertThat(row.getLong(6)).isEqualTo(7L);
        assertThat(row.getLong(7)).isEqualTo(9L);
        assertThat(row.getLong(8)).isEqualTo(8L);
        assertThat(row.getLong(9)).isEqualTo(6L);
        assertThat(row.getFloat(10)).isEqualTo(-95.5f);
        assertThat(row.getFloat(11)).isEqualTo(12.25f);
        assertThat(row.getFloat(12)).isEqualTo(45.5f);
        assertThat(row.getFloat(13)).isEqualTo(88.75f);
        assertThat(row.getFloat(14)).isEqualTo(0.5f);
        assertThat(row.getFloat(15)).isEqualTo(99.25f);
        assertThat(row.getFloat(16)).isEqualTo(98.5f);
        assertThat(row.getString(17).toString()).isEqualTo("JOINED");
        assertThat(row.getString(18).toString()).isEqualTo("MIN_1");
        assertThat(row.getString(19).toString()).isEqualTo("2026-06-02");
        assertThat(row.getString(20).toString()).isEqualTo("07");
    }
}
