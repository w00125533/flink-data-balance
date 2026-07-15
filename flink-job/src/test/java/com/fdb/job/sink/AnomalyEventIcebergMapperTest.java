package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.Severity;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AnomalyEventIcebergMapperTest {

    @Test
    void maps_anomaly_event_to_row_data_with_utc_partitions() throws Exception {
        AnomalyEvent event = AnomalyEvent.newBuilder()
            .setDetectionTs(1780383661000L)
            .setEventTs(1780383600000L)
            .setEntityType(EntityType.CELL)
            .setEntityId("cell-a")
            .setWindowStartTs(1780383540000L)
            .setWindowEndTs(1780383600000L)
            .setImsi("imsi-a")
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setGridId("grid-a")
            .setLatitude(31.2304d)
            .setLongitude(121.4737d)
            .setAnomalyType(AnomalyType.LOW_SIGNAL)
            .setSeverity(Severity.HIGH)
            .setRuleVersion("rules-v1")
            .setContextJson("{\"rsrp\":-118}")
            .build();

        RowData row = new AnomalyEventIcebergMapper().map(event);

        assertThat(row.getLong(0)).isEqualTo(1780383661000L);
        assertThat(row.getLong(1)).isEqualTo(1780383600000L);
        assertThat(row.getString(2).toString()).isEqualTo("site-a");
        assertThat(row.getString(3).toString()).isEqualTo("cell-a");
        assertThat(row.getString(4).toString()).isEqualTo("grid-a");
        assertThat(row.getDouble(5)).isEqualTo(31.2304d);
        assertThat(row.getDouble(6)).isEqualTo(121.4737d);
        assertThat(row.getString(7).toString()).isEqualTo("LOW_SIGNAL");
        assertThat(row.getString(8).toString()).isEqualTo("HIGH");
        assertThat(row.getString(9).toString()).isEqualTo("rules-v1");
        assertThat(row.getString(10).toString()).isEqualTo("{\"rsrp\":-118}");
        assertThat(row.getString(11).toString()).isEqualTo("2026-06-02");
        assertThat(row.getString(12).toString()).isEqualTo("07");
    }
}
