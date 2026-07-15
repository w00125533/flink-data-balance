package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class AnomalyEventIcebergMapper implements MapFunction<AnomalyEvent, RowData> {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter
        .ofPattern("yyyy-MM-dd")
        .withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter
        .ofPattern("HH")
        .withZone(ZoneOffset.UTC);

    @Override
    public RowData map(AnomalyEvent event) {
        Instant eventTs = Instant.ofEpochMilli(event.getEventTs());
        GenericRowData row = new GenericRowData(18);
        row.setField(0, event.getDetectionTs());
        row.setField(1, event.getEventTs());
        row.setField(2, string(event.getEntityType()));
        row.setField(3, string(event.getEntityId()));
        row.setField(4, event.getWindowStartTs());
        row.setField(5, event.getWindowEndTs());
        row.setField(6, nullableString(event.getImsi()));
        row.setField(7, nullableString(event.getSiteId()));
        row.setField(8, nullableString(event.getCellId()));
        row.setField(9, nullableString(event.getGridId()));
        row.setField(10, event.getLatitude());
        row.setField(11, event.getLongitude());
        row.setField(12, string(event.getAnomalyType()));
        row.setField(13, string(event.getSeverity()));
        row.setField(14, string(event.getRuleVersion()));
        row.setField(15, string(event.getContextJson()));
        row.setField(16, string(DATE_FORMATTER.format(eventTs)));
        row.setField(17, string(HOUR_FORMATTER.format(eventTs)));
        return row;
    }

    private static StringData string(Object value) {
        return StringData.fromString(value == null ? "" : value.toString());
    }

    private static StringData nullableString(Object value) {
        return value == null ? null : StringData.fromString(value.toString());
    }
}
