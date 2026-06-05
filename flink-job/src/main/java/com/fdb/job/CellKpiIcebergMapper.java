package com.fdb.job;

import com.fdb.common.avro.CellKpi;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class CellKpiIcebergMapper implements MapFunction<CellKpi, RowData> {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter
        .ofPattern("yyyy-MM-dd")
        .withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter
        .ofPattern("HH")
        .withZone(ZoneOffset.UTC);

    @Override
    public RowData map(CellKpi kpi) {
        Instant windowStart = Instant.ofEpochMilli(kpi.getWindowStartTs());
        GenericRowData row = new GenericRowData(17);
        row.setField(0, kpi.getWindowStartTs());
        row.setField(1, kpi.getWindowEndTs());
        row.setField(2, string(kpi.getSiteId()));
        row.setField(3, string(kpi.getCellId()));
        row.setField(4, string(kpi.getGridId()));
        row.setField(5, kpi.getNumChrEvents());
        row.setField(6, kpi.getNumUsers());
        row.setField(7, kpi.getAvgRsrp());
        row.setField(8, kpi.getAvgSinr());
        row.setField(9, kpi.getAvgPrbUsageDl());
        row.setField(10, kpi.getThroughputDlMbpsAvg());
        row.setField(11, kpi.getDropRate());
        row.setField(12, kpi.getHoSuccessRate());
        row.setField(13, kpi.getAttachSuccessRate());
        row.setField(14, string(kpi.getWindowKind().toString()));
        row.setField(15, string(DATE_FORMATTER.format(windowStart)));
        row.setField(16, string(HOUR_FORMATTER.format(windowStart)));
        return row;
    }

    private static StringData string(CharSequence value) {
        return StringData.fromString(value.toString());
    }
}
