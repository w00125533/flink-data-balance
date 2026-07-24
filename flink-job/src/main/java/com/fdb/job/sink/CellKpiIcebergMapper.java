package com.fdb.job.sink;

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
        GenericRowData row = new GenericRowData(25);
        row.setField(0, kpi.getWindowStartTs());
        row.setField(1, kpi.getWindowEndTs());
        row.setField(2, kpi.getSourceEventTsAvg());
        row.setField(3, kpi.getSourceEventTsMin());
        row.setField(4, kpi.getSourceEventTsMax());
        row.setField(5, kpi.getSourceEventCount());
        row.setField(6, string(kpi.getSiteId()));
        row.setField(7, string(kpi.getCellId()));
        row.setField(8, string(kpi.getGridId()));
        row.setField(9, kpi.getNumChrEvents());
        row.setField(10, kpi.getNumUsers());
        row.setField(11, kpi.getRsrpSampleCount());
        row.setField(12, kpi.getSinrSampleCount());
        row.setField(13, kpi.getAttachAttempts());
        row.setField(14, kpi.getAvgRsrp());
        row.setField(15, kpi.getAvgSinr());
        row.setField(16, kpi.getAvgPrbUsageDl());
        row.setField(17, kpi.getThroughputDlMbpsAvg());
        row.setField(18, kpi.getDropRate());
        row.setField(19, kpi.getHoSuccessRate());
        row.setField(20, kpi.getAttachSuccessRate());
        row.setField(21, string(kpi.getJoinQuality().name()));
        row.setField(22, string(kpi.getWindowKind().toString()));
        row.setField(23, string(DATE_FORMATTER.format(windowStart)));
        row.setField(24, string(HOUR_FORMATTER.format(windowStart)));
        return row;
    }

    private static StringData string(CharSequence value) {
        return StringData.fromString(value.toString());
    }
}
