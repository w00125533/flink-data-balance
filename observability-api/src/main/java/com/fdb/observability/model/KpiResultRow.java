package com.fdb.observability.model;

public record KpiResultRow(
    Long windowStartTs,
    Long windowEndTs,
    String windowKind,
    String joinQuality,
    String siteId,
    String cellId,
    String gridId,
    Long numChrEvents,
    Long numUsers,
    Double avgRsrp,
    Double avgSinr,
    Double avgPrbUsageDl,
    Double throughputDlMbpsAvg,
    Double dropRate,
    Double hoSuccessRate,
    Double attachSuccessRate
) {
}
