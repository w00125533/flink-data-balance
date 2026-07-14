package com.fdb.job;

import java.io.Serializable;

public record PmMinuteFact(
    String cellId,
    String siteId,
    long minuteTs,
    long pmWindowCount,
    double prbUsageDlSum,
    double throughputDlMbpsSum,
    long dropCount,
    long handoverSuccess,
    long handoverFailure) implements Serializable {
}
