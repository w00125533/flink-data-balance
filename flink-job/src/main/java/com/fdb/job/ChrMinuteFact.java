package com.fdb.job;

import java.io.Serializable;

public record ChrMinuteFact(
    String cellId,
    String siteId,
    long minuteTs,
    long count,
    long uniqueUsers,
    double rsrpSum,
    double sinrSum,
    long attachAttempts,
    long attachSuccess,
    long rsrpCount,
    long sinrCount) implements Serializable {

    public ChrMinuteFact(
        String cellId,
        String siteId,
        long minuteTs,
        long count,
        long uniqueUsers,
        double rsrpSum,
        double sinrSum,
        long attachAttempts,
        long attachSuccess) {
        this(cellId, siteId, minuteTs, count, uniqueUsers, rsrpSum, sinrSum,
            attachAttempts, attachSuccess, count, count);
    }
}
