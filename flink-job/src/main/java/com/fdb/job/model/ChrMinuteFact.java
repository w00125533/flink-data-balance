package com.fdb.job.model;

import java.io.Serializable;
import java.util.Objects;

public class ChrMinuteFact implements Serializable {

    private String cellId;
    private String siteId;
    private long minuteTs;
    private long count;
    private long uniqueUsers;
    private double rsrpSum;
    private double sinrSum;
    private long attachAttempts;
    private long attachSuccess;
    private long rsrpCount;
    private long sinrCount;
    private long sourceEventTsAvg;
    private long sourceEventTsMin;
    private long sourceEventTsMax;
    private long sourceEventCount;

    public ChrMinuteFact() {
    }

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

    public ChrMinuteFact(
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
        long sinrCount) {
        this(cellId, siteId, minuteTs, count, uniqueUsers, rsrpSum, sinrSum,
            attachAttempts, attachSuccess, rsrpCount, sinrCount, 0L, 0L, 0L, 0L);
    }

    public ChrMinuteFact(
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
        long sinrCount,
        long sourceEventTsAvg,
        long sourceEventTsMin,
        long sourceEventTsMax,
        long sourceEventCount) {
        this.cellId = cellId;
        this.siteId = siteId;
        this.minuteTs = minuteTs;
        this.count = count;
        this.uniqueUsers = uniqueUsers;
        this.rsrpSum = rsrpSum;
        this.sinrSum = sinrSum;
        this.attachAttempts = attachAttempts;
        this.attachSuccess = attachSuccess;
        this.rsrpCount = rsrpCount;
        this.sinrCount = sinrCount;
        this.sourceEventTsAvg = sourceEventTsAvg;
        this.sourceEventTsMin = sourceEventTsMin;
        this.sourceEventTsMax = sourceEventTsMax;
        this.sourceEventCount = sourceEventCount;
    }

    public String cellId() {
        return cellId;
    }

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }

    public String siteId() {
        return siteId;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public long minuteTs() {
        return minuteTs;
    }

    public long getMinuteTs() {
        return minuteTs;
    }

    public void setMinuteTs(long minuteTs) {
        this.minuteTs = minuteTs;
    }

    public long count() {
        return count;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long uniqueUsers() {
        return uniqueUsers;
    }

    public long getUniqueUsers() {
        return uniqueUsers;
    }

    public void setUniqueUsers(long uniqueUsers) {
        this.uniqueUsers = uniqueUsers;
    }

    public double rsrpSum() {
        return rsrpSum;
    }

    public double getRsrpSum() {
        return rsrpSum;
    }

    public void setRsrpSum(double rsrpSum) {
        this.rsrpSum = rsrpSum;
    }

    public double sinrSum() {
        return sinrSum;
    }

    public double getSinrSum() {
        return sinrSum;
    }

    public void setSinrSum(double sinrSum) {
        this.sinrSum = sinrSum;
    }

    public long attachAttempts() {
        return attachAttempts;
    }

    public long getAttachAttempts() {
        return attachAttempts;
    }

    public void setAttachAttempts(long attachAttempts) {
        this.attachAttempts = attachAttempts;
    }

    public long attachSuccess() {
        return attachSuccess;
    }

    public long getAttachSuccess() {
        return attachSuccess;
    }

    public void setAttachSuccess(long attachSuccess) {
        this.attachSuccess = attachSuccess;
    }

    public long rsrpCount() {
        return rsrpCount;
    }

    public long getRsrpCount() {
        return rsrpCount;
    }

    public void setRsrpCount(long rsrpCount) {
        this.rsrpCount = rsrpCount;
    }

    public long sinrCount() {
        return sinrCount;
    }

    public long getSinrCount() {
        return sinrCount;
    }

    public void setSinrCount(long sinrCount) {
        this.sinrCount = sinrCount;
    }

    public long sourceEventTsAvg() {
        return sourceEventTsAvg;
    }

    public long getSourceEventTsAvg() {
        return sourceEventTsAvg;
    }

    public void setSourceEventTsAvg(long sourceEventTsAvg) {
        this.sourceEventTsAvg = sourceEventTsAvg;
    }

    public long sourceEventTsMin() {
        return sourceEventTsMin;
    }

    public long getSourceEventTsMin() {
        return sourceEventTsMin;
    }

    public void setSourceEventTsMin(long sourceEventTsMin) {
        this.sourceEventTsMin = sourceEventTsMin;
    }

    public long sourceEventTsMax() {
        return sourceEventTsMax;
    }

    public long getSourceEventTsMax() {
        return sourceEventTsMax;
    }

    public void setSourceEventTsMax(long sourceEventTsMax) {
        this.sourceEventTsMax = sourceEventTsMax;
    }

    public long sourceEventCount() {
        return sourceEventCount;
    }

    public long getSourceEventCount() {
        return sourceEventCount;
    }

    public void setSourceEventCount(long sourceEventCount) {
        this.sourceEventCount = sourceEventCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ChrMinuteFact that)) {
            return false;
        }
        return minuteTs == that.minuteTs
            && count == that.count
            && uniqueUsers == that.uniqueUsers
            && Double.compare(that.rsrpSum, rsrpSum) == 0
            && Double.compare(that.sinrSum, sinrSum) == 0
            && attachAttempts == that.attachAttempts
            && attachSuccess == that.attachSuccess
            && rsrpCount == that.rsrpCount
            && sinrCount == that.sinrCount
            && sourceEventTsAvg == that.sourceEventTsAvg
            && sourceEventTsMin == that.sourceEventTsMin
            && sourceEventTsMax == that.sourceEventTsMax
            && sourceEventCount == that.sourceEventCount
            && Objects.equals(cellId, that.cellId)
            && Objects.equals(siteId, that.siteId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cellId, siteId, minuteTs, count, uniqueUsers, rsrpSum, sinrSum,
            attachAttempts, attachSuccess, rsrpCount, sinrCount, sourceEventTsAvg,
            sourceEventTsMin, sourceEventTsMax, sourceEventCount);
    }

    @Override
    public String toString() {
        return "ChrMinuteFact{"
            + "cellId='" + cellId + '\''
            + ", siteId='" + siteId + '\''
            + ", minuteTs=" + minuteTs
            + ", count=" + count
            + ", uniqueUsers=" + uniqueUsers
            + ", rsrpSum=" + rsrpSum
            + ", sinrSum=" + sinrSum
            + ", attachAttempts=" + attachAttempts
            + ", attachSuccess=" + attachSuccess
            + ", rsrpCount=" + rsrpCount
            + ", sinrCount=" + sinrCount
            + ", sourceEventTsAvg=" + sourceEventTsAvg
            + ", sourceEventTsMin=" + sourceEventTsMin
            + ", sourceEventTsMax=" + sourceEventTsMax
            + ", sourceEventCount=" + sourceEventCount
            + '}';
    }
}
