package com.fdb.job.model;

import java.io.Serializable;
import java.util.Objects;

public class PmMinuteFact implements Serializable {

    private String cellId;
    private String siteId;
    private long minuteTs;
    private long pmWindowCount;
    private double prbUsageDlSum;
    private double throughputDlMbpsSum;
    private long activeUsersSum;
    private long dropCount;
    private long handoverSuccess;
    private long handoverFailure;

    public PmMinuteFact() {
    }

    public PmMinuteFact(
        String cellId,
        String siteId,
        long minuteTs,
        long pmWindowCount,
        double prbUsageDlSum,
        double throughputDlMbpsSum,
        long dropCount,
        long handoverSuccess,
        long handoverFailure) {
        this(cellId, siteId, minuteTs, pmWindowCount, prbUsageDlSum, throughputDlMbpsSum,
            0L, dropCount, handoverSuccess, handoverFailure);
    }

    public PmMinuteFact(
        String cellId,
        String siteId,
        long minuteTs,
        long pmWindowCount,
        double prbUsageDlSum,
        double throughputDlMbpsSum,
        long activeUsersSum,
        long dropCount,
        long handoverSuccess,
        long handoverFailure) {
        this.cellId = cellId;
        this.siteId = siteId;
        this.minuteTs = minuteTs;
        this.pmWindowCount = pmWindowCount;
        this.prbUsageDlSum = prbUsageDlSum;
        this.throughputDlMbpsSum = throughputDlMbpsSum;
        this.activeUsersSum = activeUsersSum;
        this.dropCount = dropCount;
        this.handoverSuccess = handoverSuccess;
        this.handoverFailure = handoverFailure;
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

    public long pmWindowCount() {
        return pmWindowCount;
    }

    public long getPmWindowCount() {
        return pmWindowCount;
    }

    public void setPmWindowCount(long pmWindowCount) {
        this.pmWindowCount = pmWindowCount;
    }

    public double prbUsageDlSum() {
        return prbUsageDlSum;
    }

    public double getPrbUsageDlSum() {
        return prbUsageDlSum;
    }

    public void setPrbUsageDlSum(double prbUsageDlSum) {
        this.prbUsageDlSum = prbUsageDlSum;
    }

    public double throughputDlMbpsSum() {
        return throughputDlMbpsSum;
    }

    public double getThroughputDlMbpsSum() {
        return throughputDlMbpsSum;
    }

    public void setThroughputDlMbpsSum(double throughputDlMbpsSum) {
        this.throughputDlMbpsSum = throughputDlMbpsSum;
    }

    public long activeUsersSum() {
        return activeUsersSum;
    }

    public long getActiveUsersSum() {
        return activeUsersSum;
    }

    public void setActiveUsersSum(long activeUsersSum) {
        this.activeUsersSum = activeUsersSum;
    }

    public long dropCount() {
        return dropCount;
    }

    public long getDropCount() {
        return dropCount;
    }

    public void setDropCount(long dropCount) {
        this.dropCount = dropCount;
    }

    public long handoverSuccess() {
        return handoverSuccess;
    }

    public long getHandoverSuccess() {
        return handoverSuccess;
    }

    public void setHandoverSuccess(long handoverSuccess) {
        this.handoverSuccess = handoverSuccess;
    }

    public long handoverFailure() {
        return handoverFailure;
    }

    public long getHandoverFailure() {
        return handoverFailure;
    }

    public void setHandoverFailure(long handoverFailure) {
        this.handoverFailure = handoverFailure;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PmMinuteFact that)) {
            return false;
        }
        return minuteTs == that.minuteTs
            && pmWindowCount == that.pmWindowCount
            && Double.compare(that.prbUsageDlSum, prbUsageDlSum) == 0
            && Double.compare(that.throughputDlMbpsSum, throughputDlMbpsSum) == 0
            && activeUsersSum == that.activeUsersSum
            && dropCount == that.dropCount
            && handoverSuccess == that.handoverSuccess
            && handoverFailure == that.handoverFailure
            && Objects.equals(cellId, that.cellId)
            && Objects.equals(siteId, that.siteId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cellId, siteId, minuteTs, pmWindowCount, prbUsageDlSum,
            throughputDlMbpsSum, activeUsersSum, dropCount, handoverSuccess, handoverFailure);
    }

    @Override
    public String toString() {
        return "PmMinuteFact{"
            + "cellId='" + cellId + '\''
            + ", siteId='" + siteId + '\''
            + ", minuteTs=" + minuteTs
            + ", pmWindowCount=" + pmWindowCount
            + ", prbUsageDlSum=" + prbUsageDlSum
            + ", throughputDlMbpsSum=" + throughputDlMbpsSum
            + ", activeUsersSum=" + activeUsersSum
            + ", dropCount=" + dropCount
            + ", handoverSuccess=" + handoverSuccess
            + ", handoverFailure=" + handoverFailure
            + '}';
    }
}
