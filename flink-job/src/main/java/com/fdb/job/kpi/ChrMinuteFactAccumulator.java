package com.fdb.job.kpi;

import com.fdb.job.model.ChrMinuteFact;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class ChrMinuteFactAccumulator implements Serializable {

    private String cellId = "";
    private String siteId = "";
    private long count;
    private Set<String> users = new HashSet<>();
    private double rsrpSum;
    private long rsrpCount;
    private double sinrSum;
    private long sinrCount;
    private long attachAttempts;
    private long attachSuccess;

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public Set<String> getUsers() {
        return users;
    }

    public void setUsers(Set<String> users) {
        this.users = users;
    }

    public double getRsrpSum() {
        return rsrpSum;
    }

    public void setRsrpSum(double rsrpSum) {
        this.rsrpSum = rsrpSum;
    }

    public long getRsrpCount() {
        return rsrpCount;
    }

    public void setRsrpCount(long rsrpCount) {
        this.rsrpCount = rsrpCount;
    }

    public double getSinrSum() {
        return sinrSum;
    }

    public void setSinrSum(double sinrSum) {
        this.sinrSum = sinrSum;
    }

    public long getSinrCount() {
        return sinrCount;
    }

    public void setSinrCount(long sinrCount) {
        this.sinrCount = sinrCount;
    }

    public long getAttachAttempts() {
        return attachAttempts;
    }

    public void setAttachAttempts(long attachAttempts) {
        this.attachAttempts = attachAttempts;
    }

    public long getAttachSuccess() {
        return attachSuccess;
    }

    public void setAttachSuccess(long attachSuccess) {
        this.attachSuccess = attachSuccess;
    }

    void addUser(String imsi) {
        users.add(imsi);
    }

    void mergeUsers(Set<String> otherUsers) {
        users.addAll(otherUsers);
    }

    ChrMinuteFact toMinuteFact(String fallbackCellId, long minuteTs) {
        String effectiveCellId = cellId == null || cellId.isBlank() ? fallbackCellId : cellId;
        return new ChrMinuteFact(effectiveCellId, siteId, minuteTs, count, users.size(),
            rsrpSum, sinrSum, attachAttempts, attachSuccess, rsrpCount, sinrCount);
    }
}
