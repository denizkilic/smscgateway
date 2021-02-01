package org.mobicents.smsc.library;

import java.util.Date;

public class Alert {
    private String msisdn;
    private String partialTargetId;
    private Date createdDate;
    private String smscName;

    public Alert(String msisdn, String partialTargetId, String smscName, Date createdDate) {
        this.msisdn = msisdn;
        this.partialTargetId = partialTargetId;
        this.createdDate = createdDate;
        this.smscName = smscName;
    }

    public String getPartialTargetId() {
        return partialTargetId;
    }

    public void setPartialTargetId(String partialTargetId) {
        this.partialTargetId = partialTargetId;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    public String getSmscName() {
        return smscName;
    }

    public void setSmscName(String smscName) {
        this.smscName = smscName;
    }

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "msisdn='" + msisdn + '\'' +
                ", partialTargetId='" + partialTargetId + '\'' +
                ", createdDate=" + createdDate +
                ", smscName='" + smscName + '\'' +
                '}';
    }
}