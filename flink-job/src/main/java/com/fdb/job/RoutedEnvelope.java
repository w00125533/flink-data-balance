package com.fdb.job;

import java.io.Serializable;

public class RoutedEnvelope implements Serializable {
    private InputEnvelope envelope;
    private int vbucketId;

    public RoutedEnvelope() {}

    public RoutedEnvelope(InputEnvelope envelope, int vbucketId) {
        this.envelope = envelope;
        this.vbucketId = vbucketId;
    }

    public InputEnvelope envelope() {
        return envelope;
    }

    public int vbucketId() {
        return vbucketId;
    }

    public void setEnvelope(InputEnvelope envelope) {
        this.envelope = envelope;
    }

    public void setVbucketId(int vbucketId) {
        this.vbucketId = vbucketId;
    }

    public String siteId() {
        if (envelope instanceof InputEnvelope.ChrEnv e) return e.chrEvent().getSiteId().toString();
        if (envelope instanceof InputEnvelope.MrEnv e) return e.mrStat().getSiteId().toString();
        return ((InputEnvelope.CmEnv) envelope).cmConfig().getSiteId().toString();
    }

    public String stateKey() {
        return envelope.cellId();
    }
}
