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
        return stateKey();
    }

    public String stateKey() {
        return envelope.cellId();
    }
}
