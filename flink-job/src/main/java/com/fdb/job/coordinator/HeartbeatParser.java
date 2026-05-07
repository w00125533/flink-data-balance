package com.fdb.job.coordinator;

import org.apache.flink.api.common.functions.MapFunction;

public class HeartbeatParser implements MapFunction<String, HeartbeatPayload> {

    @Override
    public HeartbeatPayload map(String csvLine) throws Exception {
        return HeartbeatPayload.fromCsv(csvLine);
    }
}
