package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FlinkRestClientTest {
  @Test
  void reads_running_job_checkpoint_and_metrics_summary() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{\"latest\":{\"completed\":{\"duration\":42000}},\"counts\":{\"failed\":1}}",
        "/taskmanagers", "{\"taskmanagers\":[{\"id\":\"tm-1\",\"slotsNumber\":4},{\"id\":\"tm-2\",\"slotsNumber\":4}]}",
        "/jobs/job-a",
            """
                {"vertices":[{
                  "id":"v1",
                  "name":"source",
                  "parallelism":4,
                  "metrics":{
                    "read-records":100000,
                    "write-records":95000,
                    "read-bytes":819200,
                    "write-bytes":409600,
                    "busyTimeMsPerSecond":800,
                    "idleTimeMsPerSecond":175,
                    "backPressuredTimeMsPerSecond":25
                  }
                },{
                  "id":"v2",
                  "name":"sink-starrocks",
                  "parallelism":4,
                  "metrics":{
                    "read-records":95000,
                    "write-records":90000,
                    "read-bytes":409600,
                    "write-bytes":204800,
                    "busyTimeMsPerSecond":500,
                    "idleTimeMsPerSecond":475,
                    "backPressuredTimeMsPerSecond":25
                  }
                }]}
                """,
        "/jobs/job-a/vertices/v1/metrics?get=numRecordsInPerSecond,numRecordsOutPerSecond,numBytesInPerSecond,numBytesOutPerSecond,busyTimeMsPerSecond,idleTimeMsPerSecond,backPressuredTimeMsPerSecond,pendingRecords",
            """
                [
                  {"id":"numRecordsInPerSecond","value":"120.5"},
                  {"id":"numRecordsOutPerSecond","value":"118.25"},
                  {"id":"numBytesInPerSecond","value":"2048"},
                  {"id":"numBytesOutPerSecond","value":"1024"},
                  {"id":"busyTimeMsPerSecond","value":"800"},
                  {"id":"idleTimeMsPerSecond","value":"175"},
                  {"id":"backPressuredTimeMsPerSecond","value":"25"},
                  {"id":"pendingRecords","value":"42"}
                ]
                """,
        "/jobs/job-a/vertices/v2/metrics?get=numRecordsInPerSecond,numRecordsOutPerSecond,numBytesInPerSecond,numBytesOutPerSecond,busyTimeMsPerSecond,idleTimeMsPerSecond,backPressuredTimeMsPerSecond,pendingRecords",
            """
                [
                  {"id":"numRecordsInPerSecond","value":"118.25"},
                  {"id":"numRecordsOutPerSecond","value":"117"},
                  {"id":"numBytesInPerSecond","value":"1024"},
                  {"id":"numBytesOutPerSecond","value":"768"},
                  {"id":"busyTimeMsPerSecond","value":"500"},
                  {"id":"idleTimeMsPerSecond","value":"475"},
                  {"id":"backPressuredTimeMsPerSecond","value":"25"},
                  {"id":"pendingRecords","value":"7"}
                ]
                """));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.jobStatus()).isEqualTo("RUNNING");
    assertThat(snapshot.checkpointDurationMs()).isEqualTo(42_000);
    assertThat(snapshot.recordsInPerSec()).isCloseTo(238.75, within(0.0001));
    assertThat(snapshot.recordsOutPerSec()).isCloseTo(235.25, within(0.0001));
    assertThat(snapshot.recordsInTotal()).isEqualTo(195000);
    assertThat(snapshot.recordsOutTotal()).isEqualTo(185000);
    assertThat(snapshot.sourceBacklogRecords()).isEqualTo(42);
    assertThat(snapshot.taskManagers()).isEqualTo(2);
    assertThat(snapshot.slots()).isEqualTo(8);
    assertThat(snapshot.backpressureRatio()).isCloseTo(0.025, within(0.0001));
    assertThat(snapshot.operators()).hasSize(2);
    assertThat(snapshot.operators().get(0)).satisfies(operator -> {
      assertThat(operator.id()).isEqualTo("v1");
      assertThat(operator.name()).isEqualTo("source");
      assertThat(operator.parallelism()).isEqualTo(4);
      assertThat(operator.recordsInPerSec()).isEqualTo(120.5);
      assertThat(operator.recordsOutPerSec()).isEqualTo(118.25);
      assertThat(operator.recordsInTotal()).isEqualTo(100000);
      assertThat(operator.recordsOutTotal()).isEqualTo(95000);
      assertThat(operator.bytesInPerSec()).isEqualTo(2048);
      assertThat(operator.bytesOutPerSec()).isEqualTo(1024);
      assertThat(operator.pendingRecords()).isEqualTo(42);
      assertThat(operator.busyRatio()).isCloseTo(0.8, within(0.0001));
      assertThat(operator.idleRatio()).isCloseTo(0.175, within(0.0001));
      assertThat(operator.backpressureRatio()).isCloseTo(0.025, within(0.0001));
    });
  }
}
