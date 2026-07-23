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
        "/jobs/job-a/checkpoints", "{\"latest\":{\"completed\":{\"end_to_end_duration\":42000}},\"counts\":{\"failed\":1}}",
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
                }],
                "plan":{"nodes":[{
                  "id":"v1"
                },{
                  "id":"v2",
                  "inputs":[{"id":"v1"}]
                }]}}
                """,
        "/jobs/job-a/vertices/v1/metrics?get=numRecordsInPerSecond,numRecordsOutPerSecond,numBytesInPerSecond,numBytesOutPerSecond,busyTimeMsPerSecond,idleTimeMsPerSecond,backPressuredTimeMsPerSecond,pendingRecords,currentInputWatermark,currentOutputWatermark",
            """
                [
                  {"id":"numRecordsInPerSecond","value":"120.5"},
                  {"id":"numRecordsOutPerSecond","value":"118.25"},
                  {"id":"numBytesInPerSecond","value":"2048"},
                  {"id":"numBytesOutPerSecond","value":"1024"},
                  {"id":"busyTimeMsPerSecond","value":"800"},
                  {"id":"idleTimeMsPerSecond","value":"175"},
                  {"id":"backPressuredTimeMsPerSecond","value":"25"},
                  {"id":"pendingRecords","value":"42"},
                  {"id":"currentInputWatermark","value":"178000"},
                  {"id":"currentOutputWatermark","value":"179000"}
                ]
                """,
        "/jobs/job-a/vertices/v2/metrics?get=numRecordsInPerSecond,numRecordsOutPerSecond,numBytesInPerSecond,numBytesOutPerSecond,busyTimeMsPerSecond,idleTimeMsPerSecond,backPressuredTimeMsPerSecond,pendingRecords,currentInputWatermark,currentOutputWatermark",
            """
                [
                  {"id":"numRecordsInPerSecond","value":"118.25"},
                  {"id":"numRecordsOutPerSecond","value":"117"},
                  {"id":"numBytesInPerSecond","value":"1024"},
                  {"id":"numBytesOutPerSecond","value":"768"},
                  {"id":"busyTimeMsPerSecond","value":"500"},
                  {"id":"idleTimeMsPerSecond","value":"475"},
                  {"id":"backPressuredTimeMsPerSecond","value":"25"},
                  {"id":"pendingRecords","value":"7"},
                  {"id":"currentInputWatermark","value":"179000"},
                  {"id":"currentOutputWatermark","value":"180000"}
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
    assertThat(snapshot.operatorEdges()).containsExactly(new FlinkOperatorEdge("v1", "v2"));
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
      assertThat(operator.currentInputWatermarkMs()).isEqualTo(178000);
      assertThat(operator.currentOutputWatermarkMs()).isEqualTo(179000);
      assertThat(operator.busyRatio()).isCloseTo(0.8, within(0.0001));
      assertThat(operator.idleRatio()).isCloseTo(0.175, within(0.0001));
      assertThat(operator.backpressureRatio()).isCloseTo(0.025, within(0.0001));
    });
  }

  @Test
  void reads_operator_latency_marker_p95_when_exposed_by_flink_metrics() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/job-a",
            """
                {"vertices":[{
                  "id":"v1",
                  "name":"kpi-1m-full-join",
                  "parallelism":2,
                  "metrics":{}
                }]}
                """,
        metricPath("job-a", "v1"),
            """
                [
                  {"id":"numRecordsInPerSecond","value":"100"}
                ]
                """,
        "/jobs/job-a/vertices/v1/metrics",
            """
                [
                  {"id":"latency.source_id.operator_id.latency_p95","value":"37"},
                  {"id":"latency.source_id.operator_id.latency_p99","value":"52"}
                ]
                """));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.operators()).hasSize(1);
    assertThat(snapshot.operators().get(0).flinkMarkerP95Ms()).isEqualTo(37);
  }

  @Test
  void reads_operator_latency_marker_p95_by_input_edge_when_exposed_by_flink_metrics() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/job-a",
            """
                {"vertices":[
                  {"id":"chr-source","name":"Source: chr-source","parallelism":1,"metrics":{}},
                  {"id":"pm-source","name":"Source: pm-source","parallelism":1,"metrics":{}},
                  {"id":"kpi-join","name":"kpi-1m-full-join","parallelism":2,"metrics":{}}
                ],
                "plan":{"nodes":[
                  {"id":"chr-source"},
                  {"id":"pm-source"},
                  {"id":"kpi-join","inputs":[{"id":"chr-source"},{"id":"pm-source"}]}
                ]}}
                """,
        metricPath("job-a", "chr-source"), "[]",
        metricPath("job-a", "pm-source"), "[]",
        metricPath("job-a", "kpi-join"), "[]",
        "/jobs/job-a/vertices/chr-source/metrics", "[]",
        "/jobs/job-a/vertices/pm-source/metrics", "[]",
        "/jobs/job-a/vertices/kpi-join/metrics",
            """
                [
                  {"id":"latency.chr-source.kpi-join.latency_p95","value":"230"},
                  {"id":"latency.pm-source.kpi-join.latency_p95","value":"18"}
                ]
                """));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.operators()).hasSize(3);
    assertThat(snapshot.operators().get(2).flinkMarkerP95Ms()).isEqualTo(230);
    assertThat(snapshot.operators().get(2).flinkMarkerLatencies()).containsExactly(
        new FlinkMarkerLatencySnapshot("chr-source", "kpi-join", 230,
            "latency.chr-source.kpi-join.latency_p95"),
        new FlinkMarkerLatencySnapshot("pm-source", "kpi-join", 18,
            "latency.pm-source.kpi-join.latency_p95"));
  }

  @Test
  void reads_operator_latency_marker_values_after_listing_marker_metric_ids() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/job-a",
            """
                {"vertices":[
                  {"id":"chr-source","name":"Source: chr-source","parallelism":1,"metrics":{}},
                  {"id":"kpi-join","name":"kpi-1m-full-join","parallelism":2,"metrics":{}}
                ],
                "plan":{"nodes":[
                  {"id":"chr-source"},
                  {"id":"kpi-join","inputs":[{"id":"chr-source"}]}
                ]}}
                """,
        metricPath("job-a", "chr-source"), "[]",
        metricPath("job-a", "kpi-join"), "[]",
        "/jobs/job-a/vertices/chr-source/metrics", "[]",
        "/jobs/job-a/vertices/kpi-join/metrics",
            """
                [
                  {"id":"latency.chr-source.kpi-join.latency_p95"},
                  {"id":"latency.chr-source.kpi-join.latency_p99"}
                ]
                """,
        "/jobs/job-a/vertices/kpi-join/metrics?get=latency.chr-source.kpi-join.latency_p95",
            """
                [
                  {"id":"latency.chr-source.kpi-join.latency_p95","value":"145"}
                ]
                """));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.operators().get(1).flinkMarkerP95Ms()).isEqualTo(145);
    assertThat(snapshot.operators().get(1).flinkMarkerLatencies()).containsExactly(
        new FlinkMarkerLatencySnapshot("chr-source", "kpi-join", 145,
            "latency.chr-source.kpi-join.latency_p95"));
  }

  @Test
  void source_backlog_records_uses_max_pending_records_across_source_operators() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/job-a",
            """
                {"vertices":[{
                  "id":"v1",
                  "name":"chr-source",
                  "parallelism":1,
                  "metrics":{}
                },{
                  "id":"v2",
                  "name":"pm-source",
                  "parallelism":1,
                  "metrics":{}
                }]}
                """,
        metricPath("job-a", "v1"),
            """
                [
                  {"id":"pendingRecords","value":"42"}
                ]
                """,
        metricPath("job-a", "v2"),
            """
                [
                  {"id":"pendingRecords","value":"30"}
                ]
                """));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.sourceBacklogRecords()).isEqualTo(42);
  }

  @Test
  void invalid_or_missing_pending_records_are_treated_as_zero() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/job-a",
            """
                {"vertices":[{
                  "id":"v1",
                  "name":"chr-source",
                  "parallelism":1,
                  "metrics":{}
                },{
                  "id":"v2",
                  "name":"pm-source",
                  "parallelism":1,
                  "metrics":{}
                }]}
                """,
        metricPath("job-a", "v1"),
            """
                [
                  {"id":"pendingRecords","value":"not-a-number"}
                ]
                """,
        metricPath("job-a", "v2"),
            """
                [
                  {"id":"numRecordsOutPerSecond","value":"10"}
                ]
                """));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.sourceBacklogRecords()).isZero();
    assertThat(snapshot.operators())
        .extracting(FlinkOperatorSnapshot::pendingRecords)
        .containsExactly(0L, 0L);
  }

  @Test
  void unavailable_jobs_overview_returns_unknown_snapshot() throws Exception {
    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), uri -> {
      throw new java.net.http.HttpTimeoutException("request timed out");
    }).snapshot();

    assertThat(snapshot.jobStatus()).isEqualTo("UNKNOWN");
    assertThat(snapshot.operators()).isEmpty();
    assertThat(snapshot.recordsInTotal()).isZero();
    assertThat(snapshot.recordsOutTotal()).isZero();
  }

  @Test
  void source_backlog_records_is_zero_when_no_operator_name_contains_source() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/job-a",
            """
                {"vertices":[{
                  "id":"v1",
                  "name":"map",
                  "parallelism":1,
                  "metrics":{}
                },{
                  "id":"v2",
                  "name":"sink",
                  "parallelism":1,
                  "metrics":{}
                }]}
                """,
        metricPath("job-a", "v1"),
            """
                [
                  {"id":"pendingRecords","value":"42"}
                ]
                """,
        metricPath("job-a", "v2"),
            """
                [
                  {"id":"pendingRecords","value":"30"}
                ]
                """));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.sourceBacklogRecords()).isZero();
  }

  private static String metricPath(String jobId, String vertexId) {
    return "/jobs/" + jobId + "/vertices/" + vertexId
        + "/metrics?get=numRecordsInPerSecond,numRecordsOutPerSecond,numBytesInPerSecond,numBytesOutPerSecond,"
        + "busyTimeMsPerSecond,idleTimeMsPerSecond,backPressuredTimeMsPerSecond,pendingRecords,"
        + "currentInputWatermark,currentOutputWatermark";
  }
}
