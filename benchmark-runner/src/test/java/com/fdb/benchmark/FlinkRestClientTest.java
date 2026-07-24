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
        metricPath("job-a", "v1"),
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
                  {"id":"currentOutputWatermark","value":"179000"},
                  {"id":"numRecordsIn","value":"100000"},
                  {"id":"numRecordsOut","value":"95000"}
                ]
                """,
        metricPath("job-a", "v2"),
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
                  {"id":"currentOutputWatermark","value":"180000"},
                  {"id":"numRecordsIn","value":"95000"},
                  {"id":"numRecordsOut","value":"90000"}
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
      assertThat(operator.metricsAvailable()).isTrue();
    });
  }

  @Test
  void reads_cumulative_record_totals_from_vertex_metric_endpoint() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/job-a",
            """
                {"vertices":[{"id":"v1","name":"source","parallelism":1,"metrics":{}}]}
                """,
        metricPath("job-a", "v1"),
            """
                [
                  {"id":"numRecordsIn","value":"1234"},
                  {"id":"numRecordsOut","value":"1200"},
                  {"id":"numRecordsInPerSecond","value":"10"},
                  {"id":"numRecordsOutPerSecond","value":"9"}
                ]
                """));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.recordsInTotal()).isEqualTo(1234);
    assertThat(snapshot.recordsOutTotal()).isEqualTo(1200);
    assertThat(snapshot.operators()).singleElement().satisfies(operator -> {
      assertThat(operator.recordsInTotal()).isEqualTo(1234);
      assertThat(operator.recordsOutTotal()).isEqualTo(1200);
      assertThat(operator.metricsAvailable()).isTrue();
    });
  }

  @Test
  void selects_newest_terminal_job_when_no_active_job_exists() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview",
            """
                {"jobs":[
                  {"jid":"old-job","state":"CANCELED","start-time":1000},
                  {"jid":"new-job","state":"CANCELED","start-time":2000}
                ]}
                """,
        "/jobs/old-job/checkpoints", "{}",
        "/jobs/new-job/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/old-job",
            """
                {"vertices":[{"id":"old-v","name":"old-source","parallelism":1,"metrics":{}}]}
                """,
        "/jobs/new-job",
            """
                {"vertices":[{"id":"new-v","name":"new-source","parallelism":1,"metrics":{}}]}
                """,
        "/jobs/old-job/vertices/old-v/metrics",
            """
                [
                  {"id":"numRecordsOut","value":"1"},
                  {"id":"numRecordsOutPerSecond","value":"1"}
                ]
                """,
        "/jobs/new-job/vertices/new-v/metrics",
            """
                [
                  {"id":"numRecordsOut","value":"2000"},
                  {"id":"numRecordsOutPerSecond","value":"200"}
                ]
                """));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.jobStatus()).isEqualTo("CANCELED");
    assertThat(snapshot.recordsOutTotal()).isEqualTo(2000);
    assertThat(snapshot.operators()).singleElement().satisfies(operator -> {
      assertThat(operator.id()).isEqualTo("new-v");
      assertThat(operator.recordsOutTotal()).isEqualTo(2000);
    });
  }

  @Test
  void reads_standard_subtask_metrics_without_listing_metric_ids() throws Exception {
    Map<String, String> responses = Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/job-a",
            """
                {"vertices":[{
                  "id":"v1",
                  "name":"Source: chr-source -> to-chr-env",
                  "parallelism":2,
                  "metrics":{}
                }]}
                """);
    HttpGateway http = uri -> {
      String key = uri.getQuery() == null ? uri.getPath() : uri.getPath() + "?" + uri.getQuery();
      if (key.startsWith("/jobs/job-a/vertices/v1/metrics?get=")) {
        String[] ids = uri.getQuery().substring("get=".length()).split(",");
        return containsSubtaskMetric(ids) ? metricValues(ids, 1) : "[]";
      }
      String body = responses.get(key);
      if (body == null) {
        throw new java.io.IOException("missing fake response for " + key);
      }
      return body;
    };

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.recordsInPerSec()).isEqualTo(2);
    assertThat(snapshot.recordsOutPerSec()).isEqualTo(2);
    assertThat(snapshot.recordsInTotal()).isEqualTo(2);
    assertThat(snapshot.recordsOutTotal()).isEqualTo(2);
    assertThat(snapshot.operators()).singleElement().satisfies(operator -> {
      assertThat(operator.metricsAvailable()).isTrue();
      assertThat(operator.busyRatio()).isCloseTo(0.001, within(0.0001));
    });
  }

  private static boolean containsSubtaskMetric(String[] ids) {
    for (String id : ids) {
      if (id.startsWith("0.") || id.startsWith("1.")) {
        return true;
      }
    }
    return false;
  }

  @Test
  void aggregates_subtask_prefixed_metrics_exposed_by_flink_rest() throws Exception {
    Map<String, String> responses = Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/job-a",
            """
                {"vertices":[{
                  "id":"v1",
                  "name":"Source: chr-source -> to-chr-env",
                  "parallelism":2,
                  "metrics":{}
                }]}
                """,
        "/jobs/job-a/vertices/v1/metrics",
            """
                [
                  {"id":"0.numRecordsInPerSecond"},
                  {"id":"1.numRecordsInPerSecond"},
                  {"id":"0.numRecordsOutPerSecond"},
                  {"id":"1.numRecordsOutPerSecond"},
                  {"id":"0.numBytesInPerSecond"},
                  {"id":"1.numBytesInPerSecond"},
                  {"id":"0.numBytesOutPerSecond"},
                  {"id":"1.numBytesOutPerSecond"},
                  {"id":"0.busyTimeMsPerSecond"},
                  {"id":"1.busyTimeMsPerSecond"},
                  {"id":"0.idleTimeMsPerSecond"},
                  {"id":"1.idleTimeMsPerSecond"},
                  {"id":"0.backPressuredTimeMsPerSecond"},
                  {"id":"1.backPressuredTimeMsPerSecond"},
                  {"id":"0.pendingRecords"},
                  {"id":"1.pendingRecords"},
                  {"id":"0.currentInputWatermark"},
                  {"id":"1.currentInputWatermark"},
                  {"id":"0.currentOutputWatermark"},
                  {"id":"1.currentOutputWatermark"},
                  {"id":"0.numRecordsIn"},
                  {"id":"1.numRecordsIn"},
                  {"id":"0.numRecordsOut"},
                  {"id":"1.numRecordsOut"},
                  {"id":"0.numBytesIn"},
                  {"id":"1.numBytesIn"},
                  {"id":"0.numBytesOut"},
                  {"id":"1.numBytesOut"}
                ]
                """);
    String metricValues = """
        [
          {"id":"0.numRecordsInPerSecond","value":"10"},
          {"id":"1.numRecordsInPerSecond","value":"20"},
          {"id":"0.numRecordsOutPerSecond","value":"100"},
          {"id":"1.numRecordsOutPerSecond","value":"120"},
          {"id":"0.numBytesInPerSecond","value":"1000"},
          {"id":"1.numBytesInPerSecond","value":"1200"},
          {"id":"0.numBytesOutPerSecond","value":"2000"},
          {"id":"1.numBytesOutPerSecond","value":"2200"},
          {"id":"0.busyTimeMsPerSecond","value":"600"},
          {"id":"1.busyTimeMsPerSecond","value":"800"},
          {"id":"0.idleTimeMsPerSecond","value":"300"},
          {"id":"1.idleTimeMsPerSecond","value":"100"},
          {"id":"0.backPressuredTimeMsPerSecond","value":"100"},
          {"id":"1.backPressuredTimeMsPerSecond","value":"100"},
          {"id":"0.pendingRecords","value":"5"},
          {"id":"1.pendingRecords","value":"7"},
          {"id":"0.currentInputWatermark","value":"178000"},
          {"id":"1.currentInputWatermark","value":"178500"},
          {"id":"0.currentOutputWatermark","value":"179000"},
          {"id":"1.currentOutputWatermark","value":"179500"},
          {"id":"0.numRecordsIn","value":"1000"},
          {"id":"1.numRecordsIn","value":"1200"},
          {"id":"0.numRecordsOut","value":"5000"},
          {"id":"1.numRecordsOut","value":"6000"},
          {"id":"0.numBytesIn","value":"10000"},
          {"id":"1.numBytesIn","value":"12000"},
          {"id":"0.numBytesOut","value":"20000"},
          {"id":"1.numBytesOut","value":"22000"}
        ]
        """;
    HttpGateway http = uri -> {
      String key = uri.getQuery() == null ? uri.getPath() : uri.getPath() + "?" + uri.getQuery();
      if (key.startsWith("/jobs/job-a/vertices/v1/metrics?get=")) {
        return metricValues;
      }
      String body = responses.get(key);
      if (body == null) {
        throw new java.io.IOException("missing fake response for " + key);
      }
      return body;
    };

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.hasOperatorMetrics()).isTrue();
    assertThat(snapshot.recordsInPerSec()).isEqualTo(30);
    assertThat(snapshot.recordsOutPerSec()).isEqualTo(220);
    assertThat(snapshot.recordsInTotal()).isEqualTo(2200);
    assertThat(snapshot.recordsOutTotal()).isEqualTo(11000);
    assertThat(snapshot.sourceBacklogRecords()).isEqualTo(12);
    assertThat(snapshot.operators()).singleElement().satisfies(operator -> {
      assertThat(operator.bytesInPerSec()).isEqualTo(2200);
      assertThat(operator.bytesOutPerSec()).isEqualTo(4200);
      assertThat(operator.busyRatio()).isCloseTo(0.7, within(0.0001));
      assertThat(operator.idleRatio()).isCloseTo(0.2, within(0.0001));
      assertThat(operator.backpressureRatio()).isCloseTo(0.1, within(0.0001));
      assertThat(operator.pendingRecords()).isEqualTo(12);
      assertThat(operator.currentInputWatermarkMs()).isEqualTo(178000);
      assertThat(operator.currentOutputWatermarkMs()).isEqualTo(179000);
      assertThat(operator.metricsAvailable()).isTrue();
    });
  }

  @Test
  void reads_discovered_metric_values_in_batches() throws Exception {
    Map<String, String> responses = Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/job-a",
            """
                {"vertices":[{
                  "id":"v1",
                  "name":"wide chained operator",
                  "parallelism":40,
                  "metrics":{}
                }]}
                """,
        "/jobs/job-a/vertices/v1/metrics", metricIdList("numRecordsOut", 40));
    java.util.concurrent.atomic.AtomicInteger valueRequests = new java.util.concurrent.atomic.AtomicInteger();
    HttpGateway http = uri -> {
      String key = uri.getQuery() == null ? uri.getPath() : uri.getPath() + "?" + uri.getQuery();
      if (key.startsWith("/jobs/job-a/vertices/v1/metrics?get=")) {
        valueRequests.incrementAndGet();
        String[] ids = uri.getQuery().substring("get=".length()).split(",");
        assertThat(ids).hasSizeLessThanOrEqualTo(32);
        return metricValues(ids, 1);
      }
      String body = responses.get(key);
      if (body == null) {
        throw new java.io.IOException("missing fake response for " + key);
      }
      return body;
    };

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(valueRequests.get()).isGreaterThan(1);
    assertThat(snapshot.recordsOutTotal()).isEqualTo(40);
    assertThat(snapshot.hasOperatorMetrics()).isTrue();
  }

  @Test
  void ignores_job_detail_vertex_metrics_when_vertex_metric_endpoint_has_no_values() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/job-a",
            """
                {"vertices":[{
                  "id":"v1",
                  "name":"source",
                  "parallelism":1,
                  "metrics":{"read-records":999,"write-records":998}
                }]}
                """,
        metricPath("job-a", "v1"), "[]",
        "/jobs/job-a/vertices/v1/metrics", "[]"));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.hasOperatorMetrics()).isFalse();
    assertThat(snapshot.recordsInTotal()).isZero();
    assertThat(snapshot.recordsOutTotal()).isZero();
    assertThat(snapshot.operators()).singleElement().satisfies(operator -> {
      assertThat(operator.metricsAvailable()).isFalse();
      assertThat(operator.recordsInTotal()).isZero();
      assertThat(operator.recordsOutTotal()).isZero();
    });
  }

  @Test
  void marks_operator_metrics_unavailable_when_flink_exposes_no_metric_values() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{}",
        "/taskmanagers", "{\"taskmanagers\":[]}",
        "/jobs/job-a",
            """
                {"vertices":[{"id":"v1","name":"source","parallelism":1,"metrics":{}}]}
                """,
        metricPath("job-a", "v1"), "[]",
        "/jobs/job-a/vertices/v1/metrics", "[]"));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.hasOperatorMetrics()).isFalse();
    assertThat(snapshot.operators()).singleElement().satisfies(operator -> {
      assertThat(operator.metricsAvailable()).isFalse();
      assertThat(operator.recordsInTotal()).isZero();
      assertThat(operator.recordsOutTotal()).isZero();
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
  void ignores_mailbox_latency_when_reading_flink_latency_markers() throws Exception {
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
                }]}
                """,
        metricPath("job-a", "v1"), "[]",
        "/jobs/job-a/vertices/v1/metrics",
            """
                [
                  {"id":"0.mailboxLatencyMs_p95","value":"123"}
                ]
                """));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.operators()).singleElement().satisfies(operator -> {
      assertThat(operator.flinkMarkerP95Ms()).isEqualTo(-1);
      assertThat(operator.flinkMarkerLatencies()).isEmpty();
    });
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
        + "currentInputWatermark,currentOutputWatermark,numRecordsIn,numRecordsOut,numBytesIn,numBytesOut";
  }

  private static String metricIdList(String metricName, int count) {
    StringBuilder ids = new StringBuilder("[");
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        ids.append(',');
      }
      ids.append("{\"id\":\"").append(i).append('.').append(metricName).append("\"}");
    }
    return ids.append(']').toString();
  }

  private static String metricValues(String[] ids, int value) {
    StringBuilder values = new StringBuilder("[");
    for (int i = 0; i < ids.length; i++) {
      if (i > 0) {
        values.append(',');
      }
      values.append("{\"id\":\"").append(ids[i]).append("\",\"value\":\"").append(value).append("\"}");
    }
    return values.append(']').toString();
  }
}
