package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FlinkRestClientTest {
  @Test
  void reads_running_job_checkpoint_and_metrics_summary() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{\"latest\":{\"completed\":{\"duration\":42000}},\"counts\":{\"failed\":1}}",
        "/jobs/job-a/vertices",
            "{\"vertices\":[{\"id\":\"v1\",\"name\":\"sink\",\"metrics\":{\"read-records\":1000,\"write-records\":950}}]}"));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.jobStatus()).isEqualTo("RUNNING");
    assertThat(snapshot.checkpointDurationMs()).isEqualTo(42_000);
    assertThat(snapshot.recordsInPerSec()).isEqualTo(1000);
    assertThat(snapshot.recordsOutPerSec()).isEqualTo(950);
  }
}
