package com.fdb.benchmark;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ShellDeployCommandClient implements DeployCommandClient {
  private final String target;
  private final ProcessCommandRunner commandRunner;
  private final Map<String, String> baseEnv;

  public ShellDeployCommandClient(String target, ProcessCommandRunner commandRunner, Map<String, String> baseEnv) {
    this.target = target;
    this.commandRunner = commandRunner;
    this.baseEnv = Map.copyOf(baseEnv);
  }

  @Override
  public void prepare(BenchmarkRunPlan plan) throws Exception {
    runDeploy(plan, "prepare");
  }

  @Override
  public void submit(BenchmarkRunPlan plan) throws Exception {
    runDeploy(plan, "submit");
  }

  @Override
  public void stop(BenchmarkRunPlan plan) throws Exception {
    runDeploy(plan, "stop");
  }

  private void runDeploy(BenchmarkRunPlan plan, String command) throws IOException, InterruptedException {
    CommandResult result = commandRunner.run(deployCommand(command), envFor(plan));
    if (!result.success()) {
      throw new IOException("deploy " + command + " failed: " + result.stderr() + result.stdout());
    }
  }

  List<String> deployCommand(String command) {
    String bash = baseEnv.get("FDB_BENCHMARK_BASH");
    if (bash == null || bash.isBlank()) {
      bash = "bash";
    }
    return List.of(bash, "scripts/deploy.sh", target, command);
  }

  private Map<String, String> envFor(BenchmarkRunPlan plan) {
    Map<String, String> env = new HashMap<>(baseEnv);
    env.put("FDB_RUN_ID", plan.runId());
    env.put("FDB_RUN_LABEL", plan.runLabel());
    env.put("FDB_RESULT_SINK", plan.sink().value());
    env.put("FDB_SITES_COUNT", String.valueOf(plan.cellLevel()));
    env.put("FDB_RATE_EPS", String.valueOf(plan.targetChrEps()));
    return env;
  }
}
