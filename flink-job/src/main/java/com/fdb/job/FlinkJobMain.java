package com.fdb.job;

import com.fdb.job.anomaly.CellKpiCepAnomalyDetector;
import com.fdb.job.anomaly.CoverageHoleDetector;
import com.fdb.job.anomaly.UserEventCepAnomalyDetector;
import com.fdb.job.balance.RoutingAssigner;
import com.fdb.job.balance.VBucketLoadMeter;
import com.fdb.job.config.JobConfig;
import com.fdb.job.config.ResultSinkConfig;
import com.fdb.job.config.ResultSinkType;
import com.fdb.job.config.RuleConfig;
import com.fdb.job.enrich.EnrichmentProcessFunction;
import com.fdb.job.kpi.CellKpiRollupAggregator;
import com.fdb.job.kpi.ChrMinuteFactWindowFunction;
import com.fdb.job.kpi.MinuteKpiJoinFunction;
import com.fdb.job.kpi.PmMinuteFactWindowFunction;
import com.fdb.job.metrics.StageMetricsProbe;
import com.fdb.job.metrics.MetricRuntimeConfig;
import com.fdb.job.model.ChrMinuteFact;
import com.fdb.job.model.EnrichedChr;
import com.fdb.job.model.InputEnvelope;
import com.fdb.job.model.MinuteFactEnvelope;
import com.fdb.job.model.PmMinuteFact;
import com.fdb.job.model.RoutedEnvelope;
import com.fdb.job.sink.IcebergConfig;
import com.fdb.job.sink.ResultSinks;
import com.fdb.job.source.FlinkAvroDeserializer;
import com.fdb.job.source.FlinkAvroSerializationSchema;
import com.fdb.common.avro.*;
import com.fdb.job.balance.coordinator.HeartbeatParser;
import com.fdb.job.balance.coordinator.HeartbeatPayload;
import com.fdb.job.balance.coordinator.LoadCoordinator;
import com.fdb.job.balance.coordinator.RoutingEntry;
import com.fdb.job.balance.coordinator.RoutingCsvSerializationSchema;
import com.fdb.common.hash.Hashes;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fdb.common.geo.Geohash;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class FlinkJobMain {

    private static final Logger log = LoggerFactory.getLogger(FlinkJobMain.class);
    private static final int DIRECT_ROUTE_VBUCKETS = 1024;
    private static final int DEFAULT_PARALLELISM = 4;

    public static void main(String[] args) throws Exception {
        Map<String, String> envVars = System.getenv();
        Properties systemProperties = System.getProperties();
        ResultSinkConfig resultSinkConfig = ResultSinkConfig.resolve(envVars, systemProperties);
        String bootstrap = envVars.getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
        String groupId = "fdb-flink-job";
        IcebergConfig icebergConfig = resolveIcebergConfig(envVars, systemProperties);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(effectiveCheckpointIntervalMs(
            resultSinkConfig.resultSink(),
            resolveCheckpointIntervalMs(envVars, systemProperties)));
        env.getCheckpointConfig().setCheckpointStorage(resolveCheckpointStorage(envVars, systemProperties));
        int parallelism = resolveParallelism(envVars, systemProperties);
        env.setParallelism(parallelism);
        boolean dynamicBalancingEnabled = resolveDynamicBalancingEnabled(envVars, systemProperties);
        MetricRuntimeConfig metricConfig = MetricRuntimeConfig.from(resultSinkConfig, parallelism);

        // Main pipeline: CHR + PM + CFG

        KafkaSource<ChrEvent> chrSource = KafkaSource.<ChrEvent>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("chr-events")
            .setGroupId(groupId + "-chr")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new FlinkAvroDeserializer<>(ChrEvent.class))
            .build();

        KafkaSource<PmStat> pmSource = KafkaSource.<PmStat>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("pm-stats")
            .setGroupId(groupId + "-pm")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new FlinkAvroDeserializer<>(PmStat.class))
            .build();

        KafkaSource<CfgConfig> cfgSource = KafkaSource.<CfgConfig>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("cfg-config")
            .setGroupId(groupId + "-cfg")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new FlinkAvroDeserializer<>(CfgConfig.class))
            .build();

        DataStream<ChrEvent> chrStream = env.fromSource(chrSource,
            WatermarkStrategy.<ChrEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withIdleness(Duration.ofMinutes(1))
                .withTimestampAssigner((event, ts) -> event.getEventTs()),
            "chr-source")
            .process(stageMetricsProbe("chr-source", "CHR Source", "healthy", resultSinkConfig, metricConfig))
            .name("chr-source-metrics");

        DataStream<PmStat> pmStream = env.fromSource(pmSource,
            WatermarkStrategy.<PmStat>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                .withIdleness(Duration.ofMinutes(1))
                .withTimestampAssigner((pm, ts) -> pmEventTimestamp(pm)),
            "pm-source")
            .process(stageMetricsProbe("pm-source", "PM Source", "healthy", resultSinkConfig, metricConfig))
            .name("pm-source-metrics");

        DataStream<CfgConfig> cfgStream = env.fromSource(cfgSource,
            WatermarkStrategy.<CfgConfig>forMonotonousTimestamps()
                .withIdleness(Duration.ofMinutes(1)),
            "cfg-source")
            .process(stageMetricsProbe("cfg-source", "CFG Source", "healthy", resultSinkConfig, metricConfig))
            .name("cfg-source-metrics");

        // Enrichment pipeline: unify CHR + PM + CFG, enrich, detect anomalies and KPI

        DataStream<InputEnvelope> chrEnv = chrStream
            .map(chr -> (InputEnvelope) new InputEnvelope.ChrEnv(chr))
            .returns(new GenericTypeInfo<>(InputEnvelope.class))
            .name("to-chr-env");
        DataStream<InputEnvelope> pmEnv = pmStream
            .map(pm -> (InputEnvelope) new InputEnvelope.PmEnv(pm))
            .returns(new GenericTypeInfo<>(InputEnvelope.class))
            .name("to-pm-env");
        DataStream<InputEnvelope> cfgEnv = cfgStream
            .map(cfg -> (InputEnvelope) new InputEnvelope.CfgEnv(cfg))
            .returns(new GenericTypeInfo<>(InputEnvelope.class))
            .name("to-cfg-env");

        DataStream<InputEnvelope> mergedInput = chrEnv.union(pmEnv, cfgEnv)
            .process(stageMetricsProbe("kafka", "Kafka Topics", "healthy", resultSinkConfig, metricConfig))
            .name("kafka-topics-metrics");

        DataStream<RoutedEnvelope> assigned;
        if (dynamicBalancingEnabled) {
            assigned = buildDynamicallyAssignedStream(env, mergedInput, bootstrap, groupId,
                resultSinkConfig, metricConfig);
        } else {
            assigned = mergedInput
                .map(FlinkJobMain::directRoute)
                .returns(new GenericTypeInfo<>(RoutedEnvelope.class))
                .name("direct-cell-routing");
        }

        SingleOutputStreamOperator<EnrichedChr> enrichedRaw = assigned
            .keyBy(RoutedEnvelope::stateKey)
            .process(new EnrichmentProcessFunction(), new GenericTypeInfo<>(EnrichedChr.class))
            .name("enrichment")
            .uid("enrichment");
        DataStream<EnrichedChr> enriched = enrichedRaw
            .process(stageMetricsProbe("enrichment", "Enrichment Process", "healthy", resultSinkConfig, metricConfig))
            .name("enrichment-metrics");

        if (resultSinkConfig.dlqEnabled()) {
            KafkaSink<ChrEvent> enrichmentLateSink = KafkaSink.<ChrEvent>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                    .setTopic("enrichment-late")
                    .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(ChrEvent.class))
                    .build())
                .build();
            enrichedRaw.getSideOutput(EnrichmentProcessFunction.ENRICHMENT_LATE)
                .sinkTo(enrichmentLateSink).name("enrichment-late-sink");
        }

        // Anomaly detection

        RuleConfig rules = JobConfig.load().rules();
        DataStream<AnomalyEvent> userAnomalies = UserEventCepAnomalyDetector
            .detect(enriched, rules);
        DataStream<AnomalyEvent> coverageAnomalies = enriched
            .keyBy(ec -> Geohash.encode(ec.chrEvent().getLatitude(), ec.chrEvent().getLongitude(), 6))
            .process(new CoverageHoleDetector(rules), new GenericTypeInfo<>(AnomalyEvent.class))
            .name("coverage-hole-detector")
            .uid("coverage-hole-detector");

        // KPI aggregation (1-minute CHR/PM event-time full join)

        DataStream<ChrMinuteFact> chrMinuteFacts = chrStream
            .keyBy(chr -> chr.getCellId().toString())
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .process(new ChrMinuteFactWindowFunction(), new GenericTypeInfo<>(ChrMinuteFact.class))
            .name("chr-1m-fact");

        DataStream<PmMinuteFact> pmMinuteFacts = pmStream
            .keyBy(pm -> pm.getCellId().toString())
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .process(new PmMinuteFactWindowFunction(), new GenericTypeInfo<>(PmMinuteFact.class))
            .name("pm-1m-fact");

        DataStream<MinuteFactEnvelope> chrFactEnv = chrMinuteFacts
            .process(new ProcessFunction<ChrMinuteFact, MinuteFactEnvelope>() {
                @Override
                public void processElement(ChrMinuteFact value, Context ctx, Collector<MinuteFactEnvelope> out) {
                    out.collect(MinuteFactEnvelope.chr(value));
                }
            }, new GenericTypeInfo<>(MinuteFactEnvelope.class))
            .name("to-chr-minute-fact-env");
        DataStream<MinuteFactEnvelope> pmFactEnv = pmMinuteFacts
            .process(new ProcessFunction<PmMinuteFact, MinuteFactEnvelope>() {
                @Override
                public void processElement(PmMinuteFact value, Context ctx, Collector<MinuteFactEnvelope> out) {
                    out.collect(MinuteFactEnvelope.pm(value));
                }
            }, new GenericTypeInfo<>(MinuteFactEnvelope.class))
            .name("to-pm-minute-fact-env");
        DataStream<MinuteFactEnvelope> cfgMinuteEnv = cfgStream
            .process(new ProcessFunction<CfgConfig, MinuteFactEnvelope>() {
                @Override
                public void processElement(CfgConfig value, Context ctx, Collector<MinuteFactEnvelope> out) {
                    out.collect(MinuteFactEnvelope.cfg(value));
                }
            }, new GenericTypeInfo<>(MinuteFactEnvelope.class))
            .name("to-cfg-minute-fact-env");

        DataStream<CellKpi> cellKpi1m = chrFactEnv.union(pmFactEnv, cfgMinuteEnv)
            .keyBy(MinuteFactEnvelope::cellId)
            .process(new MinuteKpiJoinFunction(Duration.ofMinutes(2)), new GenericTypeInfo<>(CellKpi.class))
            .name("kpi-1m-full-join")
            .uid("kpi-1m-full-join");
        DataStream<AnomalyEvent> cellAnomalies = CellKpiCepAnomalyDetector
            .detect(cellKpi1m, rules);

        DataStream<CellKpi> cellKpi5m = cellKpi1m
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<CellKpi>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                    .withIdleness(Duration.ofMinutes(1))
                    .withTimestampAssigner((kpi, ts) -> Math.subtractExact(kpi.getWindowEndTs(), 1L)))
            .keyBy(kpi -> kpi.getCellId().toString())
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .process(new CellKpiRollupAggregator(), new GenericTypeInfo<>(CellKpi.class))
            .name("kpi-5m-rollup")
            .uid("kpi-5m-rollup");

        ResultSinks.attachBusinessResultSinks(
            cellKpi1m, cellKpi5m, cellAnomalies, coverageAnomalies,
            resultSinkConfig, bootstrap, icebergConfig, metricConfig);

        env.execute("fdb-flink-job");
    }

    private static DataStream<RoutedEnvelope> buildDynamicallyAssignedStream(
        StreamExecutionEnvironment env,
        DataStream<InputEnvelope> mergedInput,
        String bootstrap,
        String groupId,
        ResultSinkConfig resultSinkConfig,
        MetricRuntimeConfig metricConfig) {
        KafkaSource<String> routingSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap).setTopics("lb-routing")
            .setGroupId(groupId + "-routing").setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        BroadcastStream<String> routingBroadcast = env.fromSource(routingSource,
            WatermarkStrategy.noWatermarks(), "lb-routing-source")
            .broadcast(RoutingAssigner.ROUTING_STATE);
        SingleOutputStreamOperator<RoutedEnvelope> metered = mergedInput.connect(routingBroadcast)
            .process(new RoutingAssigner(), new GenericTypeInfo<>(RoutedEnvelope.class)).name("routing-assigner")
            .keyBy(RoutedEnvelope::vbucketId)
            .process(new VBucketLoadMeter(), new GenericTypeInfo<>(RoutedEnvelope.class))
            .name("vbucket-load-meter");
        DataStream<RoutedEnvelope> assigned = metered
            .process(stageMetricsProbe("assigner", "VBucket Assigner", "healthy", resultSinkConfig, metricConfig))
            .name("vbucket-assigner-metrics");

        KafkaSink<String> heartbeatKafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("lb-heartbeat").setValueSerializationSchema(new SimpleStringSchema()).build())
            .build();
        metered.getSideOutput(VBucketLoadMeter.HEARTBEATS)
            .sinkTo(heartbeatKafkaSink).name("lb-heartbeat-sink");

        // Coordinator pipeline: lb-heartbeat to LoadCoordinator to lb-routing

        KafkaSource<String> heartbeatSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics("lb-heartbeat")
            .setGroupId(groupId + "-coordinator")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<RoutingEntry> routingStream = env
            .fromSource(heartbeatSource,
                WatermarkStrategy.<String>forMonotonousTimestamps()
                    .withIdleness(Duration.ofMinutes(1)),
                "lb-heartbeat-source")
            .map(new HeartbeatParser())
            .returns(new GenericTypeInfo<>(HeartbeatPayload.class))
            .name("heartbeat-parser")
            .keyBy(hb -> "coordinator")
            .process(new LoadCoordinator(metricConfig), new GenericTypeInfo<>(RoutingEntry.class))
            .name("load-coordinator")
            .setParallelism(1)
            .process(stageMetricsProbe("load-coordinator", "Load Coordinator", "healthy",
                resultSinkConfig, metricConfig))
            .name("load-coordinator-metrics");

        KafkaSink<String> routingSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(new RoutingCsvSerializationSchema("lb-routing"))
            .build();

        routingStream
            .map(RoutingEntry::toCsv)
            .name("routing-to-csv")
            .sinkTo(routingSink)
            .name("lb-routing-sink");

        return assigned;
    }

    static <T> StageMetricsProbe<T> stageMetricsProbe(String stageId, String displayName, String status,
                                                      ResultSinkConfig resultSinkConfig,
                                                      MetricRuntimeConfig metricConfig) {
        return new StageMetricsProbe<>(
            stageId, displayName, status, resultSinkConfig.metricsEmitIntervalMs(), metricConfig);
    }

    static RoutedEnvelope directRoute(InputEnvelope envelope) {
        return new RoutedEnvelope(envelope, Hashes.toVBucket(envelope.cellId(), DIRECT_ROUTE_VBUCKETS));
    }

    static boolean resolveDynamicBalancingEnabled(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_DYNAMIC_BALANCING_ENABLED");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.dynamic.balancing.enabled");
        }
        return configured != null && "true".equalsIgnoreCase(configured.trim());
    }

    static int resolveParallelism(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_FLINK_PARALLELISM");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.flink.parallelism");
        }
        if (configured == null || configured.isBlank()) {
            return DEFAULT_PARALLELISM;
        }
        try {
            int parallelism = Integer.parseInt(configured.trim());
            return parallelism > 0 ? parallelism : DEFAULT_PARALLELISM;
        } catch (NumberFormatException e) {
            log.warn("Invalid Flink parallelism '{}', falling back to {}", configured, DEFAULT_PARALLELISM);
            return DEFAULT_PARALLELISM;
        }
    }

    static long resolveCheckpointIntervalMs(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_FLINK_CHECKPOINT_INTERVAL_MS");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.flink.checkpoint.interval.ms");
        }
        if (configured == null || configured.isBlank()) {
            return 30_000L;
        }
        try {
            long intervalMs = Long.parseLong(configured.trim());
            return intervalMs > 0 ? intervalMs : 30_000L;
        } catch (NumberFormatException e) {
            log.warn("Invalid Flink checkpoint interval '{}', falling back to 30000 ms", configured);
            return 30_000L;
        }
    }

    static long effectiveCheckpointIntervalMs(ResultSinkType resultSink, long configuredIntervalMs) {
        return ResultSinkConfig.effectiveCheckpointIntervalMs(resultSink, configuredIntervalMs);
    }

    static String resolveCheckpointStorage(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_FLINK_CHECKPOINT_DIR");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.flink.checkpoint.dir");
        }
        if (configured == null || configured.isBlank()) {
            return "file:///tmp/fdb-checkpoints";
        }
        return configured.trim();
    }

    static IcebergConfig resolveIcebergConfig(Map<String, String> env, Properties properties) {
        return IcebergConfig.resolve(env, properties);
    }

    static long pmEventTimestamp(PmStat pm) {
        long windowStartTs = pm.getWindowStartTs();
        long windowEndTs = pm.getWindowEndTs();
        if (windowEndTs == Long.MIN_VALUE || windowEndTs <= 0L) {
            throw new IllegalArgumentException("Invalid PM windowEndTs: " + windowEndTs);
        }
        if (windowEndTs <= windowStartTs) {
            throw new IllegalArgumentException(
                "Invalid PM windowStartTs/windowEndTs: " + windowStartTs + ".." + windowEndTs);
        }
        return Math.subtractExact(windowEndTs, 1L);
    }
}
