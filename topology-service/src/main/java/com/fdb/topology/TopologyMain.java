package com.fdb.topology;

import com.fdb.common.avro.TopologyRecord;
import com.fdb.common.config.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

public class TopologyMain {

    private static final Logger log = LoggerFactory.getLogger(TopologyMain.class);

    public static void main(String[] args) throws Exception {
        String configPath = System.getProperty("config", "topology-default.yaml");

        var configFile = Path.of(configPath);
        if (!configFile.isAbsolute()) {
            configFile = Path.of(System.getProperty("user.dir")).resolve(configPath).normalize();
        }

        var rawConfig = ConfigLoader.builder()
            .defaultResource("topology-default.yaml")
            .overlayFile(configFile)
            .envPrefix("FDB_")
            .build()
            .load();

        TopologyConfig topologyConfig = parseConfig(rawConfig);

        TopologyGenerator generator = new TopologyGenerator(topologyConfig);
        List<TopologyRecord> records = generator.generate();

        log.info("Generated {} topology records from {} sites", records.size(), topologyConfig.getSites().getCount());

        String bootstrap = rawConfig.getStringOrNull("kafka.bootstrap");
        if (bootstrap == null) {
            bootstrap = System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
        }

        String topic = rawConfig.getStringOrNull("kafka.topologyTopic");
        if (topic == null) topic = "topology";

        KafkaTopologyPublisher publisher = new KafkaTopologyPublisher(bootstrap, topic);
        try {
            publisher.publishAll(records);
        } finally {
            publisher.close();
        }

        log.info("Topology service completed successfully");
        System.exit(0);
    }

    private static TopologyConfig parseConfig(ConfigLoader.Config raw) {
        return new TopologyConfig();
    }
}
