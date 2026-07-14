package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.CellKpi;
import com.fdb.job.source.FlinkAvroSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

final class KafkaResultSinks {

    private KafkaResultSinks() {}

    static KafkaSink<CellKpi> cellKpiSink(String bootstrap, String topic) {
        return KafkaSink.<CellKpi>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(CellKpi.class))
                .build())
            .build();
    }

    static KafkaSink<AnomalyEvent> anomalySink(String bootstrap, String topic) {
        return KafkaSink.<AnomalyEvent>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(AnomalyEvent.class))
                .build())
            .build();
    }
}
