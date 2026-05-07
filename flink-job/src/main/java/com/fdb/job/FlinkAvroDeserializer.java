package com.fdb.job;

import com.fdb.common.kafka.AvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class FlinkAvroDeserializer<T extends SpecificRecord>
    implements KafkaRecordDeserializationSchema<T> {

    private final Class<T> type;

    public FlinkAvroDeserializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            org.apache.flink.util.Collector<T> out) throws IOException {
        try {
            T value = AvroSerde.deserializer(type).deserialize(record.topic(), record.value());
            out.collect(value);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize Avro record from topic " + record.topic(), e);
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(type);
    }
}
