package com.fdb.job;

import com.fdb.common.kafka.AvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class FlinkAvroSerializationSchema<T extends SpecificRecord>
    implements SerializationSchema<T> {

    private final Class<T> type;

    public FlinkAvroSerializationSchema(Class<T> type) {
        this.type = type;
    }

    @Override
    public byte[] serialize(T element) {
        if (element == null) return null;
        return AvroSerde.serializer(type).serialize(null, element);
    }
}
