package com.fdb.common.kafka;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public final class AvroSerde {

    private AvroSerde() {}

    public static <T extends SpecificRecord> Serializer<T> serializer(Class<T> type) {
        DatumWriter<T> writer = new SpecificDatumWriter<>(type);
        return new Serializer<T>() {
            @Override
            public byte[] serialize(String topic, T record) {
                if (record == null) return null;
                try {
                    ByteArrayOutputStream out = new ByteArrayOutputStream(256);
                    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                    writer.write(record, encoder);
                    encoder.flush();
                    return out.toByteArray();
                } catch (Exception e) {
                    throw new SerializationException("Avro serialize failed for " + type.getName(), e);
                }
            }
        };
    }

    public static <T extends SpecificRecord> Deserializer<T> deserializer(Class<T> type) {
        DatumReader<T> reader = new SpecificDatumReader<>(type);
        return new Deserializer<T>() {
            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null) return null;
                try {
                    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(data), null);
                    return reader.read(null, decoder);
                } catch (Exception e) {
                    throw new SerializationException("Avro deserialize failed for " + type.getName(), e);
                }
            }
        };
    }
}
