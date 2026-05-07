package com.fdb.common.kafka;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import com.fdb.common.avro.RatType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AvroSerdeTest {

    private static ChrEvent sample() {
        return ChrEvent.newBuilder()
            .setChrId("c-1").setEventTs(123L).setImsi("imsi-1")
            .setSiteId("S1").setCellId("C1")
            .setEventType(ChrEventType.ATTACH).setRatType(RatType.NR_SA)
            .setPci(1).setTac(1).setEci(1L).setMcc("460").setMnc("00")
            .setResultCode(0).setLatitude(0.0).setLongitude(0.0)
            .build();
    }

    @Test
    void serializer_then_deserializer_roundtrip() {
        Serializer<ChrEvent> ser = AvroSerde.serializer(ChrEvent.class);
        Deserializer<ChrEvent> de = AvroSerde.deserializer(ChrEvent.class);

        ChrEvent original = sample();
        byte[] bytes = ser.serialize("topic", original);
        ChrEvent decoded = de.deserialize("topic", bytes);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void serializer_returns_null_for_null_input() {
        Serializer<ChrEvent> ser = AvroSerde.serializer(ChrEvent.class);
        assertThat(ser.serialize("topic", null)).isNull();
    }

    @Test
    void deserializer_returns_null_for_null_bytes() {
        Deserializer<ChrEvent> de = AvroSerde.deserializer(ChrEvent.class);
        assertThat(de.deserialize("topic", null)).isNull();
    }
}
