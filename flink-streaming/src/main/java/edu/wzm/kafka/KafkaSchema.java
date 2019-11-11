package edu.wzm.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.nio.charset.Charset;

public class KafkaSchema implements SerializationSchema<Tuple2<String, Integer>>, DeserializationSchema<Event> {
    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        return Event.fromString(new String(bytes));
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public byte[] serialize(Tuple2<String, Integer> event) {
        return event.toString().getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
