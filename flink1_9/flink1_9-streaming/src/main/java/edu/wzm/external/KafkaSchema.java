package edu.wzm.external;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

public class KafkaSchema implements SerializationSchema<String>, DeserializationSchema<Order> {
    @Override
    public Order deserialize(byte[] bytes) throws IOException {
        return Order.of(new String(bytes));
    }

    @Override
    public boolean isEndOfStream(Order order) {
        return false;
    }

    @Override
    public byte[] serialize(String event) {
        return event.getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<Order> getProducedType() {
        return TypeInformation.of(Order.class);
    }
}