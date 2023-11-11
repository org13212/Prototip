package org.example;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class Consumer {
    private static final KafkaConsumer<String, String> consumer = createConsumer();

    private Consumer() {
    }

    public static KafkaConsumer<String, String> getInstance(){
        return consumer;
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.BOOTSTRAP_SERVER_IP + ":" + Config.BOOTSTRAP_SERVER_PORT);
        props.put("group.id", "test-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singleton("news"));

        return kafkaConsumer;
    }
}
