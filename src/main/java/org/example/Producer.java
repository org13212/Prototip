package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    private static final KafkaProducer<String, String> producer = createProducer();

    private Producer() {
    }

    public static KafkaProducer<String, String> getInstance() {
        return producer;
    }

    public void send(){

    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.BOOTSTRAP_SERVER_IP + ":" + Config.BOOTSTRAP_SERVER_PORT);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
