package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    private  KafkaProducer<String, String> kafkaProducer;

    public Producer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.BOOTSTRAP_SERVERS);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        kafkaProducer = new KafkaProducer<>(props);
    }

    public KafkaProducer<String, String> getKafkaProducer() {
        return kafkaProducer;
    }
}
