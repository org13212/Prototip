package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Arrays;

public class Interpretor {
    public Interpretor() {
    }

    public void interpreteaza(String sir) {
        String[] splitString = sir.split(" ");

        String comanda = splitString[0];

        switch (comanda) {
            case "trimite":
                handleTrimite(splitString);
                break;
            case "afiseaza":
                handleAfiseaza(splitString);
                break;
            default:
                System.out.println("Comanda tastata este invalida.");
        }
    }

    private void handleTrimite(String[] splitString) {
        System.out.println(splitString[1]);
        String topic = splitString[1];
        String continut = splitString[2];
        Producer producer = new Producer();
        KafkaProducer<String, String> kafkaProducer = producer.getKafkaProducer();

        if(!TopicChecker.topicExists(Config.BOOTSTRAP_SERVERS, topic)) {
            TopicBuilder topicBuilder = new TopicBuilder.Builder(topic)
                    .partitions(1)
                    .replicationFactor((short) 3)
                    .build();
            topicBuilder.createTopic();
        }

        kafkaProducer.send(new ProducerRecord<>(topic, null, continut));
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private void handleAfiseaza(String[] splitString) {
        String topic = splitString[1];

        Consumer consumer = new Consumer();
        consumer.assignPartitions(topic);

        if(!TopicChecker.topicExists(Config.BOOTSTRAP_SERVERS, topic)) {
            TopicBuilder topicBuilder = new TopicBuilder.Builder(topic)
                    .partitions(1)
                    .replicationFactor((short) 3)
                    .build();
            topicBuilder.createTopic();
        }
        KafkaConsumer<String, String> kafkaConsumer = consumer.getKafkaConsumer();

        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s\n",
                    record.offset(), record.key(), record.value());
        }

        kafkaConsumer.close();

    }

}
