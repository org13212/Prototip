<<<<<<< Updated upstream:src/main/java/org/example/Interpretor.java
package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;

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
        String topic = splitString[1];
        String continut = splitString[2];
        Producer producer = new Producer();
        KafkaProducer<String, String> kafkaProducer = producer.getKafkaProducer();
        kafkaProducer.send(new ProducerRecord<>(topic, null, continut));
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private void handleAfiseaza(String[] splitString) {
        String topic = splitString[1];

        Consumer consumer = new Consumer();
        consumer.assignPartitions(topic);
        KafkaConsumer<String, String> kafkaConsumer = consumer.getKafkaConsumer();

        //autocomit = false
//        Set<TopicPartition> assignedPartitions = kafkaConsumer.assignment();
//        kafkaConsumer.seekToBeginning(assignedPartitions);

        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s\n",
                    record.offset(), record.key(), record.value());
        }

        kafkaConsumer.close();

//        List<ConsumerRecord<String, String>> records = ConsumerThread.getFetchedData();
//        for (ConsumerRecord<String, String> record : records) {
//            // print the offset,key and value for the consumer records.
//            System.out.printf("offset = %d, key = %s, value = %s\n",
//                    record.offset(), record.key(), record.value());
//        }
    }

}
=======
package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

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
            case "f":
                handle();
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

        // Daca topicul nu exista atunci este creat imediat
        if (!TopicChecker.topicExists(Config.BOOTSTRAP_SERVERS, topic)) {
            TopicBuilder topicBuilder = new TopicBuilder.Builder(topic)
                    .partitions(1)
                    .replicationFactor((short) 1)
                    .build();
            topicBuilder.createTopic();
        }

        kafkaProducer.send(new ProducerRecord<>(topic, null, continut));
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private void handleAfiseaza(String[] splitString) {
        ArrayList<String> topics = new ArrayList<>();

        String topic = splitString[1];
        topics.add(topic);

        Consumer consumer = new Consumer();
        consumer.assignPartitions(topics);

        KafkaConsumer<String, String> kafkaConsumer = consumer.getKafkaConsumer();
        kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());

        afiseazaRecords(kafkaConsumer.poll(Duration.ofMillis(1000)));

        kafkaConsumer.close();
    }

    private void handle(){
        afiseazaRecords(ConsumerThread.getFetchedData());
        ConsumerThread.getFetchedData().clear();
    }

    private void afiseazaRecords(Iterable<ConsumerRecord<String, String>> records){
        System.out.println("--- records:");
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s\n",
                    record.offset(), record.key(), record.value());
        }
        System.out.println("---");
    }

}
>>>>>>> Stashed changes:Prototip/src/main/java/org/example/Interpretor.java
