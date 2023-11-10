package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;

public class Interpretor {
    public Interpretor() {
    }
    public void interpreteaza(String sir){
        String[] splitString = sir.split(" ");

        String comanda = splitString[0];

        switch (comanda){
            case "trimite":
                String topic = splitString[1];
                String continut = splitString[2];
                KafkaProducer<String, String> producer = Producer.getInstance();
                producer.send(new ProducerRecord<>(topic,null, continut));
                producer.close();
                break;
            case "afiseaza":
                String topic2 = splitString[1];
                KafkaConsumer<String, String> consumer = Consumer.getInstance();
                consumer.subscribe(Collections.singleton(topic2));
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    // print the offset,key and value for the consumer records.
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());
                break;
            default:
                System.out.println("Comanda tastata este invalida.");
        }
    }
}
