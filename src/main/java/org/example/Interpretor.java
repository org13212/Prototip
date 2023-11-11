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
                handleTrimite(splitString);
                break;
            case "afiseaza":
                handleAfiseaza(splitString);
                break;
            default:
                System.out.println("Comanda tastata este invalida.");
        }
    }

    private void handleTrimite(String[] splitString){
        String topic = splitString[1];
        String continut = splitString[2];
        KafkaProducer<String, String> producer = Producer.getInstance();
        producer.send(new ProducerRecord<>(topic,null, continut));
        producer.flush();
    }

    private void handleAfiseaza(String[] splitString){
        String topic = splitString[1];
        String arg2 = splitString[2];

        KafkaConsumer<String, String> consumer = Consumer.getInstance();
        //consumer.subscribe(Collections.singleton(topic));

        if (arg2.equals("toate")){
            consumer.seekToBeginning(consumer.assignment());
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records)
                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
        }
        else {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records)
                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
        }
    }
}
