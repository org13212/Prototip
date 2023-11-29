<<<<<<< Updated upstream:src/main/java/org/example/ConsumerThread.java
package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class ConsumerThread extends Thread{
    private static List<ConsumerRecord<String, String>> recordsList = new ArrayList<>();

    @Override
    public void run(){
        Consumer consumer = new Consumer();
        while (true) {
            ConsumerRecords<String, String> records = consumer.getKafkaConsumer().poll(100);
            for (ConsumerRecord<String, String> record : records) {
                //System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                recordsList.add(record);
            }
        }
    }

    public static List<ConsumerRecord<String, String>> getFetchedData() {
        return recordsList;
    }
}
=======
package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ConsumerThread extends Thread{
    private static List<ConsumerRecord<String, String>> recordsList = new ArrayList<>();

    @Override
    public void run(){
        Consumer consumer = new Consumer();

        ArrayList<String> topics = new ArrayList<>();
        topics.add("importan3");
        topics.add("importan4");

        consumer.assignPartitions(topics);

        while (true) {
            for (ConsumerRecord<String, String> record : consumer.getKafkaConsumer().poll(1000)) {
                recordsList.add(record);
            }
            consumer.getKafkaConsumer().commitSync();
        }
    }

    public static List<ConsumerRecord<String, String>> getFetchedData() {
        return recordsList;
    }
}
>>>>>>> Stashed changes:Prototip/src/main/java/org/example/ConsumerThread.java
