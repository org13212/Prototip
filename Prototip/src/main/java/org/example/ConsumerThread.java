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
