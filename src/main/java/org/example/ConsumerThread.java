package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;

public class ConsumerThread extends Thread {
    private static Consumer consumer = new Consumer();
    private static ArrayList<String> subscribedTopics = new ArrayList<>();
    private static ArrayList<ConsumerRecord<String, String>> buffer = new ArrayList<>();
    private static boolean subscribedTopicsChangedFlag = false;

    @Override
    public void run() {

        // Abonare la un topic implicit
        subscribedTopics.add("importan3");
        consumer.assignPartitions(subscribedTopics);

        // Consumer loop
        while (!Thread.interrupted()) {
            try {
                if (subscribedTopicsChangedFlag) {
                    consumer.assignPartitions(subscribedTopics);
                    subscribedTopicsChangedFlag = false;
                }

                for (ConsumerRecord<String, String> record : consumer.getKafkaConsumer().poll(1000)) {
                    buffer.add(record);
                    System.out.println(record);
                }
            } catch (Exception e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }

        System.out.println("Final consumer thread");
    }

    public static ArrayList<ConsumerRecord<String, String>> getFetchedData() {
        return buffer;
    }

    public static void clearBuffer() {
        buffer.clear();
    }

    public static void addSubscribedTopic(String topic) {
        subscribedTopics.add(topic);
        subscribedTopicsChangedFlag = true;
    }

    public static void removeSubscribedTopic(String topic) {
        if (subscribedTopics.size() > 1) {
            subscribedTopics.remove(topic);
        }
        subscribedTopicsChangedFlag = true;
    }

    public static String getSubscribedTopics() {
        return subscribedTopics.toString();
    }
}