package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Config {

    public static List<String> BOOTSTRAP_SERVERS = new ArrayList<>();

    public static short replicationFactor = 1;

    public static int numPartitionsOfTopic = 1;
    public static String CLIENT_ID_CONFIG= UUID.randomUUID().toString();
    private Config() {
    }
}