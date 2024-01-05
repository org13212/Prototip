package org.example;

import java.util.ArrayList;
import java.util.List;

public class Config {
    public static List<String> BOOTSTRAP_SERVERS = new ArrayList<>();

    public static short replicationFactor = 1;

    public static int numPartitionsOfTopic = 1;

    private Config() {
    }
}
