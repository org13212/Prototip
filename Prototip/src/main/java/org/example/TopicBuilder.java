package org.example;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;

public class TopicBuilder {


    private final String topicName;
    private final int partitions;
    private final short replicationFactor;

    private TopicBuilder(Builder builder) {

        this.topicName = builder.topicName;
        this.partitions = builder.partitions;
        this.replicationFactor = builder.replicationFactor;
    }

    public static class Builder {
        // Required parameters

        private final String topicName;

        // Optional parameters with default values
        private int partitions = 1;
        private short replicationFactor = 1;

        public Builder( String topicName) {
            this.topicName = topicName;
        }

        public Builder partitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        public Builder replicationFactor(short replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public TopicBuilder build() {
            return new TopicBuilder(this);
        }
    }

    public void createTopic() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(properties)) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            newTopic.configs(Collections.singletonMap("min.insync.replicas",String.valueOf(1)));
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            System.out.println("Topic created successfully: " + topicName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
