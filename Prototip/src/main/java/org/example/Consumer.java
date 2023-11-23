package org.example;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {
    private KafkaConsumer<String, String> kafkaConsumer;

    public Consumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers",Config.BOOTSTRAP_SERVERS);
        props.put("group.id", "test-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", false);

        kafkaConsumer = new KafkaConsumer<>(props);

        assignPartitions("importan3");
    }

    public KafkaConsumer<String, String> getKafkaConsumer() {
        return kafkaConsumer;
    }

    public void assignPartitions(String topic){
        List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(topic);
        List<TopicPartition> partitions1 = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitions) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            partitions1.add(topicPartition);
            System.out.println(partitionInfo.toString());
        }
        kafkaConsumer.assign(partitions1);
        System.out.println("partitii:");
        System.out.println(kafkaConsumer.assignment());
    }
}
