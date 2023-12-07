package org.example;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
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
        props.put("enable.auto.commit", true);
        props.put("max.poll.records",10000);

        kafkaConsumer = new KafkaConsumer<>(props);
    }

    public KafkaConsumer<String, String> getKafkaConsumer() {
        return kafkaConsumer;
    }

    public void assignPartitions(ArrayList<String> topics){
        System.out.println("--- partitii:");

        List<TopicPartition> topicPartitionList = new ArrayList<>();

        for(String topic: topics) {
            List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);
            for (PartitionInfo partitionInfo : partitionInfoList) {
                System.out.println(partitionInfo);
                TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                topicPartitionList.add(topicPartition);
            }
        }

        kafkaConsumer.assign(topicPartitionList);
        System.out.println(kafkaConsumer.assignment());
        System.out.println("--- ");
    }
}