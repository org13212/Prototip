package org.example;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class Rebalance implements ConsumerRebalanceListener {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // This method is called when Kafka is about to revoke partitions from this consumer.
        // Log that partitions are being revoked.
        for (TopicPartition partition : partitions) {
            System.out.println("Partition " + partition + " is being revoked.");
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // This method is called when Kafka has assigned partitions to this consumer.
        // Log that partitions are being assigned.
        for (TopicPartition partition : partitions) {
            System.out.println("Partition " + partition + " is being assigned.");
        }
    }
}
