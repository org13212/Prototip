package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class Producer implements Runnable {
    private final KafkaProducer<String,String> producer;
    private final String topic;
    public Producer(KafkaProducer<String,String> producer,String topic){
        this.producer=producer;
        this.topic=topic;
    }
    @Override
    public void run() {

        System.out.println("A message on the curent topic:");
        Scanner messageScanner=new Scanner(System.in);
        String message=messageScanner.nextLine();

        producer.send(new ProducerRecord<String,String>(topic,null,message));
        producer.close();



    }
}
