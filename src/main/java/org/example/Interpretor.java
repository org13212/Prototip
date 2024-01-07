package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

public class Interpretor {
    public Interpretor() {
    }

    public void interpreteaza(String sir) {
        String[] splitString = sir.split(" ");

        String comanda = splitString[0];

        switch (comanda) {
            case "trimite":
                handleLogg(handleTrimite(splitString));
                break;
            case "afiseaza":
                handleLogg(handleAfiseaza(splitString));
                break;
            case "aboneaza":
                handleLogg(handleAboneaza(splitString));
                break;
            case "dezaboneaza":
                handleLogg(handleDezaboneaza(splitString));
                break;
            //case "sterge":
            //handleStergeTopic(splitString);
            //    break;
            case "stress":
                stressTest(splitString);
                break;
            default:
                System.out.println("Comanda tastata este invalida.");
        }
    }

    private void stressTest(String[] splitString) {
        String topic = splitString[1];
        Producer producer = new Producer();
        KafkaProducer<String, String> kafkaProducer = producer.getKafkaProducer();

        if (!TopicChecker.topicExists(Config.BOOTSTRAP_SERVERS, topic)) {
            TopicBuilder topicBuilder = new TopicBuilder.Builder(topic)
                    .partitions(Config.numPartitionsOfTopic)
                    .replicationFactor(Config.replicationFactor)
                    .build();
            topicBuilder.createTopic();
        }
        int i = 0;

        while (i < 10000) {

            kafkaProducer.send(new ProducerRecord<>(topic, null, String.valueOf(i)));
            i++;
        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private String handleTrimite(String[] splitString) {
        String topic = splitString[1];
        String continut = splitString[2];
        CreazaSiTrimitePeTopic(topic,continut);
        return getTimeStamp()+" UserID:'"+Config.CLIENT_ID_CONFIG+"' Mesajul '"+continut+"' a fost trimis pe topicul '"+topic+"'";
    }
    private void handleLogg(String logg){
        if(!logg.equals(""))
            CreazaSiTrimitePeTopic("logg",logg);
    }
private void CreazaSiTrimitePeTopic(String topic,String mesaj){
        Producer producer = new Producer();
        KafkaProducer<String, String> kafkaProducer = producer.getKafkaProducer();
        if (!TopicChecker.topicExists(Config.BOOTSTRAP_SERVERS, topic)) {
            TopicBuilder topicBuilder = new TopicBuilder.Builder(topic)
                    .partitions(Config.numPartitionsOfTopic)
                    .replicationFactor(Config.replicationFactor)
                    .build();
            topicBuilder.createTopic();
        }

        kafkaProducer.send(new ProducerRecord<>(topic, null, mesaj));
        kafkaProducer.flush();
        kafkaProducer.close();
    }
    private String handleAfiseaza(String[] splitString) {
        if (splitString[1].equals("abonamente")) {
            afiseazaAbonamente();
            return getTimeStamp()+" Utilizatorul cu idul:'"+Config.CLIENT_ID_CONFIG+"' a vizualizat lista de abonamente";
        }

        if (splitString[1].equals("recente")) {
            afiseazaRecords(ConsumerThread.getFetchedData());
            ConsumerThread.clearBuffer();
            return getTimeStamp()+" Utilizatorul cu idul:'"+Config.CLIENT_ID_CONFIG+"' a verificat noutatile abonamentelor";
        }

        ArrayList<String> topics = new ArrayList<>();
        topics.add(splitString[1]);

        Consumer consumer = new Consumer();
        consumer.assignPartitions(topics);

        KafkaConsumer<String, String> kafkaConsumer = consumer.getKafkaConsumer();
        kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());

        afiseazaRecords(kafkaConsumer.poll(Duration.ofMillis(1000)));

        kafkaConsumer.close();
        if(splitString[1].equals("logg"))
            return "";
        else 
            return getTimeStamp() + " Utilizatorul cu idul:'"+Config.CLIENT_ID_CONFIG+"' a vizualizat topicul '" + splitString[1] + "'";
   
    }

    private String handleAboneaza(String[] splitString) {
        ConsumerThread.addSubscribedTopic(splitString[1]);
        return getTimeStamp()+" Utilizatorul cu idul:'"+Config.CLIENT_ID_CONFIG+"' s-a abonat la topicul '"+splitString[1]+"'";
    }

    private String handleDezaboneaza(String[] splitString) {
        ConsumerThread.removeSubscribedTopic(splitString[1]);
        return getTimeStamp()+" Utilizatorul cu idul:'"+Config.CLIENT_ID_CONFIG+"' s-a dezabonat de la topicul '"+splitString[1]+"'";
    }

// STERGERE TOPIC
//        Properties adminClientProps = new Properties();
//        adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "your-bootstrap-server");
//
//        // Create the AdminClient
//        try (AdminClient adminClient = AdminClient.create(adminClientProps)) {
//            // Specify the topic name you want to delete
//            String topicToDelete = "your-topic-name";
//
//            // Create the DeleteTopicsOptions with optional settings
//            DeleteTopicsOptions deleteTopicsOptions = new DeleteTopicsOptions().timeoutMs(5000);
//
//            // Create a DeleteTopicsResult
//            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topicToDelete), deleteTopicsOptions);
//
//            // Wait for the deletion to complete
//            deleteTopicsResult.values().get(topicToDelete).get();
//            System.out.println("Topic '" + topicToDelete + "' deleted successfully.");
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//        }

    private void afiseazaRecords(Iterable<ConsumerRecord<String, String>> records) {
        System.out.println("--- records:");
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s\n",
                    record.offset(), record.key(), record.value());
        }
        System.out.println("---");
    }

    private void afiseazaAbonamente() {
        System.out.println("--- abonamente:");
        System.out.println(ConsumerThread.getSubscribedTopics());
        System.out.println("---");
    }
    private String getTimeStamp(){
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd-HH:mm:ss.SSS");
        return dtf.format(LocalDateTime.now());
    }
}