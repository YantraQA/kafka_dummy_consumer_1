package com.yantraQA;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import lombok.extern.log4j.Log4j2;

import java.util.*;

import static io.restassured.RestAssured.given;

@Log4j2
public class EventConsumer {

    public static final String BOOTSTRAPSERVERS = "kafka:9092"; // if running from Docker compose, this will resolve to active broker
    public static final String TOPIC = "first_topic";
    public static final String KSERIALIZER="org.apache.kafka.common.serialization.StringSerializer";
    public static final String KDSERIALIZER="org.apache.kafka.common.serialization.StringDeserializer";

    public static void main(String[] args) throws JsonProcessingException, InterruptedException {

        String topic = "notification";
        List<String> topicList = new ArrayList();
        topicList.add(topic);
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", BOOTSTRAPSERVERS);
        consumerProperties.put("group.id", "Demo_Group");
        consumerProperties.put("key.deserializer",KDSERIALIZER);
        consumerProperties.put("value.deserializer", KDSERIALIZER);

        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put("auto.commit.interval.ms", "1000");
        consumerProperties.put("session.timeout.ms", "30000");


        KafkaConsumer<String, String> demoKafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);

        demoKafkaConsumer.subscribe(topicList);
        log.info("Subscribed to topic " + topic);
        int i = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> records = demoKafkaConsumer.poll(500);
                for (ConsumerRecord<String, String> record : records)
                    log.info("offset = " + record.offset() + "key =" + record.key() + "value =" + record.value());

                // TODO : Do processing for data here
                demoKafkaConsumer.commitAsync(new OffsetCommitCallback() {
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    	//log.info("Call back On Completion:" + map.toString());
                    }
                });

            }
        } catch (Exception ex) {
            // TODO : Log Exception Here
        } finally {
            try {
                demoKafkaConsumer.commitSync();

            } finally {
                demoKafkaConsumer.close();
            }
        }
    }
}
