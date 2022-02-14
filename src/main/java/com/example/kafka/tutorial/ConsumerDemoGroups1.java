package com.example.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups1 {

    static Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups1.class);




    public static void main(String[] args) {
        //https://kafka.apache.org/documentation/#consumerconfigs

        String topic = "topic_testing_group";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-second-consumer-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //crate Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //need subscribe the topic or topics
        consumer.subscribe(Arrays.asList(topic));
        while (true){
            Duration millis = Duration.ofMillis(100);
            ConsumerRecords<String, String> records = consumer.poll(millis);

            for (ConsumerRecord record :
                    records) {
                logger.info("Key: " + record.key() + "\n" +
                        "Value: " + record.value() + "\n" +
                        "Offset: " + record.offset() + "\n" +
                        "Topic: " + record.topic());
            }

        }


    }




}
