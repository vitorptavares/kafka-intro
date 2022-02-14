package com.example.kafka.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeyWithCallBack {
    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeyWithCallBack.class);

    public static void main(String[] args) {
        //Create producer properties
        //https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        for (int i = 0; i < 10; i++) {
            String topic = "topic_testing_group";
            String value = "this is the second time executed";
            String key = String.valueOf(i);
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key, value);


            //send data
            //as this is async, only send is not enouth. We need flush or close
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is succesfully sent or an exception is thrown
                    if(e==null){
                        logger.info("Received new metadata.\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " +recordMetadata.partition() +"\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp());
                    }else{
                        logger.error("Error while producing: " + e);
                    }
                }
            });
            producer.close();
        }



        System.out.println("Done");

    }
}
