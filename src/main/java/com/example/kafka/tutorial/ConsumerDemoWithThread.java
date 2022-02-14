package com.example.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


/*This example can be used as Framework to create Thread consumer*/
public class ConsumerDemoWithThread {
    String topic = "topic_thread";
    String group = "my-consumer-application";
    String bootstrap = "127.0.0.1:9092";

    Thread t = new Thread(new ConsumerThread(new CountDownLatch(1), topic, group, bootstrap));


    static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);


    public static void main(String[] args) {

        new ConsumerDemoWithThread().t.start();
        new ConsumerDemoWithThread().t.start();

    }


    public class ConsumerThread implements Runnable{

        //CountDownLatch is a Concurrent java class that helps handle theads
        private CountDownLatch latch;

        //crate Kafka consumer
        KafkaConsumer<String, String> consumer;

        public ConsumerThread(CountDownLatch latch, String topic, String group, String bootstrap){
            logger.info("Creating the consumer thread");
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            this.consumer =  new KafkaConsumer<String, String>(properties);

            //need subscribe the topic or topics
            consumer.subscribe(Arrays.asList(topic));
           Runtime.getRuntime().addShutdownHook(new Thread(() -> {
               logger.info("Caught shutdown hook");
               this.shutDown();
               try {
                   latch.wait();
               } catch (InterruptedException e) {
                   e.printStackTrace();
               }
               logger.info("Application has exit");
           }));

            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application got interrupted", e);
            }
            finally {
                logger.info("Application is closing");
            }

        }



        @Override
        public void run() {
            try{
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
            catch (WakeupException e){
                logger.info("Received shutdown signal");
            }
            finally {
                consumer.close();
                //tell our main code we´re done with the consumer
                latch.countDown();
            }


        }

        public void shutDown(){
            //wakeup interrupts consumer.poll
            consumer.wakeup();
        }
    }


}
