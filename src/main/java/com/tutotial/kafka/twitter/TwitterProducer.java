package com.tutotial.kafka.twitter;

import com.example.kafka.tutorial.ConsumerDemo;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.joauth.UnpackedRequest;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private static String API_KEY = "EDjw1fCdk4RRQVvG2Z8cuDYf0";
    private static String API_SECRET_KEY = "rc4S5zlJdv3o3ILxI3eh2nQY0CMCdKyDHnxQX9GesPdXFahI9a";
    private static String ACCESS_TOKEN = "1251981544094420992-jIpYn3ZtQ84pq70j3LybM39B9S9sRp";
    private static String ACCESS_TOKEN_SECRET = "CGeAVAXxR7jbriBulFDFxSAWPpmRdCuNKT0PEWmUW9fi0";

    // Optional: set up some followings and track terms
    //  ArrayList<Long> followings = Lists.newArrayList(1234L, 566788L);
    ArrayList<String> terms = Lists.newArrayList("vitor_tavares");



    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    private void run(){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
     //   BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        //create twitter client
            Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();

        //create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("stopping application....");
            client.stop();
            //close producer é preciso para que o producer envie todas as msgs da memoria ao consumer antes de desligar
            logger.info("closing producer...");
            producer.close();
            logger.info("Done");
        }));

        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg =msgQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(!msg.isEmpty()){
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, e) -> {
                    if(e!=null){
                        logger.error("Something went wrong", e);
                    }
                });
            }
        }
        logger.info("End of application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        //Create producer properties
        //https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create producer
        //key = string
        //value = string
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue ){


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();


      //  hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
//        new OAuthBearerToken()
        Authentication hosebirdAuth = new OAuth1(API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);


        //create client
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;


    }

}
