package com.github.abiram.tutorials.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger log = LoggerFactory.getLogger(RecordMetadata.class);


        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i =0; i<=10;i++) {

            String topic = "first_topic";
            String value = "Hello world! " + Integer.toString(i);
            String key =  "id_" + Integer.toString(i);
            // create Producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key, value);
            log.info("Key "+key);
            // id 0 part 1
            // id 1 part 0
            // id 2 part 2
            // id 3 part 0
            // id 4 part 2
            // id 5 part 2
            // id 6 part 0
            // id 7 part 2
            // id 8 part 1
            // id 9 part 2
            // id 10 part 2
            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("The Metadata are: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        log.info("Producer failed to produce", e);
                    }
                }
                //block .send to be synchronous - not in PROD!!
            }).get();
        }
        //flush
        producer.close();
    }
}
