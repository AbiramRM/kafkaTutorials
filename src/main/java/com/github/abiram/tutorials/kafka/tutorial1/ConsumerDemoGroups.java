package com.github.abiram.tutorials.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());
        Properties properties = new Properties();
        String server = "127.0.0.1:9092";
        String groupId = "my-fifth-application"; // Class is just to have multiple consumer of same groupConsumerDemo
        String topic = "first_topic";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //consume topics
        consumer.subscribe(Collections.singleton(topic));

        //poll for data
        while(true) {
            // consumer.poll(100); deprecated
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> rec : records){
                log.info("key: "+rec.key()+"\n"
                +"value: "+rec.value()+"\n"
                +"partition: "+rec.partition()+"\n"
                +"offset: "+rec.offset());
            }
        }
    }
}
