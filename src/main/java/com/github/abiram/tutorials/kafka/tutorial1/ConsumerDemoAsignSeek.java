package com.github.abiram.tutorials.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAsignSeek {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ConsumerDemoAsignSeek.class.getName());
        Properties properties = new Properties();
        String server = "127.0.0.1:9092";
        //String groupId = "my-fifth-application";
        String topic = "first_topic";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //consume topics--> rather than consuming, lets read whatever is produced
        //consumer.subscribe(Collections.singleton(topic));

        //asign
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        //exit condition
         int messagesToReadFrom = 5;
         int numberOfMsgRead = 0;
         boolean keepReading = true;
        //poll for data
        while(keepReading) {
            // consumer.poll(100); deprecated
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> rec : records){
                numberOfMsgRead++;
                log.info("key: "+rec.key()+"\n"
                +"value: "+rec.value()+"\n"
                +"partition: "+rec.partition()+"\n"
                +"offset: "+rec.offset());
                if(numberOfMsgRead >= messagesToReadFrom){
                    keepReading = false;
                    break;

                }
            }
        }
        log.info("Existing the application");
    }
}
