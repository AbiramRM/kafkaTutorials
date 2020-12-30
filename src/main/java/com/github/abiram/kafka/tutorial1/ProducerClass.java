package com.github.abiram.kafka.tutorial1;

import com.apple.eawt.AppEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import sun.jvm.hotspot.tools.SysPropsDumper;

import java.util.Properties;

public class ProducerClass {
    public static void main(String[] args) {
       // create producer Properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create Record
        ProducerRecord<String,String> record =
                new ProducerRecord<String, String>("first_topic","Hello World!");

        //send data
        producer.send(record);

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();

    }
}
