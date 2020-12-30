package com.github.abiram.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.jvm.hotspot.HelloWorld;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerClassKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerClassKeys.class);

       // create producer Properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create Record
        for(int i=0;i<10;i++) {
            String topic = "first_topic";
            String value = "HelloWorld!"+Integer.toString(i);
            String key = "id_" +Integer.toString(i);

            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key:"+key ); //log the key
            //id_0 = 1
            //id_1 = 0
            //id_2 = 2
            //id_3 = 0
            //id_4 = 2
            //id_5 = 2
            //id_6 = 0
            //id_7 = 2
            //id_8 = 1
            //id_9 = 2
            //send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute everytime record is successfully sent or an exception is thrown
                    if (e == null) {
                        //record successfully sent
                        logger.info("Received new Metadata: \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "TimeSTAMP:" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //block the .send to make it synchronous - dont do in production
        }
        //flush data
        producer.flush();

        //flush and close producer
        producer.close();

    }
}
