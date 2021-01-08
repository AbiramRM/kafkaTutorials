package com.github.abiram.tutorials.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {

        new ConsumerDemoWithThreads().run();

        //consume topics
        //consumer.subscribe(Collections.singleton(topic));


    }
    public ConsumerDemoWithThreads(){

    }
    public void run(){
        Logger log = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
        String server = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";
        // latch to deal with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        log.info("Creating consumer thread");
        // start consumer runnable
        Runnable myConsumerThread = new ConsumerThread(server,groupId,topic,latch);
        // Start the thread
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            log.info("Caught shutdown hook");
            ((ConsumerThread)myConsumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Apllication has exited!");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            //e.printStackTrace();
            log.info("Application got interrupted");
        } finally {
            log.info("Application is closing");
        }
    }
}
    class ConsumerThread implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;

        private Logger log = LoggerFactory.getLogger(ConsumerThread.class.getName());
        Properties properties = new Properties();
        /*String server = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";
*/


        public ConsumerThread(String server, String groupId, String topic, CountDownLatch latch){
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            this.latch = latch;
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                //poll for data
                while (true) {
                    // consumer.poll(100); deprecated
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> rec : records) {
                        log.info("key: " + rec.key() + "\n"
                                + "value: " + rec.value() + "\n"
                                + "partition: " + rec.partition() + "\n"
                                + "offset: " + rec.offset());
                    }
                }
            }catch (WakeupException e){
                log.info("Received Shutdown signal!");
            }finally {
                consumer.close();
                //tell main we are done
                latch.countDown();
            }
        }
        public void shutdown(){
            // shutdown consumer threads
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }

