package io.conduktor.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class);

    public static void main(String[] args) {
        log.info("I'm a Kafka Consumer");
        String groupId = "myJavaAppConsumerGroup";
        String topic = "demo_go";

        //Create Consumer Properties
        Properties properties = new Properties();

        //connect to docker desktop kafka container
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");//none/earlies/latest
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        //properties.setProperty("group.instance.id","..");//strategy for static group membership or static assignment


        //Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Get a reference to Main Thread
        Thread mainThread = Thread.currentThread();

        //Adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void  run(){
              log.info("Detected a shutdown, let's exit by calling consumer.wakeup()..");
              consumer.wakeup();

              //join the main thread to allow the execution of code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        });

        try {
            //Subscribe to topic
            consumer.subscribe(List.of(topic));

            //Poll for data
            while(true){
                log.info("Polling");

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record: records){
                    log.info("Key :" +record.key() +", Value :" +record.value());
                    log.info("Partition :" +record.partition() + ", offset :" +record.offset());
                }
            }
        } catch (WakeupException e){
            log.info("Consumer is starting to shutdown");
        } catch (Exception e){
            log.error("Unhandled exception in consumer", e);
        } finally {
            consumer.close();
            log.info("Consumer is shutdown gracefully");
        }



    }
}
