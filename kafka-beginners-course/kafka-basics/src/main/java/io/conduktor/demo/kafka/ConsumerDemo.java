package io.conduktor.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        log.info("I'm a Kafka Consumer");
        String groupId = "myJavaAppConsumerGroup";
        String topic = "demo_java";

        //Create Consumer Properties
        Properties properties = new Properties();

        //connect to docker desktop kafka container
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");//none/earlies/latest

        //Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

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

    }
}
