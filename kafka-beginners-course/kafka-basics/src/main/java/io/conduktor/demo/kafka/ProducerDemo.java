package io.conduktor.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("I'm a Kafka Producer");
        //Create Producer Properties
        Properties properties = new Properties();

        //connect to docker desktop kafka container
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello_world");

        //Send data
        producer.send(producerRecord);

        //tell the producer to send all data and block until done - synchronous operation
        producer.flush();

        //Flush and close the producer
        producer.close();
    }
}
