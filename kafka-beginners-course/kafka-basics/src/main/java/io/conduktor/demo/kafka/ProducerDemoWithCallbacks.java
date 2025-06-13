package io.conduktor.demo.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);

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
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time a record successfully sent or an exception is thrown
                if(e == null){
                    //the record was successfully sent
                    log.info("Received was metadata \n" +
                            "Topic :" + recordMetadata.topic() + "\n" +
                            "Partitions :" + recordMetadata.partition() + "\n" +
                            "Offset :" + recordMetadata.offset() + "\n" +
                            "Timestamp :" + recordMetadata.timestamp());
                } else{
                    log.error("Error while producing", e);
                }
            }
        });

        //tell the producer to send all data and block until done - synchronous operation
        producer.flush();

        //Flush and close the producer
        producer.close();
    }
}
