package io.conduktor.demo.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    KafkaProducer<String, String> producer;
    String topic;

    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class);

    public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen()  {
        //when stream is opened
        //nothing to do here
    }

    @Override
    public void onClosed(){
        //when stream is getting closed
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
      //asynchronous
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s)  {
      //nothing here
    }

    @Override
    public void onError(Throwable throwable) {
      log.error("Error in Stream Reader", throwable);
    }
}
