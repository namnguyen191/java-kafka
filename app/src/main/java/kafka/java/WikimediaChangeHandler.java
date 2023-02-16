package kafka.java;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {
  KafkaProducer<String, String> kafkaProducer;
  String topic;
  private final Logger logger =
      LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

  public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
    this.kafkaProducer = kafkaProducer;
    this.topic = topic;
  }

  @Override
  public void onOpen() {
    this.logger.info("A stream has been opened");
  }

  @Override
  public void onClosed() {
    this.kafkaProducer.close();
  }

  @Override
  public void onMessage(String event, MessageEvent messageEvent) throws Exception {
    this.logger.info("Receiving message: " + messageEvent.getData());
    this.kafkaProducer.send(new ProducerRecord<String, String>(this.topic, messageEvent.getData()));
  }

  @Override
  public void onComment(String comment) throws Exception {
    // UNUSED
  }

  @Override
  public void onError(Throwable t) {
    this.logger.error("An error has occured: " + t.getMessage());
  }
}
