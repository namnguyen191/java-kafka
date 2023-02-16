package kafka.java;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import java.net.URI;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class WikimediaKafkaProducer {
  private String bootStrapServer = "127.0.0.1:9092";
  private String wikimediaUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

  private Properties producerProperties;
  private KafkaProducer<String, String> producer;
  private String topic = "wikimedia.recentChange";

  public WikimediaKafkaProducer() {
    this.producerProperties = new Properties();
    this.configProducerBasicProperties();
    this.configProducerForHighThroughPut();
    this.producer = new KafkaProducer<>(producerProperties);
  }

  // Start producing in a worker thread which is non-blocking
  public void startProducing() {
    EventHandler eventHandler = new WikimediaChangeHandler(this.producer, this.topic);
    EventSource.Builder builder =
        new EventSource.Builder(eventHandler, URI.create(this.wikimediaUrl));
    EventSource eventSource = builder.build();

    eventSource.start();
  }

  private void configProducerBasicProperties() {
    this.producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
    this.producerProperties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    this.producerProperties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  }

  private void configProducerForHighThroughPut() {
    int bToKb = 1024;
    this.producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    this.producerProperties.setProperty(
        ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * bToKb));
    this.producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
  }
}
