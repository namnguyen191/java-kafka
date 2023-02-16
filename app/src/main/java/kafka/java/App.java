/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package kafka.java;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class App {
  public static void main(String[] args) throws InterruptedException, IOException {
    WikimediaKafkaProducer producer = new WikimediaKafkaProducer();
    producer.startProducing();

    Thread mainThread = Thread.currentThread();
    WikimediaKafkaConsumer consumer = new WikimediaKafkaConsumer(mainThread);
    consumer.startConsumming();

    TimeUnit.SECONDS.sleep(10);

    System.out.println("Exiting program!");
  }
}