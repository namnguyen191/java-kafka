package kafka.java;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaKafkaConsumer {
  Thread mainThread;
  private Logger logger = LoggerFactory.getLogger(App.class.getName());
  private String indexName = "wikimedia";
  private String topic = "wikimedia.recentChange";

  // create Open Search client
  private final RestHighLevelClient openSearchClient = createOpenSearchClient();
  // create Kafka consumer
  private final KafkaConsumer<String, String> consumer = createKafkaConsumer();

  public WikimediaKafkaConsumer(Thread mainThread) throws IOException {
    this.mainThread = mainThread;
    this.createOpenSearchIndex();
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              public void run() {
                try {
                  openSearchClient.close();
                } catch (IOException e) {
                  logger.error("Problem closing the Open Search client");
                  e.printStackTrace();
                }

                logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.close();
                consumer.wakeup();
                logger.info("Consumer shutdown cleanly!");
                // join the main thread to allow the execution of the code in the main thread
                try {
                  mainThread.join();
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
            });
  }

  // This will block current thread
  public void startConsumming() throws IOException {
    new Thread(
            () -> {
              try {

                consumer.subscribe(Collections.singleton(topic));

                while (this.mainThread.isAlive()) {
                  ConsumerRecords<String, String> records = this.getRecordsFromConsumer();
                  if (records.count() <= 0) {
                    continue;
                  }
                  this.sendBulkRequestsToOpenSearch(records);
                  // commit batch
                  consumer.commitSync();
                  logger.info("Offset has been commited");

                  Thread.sleep(1000);
                }
              } catch (WakeupException e) {
                logger.info("Consumer is starting to shut down");
              } catch (Exception e) {
                logger.error("Unexpected exception in the consumer", e);
              } finally {
                consumer.close(); // close the consumer, this will also commit offsets
                try {
                  openSearchClient.close();
                } catch (IOException e) {
                  logger.error("Unexpected exception when closing Open Search client", e);
                }
                logger.info("The consumer is now gracefully shut down");
              }
            })
        .start();
  }

  private void sendBulkRequestsToOpenSearch(ConsumerRecords<String, String> records)
      throws IOException {
    BulkRequest bulkRequest = new BulkRequest();

    for (ConsumerRecord<String, String> record : records) {
      String recordId = extractId(record.value());

      // create request
      IndexRequest indexRequest =
          new IndexRequest(indexName).source(record.value(), XContentType.JSON).id(recordId);
      // add to builk
      bulkRequest.add(indexRequest);
    }

    if (bulkRequest.numberOfActions() > 0) {
      logger.info("Bulk request: " + bulkRequest.numberOfActions());
      BulkResponse bulkResponse = this.openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
      logger.info("Inserted " + bulkResponse.getItems().length + " records into Open Search");
    }
  }

  private ConsumerRecords<String, String> getRecordsFromConsumer() {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
    int recordsCount = records.count();
    logger.info("Received " + recordsCount + " records from " + topic);
    return records;
  }

  private void createOpenSearchIndex() throws IOException {
    boolean indexExist =
        openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);

    if (!indexExist) {
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
      openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
      logger.info("The Wikimedia Index has been created");
    } else {
      logger.info("Index already exists");
    }
  }

  private RestHighLevelClient createOpenSearchClient() {
    // replace this with your Open Search Connection String
    String connString = "";

    // we build a URI from the connection string
    RestHighLevelClient restHighLevelClient;
    URI connUri = URI.create(connString);
    // extract login information if it exists
    String userInfo = connUri.getUserInfo();

    if (userInfo == null) {
      // REST client without security
      restHighLevelClient =
          new RestHighLevelClient(
              RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

    } else {
      // REST client with security
      String[] auth = userInfo.split(":");

      CredentialsProvider cp = new BasicCredentialsProvider();
      cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

      restHighLevelClient =
          new RestHighLevelClient(
              RestClient.builder(
                      new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                  .setHttpClientConfigCallback(
                      httpAsyncClientBuilder ->
                          httpAsyncClientBuilder
                              .setDefaultCredentialsProvider(cp)
                              .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
    }

    return restHighLevelClient;
  }

  private String extractId(String rawJson) {
    return JsonParser.parseString(rawJson)
        .getAsJsonObject()
        .get("meta")
        .getAsJsonObject()
        .get("id")
        .getAsString();
  }

  private KafkaConsumer<String, String> createKafkaConsumer() {
    String bootstrapServer = "127.0.0.1:9092";
    String groupId = "consumer-opensearch";

    // create consumer configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // consumer
    return new KafkaConsumer<String, String>(properties);
  }
}
