package org.example.debexeno.kafka;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.debexeno.config.KafkaConfig;
import org.example.debexeno.reader.ChangeEvent;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
public class KafkaChangeEventProducer {

  private static final Logger logger = LoggerFactory.getLogger(KafkaChangeEventProducer.class);
  @Autowired
  private KafkaConfig kafkaConfig;

  private KafkaProducer<String, String> producer;
  private AdminClient adminClient;

  /**
   * Constructor-based dependency injection Initialize Kafka producer and admin client. Called after
   * the bean is created
   */
  @PostConstruct
  public void init() {
    // Configure Kafka producer
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    // Create Kafka producer
    producer = new KafkaProducer<>(producerProps);

    // Create Kafka admin client
    Properties adminProps = new Properties();
    adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
    adminClient = AdminClient.create(adminProps);

    logger.info("Kafka producer initialized with bootstrap servers: {}",
        kafkaConfig.getBootstrapServers());
  }

  /**
   * Close Kafka producer and admin client Called when the application is shutting down
   */
  @PreDestroy
  public void close() {
    if (producer != null) {
      producer.close();
      logger.info("Kafka producer closed");
    }
    if (adminClient != null) {
      adminClient.close();
      logger.info("Kafka admin client closed");
    }
  }

  /**
   * Get the topic name for a given schema and table
   *
   * @param schema Schema name
   * @param table  Table name
   * @return Topic name
   */
  public String getTopicName(String schema, String table) {
    return String.format("%s.%s.%s", kafkaConfig.getTopicPrefix(), schema, table);
  }

  /**
   * Create a Kafka topic if it does not exist
   *
   * @param topicName Topic name
   */

  public void createTopicIfNotExists(String topicName) {
    try {
      boolean topicExists = adminClient.listTopics().names().get().contains(topicName);
      if (!topicExists) {
        logger.info("Creating topic {}", topicName);
        NewTopic newTopic = new NewTopic(topicName, kafkaConfig.getNumPartitions(),
            kafkaConfig.getReplicationFactor());
        //singleton : Returns an immutable "set" containing only the specified object.
        //all: Return a future which will be completed when all the requests complete.
        //get: Wait for the computation to complete, and then retrieves its result. -> blocking
        adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        logger.info("Topic {} created", topicName);
      }
    } catch (InterruptedException | ExecutionException e) {
      logger.error("Error checking if topic exists", e);
      throw new RuntimeException("Failed to create Kafka topic", e);
    }
  }

  /**
   * Convert ChangeEvent to JSON string
   *
   * @param event ChangeEvent to convert
   * @return JSON string representation
   */
  public String serializeChangeEvent(ChangeEvent event) {
    JSONObject json = new JSONObject();
    json.put("id", event.getTxId());
    json.put("type", event.getType().toString());
    json.put("schema", event.getSchema());
    json.put("table", event.getTable());
    json.put("timestamp", event.getTimestamp().toString());
    json.put("data", new JSONObject(event.getColumnValues()));
    event.getOldData().ifPresent(oldData -> json.put("oldData", new JSONObject(oldData)));
    return json.toString();
  }

  /**
   * Send ChangeEvent to Kafka
   *
   * @param event ChangeEvent to send
   */

  public void sendChangeEvent(ChangeEvent event) {
    String topicName = getTopicName(event.getSchema(), event.getTable());
    createTopicIfNotExists(topicName);

    // Use {Type}.{Schema}.{Table} as key of Kafka message
    String key = event.getType().toString() + ":" + event.getSchema() + ":" + event.getTable();
    String value = serializeChangeEvent(event);
    ProducerRecord<String, String> message = new ProducerRecord<>(topicName, key, value);
    logger.debug("Sending message to Kafka topic {}", topicName);
    producer.send(message, (metadata, exception) -> {
      if (exception != null) {
        logger.error("Failed to send message to topic {}", topicName, exception);
      } else {
        logger.debug("Message sent to topic {} partition {} offset {}", metadata.topic(),
            metadata.partition(), metadata.offset());
      }
    });

  }
}
