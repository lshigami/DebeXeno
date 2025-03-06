package org.example.debexeno.error;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.debexeno.kafka.KafkaChangeEventProducer;
import org.example.debexeno.reader.ChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DeadLetterQueue {

  private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueue.class);

  @Autowired
  private KafkaChangeEventProducer kafkaProducer;

  @Autowired
  private final ObjectMapper mapper = new ObjectMapper();

  @Value("${error.dlq.topic:debexeno-dlq}")
  private String dlqTopic;

  /**
   * Send the event to the dead letter queue
   *
   * @param event
   * @param error
   * @param phase
   */
  public void sendToDeadLetterQueue(ChangeEvent event, Throwable error, String phase) {

    String key = UUID.randomUUID().toString();
    try {
      String value = kafkaProducer.serializeChangeEvent(event);

      ObjectNode node = (ObjectNode) mapper.readTree(value);
      node.put("message", error.getMessage());
      node.put("type", error.getClass().getName());
      node.put("phase", phase);
      node.put("timestamp", Instant.now().toString());

      ProducerRecord<String, String> record = new ProducerRecord<>(dlqTopic, key, node.toString());

      kafkaProducer.getProducer().send(record, (metadata, exception) -> {
        if (exception != null) {
          logger.error("Failed to send event to dead letter queue", exception);
        } else {
          logger.info("Event sent to dead letter queue: topic={}, partition={}, offset={}",
              metadata.topic(), metadata.partition(), metadata.offset());
        }
      });
    } catch (Exception e) {
      logger.error("Failed to send event to dead letter queue", e);
    }
  }

  public void ensureTopicExists() {
    kafkaProducer.createTopicIfNotExists(dlqTopic);
  }


}
