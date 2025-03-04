package org.example.debexeno.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class KafkaConfig {

  @Value("${kafka.bootstrap.servers}")
  private String bootstrapServers; // Kafka broker addresses :e.g. localhost:9092

  @Value("${kafka.topic.prefix:cdc}")
  private String topicPrefix; // Prefix for Kafka topics : default is cdc

  @Value("${kafka.num.partitions:3}")
  private int numPartitions; // Number of partitions for a Kafka topic : default is 3

  @Value("${kafka.replication.factor:1}")
  private short replicationFactor; // Replication factor is a number of copies of a topic in a Kafka cluster : default is 1

}
