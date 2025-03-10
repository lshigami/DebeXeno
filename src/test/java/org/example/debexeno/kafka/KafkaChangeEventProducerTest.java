package org.example.debexeno.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.example.debexeno.config.KafkaConfig;
import org.example.debexeno.reader.ChangeEvent;
import org.example.debexeno.ultilizes.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class KafkaChangeEventProducerTest {

  @Mock
  private KafkaConfig kafkaConfig;

  @Mock
  private AdminClient adminClient;

  @Mock
  private KafkaProducer<String, String> producer;

  @Mock
  private ListTopicsResult listTopicsResult;

  @Mock
  private CreateTopicsResult createTopicsResult;

  @Mock
  private KafkaFuture<Set<String>> topicsFuture;

  @Mock
  private KafkaFuture<Void> createTopicFuture;

  @InjectMocks
  private KafkaChangeEventProducer kafkaChangeEventProducer;

  @BeforeEach
  public void setUp() throws Exception {
    // Configure mocks
    when(kafkaConfig.getBootstrapServers()).thenReturn("localhost:9092");
    when(kafkaConfig.getTopicPrefix()).thenReturn("cdc");
    when(kafkaConfig.getNumPartitions()).thenReturn(3);
    when(kafkaConfig.getReplicationFactor()).thenReturn((short) 1);


  }

  @Test
  public void getTopicName_shouldReturnTopicName() {
    // When
    String topicName = kafkaChangeEventProducer.getTopicName("public", "product");

    // Then
    assertEquals("cdc.public.product", topicName);
  }

  @Test
  public void createTopicIfNotExists_shouldCreateTopic_whenTopicDoesNotExist() throws Exception {
    // Given
    Set<String> existingTopics = new HashSet<>();

    when(adminClient.listTopics()).thenReturn(listTopicsResult);
    when(listTopicsResult.names()).thenReturn(topicsFuture);
    when(topicsFuture.get()).thenReturn(existingTopics);
    when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
    when(createTopicsResult.all()).thenReturn(createTopicFuture);
    // When
    kafkaChangeEventProducer.createTopicIfNotExists("cdc.public.product");

    // Then
    verify(adminClient).createTopics(any());
  }

  @Test
  public void createTopicIfNotExists_shouldNotCreateTopic_whenTopicExists() throws Exception {
    // Given
    Set<String> existingTopics = new HashSet<>();
    existingTopics.add("cdc.public.product");

    when(adminClient.listTopics()).thenReturn(listTopicsResult);
    when(listTopicsResult.names()).thenReturn(topicsFuture);
    when(topicsFuture.get()).thenReturn(existingTopics);

    // When
    kafkaChangeEventProducer.createTopicIfNotExists("cdc.public.product");

    // Then
    verify(adminClient, never()).createTopics(any());
  }

  @Test
  public void sendChangeEvent_shouldSendMessageToKafka() throws Exception {
    //Given
    ChangeEvent mockEvent = new ChangeEvent("tx1", Type.INSERT, "public", "product",
        new HashMap<>(), Instant.now(), Optional.empty(), "1");
    Set<String> existingTopics = new HashSet<>();
    existingTopics.add("cdc.public.product");
    when(adminClient.listTopics()).thenReturn(listTopicsResult);
    when(listTopicsResult.names()).thenReturn(topicsFuture);
    when(topicsFuture.get()).thenReturn(existingTopics);

    when(producer.send(any(ProducerRecord.class), any())).thenReturn(null);
    // When
    kafkaChangeEventProducer.sendChangeEvent(mockEvent);

    // Then
    verify(producer).send(any(ProducerRecord.class), any());
  }
}
