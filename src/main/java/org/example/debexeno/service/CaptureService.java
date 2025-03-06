package org.example.debexeno.service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.example.debexeno.component.KafkaChangeEventProducer;
import org.example.debexeno.config.DatabaseConfig;
import org.example.debexeno.offset.OffsetManager;
import org.example.debexeno.reader.ChangeEvent;
import org.example.debexeno.reader.PostgresChangeLogReader;
import org.example.debexeno.schema.SchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class CaptureService {

  private static final Logger logger = LoggerFactory.getLogger(CaptureService.class);

  @Autowired
  private DatabaseConfig databaseConfig;

  //Make sure only one thread is running
  private final AtomicBoolean running = new AtomicBoolean(false);

  //Executor service to run the capture in a separate thread
  private ExecutorService executorService;
  private ScheduledExecutorService scheduledExecutor;

  @Autowired
  private KafkaChangeEventProducer kafkaProducer;

  @Autowired
  private OffsetManager offsetManager;

  @Value("${capture.schema.check.interval.minutes:15}")
  private int schemaCheckIntervalMinutes;

  @Autowired
  private SchemaManager schemaManager;

  /**
   * Initialize capturing in a separate thread. Create a new thread to capture changes
   */
  public void startCapture(Set<String> trackedTables) {
    if (running.compareAndSet(false, true)) {

      scheduledExecutor = Executors.newScheduledThreadPool(1);
      // Start a scheduled executor to check for schema changes
      scheduledExecutor.scheduleAtFixedRate(() -> checkSchemaChanges(trackedTables),
          schemaCheckIntervalMinutes, schemaCheckIntervalMinutes, TimeUnit.MINUTES);

      executorService = Executors.newSingleThreadExecutor();
      executorService.submit(() -> captureChanges(trackedTables));
      logger.info("Change capture service started");
    } else {
      logger.warn("Change capture service is already running");
    }
  }

  /**
   * Stop capturing changes
   */
  public void stopCapture() {
    if (running.compareAndSet(true, false)) {
      executorService.shutdown();
      logger.info("Change capture service stopped");
    }
  }

  /**
   * Capture and process changes
   */
  public void captureChanges(Set<String> trackedTables) {

    // Track only specific tables (empty set means all tables)

    PostgresChangeLogReader reader = new PostgresChangeLogReader(databaseConfig.getJdbcUrl(),
        databaseConfig.getUsername(), databaseConfig.getPassword(), databaseConfig.getSlotName(),
        trackedTables);

    // Get the last processed LSN from the offset manager
    String lastLsn = offsetManager.getOffset(databaseConfig.getSlotName());
    logger.info("Starting capture from LSN: {}", lastLsn != null ? lastLsn : "beginning");

    try {
      reader.connect();
      reader.setLastLsn(lastLsn);
      // Read changes continuously
      while (running.get()) {
        List<ChangeEvent> changes = reader.readChanges(100);

        // Get the current LSN after reading
        String currentLsn = reader.getLastLsn();

        // Filter out changes that have already been processed based on the offset
        List<ChangeEvent> newChanges = fillterOut(changes, lastLsn, reader);

        //TODO: Transform the changes to a format that can be sent to Kafka

        boolean allChangesSuccess = true;
        for (ChangeEvent change : newChanges) {

          try {
            kafkaProducer.sendChangeEvent(change);
          } catch (Exception e) {
            allChangesSuccess = false;
            logger.error("Failed to process change: {}", change, e);
            //TODO: Implement error handling like Dead Letter Queue
          }
        }

        if (allChangesSuccess) {
          // Update the offset with the last processed LSN
          offsetManager.updateOffset(databaseConfig.getSlotName(), currentLsn);
          logger.info("Updated offset to LSN: {}", currentLsn);

          // Then consume changes up to the current LSN
          reader.consumeChanges(currentLsn);
          logger.info("Consumed changes up to LSN: {}", currentLsn);

        }

        // Sleep for a bit to avoid hammering the database
        if (changes.isEmpty()) {
          Thread.sleep(1000);
        }
      }
    } catch (SQLException e) {
      logger.error("Database error", e);
      running.set(false);
    } catch (InterruptedException e) {
      logger.error("Interrupted", e);
      Thread.currentThread()
          .interrupt(); // Restore interrupted status to allow other threads to handle it
      running.set(false);
    } finally {
      reader.close();
    }
  }

  public List<ChangeEvent> fillterOut(List<ChangeEvent> changes, String lastLsn,
      PostgresChangeLogReader reader) {
    ArrayList<ChangeEvent> newChanges = new ArrayList<>();
    if (!changes.isEmpty()) {
      for (ChangeEvent change : changes) {
        // Compare the LSN of each change with the last processed LSN
        String changeLsn = change.getLsn();
        if (lastLsn == null || (changeLsn != null && reader.compareLsn(changeLsn, lastLsn) > 0)) {
          newChanges.add(change);
        }
      }

      logger.debug("Filtered {} changes, {} are new since last processed LSN: {}", changes.size(),
          newChanges.size(), lastLsn);
    }
    return newChanges;
  }

  /**
   * Periodically checks for schema changes
   */
  private void checkSchemaChanges(Set<String> trackedTables) {

    try {

      PostgresChangeLogReader reader = new PostgresChangeLogReader(databaseConfig.getJdbcUrl(),
          databaseConfig.getUsername(), databaseConfig.getPassword(), databaseConfig.getSlotName(),
          trackedTables);
      reader.connect();

      for (String table : trackedTables) {
        String[] parts = table.split("\\."); // Use "\\" because "." is a special character in regex
        String schema = parts[0];
        String tableName = parts[1];

        if (schemaManager.hasSchemaChanged(reader.getConnection(), schema, tableName)) {
          logger.info("Schema changed for table {}", table);
        }
      }
    } catch (SQLException e) {
      logger.error("Error checking schema changes", e);
    }
  }
}