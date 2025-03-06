package org.example.debexeno.service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.example.debexeno.component.KafkaChangeEventProducer;
import org.example.debexeno.config.DatabaseConfig;
import org.example.debexeno.offset.OffsetManager;
import org.example.debexeno.reader.ChangeEvent;
import org.example.debexeno.reader.PostgresChangeLogReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

  @Autowired
  private KafkaChangeEventProducer kafkaProducer;

  @Autowired
  private OffsetManager offsetManager;

  /**
   * Initialize capturing in a separate thread. Create a new thread to capture changes
   */
  public void startCapture() {
    if (running.compareAndSet(false, true)) {
      executorService = Executors.newSingleThreadExecutor();
      executorService.submit(this::captureChanges);
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
  public void captureChanges() {

    // Track only specific tables (empty set means all tables)
    Set<String> trackedTables = new HashSet<>();
    trackedTables.add("public.test_table");

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
}
