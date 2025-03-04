package org.example.debexeno.service;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.example.debexeno.component.KafkaChangeEventProducer;
import org.example.debexeno.config.DatabaseConfig;
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

    try {
      reader.connect();

      // Read changes continuously
      while (running.get()) {
        List<ChangeEvent> changes = reader.readChanges(100);

        for (ChangeEvent change : changes) {
          logger.info(change.toString());

          // Process changes here
          kafkaProducer.sendChangeEvent(change);
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
}
