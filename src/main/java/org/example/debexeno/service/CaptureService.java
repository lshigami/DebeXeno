package org.example.debexeno.service;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.example.debexeno.component.DatabaseConfig;
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

  /**
   * Initialize capturing
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
      while (true) {
        List<ChangeEvent> changes = reader.readChanges(100);

        for (ChangeEvent change : changes) {
          logger.info(change.toString());

          // Process changes here
        }

        // Sleep for a bit to avoid hammering the database
        if (changes.isEmpty()) {
          Thread.sleep(1000);
        }
      }
    } catch (SQLException e) {
      logger.error("Database error", e);
    } catch (InterruptedException e) {
      logger.error("Interrupted", e);
    } finally {
      reader.close();
    }
  }
}
