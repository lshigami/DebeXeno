package org.example.debexeno.reader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.NonFinal;
import org.example.debexeno.ultilizes.Type;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@Setter
public class PostgresChangeLogReader {

  private static final Logger logger = LoggerFactory.getLogger(PostgresChangeLogReader.class);

  String QUERY_SELECT_SLOT_NAME = "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '";
  String QUERY_CREATE_REPLICATION_SLOT = "SELECT pg_create_logical_replication_slot('";

  String jdbcUrl;
  String username;
  String password;
  String slotName;
  Set<String> trackedTables;
  @NonFinal
  Connection connection;
  //Last log sequence number
  @NonFinal
  String lastLsn;

  public PostgresChangeLogReader(String jdbcUrl, String username, String password, String slotName,
      Set<String> trackedTables) {
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
    this.slotName = slotName;
    this.trackedTables = trackedTables;
    this.lastLsn = null;
  }

  /**
   * Ensures replication slot already exists, else create a new one using wal2json plugin Plugin
   * wal2json is required to convert WAL to JSON format
   *
   * @throws SQLException
   */
  public void ensureReplicationSlotExists() throws SQLException {
    logger.info("Checking if replication slot '{}' exists", slotName);
    try (Statement stmt = connection.createStatement()) {
      ResultSet rs = stmt.executeQuery(QUERY_SELECT_SLOT_NAME + slotName + "'");

      if (!rs.next()) {
        logger.info("Creating replication slot '{}'", slotName);
        stmt.execute(QUERY_CREATE_REPLICATION_SLOT + slotName + "', 'wal2json')");
        logger.info("Replication slot '{}' created successfully", slotName);
      } else {
        logger.info("Replication slot '{}' already exists", slotName);
      }
    }
  }

  /**
   * Helper function to compare two LSNs
   *
   * @param lsn
   * @param lastProcessedLsn
   * @return 0 if equal, -1 if lsn is less than lastProcessedLsn, 1 if lsn is greater than
   */
  public int compareLsn(String lsn, String lastProcessedLsn) {
    if (lsn == null && lastProcessedLsn == null) {
      return 0;
    }
    if (lsn == null) {
      return -1;
    }
    if (lastProcessedLsn == null) {
      return 1;
    }
    String[] lsnParts = lsn.split("/");
    String[] lastProcessedLsnParts = lastProcessedLsn.split("/");

    // Check if LSN has 2 parts
    if (lsnParts.length != 2 || lastProcessedLsnParts.length != 2) {
      logger.warn("Lsn does not match lastProcessedLsn parts");
      return lsn.compareTo(lastProcessedLsn);
    }

    // Compare first part
    long firstLsn = Long.parseLong(lsnParts[0], 16); // Hex to long
    long firstLastProcessedLsn = Long.parseLong(lastProcessedLsnParts[0], 16);
    if (firstLsn != firstLastProcessedLsn) {
      return Long.compare(firstLsn, firstLastProcessedLsn);
    }
    // Compare second part
    long secondLsn = Long.parseLong(lsnParts[1], 16);
    long secondLastProcessedLsn = Long.parseLong(lastProcessedLsnParts[1], 16);
    return Long.compare(secondLsn, secondLastProcessedLsn);
  }

  /**
   * Connect to PostgresSQL
   *
   * @throws SQLException
   */
  public void connect() throws SQLException {
    logger.info("Connecting to PostgreSQL database: {}", jdbcUrl);
    Properties props = new Properties();
    props.put("user", username);
    props.put("password", password);
    connection = DriverManager.getConnection(jdbcUrl, props);
    ensureReplicationSlotExists();
    logger.info("Successfully connected to PostgreSQL database");
  }

  /**
   * Closes the database connection
   */
  public void close() {
    if (connection != null) {
      try {
        connection.close();
        logger.info("Database connection closed");
      } catch (SQLException e) {
        logger.error("Error closing database connection", e);
      }
    }
  }

  /**
   * Create SQL statement to get replication logs limited by LSN, "without consuming them"
   * Peek_changes is used to get the changes without consuming them
   *
   * @param slotName
   * @param limit
   * @return A query to get replication logs using peek_changes
   */
  String replicationLogsPeekChanges(String slotName, int limit) {
    return String.format(
        "SELECT * FROM pg_logical_slot_peek_changes('%s', NULL, %d, 'format-version', '2')",
        slotName, limit);
  }

  /**
   * Create SQL statement to consume replication logs up to a given LSN
   *
   * @param slotName Replication slot name
   * @param uptoLsn  Last log sequence number to consume
   * @return A query to consume replication logs
   */
  String replicationLogsConsumeChanges(String slotName, String uptoLsn) {
    if (uptoLsn == null) {
      return null;
    }
    return String.format(
        "SELECT * FROM pg_logical_slot_get_changes('%s', '%s', NULL, 'format-version', '2')",
        slotName, uptoLsn);
  }

  /**
   * Parses the JSON change data from wal2json
   *
   * @param changeData
   * @param xid
   * @param type
   * @return ChangeEvent object parsed from the JSON, or null if the table is not tracked
   */
  public ChangeEvent parseChangeData(JSONObject changeData, String xid, Type type, String lsn) {
    if (!changeData.has("schema") || !changeData.has("table")) {
      return null;
    }

    String schema = changeData.getString("schema");
    String table = changeData.getString("table");
    // Check if schema.table is in trackedTables
    if (!trackedTables.isEmpty() && !trackedTables.contains(schema + "." + table)) {
      return null;
    }
    /* Sample data
    {
      "action": "I",
      "schema": "public",
      "table": "test_table",
      "columns": [
        { "name": "id", "type": "integer", "value": 126 },
        { "name": "name", "type": "text", "value": "X" }
      ]
    }
    */
    // Get column values
    Map<String, Object> columnValues = new HashMap<>();
    if (changeData.has("columns")) {
      JSONArray columns = changeData.getJSONArray("columns");
      for (int i = 0; i < columns.length(); i++) {
        JSONObject column = columns.getJSONObject(i);
        String columnName = column.getString("name");
        Object value = column.opt("value"); // OPT: return null if not found
        columnValues.put(columnName, value);
      }
    }
    /*
    {
    "action": "U",
    "schema": "public",
    "table": "test_table",
    "columns": [
      { "name": "id", "type": "integer", "value": 70 },
      { "name": "name", "type": "text", "value": "SGP" }
    ],
    "identity": [
      { "name": "id", "type": "integer", "value": 70 },
      { "name": "name", "type": "text", "value": "HKA" }
     ]
    }
     */
    // Get old column values for updates or deletes
    Optional<Map<String, Object>> oldColumnValues = Optional.empty();
    if ((type == Type.UPDATE || type == Type.DELETE) && changeData.has("identity")) {
      JSONArray columns = changeData.getJSONArray("identity");
      Map<String, Object> columnValue = new HashMap<>();
      for (int i = 0; i < columns.length(); i++) {
        JSONObject column = columns.getJSONObject(i);
        String columnName = column.getString("name");
        Object value = column.opt("value");
        columnValue.put(columnName, value);
      }
      oldColumnValues = Optional.of(columnValue);
    }

    return new ChangeEvent(xid, type, schema, table, columnValues, Instant.now(), oldColumnValues,
        lsn);


  }

  /**
   * Reads changes from the WAL using the configured replication slot without consuming them
   *
   * @param limit Maximum number of changes to read
   * @return List of ChangeEvent objects representing the changes
   * @throws SQLException
   */
  public List<ChangeEvent> readChanges(int limit) throws SQLException {
    if (connection == null || connection.isClosed()) {
      connect();
    }

    List<ChangeEvent> changes = new ArrayList<>();
    logger.info("Reading changes from LSN: {}", lastLsn);
    String lastProcessedLsn = null;

    try (Statement stmt = connection.createStatement()) {
      logger.info("Peeking at changes from replication slot");
      String query = replicationLogsPeekChanges(slotName, limit);

      ResultSet rs = stmt.executeQuery(query);
      while (rs.next()) {
        // After the query, output is look like
        /*
        [
          {
            "lsn": "0/1763D90",
            "xid": "800",
            "data": "{\"action\":\"B\"}"
          },
          {
            "lsn": "0/1763D90",
            "xid": "800",
            "data": "{\"action\":\"U\",\"schema\":\"public\",\"table\":\"test_table\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"value\":83},{\"name\":\"name\",\"type\":\"text\",\"value\":\"k\"}],\"identity\":[{\"name\":\"id\",\"type\":\"integer\",\"value\":83}]}"
          },
          {
            "lsn": "0/1763E08",
            "xid": "800",
            "data": "{\"action\":\"C\"}"
          }
        ]
         */
        String lsn = rs.getString("lsn");
        String xid = rs.getString("xid");
        String jsonData = rs.getString("data");

        if (compareLsn(lsn, lastProcessedLsn) > 0) {
          lastProcessedLsn = lsn;
        }

        try {
          Type type = null;
          ChangeEvent event = null;

          JSONObject changeData = new JSONObject(jsonData);
          String action = changeData.getString("action");
          // Handle different actions (B = Begin, C = Commit, I = Insert, U = Update, D = Delete)
          switch (action) {
            case "B":
              logger.info("Start transaction with LSN: {}", lsn);
              break;
            case "C":
              logger.info("Commit transaction with LSN: {}", lsn);
              break;
            case "I":
              type = Type.INSERT;
              event = parseChangeData(changeData, xid, type, lsn);
              if (event != null) {
                changes.add(event);
              }
              break;
            case "U":
              type = Type.UPDATE;
              event = parseChangeData(changeData, xid, type, lsn);
              if (event != null) {
                changes.add(event);
              }
              break;
            case "D":
              type = Type.DELETE;
              event = parseChangeData(changeData, xid, type, lsn);
              if (event != null) {
                changes.add(event);
              }
              break;
            default:
              logger.warn("Unknown action type: {}", action);
          }
        } catch (Exception e) {
          logger.error("Error parsing JSON data", e);
        }

        if (lastProcessedLsn != null) {
          this.lastLsn = lastProcessedLsn;
        }
      }

    }
    logger.info("Read {} changes, last LSN: {}", changes.size(), this.lastLsn);
    return changes;
  }

  /**
   * Consumes changes from the replication slot up to the specified LSN. This should be called after
   * successfully processing the changes
   *
   * @param uptoLsn Last log sequence number to consume
   * @throws SQLException
   */
  public void consumeChanges(String uptoLsn) throws SQLException {
    if (uptoLsn == null) {
      logger.debug("No LSN provided to consume changes");
      return;
    }
    if (connection == null || connection.isClosed()) {
      connect();
    }
    logger.info("Consuming changes up to LSN: {}", uptoLsn);

    try (Statement stmt = connection.createStatement()) {
      String query = replicationLogsConsumeChanges(slotName, uptoLsn);
      try {
        if (query != null) {
          ResultSet rs = stmt.executeQuery(query);
          int count = rs.getRow();
          logger.info("Consumed {} changes up to LSN: {}", count, uptoLsn);
        }
      } catch (SQLException e) {
        logger.error("Error consuming changes up to LSN {}: {}", uptoLsn, e.getMessage(), e);
      }

    }

  }


}
