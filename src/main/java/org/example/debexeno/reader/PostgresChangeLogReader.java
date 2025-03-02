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
import java.util.Properties;
import java.util.Set;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.example.debexeno.ultilizes.Type;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Value
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
   * Ensures replication slot already exists, else create a new one
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
   * Create SQL statement to get replication logs
   *
   * @param slotName
   * @param lastLsn
   * @param maxChanges
   * @return A query to get replication logs
   */
  String replicationLogsGetChanges(String slotName, String lastLsn, int maxChanges) {
    if (lastLsn == null) {
      return String.format(
          "SELECT * FROM pg_logical_slot_get_changes('%s', NULL, %d, 'format-version', '2')",
          slotName, maxChanges);
    }
    return String.format(
        "SELECT * FROM pg_logical_slot_get_changes('%s', '%s', %d, 'format-version', '2')",
        slotName, lastLsn, maxChanges);
  }

  /**
   * Parses the JSON change data from wal2json
   *
   * @param changeData
   * @param xid
   * @param type
   * @return ChangeEvent object parsed from the JSON, or null if the table is not tracked
   */
  public ChangeEvent parseChangeData(JSONObject changeData, String xid, Type type) {
    if (!changeData.has("schema") || !changeData.has("table")) {
      return null;
    }

    String schema = changeData.getString("schema");
    String table = changeData.getString("table");
    // Check if schema.table is in trackedTables
    if (!trackedTables.isEmpty() && !trackedTables.contains(schema + "." + table)) {
      return null;
    }
    Map<String, Object> columnValues = new HashMap<>();

    if (changeData.has("columns")) {
      JSONArray columns = changeData.getJSONArray("columns");
      for (int i = 0; i < columns.length(); i++) {
        JSONObject column = columns.getJSONObject(i);
        String columnName = column.getString("name");
        Object value = column.opt("value");
        columnValues.put(columnName, value);
      }
    }

    return new ChangeEvent(xid, type, schema, table, columnValues, Instant.now());


  }

  /**
   * Reads changes from the WAL using the configured replication slot
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

    try (Statement stmt = connection.createStatement()) {

      String query = replicationLogsGetChanges(slotName, lastLsn, limit);
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
        Type type = null;
        ChangeEvent event;
        lastLsn = lsn;

        JSONObject changeData = new JSONObject(jsonData);
        String action = changeData.getString("action");
        // Handle different actions (B = Begin, C = Commit, I = Insert, U = Update, D = Delete)
        switch (action) {
          case "B":
            logger.info("Start transaction with LSN: {}", lsn);
            break;
          case "C":
            logger.info("Commit transaction with LSN: {}", lsn);
            changes.clear();
            break;
          case "I":
            type = Type.INSERT;
            event = parseChangeData(changeData, xid, type);
            if (event != null) {
              changes.add(event);
            }
            break;
          case "U":
            type = Type.UPDATE;
            event = parseChangeData(changeData, xid, type);
            if (event != null) {
              changes.add(event);
            }
            break;
          case "D":
            type = Type.DELETE;
            event = parseChangeData(changeData, xid, type);
            if (event != null) {
              changes.add(event);
            }
            break;
          default:
            logger.warn("Unknown action type: {}", action);
        }

      }

    }
    logger.info("Read {} changes", changes.size());
    return changes;
  }

}
