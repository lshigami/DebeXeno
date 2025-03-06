package org.example.debexeno.schema;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SchemaManager {

  Logger logger = LoggerFactory.getLogger(SchemaManager.class);

  //TODO : Implement schema caching using Redis or other caching mechanism
  Map<String, TableSchema> schemaCache = new ConcurrentHashMap<>();

  /**
   * Refreshes schema information for a specific table
   *
   * @param connection
   * @param schemaName
   * @param tableName
   * @return TableSchema
   * @throws SQLException
   */
  public TableSchema refreshTableSchema(Connection connection, String schemaName, String tableName)
      throws SQLException {
    String tableKey = schemaName + "." + tableName;

    TableSchema tableSchema = new TableSchema(schemaName, tableName);

    try {
      DatabaseMetaData metaData = connection.getMetaData();

      //Handle primary keys
      ResultSet primaryKeys = metaData.getPrimaryKeys(null, schemaName, tableName);
      while (primaryKeys.next()) {
        String columnName = primaryKeys.getString("column_name");
        tableSchema.addPrimaryKeyColumn(columnName);
      }

      //Handle columns
      ResultSet columns = metaData.getColumns(null, schemaName, tableName, null);
      while (columns.next()) {
        String columnName = columns.getString("COLUMN_NAME");  // Ensure correct case
        int dataType = columns.getInt("DATA_TYPE");
        String typeName = columns.getString("TYPE_NAME");
        int columnSize = columns.getInt("COLUMN_SIZE");
        boolean nullable = columns.getInt("NULLABLE") == DatabaseMetaData.columnNullable;

        ColumnSchema column = new ColumnSchema(columnName, dataType, typeName, columnSize,
            nullable);
        tableSchema.addColumn(column);
      }

      schemaCache.put(tableKey, tableSchema);
      logger.info("Schema refreshed for table: {}", tableKey);

    } catch (SQLException e) {
      logger.error("Error refreshing table schema for table: {}", tableKey, e);
      throw e;
    }

    return tableSchema;
  }


  /**
   * Gets the schema for a table, refreshing from the database if not cached
   *
   * @param connection Database connection
   * @param schemaName Schema name
   * @param tableName  Table name
   * @return The table schema
   * @throws SQLException
   */
  public TableSchema getTableSchema(Connection connection, String schemaName, String tableName)
      throws SQLException {
    String tableKey = schemaName + "." + tableName;
    TableSchema cachedSchema = schemaCache.get(tableKey);

    if (cachedSchema == null) {
      return refreshTableSchema(connection, schemaName, tableName);
    }

    return cachedSchema;
  }


  /**
   * Check if the schema for a table has changed
   *
   * @param connection Database connection
   * @param schemaName Schema name
   * @param tableName  Table name
   * @return true if the schema has changed
   */
  public boolean hasSchemaChanged(Connection connection, String schemaName, String tableName)
      throws SQLException {
    String tableKey = schemaName + "." + tableName;

    TableSchema cachedSchema = schemaCache.get(tableKey);
    if (cachedSchema == null) {
      refreshTableSchema(connection, schemaName, tableName);
      return false;
    }

    TableSchema currentSchema = refreshTableSchema(connection, schemaName, tableName);

    boolean changed = !cachedSchema.equals(currentSchema);
    if (changed) {
      logger.info("Schema changed for {}.{}", schemaName, tableName);
      schemaCache.put(tableKey, currentSchema);
      handleSchemaChange(cachedSchema, currentSchema);
    }
    return changed;
  }


  public void handleSchemaChange(TableSchema oldSchema, TableSchema newSchema) {

    Set<String> oldColumns = oldSchema.getColumns().values().stream().map(ColumnSchema::getName)
        .collect(Collectors.toSet());

    Set<String> newColumns = newSchema.getColumns().values().stream().map(ColumnSchema::getName)
        .collect(Collectors.toSet());

    // Find removed columns
    Set<String> removedColumns = new HashSet<>(oldColumns);
    removedColumns.removeAll(newColumns);

    //Find added columns
    Set<String> addedColumns = new HashSet<>(newColumns);
    addedColumns.removeAll(oldColumns);

    //Find common columns
    Set<String> commonColumns = new HashSet<>(oldColumns);
    commonColumns.retainAll(newColumns);

    //Find modified columns in common columns
    Set<String> modifiedColumns = commonColumns.stream().filter(
            column -> !oldSchema.getColumnByName(column).equals(newSchema.getColumnByName(column)))
        .collect(Collectors.toSet());

    logger.info("Schema changes for {}.{}: added={}, removed={}, modified={}",
        newSchema.getSchemaName(), newSchema.getTableName(), addedColumns, removedColumns,
        modifiedColumns);

    //TODO: Implement schema change handling
    /*

    1. Update schema registry if configured
    2.Publish schema change event to a special topic

     */
  }
}
