package org.example.debexeno.schema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class TableSchema {

  String schemaName;
  String tableName;
  Map<String, ColumnSchema> columns = new HashMap<>();
  Set<String> primaryKeyColumns = new HashSet<>();

  /**
   * Add a column to the table schema
   *
   * @param column
   */
  public void addColumn(ColumnSchema column) {
    columns.put(column.getName(), column);
  }

  /**
   * Add a primary key column to the table schema
   *
   * @param columnName
   */
  public void addPrimaryKeyColumn(String columnName) {
    primaryKeyColumns.add(columnName);
  }

  /**
   * Get a column by name
   *
   * @param columnName
   * @return column schema
   */
  public ColumnSchema getColumnByName(String columnName) {
    return columns.get(columnName);
  }

  /**
   * Check if a column is a primary key
   *
   * @param columnName
   * @return true if the column is a primary key
   */
  public boolean isPrimaryKey(String columnName) {
    return primaryKeyColumns.contains(columnName);
  }

  /**
   * Check if a column exists in the table schema
   *
   * @param columnName
   * @return true if the column exists
   */
  public boolean hasColumn(String columnName) {
    return columns.containsKey(columnName);
  }

  @Override
  public String toString() {
    return "TableSchema{" + "schemaName='" + schemaName + '\'' + ", tableName='" + tableName + '\''
        + ", columns=" + columns.size() + ", primaryKeyColumns=" + primaryKeyColumns + '}';
  }
}
