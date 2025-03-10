package org.example.debexeno.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SchemaManagerTest {

  @Mock
  private Connection connection;

  @Mock
  private DatabaseMetaData metaData;

  @Mock
  private ResultSet primaryKeysResultSet;

  @Mock
  private ResultSet columnsResultSet;

  @InjectMocks
  private SchemaManager schemaManager;

  @Spy
  private Map<String, TableSchema> schemaCache = new ConcurrentHashMap<>();

  private final String schemaName = "schemaName";
  private final String tableName = "tableName";
  private final String tableKey = schemaName + "." + tableName;

  @BeforeEach
  void setUp() throws SQLException {
    when(connection.getMetaData()).thenReturn(metaData);
  }

  @Test
  public void refreshTableSchema_shouldReturnTableSchema() throws SQLException {
    //Given
    when(metaData.getPrimaryKeys(isNull(), eq(schemaName), eq(tableName))).thenReturn(
        primaryKeysResultSet);
    when(metaData.getColumns(isNull(), eq(schemaName), eq(tableName), isNull())).thenReturn(
        columnsResultSet);

    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(primaryKeysResultSet.getString("column_name")).thenReturn("id");

    when(columnsResultSet.next()).thenReturn(true, true, false);

    when(columnsResultSet.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(columnsResultSet.getInt("DATA_TYPE")).thenReturn(1, 2);
    when(columnsResultSet.getString("TYPE_NAME")).thenReturn("int", "varchar");
    when(columnsResultSet.getInt("COLUMN_SIZE")).thenReturn(11, 255);
    when(columnsResultSet.getInt("NULLABLE")).thenReturn(0, 1);

    //When
    TableSchema result = schemaManager.refreshTableSchema(connection, "schemaName", "tableName");
    //Then
    assertNotNull(result);
    assertEquals(schemaName, result.getSchemaName());
    assertEquals(tableName, result.getTableName());

    ColumnSchema idColumn = result.getColumnByName("id");
    assertNotNull(idColumn);
    assertEquals("id", idColumn.getName());
    assertEquals("int", idColumn.getTypeName());
    assertEquals(11, idColumn.getSize());
    assertFalse(idColumn.isNullable());

    ColumnSchema nameColumn = result.getColumnByName("name");
    assertNotNull(nameColumn);
    assertEquals("name", nameColumn.getName());
    assertEquals("varchar", nameColumn.getTypeName());
    assertEquals(255, nameColumn.getSize());
    assertTrue(nameColumn.isNullable());

    // Verify schema was cached
    assertTrue(schemaCache.containsKey(tableKey));
    System.out.printf(result.toString());
    assertEquals(result, schemaCache.get(tableKey));
  }

  @Test
  public void getTableSchema_shouldReturnCachedSchemaIfAvailable() throws SQLException {
    //Given
    TableSchema cachedSchema = new TableSchema(schemaName, tableName);
    schemaCache.put(tableKey, cachedSchema);
    //When
    TableSchema result = schemaManager.getTableSchema(connection, schemaName, tableName);
    //Then
    assertEquals(cachedSchema, result);
  }

  @Test
  public void getTableSchema_shouldRefreshSchemaIfNotCached() throws SQLException {
    //Given
    when(metaData.getPrimaryKeys(isNull(), eq(schemaName), eq(tableName))).thenReturn(
        primaryKeysResultSet);
    when(metaData.getColumns(isNull(), eq(schemaName), eq(tableName), isNull())).thenReturn(
        columnsResultSet);
    //When
    schemaManager.getTableSchema(connection, schemaName, tableName);

    //Then
    verify(schemaCache).put(eq(tableKey), any(TableSchema.class));
    assert schemaCache.containsKey(tableKey);
  }

  @Test
  public void hasSchemaChanged_shouldReturnTrueIfSchemaChanged() throws SQLException {
    //Given
    TableSchema existingSchema = new TableSchema(schemaName, tableName);
    schemaCache.put(tableKey, existingSchema);

    when(metaData.getPrimaryKeys(isNull(), eq(schemaName), eq(tableName))).thenReturn(
        primaryKeysResultSet);
    when(metaData.getColumns(isNull(), eq(schemaName), eq(tableName), isNull())).thenReturn(
        columnsResultSet);

    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(primaryKeysResultSet.getString("column_name")).thenReturn("id");

    when(columnsResultSet.next()).thenReturn(true, true, false);

    when(columnsResultSet.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(columnsResultSet.getInt("DATA_TYPE")).thenReturn(1, 2);
    when(columnsResultSet.getString("TYPE_NAME")).thenReturn("int", "varchar");
    when(columnsResultSet.getInt("COLUMN_SIZE")).thenReturn(11, 255);
    when(columnsResultSet.getInt("NULLABLE")).thenReturn(0, 1);

    //When
    boolean result = schemaManager.hasSchemaChanged(connection, schemaName, tableName);
    // Then
    System.out.println(existingSchema.toString());
    assertEquals(true, result);
  }

  @Test
  public void handleSchemaChange_shouldDetectAddedRemovedAndModifiedColumns() {
    //Given
    TableSchema oldSchema = new TableSchema(schemaName, tableName);
    oldSchema.addColumn(new ColumnSchema("id", 1, "int", 11, false));
    oldSchema.addColumn(new ColumnSchema("name", 2, "varchar", 255, true));
    oldSchema.addColumn(new ColumnSchema("dif1", 2, "varchar", 255, false));

    TableSchema newSchema = new TableSchema(schemaName, tableName);
    newSchema.addColumn(new ColumnSchema("id", 1, "int", 11, false));
    newSchema.addColumn(new ColumnSchema("name", 2, "varchar", 255, true));
    newSchema.addColumn(new ColumnSchema("dif2", 2, "varchar", 255, false));
    //When
    schemaManager.handleSchemaChange(oldSchema, newSchema);
    //Then

  }
}
