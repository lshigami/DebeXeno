package org.example.debexeno.reader;

import static org.example.debexeno.ultilizes.Const.TIMESTAMP_FORMATTER;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import org.example.debexeno.ultilizes.Type;

@Value
@Getter()
@Setter()
public class ChangeEvent {

  String txId; // transaction id
  Type type; // type of change [INSERT, UPDATE, DELETE]
  String schema; // schema name
  String table; // table name
  Map<String, Object> columnValues; //new column values
  Instant timestamp; // timestamp of the change
  /*
   * Set  REPLICA IDENTITY FULL on the table to get old values
   */ Optional<Map<String, Object>> oldData; // old column values when type is UPDATE or DELETE

  ChangeEvent(String txId, Type type, String schema, String table, Map<String, Object> columnValues,
      Instant timestamp, Optional<Map<String, Object>> oldData) {
    this.txId = txId;
    this.type = type;
    this.schema = schema;
    this.table = table;
    this.columnValues = columnValues;
    this.timestamp = timestamp;
    this.oldData = oldData;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ChangeEvent[");
    sb.append("txId=").append(txId);
    sb.append(", type=").append(type);
    sb.append(", table=").append(schema).append(".").append(table);
    sb.append(", timestamp=").append(TIMESTAMP_FORMATTER.format(timestamp));
    sb.append(", data=").append(columnValues);
    oldData.ifPresent(o -> sb.append(", oldData=").append(o));
    sb.append("]");
    return sb.toString();
  }
}
