package org.example.debexeno.reader;

import static org.example.debexeno.ultilizes.Const.TIMESTAMP_FORMATTER;

import java.time.Instant;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import org.example.debexeno.ultilizes.Type;

@Value
@Getter()
@Setter()
public class ChangeEvent {

  String txId;
  Type type;
  String schema;
  String table;
  Map<String, Object> columnValues;
  Instant timestamp;

  ChangeEvent(String txId, Type type, String schema, String table, Map<String, Object> columnValues,
      Instant timestamp) {
    this.txId = txId;
    this.type = type;
    this.schema = schema;
    this.table = table;
    this.columnValues = columnValues;
    this.timestamp = timestamp;
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
    sb.append("]");
    return sb.toString();
  }
}
