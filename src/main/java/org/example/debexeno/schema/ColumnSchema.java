package org.example.debexeno.schema;

import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class ColumnSchema {

  String name;
  int sqlType;
  String typeName;
  int size;
  boolean nullable;

  @Override
  public String toString() {
    return "ColumnSchema{" + "name='" + name + '\'' + ", sqlType=" + sqlType + ", typeName='"
        + typeName + '\'' + ", size=" + size + ", nullable=" + nullable + '}';
  }
}
