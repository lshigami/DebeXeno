package org.example.debexeno.ultilizes;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class Const {

  public static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern(
      "yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
}
