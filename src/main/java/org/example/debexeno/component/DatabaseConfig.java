package org.example.debexeno.component;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@RequiredArgsConstructor

public class DatabaseConfig {

  @Value("${db.jdbcUrl}")
  String jdbcUrl;

  @Value("${db.username}")
  String username;

  @Value("${db.password}")
  String password;


  @Value("${db.slotName}")
  String slotName;
}
