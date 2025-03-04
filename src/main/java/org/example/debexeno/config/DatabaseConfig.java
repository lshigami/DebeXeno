package org.example.debexeno.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
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
