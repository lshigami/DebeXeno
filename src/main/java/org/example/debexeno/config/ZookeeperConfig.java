package org.example.debexeno.config;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZookeeperConfig {

  @Value("${zookeeper.connection-string:localhost:2181}")
  private String connectionString;
  @Value("${zookeeper.session-timeout-ms:60000}")
  private int sessionTimeoutMs;

  @Value("${zookeeper.connection-timeout-ms:15000}")
  private int connectionTimeoutMs;

  @Value("${zookeeper.base-path:/debexeno}")
  private String basePath;


  @Bean(initMethod = "start", destroyMethod = "close")
  public CuratorFramework curatorClient() {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    return CuratorFrameworkFactory.builder().connectString(connectionString)
        .connectionTimeoutMs(connectionTimeoutMs).sessionTimeoutMs(sessionTimeoutMs)
        .namespace(basePath.startsWith("/") ? basePath.substring(1) : basePath)
        .retryPolicy(retryPolicy).build();
  }

}
