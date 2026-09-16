package com.surajpanda.dmq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DistributedMessageQueueApplication {

  public static void main(String[] args) {
    SpringApplication.run(DistributedMessageQueueApplication.class, args);
  }
}
