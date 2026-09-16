package com.surajpanda.dmq.broker;

import com.surajpanda.dmq.topic.TopicManager;
import java.io.IOException;
import java.nio.file.Path;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BrokerConfiguration {

  @Bean
  public TopicManager topicManager(BrokerProperties brokerProperties) throws IOException {
    return new TopicManager(Path.of(brokerProperties.dataDirectory()));
  }
}
