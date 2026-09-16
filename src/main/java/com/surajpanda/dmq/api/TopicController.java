package com.surajpanda.dmq.api;

import com.surajpanda.dmq.broker.Broker;
import com.surajpanda.dmq.topic.Topic;
import java.io.IOException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/topics")
public class TopicController {

  private final Broker broker;

  public TopicController(Broker broker) {
    this.broker = broker;
  }

  @PostMapping
  public ResponseEntity<Topic> createTopic(@RequestParam String name, @RequestParam int partitions)
      throws IOException {
    return ResponseEntity.ok(broker.createTopic(name, partitions));
  }
}
