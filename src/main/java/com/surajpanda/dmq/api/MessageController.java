package com.surajpanda.dmq.api;

import com.surajpanda.dmq.broker.Broker;
import com.surajpanda.dmq.message.Message;
import java.io.IOException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/messages")
public class MessageController {

  private final Broker broker;

  public MessageController(Broker broker) {
    this.broker = broker;
  }

  @PostMapping
  public ResponseEntity<Message> publish(
      @RequestParam String topic, @RequestParam String key, @RequestParam String payload)
      throws IOException {
    Message message = broker.publish(topic, key, payload);

    return ResponseEntity.ok(message);
  }
}
