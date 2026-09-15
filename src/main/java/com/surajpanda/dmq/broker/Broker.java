package com.surajpanda.dmq.broker;

import com.surajpanda.dmq.message.Message;
import org.springframework.stereotype.Service;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Service
public class Broker {

    private final Queue<Message> messages = new ConcurrentLinkedQueue<>();

    public Message publish(String key, String payload) {
        Message message = Message.create(key, payload);
        messages.offer(message);
        return message;
    }

    public Message consume() {
        return messages.poll();
    }
}