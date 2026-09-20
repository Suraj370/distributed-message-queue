package com.surajpanda.dmq;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

/**
 * DirtiesContext so this context's RaftNodeScheduler (a real background thread) is closed right
 * after this test, instead of staying alive for the rest of the suite via Spring's normal test
 * context caching - otherwise its "raft-node-scheduler" thread can still be alive when
 * RaftNodeSchedulerTest later checks for that thread name being gone after its own close().
 */
@SpringBootTest
@DirtiesContext
class DistributedMessageQueueApplicationTests {

  @Test
  void contextLoads() {}
}
