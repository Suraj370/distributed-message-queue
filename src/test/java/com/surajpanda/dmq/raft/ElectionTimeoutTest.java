package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import org.junit.jupiter.api.Test;

class ElectionTimeoutTest {

  @Test
  void shouldNotHaveElapsedBeforeDeadline() {
    ElectionTimeout timeout = new ElectionTimeout(100, 100, new Random(1));
    timeout.reset(0);

    assertFalse(timeout.hasElapsed(50));
  }

  @Test
  void shouldHaveElapsedAtOrAfterDeadline() {
    ElectionTimeout timeout = new ElectionTimeout(100, 100, new Random(1));
    timeout.reset(0);

    assertTrue(timeout.hasElapsed(100));
    assertTrue(timeout.hasElapsed(150));
  }

  @Test
  void shouldPickRandomizedDeadlineWithinConfiguredRange() {
    Random random = new Random(42);
    ElectionTimeout timeout = new ElectionTimeout(150, 300, random);

    for (int i = 0; i < 200; i++) {
      timeout.reset(0);
      long deadline = timeout.getDeadlineMillis();
      assertTrue(deadline >= 150 && deadline < 300, "deadline out of range: " + deadline);
    }
  }

  @Test
  void shouldRejectInvalidRange() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> new ElectionTimeout(200, 100, new Random(1)));
  }
}
