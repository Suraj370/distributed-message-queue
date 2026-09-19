package com.surajpanda.dmq.raft;

import java.util.Random;

/**
 * Randomized, injectable-time election timeout. Each reset() picks a new deadline uniformly in
 * [minMillis, maxMillis) from the supplied "now", so peers in a cluster don't all time out at the
 * same instant and repeatedly split votes. Owns no thread or clock - callers decide what "now"
 * means, which keeps this fully deterministic to unit test.
 */
public class ElectionTimeout {

  private final long minMillis;
  private final long maxMillis;
  private final Random random;
  private long deadlineMillis;

  public ElectionTimeout(long minMillis, long maxMillis, Random random) {

    if (minMillis <= 0 || maxMillis < minMillis) {
      throw new IllegalArgumentException(
          "Invalid election timeout range: min=" + minMillis + ", max=" + maxMillis);
    }

    this.minMillis = minMillis;
    this.maxMillis = maxMillis;
    this.random = random;
  }

  public void reset(long nowMillis) {
    long span = maxMillis - minMillis;
    long jitter = span == 0 ? 0 : (long) (random.nextDouble() * span);
    deadlineMillis = nowMillis + minMillis + jitter;
  }

  public boolean hasElapsed(long nowMillis) {
    return nowMillis >= deadlineMillis;
  }

  public long getDeadlineMillis() {
    return deadlineMillis;
  }

  public long getMinMillis() {
    return minMillis;
  }

  public long getMaxMillis() {
    return maxMillis;
  }
}
