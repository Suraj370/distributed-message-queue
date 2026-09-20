package com.surajpanda.dmq.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PartitionSelectorTest {

  @Test
  void integerMinValueHashStaysInBoundsForEveryPartitionCount() {
    // Math.abs(Integer.MIN_VALUE) overflows back to Integer.MIN_VALUE (no positive int can
    // represent it), which used to make the old Math.abs(hash) % partitionCount formula return a
    // negative partition id for this exact hash. Math.floorMod must not have that problem.
    for (int partitionCount = 1; partitionCount <= 16; partitionCount++) {
      int partition = PartitionSelector.select(Integer.MIN_VALUE, partitionCount);
      assertTrue(
          partition >= 0 && partition < partitionCount,
          "partition " + partition + " out of bounds for partitionCount=" + partitionCount);
    }
  }

  @Test
  void selectionStaysInBoundsForAWideRangeOfHashes() {
    int[] hashes = {
      Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -1, 0, 1, Integer.MAX_VALUE, "some-key".hashCode()
    };
    for (int hash : hashes) {
      for (int partitionCount = 1; partitionCount <= 8; partitionCount++) {
        int partition = PartitionSelector.select(hash, partitionCount);
        assertTrue(partition >= 0 && partition < partitionCount);
      }
    }
  }

  @Test
  void selectionIsDeterministicForTheSameHashAndPartitionCount() {
    assertEquals(PartitionSelector.select(12345, 4), PartitionSelector.select(12345, 4));
  }
}
