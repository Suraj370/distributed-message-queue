package com.surajpanda.dmq.broker;

/**
 * Deterministic key-to-partition selection. Takes a hash rather than a String directly so the
 * Integer.MIN_VALUE edge case can be exercised without hunting for a String whose hashCode()
 * happens to equal it: Math.abs(Integer.MIN_VALUE) overflows back to Integer.MIN_VALUE itself
 * (there is no positive int large enough to represent it), which used to make Math.abs(hash) %
 * partitionCount return a negative result - Math.floorMod never does, for any int hash and any
 * positive partitionCount.
 */
public final class PartitionSelector {

  private PartitionSelector() {}

  public static int select(int hash, int partitionCount) {
    return Math.floorMod(hash, partitionCount);
  }
}
