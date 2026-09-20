# Partitioning

## Topics and partitions

A `Topic` (`topic/Topic.java`) is just a name plus a fixed `List<Partition>` created once, at topic
creation time, and never resized - there is no repartitioning. Each `Partition` is a separate,
independent durable log (its own `Wal`, its own offsets starting at 0) - see
[../design/storage.md](../design/storage.md) for the on-disk layout.

`TopicManager` (`topic/TopicManager.java`) owns topic creation and lookup:

- `createTopic(name, partitionCount)` builds `partitionCount` partitions, each backed by its own
  `<dataDirectory>/<topic>/partition-<i>.log` WAL file, then durably appends a
  `<name>|<partitionCount>` record to `topics.meta` before making the topic visible in memory.
  Rejects a duplicate name or a non-positive partition count.
- On construction, `TopicManager` replays `topics.meta` and rebuilds every topic's partitions
  (which themselves replay their own WAL on construction - see `Partition.recover()`), so topic
  configuration and message data both survive a restart.

**Topic creation is not Raft-replicated.** It only happens on whichever broker's `TopicManager` you
call `createTopic` on. A real multi-broker deployment must create every topic identically on every
broker out of band (the README's Docker demo flow does this explicitly) - only `PublishCommand`s
travel through Raft. See [raft.md](raft.md) and the README's "Known limitations"
for the reasoning and the plan to eventually replicate topic creation the same way.

## Key-to-partition routing

`PartitionSelector.select(hash, partitionCount)` (`broker/PartitionSelector.java`) is the entire
routing algorithm:

```java
Math.floorMod(hash, partitionCount)
```

`Broker.publish(topic, key, payload)` calls this with `key.hashCode()`. `Math.floorMod` (not
`Math.abs(hash) % partitionCount`) is deliberate: `Math.abs(Integer.MIN_VALUE)` overflows back to
`Integer.MIN_VALUE` itself (there is no larger positive `int`), which would make the old
`Math.abs(hash) % partitionCount` formula return a negative index for that one specific hash value.
`Math.floorMod` returns a value in `[0, partitionCount)` for every `int` input, no exception case.

Routing is deterministic and stateless - the same key always maps to the same partition for a given
partition count, with no coordination or lookup required beyond the topic's partition count itself.

## Reading a partition

- `Partition.get(offset)` - non-destructive, offset-indexed read (backs at-least-once consumption).
- `Partition.readFrom(fromOffset)` - every message from an offset onward, in order.
- `Partition.poll()` - destructive dequeue (backs the separate at-most-once path).
- `Partition.nextOffset()` - the offset the next append will receive; also used to compute
  consumer-group lag (`nextOffset() - committedOffset`).

See [../design/delivery-semantics.md](../design/delivery-semantics.md) for how these compose into
the two consumption models, and [../design/concurrency.md](../design/concurrency.md) for how
`Partition` stays consistent under concurrent readers and a single writer.
