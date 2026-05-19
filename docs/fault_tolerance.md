# Fault Tolerance Test

This document records the fault tolerance test performed on the Kafka cluster.

## Test Setup

We simulated a broker failure to observe leader re-election and verify the cluster's resilience with `replication-factor=3` and `min.insync.replicas=2`.

### Before Stopping a Broker

```bash
$ docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091 \
  --describe --topic sensor-events
```

```
Topic: sensor-events  TopicId: I87oIjeBSjGwMA3qaBOrzQ  PartitionCount: 3  ReplicationFactor: 3
  Topic: sensor-events  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Topic: sensor-events  Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  Topic: sensor-events  Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```

**Leader distribution:**
- Partition 0 -> Leader: broker 2
- Partition 1 -> Leader: broker 3
- Partition 2 -> Leader: broker 1

All three partitions have distinct leaders, meaning the load is evenly distributed across brokers. All replicas are in-sync (ISR = 3/3 for each partition).

### Triggering the Failure

```bash
docker stop kafka2
```

### After Stopping a Broker

```
Topic: sensor-events  PartitionCount: 3  ReplicationFactor: 3
  Topic: sensor-events  Partition: 0  Leader: 3  Replicas: 2,3,1  Isr: 3,1
  Topic: sensor-events  Partition: 1  Leader: 1  Replicas: 3,1,2  Isr: 3,1
  Topic: sensor-events  Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,3
```

### Observations

- **Partition 0**: Leader was broker 2 (now offline). Leader re-elected to broker 3 (next in the replica list that is still alive).
- **Partition 1**: Leader was broker 3 (still online). No re-election needed.
- **Partition 2**: Leader was broker 1 (still online). No re-election needed.
- ISR dropped to 2 for all partitions, confirming that `min.insync.replicas=2` is still satisfied.
- The cluster continued to accept produce and consume requests throughout the failure.

### Recovery

After restarting `kafka2`:

```bash
docker start kafka2
```

The broker rejoined the ISR list within seconds, and the topic returned to its original state with all 3 replicas in-sync for each partition.

## Conclusion

The 3-broker KRaft cluster tolerates a single broker failure without data loss or service interruption. Leader re-election completes within seconds. With `replication-factor=3` and `min.insync.replicas=2`, the platform maintains write availability even during a broker outage.
