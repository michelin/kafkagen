package com.michelin.kafkagen.kafka;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.List;
import java.util.Set;

public class Crc32PartitionerTest {

    @Test
    public void testPartition() {
        String topic = "test-topic";
        String key = "119de63d-8c77-497c-9d12-9246ea5ad361";

        List<PartitionInfo> partitions = List.of(
            new PartitionInfo("test-topic", 1, null, null, null),
            new PartitionInfo("test-topic", 2, null, null, null),
            new PartitionInfo("test-topic", 3, null, null, null));

        Cluster cluster = new Cluster("cluster", List.of(), partitions, Set.of(), Set.of());

        try (Crc32Partitioner partitioner = new Crc32Partitioner()) {
			int partition = partitioner.partition(topic, key, null, null, null, cluster);
			assertEquals(1, partition);
		}
    }
}
