package com.michelin.kafkagen.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * This is a custom Kafka partitioner that uses the CRC32 algorithm to determine the partition for a given key.
 * If the key is null, it uses a round-robin strategy to assign the partition.
 */
public class Crc32Partitioner implements Partitioner {
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);

        if (key == null) {
            return counter.getAndIncrement() % numPartitions;
        }

        String keyString = key.toString();
        CRC32 crc32 = new CRC32();
        crc32.update(keyString.getBytes(StandardCharsets.UTF_8));

        return (int) (crc32.getValue() % numPartitions);
    }

    @Override
    public void close() {
    }
}
