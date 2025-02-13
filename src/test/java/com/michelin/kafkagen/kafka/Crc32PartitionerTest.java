/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
