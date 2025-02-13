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

package com.michelin.kafkagen.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.kafkagen.config.KafkagenConfig;
import com.michelin.kafkagen.kafka.GenericConsumer;
import com.michelin.kafkagen.models.CompactedAssertState;
import com.michelin.kafkagen.models.Record;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

@Slf4j
@Singleton
public class AssertService {

    SchemaService schemaService;

    GenericConsumer genericConsumer;

    @Inject
    public AssertService(SchemaService schemaService,
                         GenericConsumer genericConsumer) {
        this.schemaService = schemaService;
        this.genericConsumer = genericConsumer;
    }

    /**
     * Consume a topic and check if record given as parameter are contained in this topic.
     *
     * @param topic                  - the topic to consumer
     * @param expectedRecords        - records that should be found
     * @param mostRecentAssertResult - assertion result for messages with MostRecent tag
     * @param context                - context configuration
     * @param strict                 - true if we want to have exactly the same information in the record
     * @param optionalStartTimestamp - an optional start timestamp for consumption
     * @return true if all the expected records have been found, false otherwise
     */
    public boolean assertThatTopicContains(final String topic,
                                           List<Record> expectedRecords,
                                           Map<Object, CompactedAssertState> mostRecentAssertResult,
                                           KafkagenConfig.Context context,
                                           boolean strict,
                                           Optional<Long> optionalStartTimestamp) {
        if (expectedRecords.isEmpty()) {
            return true;
        }

        var expectedRecordsToFound = new ArrayList<>(
            expectedRecords.stream().filter(e -> !e.getMostRecent() || e.getKey() == null).toList());
        var expectedRecordsLastVersionToFound =
            expectedRecords.stream().filter(e -> e.getMostRecent() && e.getKey() != null).toList();

        // Misconfiguration check
        mostRecentAssertResult.remove(null);

        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = genericConsumer.init(
            topic, context)) {
            assignPartitions(kafkaConsumer, topic, optionalStartTimestamp);

            Map<TopicPartition, Long> endOffsets =
                kafkaConsumer.endOffsets(kafkaConsumer.assignment());

            var mapper = new ObjectMapper();

            // Stop if we reached the end offset of all the partitions
            while (endOffsets.entrySet().stream()
                .anyMatch(e -> kafkaConsumer.position(e.getKey()) < e.getValue())) {

                // Or we found all the expected records
                if (expectedRecords.isEmpty()) {
                    break;
                }

                var polledRecords = genericConsumer.pollRecords(endOffsets);

                polledRecords.forEach(record -> {
                    // Convert the key and value to a map in case of Avro messages to be compared
                    // with the dataset file
                    Object key;
                    Object value;

                    try {
                        if (record.getKey() != null) {
                            key = record.getKey().getClass().isAssignableFrom(String.class)
                                ? record.getKey()
                                : mapper.readValue(record.getKey().toString(), Object.class);
                        } else {
                            key = null;
                        }

                        if (record.getValue() != null) {
                            value =
                                record.getValue().getClass().isAssignableFrom(String.class)
                                    ? record.getValue()
                                    : mapper.readValue(record.getValue().toString(), Object.class);
                        } else {
                            value = null;
                        }

                        // Remove the expected record from the list when we found a match in the polled records
                        expectedRecordsToFound.removeIf(expectedRecord ->
                            (expectedRecord.getHeaders() == null
                                || match(expectedRecord.getHeaders(), record.getHeaders(),
                                    strict))
                                && (expectedRecord.getKey() == null
                                || match(expectedRecord.getKey(), key, strict))
                                && ((expectedRecord.getValue() == null && value == null)
                                || ((expectedRecord.getValue() != null
                                && match(expectedRecord.getValue(), value, strict))))
                        );

                        // For records on which we want to check the last version for a given key, update message
                        // key / status map to compute the last status
                        expectedRecordsLastVersionToFound.forEach(expectedRecord -> {
                            if ((expectedRecord.getKey() == null
                                || match(expectedRecord.getKey(), key, strict))) {
                                if ((expectedRecord.getHeaders() == null
                                    || match(expectedRecord.getHeaders(), record.getHeaders(),
                                        strict))
                                    && ((expectedRecord.getValue() == null && value == null)
                                    || ((expectedRecord.getValue() != null
                                    && match(expectedRecord.getValue(), value, strict))))) {
                                    mostRecentAssertResult.put(expectedRecord.getKey(),
                                        CompactedAssertState.FOUND);
                                } else if (mostRecentAssertResult.get(expectedRecord.getKey())
                                    .equals(CompactedAssertState.FOUND)) {
                                    mostRecentAssertResult.put(expectedRecord.getKey(),
                                        CompactedAssertState.NEWER_VERSION_EXISTS);
                                }
                            }
                        });
                    } catch (Exception e) {
                        log.debug("", e);
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        return expectedRecordsToFound.isEmpty()
            && !mostRecentAssertResult.containsValue(CompactedAssertState.NOT_FOUND)
            && !mostRecentAssertResult.containsValue(
            CompactedAssertState.NEWER_VERSION_EXISTS);
    }

    /**
     * Check that the expected and actual objects are equals.
     * Input can be a String or a Map (representation of an Avro key/value for the comparison)
     *
     * @param expected - the expected value
     * @param actual   - the actual value
     * @param strict   - true to check that fields are identical, false to ignore fields not give in the expected object
     * @return true if the 2 objects are the same, false otherwise
     */
    @SuppressWarnings("unchecked")
    public boolean match(@NonNull Object expected, Object actual,
                         boolean strict) {
        boolean match;

        // If the expected object is a String (e.g. string key or plain text value), perform a simple equals
        if (expected.getClass().equals(String.class)) {
            match = expected.equals(actual);
        } else {
            try {
                // First, for a strict comparison, check that the number of fields is equal
                if (strict
                    && ((Map<String, Object>) expected).keySet().size()
                        != ((Map<String, Object>) actual).keySet().size()) {
                    return false;
                }
                // If it's an avro message, call the compare function
                match = compare((Map<String, Object>) expected,
                    (Map<String, Object>) actual, strict);
            } catch (Exception e) {
                // Return false for every error that could occur
                match = false;
            }
        }

        return match;
    }

    /**
     * Compare 2 avro records converted to map for an easier comparison.
     * This function is recursive as we can have inner records
     *
     * @param expected - the expected record
     * @param actual   - the actual record
     * @param strict   - true to check that fields are identical, false to ignore fields not give in the expected object
     * @return true if there are equals, false otherwise
     */
    @SuppressWarnings("unchecked")
    private boolean compare(Map<String, Object> expected,
                            Map<String, Object> actual, boolean strict) {
        // If strict assertion is request and the number of fields is different, return false
        if (strict && expected.keySet().size() != actual.keySet().size()) {
            return false;
        }

        var result = true;

        for (var key : expected.keySet()) {
            // If the value for the key is a map, we need to go deeper and start recursion
            if (Map.class.isAssignableFrom(expected.get(key).getClass())) {
                result = result && compare((Map<String, Object>) expected.get(key),
                    (Map<String, Object>) actual.get(key), strict);
            } else if (List.class.isAssignableFrom(expected.get(key).getClass())) {
                // If the value is an array
                var expectedCollection = (List<Object>) expected.get(key);
                var actualCollection = (List<Object>) actual.get(key);

                // Check the size
                if (expectedCollection.size() != actualCollection.size()) {
                    result = false;
                    break;
                }

                result = result && IntStream.range(0, expectedCollection.size())
                    .mapToObj(i -> new Object[] {expectedCollection.get(i),
                        actualCollection.get(i)})
                    .allMatch(pair -> {
                        // Recursion if array items are object
                        if (Map.class.isAssignableFrom(pair[0].getClass())) {
                            return compare((Map<String, Object>) pair[0],
                                (Map<String, Object>) pair[1], strict);
                        } else {
                            return pair[0].equals(pair[1]);
                        }
                    });
            } else {
                // Otherwise we do a simple equals and return false if it differs
                if (!expected.get(key).equals(actual.get(key))) {
                    result = false;
                    break;
                }
            }
        }

        return result;
    }

    /**
     * Init the consumer by assigning topic partitions and offset according to the optional start timestamp.
     *
     * @param consumer               - the consumer to init
     * @param topic                  - the topic
     * @param optionalStartTimestamp - an optional start timestamp for consumption
     */
    public static void assignPartitions(KafkaConsumer<byte[], byte[]> consumer,
                                        String topic,
                                        Optional<Long> optionalStartTimestamp) {

        // Assign all the partitions to the consumer
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);

        if (partitions.isEmpty()) {
            throw new RuntimeException("Topic doesn't exist. Please check the topic name and/or "
                + "the bootstrap-servers property in the configuration.");
        }

        var topicPartitions = partitions.stream()
            .map(p -> new TopicPartition(topic, p.partition()))
            .toList();

        optionalStartTimestamp.ifPresentOrElse(startTimestamp -> {
                // Get offset for timestamp for each partition
                Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                    consumer.offsetsForTimes(
                            topicPartitions.stream()
                                .collect(Collectors.toMap(tp -> tp, tp -> startTimestamp)))
                        .entrySet()
                        .stream()
                        // Remove partition with no offset the start timestamp
                        .filter(entry -> entry.getValue() != null)
                        .collect(
                            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                // Assign all the partition with records after the start timestamp
                consumer.assign(offsetsForTimes.keySet());

                // Seek from the offset corresponding to the start timestamp
                offsetsForTimes.forEach((topicPartition, offsetAndTimestamp) ->
                    consumer.seek(topicPartition, offsetAndTimestamp.offset()));
            },
            // Assign all the partition otherwise
            () -> {
                consumer.assign(topicPartitions);
                consumer.seekToBeginning(topicPartitions);
            }
        );
    }
}
