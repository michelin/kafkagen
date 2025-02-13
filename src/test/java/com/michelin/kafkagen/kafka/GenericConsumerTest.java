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

import com.michelin.kafkagen.AbstractIntegrationTest;
import com.michelin.kafkagen.KafkaTestResource;
import com.michelin.kafkagen.services.DatasetService;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@QuarkusTestResource(value = KafkaTestResource.class, restrictToAnnotatedClass = true)
public class GenericConsumerTest extends AbstractIntegrationTest {
    @Inject
    GenericConsumer genericConsumer;
    @Inject
    GenericProducer genericProducer;
    @Inject
    DatasetService datasetService;

    @InjectMock
    MockedContext context;

    @InjectMock
    MockedContext.MockedKafkaContext kafkaContext;

    @BeforeEach
    public void init() {
        Mockito.when(kafkaContext.bootstrapServers()).thenReturn(host + ":" + Integer.parseInt(port));
        Mockito.when(context.definition()).thenReturn(kafkaContext);
    }

    @Test
    public void consumeWithPoisonPill() throws Exception {
        String keyStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroKeySchema.avsc")));
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithKey-key", new AvroSchema(new Schema.Parser().parse(keyStringSchema)));
        createSubjects("avroTopicWithKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithKey", 3, (short) 1)));

        // No registry in the context to produce a poison pill (StringSerializer/StringSerializer)
        Mockito.when(kafkaContext.registryUrl()).thenReturn(Optional.empty());
        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/dataset.json").toURI()), "avroTopicWithKey", context);
        genericProducer.produce("avroTopicWithKey", dataset, 1, context);

        // Add the registry to the context
        Mockito.when(kafkaContext.registryUrl()).thenReturn(Optional.of(String.format("http://%s:%d", registryHost, Integer.parseInt(registryPort))));

        // Produce 2 records with KafkaAvroSerializer/KafkaAvroSerializer
        dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/dataset.json").toURI()), "avroTopicWithKey", context);
        genericProducer.produce("avroTopicWithKey", dataset, 1, context);

        // Init a consumer with KafkaAvroSerializer/KafkaAvroSerializer
        KafkaConsumer<?, ?> kafkaConsumer = genericConsumer.init("avroTopicWithKey", context);
        kafkaConsumer.assign(kafkaConsumer.partitionsFor("avroTopicWithKey")
                .stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .toList());

        var records = genericConsumer.pollRecords(kafkaConsumer.endOffsets(kafkaConsumer.assignment()));

        assertEquals(2, records.size());
    }
}
