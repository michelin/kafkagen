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

package com.michelin.kafkagen;

import com.michelin.kafkagen.kafka.GenericProducer;
import com.michelin.kafkagen.services.ConfigService;
import com.michelin.kafkagen.services.DatasetService;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import picocli.CommandLine;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@QuarkusTestResource(value = KafkaTestResource.class, restrictToAnnotatedClass = true)
public class ProduceSubcommandIntegrationTest extends AbstractIntegrationTest {

    @InjectMock
    MockedContext context;
    @InjectMock
    MockedContext.MockedKafkaContext kafkaContext;
    @Mock
    public ConfigService configService;
    @Mock
    CommandLine.Model.CommandSpec commandSpec;

    @Inject
    public GenericProducer genericProducer;
    @Inject
    public DatasetService datasetService;

    private final StringWriter out = new StringWriter();

    @BeforeEach
    public void init() {
        Mockito.when(kafkaContext.registryUrl()).thenReturn(Optional.of(String.format("http://%s:%d", registryHost, Integer.parseInt(registryPort))));
        Mockito.when(kafkaContext.bootstrapServers()).thenReturn(host + ":" + Integer.parseInt(port));
        Mockito.when(context.definition()).thenReturn(kafkaContext);

        MockitoAnnotations.openMocks(this);
        Mockito.when(configService.getCurrentContextName()).thenReturn("dev");
        Mockito.when(configService.getContextByName(Mockito.anyString())).thenReturn(Optional.of(context));
    }

    @Test
    public void callWithTopic() throws Exception {
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithoutKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithoutKey", 3, (short) 1)));

        var produceSubcommand = new ProduceSubcommand(configService, genericProducer, datasetService);
        initCommandLine(produceSubcommand);
        produceSubcommand.commandSpec = commandSpec;
        produceSubcommand.topic = Optional.of("avroTopicWithoutKey");
        produceSubcommand.file = Optional.of(new File(getClass().getClassLoader().getResource("avro/datasets/datasetToProduce.json").toURI()));
        produceSubcommand.keySubjectVersion = Optional.empty();
        produceSubcommand.valueSubjectVersion = Optional.empty();

        assertEquals(CommandLine.ExitCode.OK, produceSubcommand.call());
        assertEquals("Produced 8 records in avroTopicWithoutKey with StringSerializer (key) - KafkaAvroSerializer (value)", out.toString().trim());
    }

    @Test
    public void callWithoutTopic() throws Exception {
        String keyStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroKeySchema.avsc")));
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));
        String valueStringSchemaSelfReference =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroWithSelfReferenceSchema.avsc")));

        createSubjects("avroTopicWithKey-key", new AvroSchema(new Schema.Parser().parse(keyStringSchema)));
        createSubjects("avroTopicWithKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        createSubjects("selfReferencedTopic-value", new AvroSchema(new Schema.Parser().parse(valueStringSchemaSelfReference)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithKey", 3, (short) 1)));
        getAdminClient().createTopics(List.of(new NewTopic("selfReferencedTopic", 3, (short) 1)));

        var produceSubcommand = new ProduceSubcommand(configService, genericProducer, datasetService);
        initCommandLine(produceSubcommand);
        produceSubcommand.commandSpec = commandSpec;
        produceSubcommand.file = Optional.of(new File(getClass().getClassLoader().getResource("avro/datasets/datasetMultiTopic.json").toURI()));
        produceSubcommand.topic = Optional.empty();

        assertEquals(CommandLine.ExitCode.OK, produceSubcommand.call());
        assertTrue(out.toString().contains("Produced 1 records in avroTopicWithKey with KafkaAvroSerializer (key) - KafkaAvroSerializer (value)"));
        assertTrue(out.toString().contains("Produced 1 records in selfReferencedTopic with StringSerializer (key) - KafkaAvroSerializer (value)"));
    }

    @Test
    public void callWithoutContext() {
        Mockito.when(configService.getCurrentContextName()).thenReturn(null);
        Mockito.when(configService.getContextByName(Mockito.anyString())).thenReturn(Optional.empty());

        var produceSubcommand = new ProduceSubcommand(configService, genericProducer, datasetService);
        initCommandLine(produceSubcommand);
        produceSubcommand.commandSpec = commandSpec;
        produceSubcommand.file = Optional.empty();
        produceSubcommand.topic = Optional.empty();

        assertEquals(CommandLine.ExitCode.USAGE, produceSubcommand.call());
        assertEquals("No context selected. Please list/set the context with the config command", out.toString().trim());
    }

    @Test
    public void callWithoutFile() {
        var produceSubcommand = new ProduceSubcommand(configService, genericProducer, datasetService);
        initCommandLine(produceSubcommand);
        produceSubcommand.commandSpec = commandSpec;
        produceSubcommand.file = Optional.empty();
        produceSubcommand.topic = Optional.empty();

        assertThrows(CommandLine.ParameterException.class, produceSubcommand::call);
    }

    @Test
    public void callWithWrongFile() {
        var produceSubcommand = new ProduceSubcommand(configService, genericProducer, datasetService);
        initCommandLine(produceSubcommand);
        produceSubcommand.commandSpec = commandSpec;
        produceSubcommand.file = Optional.of(new File("wrongFile.json"));
        produceSubcommand.topic = Optional.empty();

        assertEquals(CommandLine.ExitCode.SOFTWARE, produceSubcommand.call());
        assertEquals("Produce failed due to the following error: Unable to read the provided file wrongFile.json. Please check the path and content (text only)", out.toString().trim());
    }

    private void initCommandLine(ProduceSubcommand produceSubcommand) {
        var commandLine = new CommandLine(produceSubcommand);
        commandLine.setOut(new PrintWriter(out));
        commandLine.setErr(new PrintWriter(out));
        Mockito.when(commandSpec.commandLine()).thenReturn(commandLine);
    }
}
