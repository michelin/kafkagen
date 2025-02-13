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
import com.michelin.kafkagen.services.FileService;
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
public class PlaySubcommandIntegrationTest extends AbstractIntegrationTest {

    @InjectMock
    MockedContext context;
    @InjectMock
    MockedContext.MockedKafkaContext kafkaContext;
    @Mock
    CommandLine.Model.CommandSpec commandSpec;
    @Mock
    public ConfigService configService;

    @Inject
    public FileService fileService;
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
    public void callWithDatasetFile() throws Exception {
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithoutKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithoutKey", 3, (short) 1)));

        var playSubcommand = new PlaySubcommand(configService, fileService, genericProducer, datasetService);
        initCommandLine(playSubcommand);
        playSubcommand.commandSpec = commandSpec;
        playSubcommand.file = Optional.of(new File(getClass().getClassLoader().getResource("scenarios/singleDatasetScenario.yaml").toURI()));

        assertEquals(CommandLine.ExitCode.OK, playSubcommand.call());
        assertEquals("Produced 8 records in avroTopicWithoutKey with StringSerializer (key) - KafkaAvroSerializer (value)", out.toString().trim());
    }

    @Test
    public void callWithMultipleDatasetFiles() throws Exception {
        String keyStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroKeySchema.avsc")));
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithoutKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithoutKey", 3, (short) 1)));

        createSubjects("avroTopicWithKey-key", new AvroSchema(new Schema.Parser().parse(keyStringSchema)));
        createSubjects("avroTopicWithKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithKey", 3, (short) 1)));

        var playSubcommand = new PlaySubcommand(configService, fileService, genericProducer, datasetService);
        initCommandLine(playSubcommand);
        playSubcommand.commandSpec = commandSpec;
        playSubcommand.file = Optional.of(new File(getClass().getClassLoader().getResource("scenarios/multipleDatasetsScenario.yaml").toURI()));

        assertEquals(CommandLine.ExitCode.OK, playSubcommand.call());
        assertTrue(out.toString().contains("Produced 8 records in avroTopicWithoutKey with StringSerializer (key) - KafkaAvroSerializer (value)"));
        assertTrue(out.toString().contains("Produced 2 records in avroTopicWithKey with KafkaAvroSerializer (key) - KafkaAvroSerializer (value)"));
    }

    @Test
    public void callWithTemplateFile() throws Exception {
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithoutKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithoutKey", 3, (short) 1)));

        var playSubcommand = new PlaySubcommand(configService, fileService, genericProducer, datasetService);
        initCommandLine(playSubcommand);
        playSubcommand.commandSpec = commandSpec;
        playSubcommand.file = Optional.of(new File(getClass().getClassLoader().getResource("scenarios/templatingScenario.yaml").toURI()));

        assertEquals(CommandLine.ExitCode.OK, playSubcommand.call());
        assertEquals("Produced 4 records in avroTopicWithoutKey with StringSerializer (key) - KafkaAvroSerializer (value)", out.toString().trim());
    }

    @Test
    public void callWithRandomRecordsFile() throws Exception {
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/random/options.avsc")));

        createSubjects("options-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("options", 3, (short) 1)));

        var playSubcommand = new PlaySubcommand(configService, fileService, genericProducer, datasetService);
        initCommandLine(playSubcommand);
        playSubcommand.commandSpec = commandSpec;
        playSubcommand.file = Optional.of(new File(getClass().getClassLoader().getResource("scenarios/randomScenario.yaml").toURI()));

        assertEquals(CommandLine.ExitCode.OK, playSubcommand.call());
        assertEquals("Produced 10 random records from avro schema (../avro/random/options.json) in options", out.toString().trim());
    }

    @Test
    public void callWithoutContext() {
        Mockito.when(configService.getCurrentContextName()).thenReturn(null);
        Mockito.when(configService.getContextByName(Mockito.anyString())).thenReturn(Optional.empty());

        var playSubcommand = new PlaySubcommand(configService, fileService, genericProducer, datasetService);
        initCommandLine(playSubcommand);
        playSubcommand.commandSpec = commandSpec;
        playSubcommand.file = Optional.empty();

        assertEquals(CommandLine.ExitCode.USAGE, playSubcommand.call());
        assertEquals("No context selected. Please list/set the context with the config command", out.toString().trim());
    }

    @Test
    public void callWithoutFile() {
        var playSubcommand = new PlaySubcommand(configService, fileService, genericProducer, datasetService);
        initCommandLine(playSubcommand);
        playSubcommand.commandSpec = commandSpec;
        playSubcommand.file = Optional.empty();

        assertThrows(CommandLine.ParameterException.class, playSubcommand::call);
    }

    @Test
    public void callWithWrongFile() {
        var playSubcommand = new PlaySubcommand(configService, fileService, genericProducer, datasetService);
        initCommandLine(playSubcommand);
        playSubcommand.commandSpec = commandSpec;
        playSubcommand.file = Optional.of(new File("wrongFile.yaml"));

        assertEquals(CommandLine.ExitCode.SOFTWARE, playSubcommand.call());
        assertEquals("Play failed due to the following error: Unable to read the provided file wrongFile.yaml. Please check the path and content (text only)", out.toString().trim());
    }

    private void initCommandLine(PlaySubcommand playSubcommand) {
        var commandLine = new CommandLine(playSubcommand);
        commandLine.setOut(new PrintWriter(out));
        commandLine.setErr(new PrintWriter(out));
        Mockito.when(commandSpec.commandLine()).thenReturn(commandLine);
    }
}
