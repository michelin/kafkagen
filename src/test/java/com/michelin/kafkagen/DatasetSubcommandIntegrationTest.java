package com.michelin.kafkagen;

import com.michelin.kafkagen.kafka.GenericProducer;
import com.michelin.kafkagen.services.ConfigService;
import com.michelin.kafkagen.services.DatasetService;
import com.michelin.kafkagen.services.SchemaService;
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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@QuarkusTestResource(value = KafkaTestResource.class, restrictToAnnotatedClass = true)
public class DatasetSubcommandIntegrationTest extends AbstractIntegrationTest {

    @InjectMock
    MockedContext context;
    @InjectMock
    MockedContext.MockedKafkaContext kafkaContext;
    @Mock
    CommandLine.Model.CommandSpec commandSpec;
    @Mock
    public ConfigService configService;

    @Inject
    public SchemaService schemaService;
    @Inject
    public DatasetService datasetService;
    @Inject
    public GenericProducer genericProducer;

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
    public void callWithDifferentOffsetRequests() throws Exception {
        String valueStringSchema =
                new String(Files.readAllBytes(Paths.get("src/test/resources/avro/schemas/avroValueSchema.avsc")));

        createSubjects("avroTopicWithoutKey-value", new AvroSchema(new Schema.Parser().parse(valueStringSchema)));
        getAdminClient().createTopics(List.of(new NewTopic("avroTopicWithoutKey", 3, (short) 1)));
        var dataset = datasetService.getDataset(new File(getClass().getClassLoader().getResource("avro/datasets/datasetToProduce.json").toURI()), "avroTopicWithoutKey", context);

        Thread.sleep(1000);

        // Partition 0: key4, key6, key7
        // Partition 1: key3, key5
        // Partition 2: key1, key2
        genericProducer.produce("avroTopicWithoutKey", dataset, 1, context);

        Thread.sleep(1000);

        var datasetSubcommand = new DatasetSubcommand(schemaService, configService, datasetService);
        initCommandLine(datasetSubcommand);
        datasetSubcommand.commandSpec = commandSpec;
        datasetSubcommand.topic = "avroTopicWithoutKey";
        datasetSubcommand.offsets = Map.of("0", Optional.of("0-2"));
        datasetSubcommand.file = Optional.empty();
        datasetSubcommand.key = Optional.empty();

        assertEquals(CommandLine.ExitCode.OK, datasetSubcommand.call());
        assertTrue(Stream.of("key4", "key6", "key7").allMatch(out.toString()::contains));
        assertFalse(Stream.of("key1", "key2", "key3", "key5").anyMatch(out.toString()::contains));

        out.getBuffer().setLength(0);
        datasetSubcommand.offsets = Map.of("1", Optional.of("1"));
        assertEquals(CommandLine.ExitCode.OK, datasetSubcommand.call());
        assertTrue(Stream.of("key5").allMatch(out.toString()::contains));
        assertFalse(Stream.of("key1", "key2", "key3", "key4", "key6", "key7").anyMatch(out.toString()::contains));

        out.getBuffer().setLength(0);
        datasetSubcommand.offsets = Map.of("2", Optional.empty());
        assertEquals(CommandLine.ExitCode.OK, datasetSubcommand.call());
        assertTrue(Stream.of("key1", "key2").allMatch(out.toString()::contains));
        assertFalse(Stream.of("key3", "key5", "key6", "key7").anyMatch(out.toString()::contains));
    }

    @Test
    public void callWithoutContext() throws Exception {
        Mockito.when(configService.getCurrentContextName()).thenReturn(null);
        Mockito.when(configService.getContextByName(Mockito.anyString())).thenReturn(Optional.empty());

        var datasetSubcommand = new DatasetSubcommand(schemaService, configService, datasetService);
        initCommandLine(datasetSubcommand);
        datasetSubcommand.commandSpec = commandSpec;

        assertEquals(CommandLine.ExitCode.USAGE, datasetSubcommand.call());
        assertEquals("No context selected. Please list/set the context with the config command", out.toString().trim());
    }

    private void initCommandLine(DatasetSubcommand datasetSubcommand) {
        var commandLine = new CommandLine(datasetSubcommand);
        commandLine.setOut(new PrintWriter(out));
        commandLine.setErr(new PrintWriter(out));
        Mockito.when(commandSpec.commandLine()).thenReturn(commandLine);
    }
}
